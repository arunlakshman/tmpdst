package sim

import (
	"bytes"
	"slices"
	"strings"

	"manifester/toys3/control"
	"manifester/toys3/format"
)

// Disturbances configures the storage model: its dynamics (dead time and
// feedback delay) and the disturbances it injects, the exogenous inputs the
// controller cannot command (EIO, torn writes, bit flips, background
// writeback, fsyncgate). Zero value: an honest, instant filesystem that still
// loses un-synced changes on crash.
type Disturbances struct {
	// EIOProb is the per-operation probability that an operation fails with
	// ErrIO. What a failed operation did is itself chosen adversarially (see
	// StorageModel.Land).
	EIOProb float64
	// TornWriteProb is the probability, per file with un-fsynced content at a
	// crash, that the file survives holding a random prefix of its volatile
	// content (instead of all of it or none of it).
	TornWriteProb float64
	// DeadTime is the [min, max] delay from issuing an actuation to its effect
	// landing in storage; FeedbackDelay is the further [min, max] delay until
	// the completion (feedback) is delivered. A crash in between leaves the
	// effect applied but unacked.
	DeadTime      [2]control.Duration
	FeedbackDelay [2]control.Duration
	// ReadCorruptProb is the per-FSRead probability that the returned bytes
	// have one bit flipped (a transient media or bus error: the stored data
	// is unchanged). This is what makes the object CRC reachable for a
	// correct server; without it the CRC check is dead code (see DESIGN.md).
	ReadCorruptProb float64
	// WritebackProb is the probability, after each landed action, that the
	// kernel's background writeback makes one random un-synced change durable
	// on its own (a directory entry, or a dirty file's content), as a journal
	// commit or the flusher thread would. Without it, un-synced changes become
	// durable only by luck at a crash; with it, e.g. a temp file's name can be
	// durable by the time it is renamed away, so a crash resurrects it as a
	// second name for the committed object's inode.
	WritebackProb float64
	// FsyncGate models Linux after a failed fsync: the dirty data is marked
	// clean, later fsyncs of that file return OK, and the data still never
	// reaches disk. Only a fresh FSWrite clears it.
	FsyncGate bool
}

// ActuationPlan is decided when an actuation is issued, so the decision
// consumes RNG draws in issue order (deterministic) regardless of when it
// lands.
type ActuationPlan struct {
	Fail          bool             // complete with ErrIO
	DeadTime      control.Duration // issue → effect (the actuation lands)
	FeedbackDelay control.Duration // effect → completion delivered
}

// InodeID names a file's content independently of its names, so a rename can
// leave two names for one inode after a crash (both the old and the new
// directory entry survived) without duplicating data.
type InodeID uint64

// inode is a file's content in two versions.
type inode struct {
	durable  []byte // what survives a crash (nil for never-synced)
	volatile []byte // what reads see now
	dirty    bool   // volatile differs from durable (un-fsynced writes)
	poisoned bool   // FsyncGate: a past fsync failed; fsyncs now lie
}

// dirent is one name in a directory, in two versions. 0 means absent.
type dirent struct {
	cur     InodeID // visible now (after creates/renames/unlinks)
	durable InodeID // what survives a crash (after FSFsyncDir)
}

// fdir is a directory. Iterate names in sorted order only (detcheck bans
// ranging over maps; determinism requires it anyway).
type fdir struct {
	entries map[string]dirent
	sorted  []string // cached sorted names; nil when stale
}

// StorageModel is the model of storage (the disk and its page cache) that
// stands in for the real filesystem in simulation: an in-memory filesystem
// with crash semantics. Every file and every directory entry has a durable
// and a volatile (current) version:
//
//   - FSWrite updates an inode's volatile content; FSFsync copies volatile to
//     durable.
//   - Creating, renaming and unlinking update directory entries' current
//     version; FSFsyncDir copies current to durable for every entry in that
//     directory.
//   - Reads and listings see the current version.
//
// On Crash, each un-synced change is resolved independently by the RNG, as an
// adversary would: every dirent with cur != durable either becomes durable or
// reverts; every dirty inode keeps its volatile content, reverts to durable,
// or (TornWriteProb) keeps a random prefix of volatile. Afterwards current ==
// durable everywhere. A correct controller survives every such outcome.
// Durable() is the true state of storage, against which the monitors check the
// controller's claims (Committed telemetry).
//
// Directories are fixed at construction (format.ObjDir and format.TmpDir of the
// root); there is no mkdir.
type StorageModel struct {
	rng     *Rand
	faults  Disturbances
	quiet   bool // no new disturbances (Settling); crash resolution still applies
	dirs    map[string]*fdir
	inodes  map[InodeID]*inode
	nextIno InodeID
	count   FSCounters
	dirList []string // sorted directory paths (fixed at construction)
	durGen  uint64   // bumped whenever anything durable may have changed
}

// FSCounters counts injected faults, for coverage.
type FSCounters struct {
	Landed          int // actions applied
	EIOs            int // actions completed with an injected ErrIO
	ReadCorruptions int // FSReads returned with a flipped bit
	Writebacks      int // background writebacks that made a change durable
}

// NewStorageModel returns an empty filesystem with the given (durable) directories.
func NewStorageModel(r *Rand, faults Disturbances, dirs ...string) *StorageModel {
	fs := &StorageModel{
		rng:    r,
		faults: faults,
		dirs:   map[string]*fdir{},
		inodes: map[InodeID]*inode{},
	}
	for _, d := range dirs {
		fs.dirs[d] = &fdir{entries: map[string]dirent{}}
		fs.dirList = append(fs.dirList, d)
	}
	slices.Sort(fs.dirList)
	fs.dirList = slices.Compact(fs.dirList)
	return fs
}

// Plan draws the fault decision, dead time and feedback delay for a
// filesystem action.
func (fs *StorageModel) Plan(a control.Action) ActuationPlan {
	var p ActuationPlan
	p.Fail = !fs.quiet && fs.rng.Chance(fs.faults.EIOProb)
	p.DeadTime = control.Duration(fs.rng.Between(int64(fs.faults.DeadTime[0]), int64(fs.faults.DeadTime[1])))
	p.FeedbackDelay = control.Duration(fs.rng.Between(int64(fs.faults.FeedbackDelay[0]), int64(fs.faults.FeedbackDelay[1])))
	return p
}

// Land applies a filesystem action and returns its completion event. With
// p.Fail the completion carries ErrIO and the effect is adversarial:
//
//	FSWrite     file may be absent, empty, or hold a prefix of Data
//	FSFsync     nothing becomes durable (and the inode is poisoned if FsyncGate)
//	FSRename    applied or not
//	FSFsyncDir  nothing becomes durable
//	FSUnlink    applied or not
//	FSRead, FSList  no effect
//
// Missing paths complete with ErrNotFound (never by chance).
func (fs *StorageModel) Land(a control.Action, p ActuationPlan) control.EventBody {
	fs.count.Landed++
	var body control.EventBody
	var err control.ErrCode
	switch a := a.(type) {
	case control.FSWrite:
		err = fs.write(a.Path, a.Data, p.Fail)
		body = control.FSWriteDone{IO: a.IO, Err: err}
	case control.FSFsync:
		err = fs.fsync(a.Path, p.Fail)
		body = control.FSFsyncDone{IO: a.IO, Err: err}
	case control.FSRename:
		err = fs.rename(a.From, a.To, p.Fail)
		body = control.FSRenameDone{IO: a.IO, Err: err}
	case control.FSFsyncDir:
		err = fs.fsyncDir(a.Dir, p.Fail)
		body = control.FSFsyncDirDone{IO: a.IO, Err: err}
	case control.FSRead:
		var data []byte
		data, err = fs.read(a.Path, p.Fail)
		body = control.FSReadDone{IO: a.IO, Data: data, Err: err}
	case control.FSUnlink:
		err = fs.unlink(a.Path, p.Fail)
		body = control.FSUnlinkDone{IO: a.IO, Err: err}
	case control.FSList:
		var names []string
		names, err = fs.list(a.Dir, p.Fail)
		body = control.FSListDone{IO: a.IO, Names: names, Err: err}
	default:
		panic("toys3/sim: StorageModel.Land: not a filesystem action")
	}
	if err == control.ErrIO {
		fs.count.EIOs++
	}
	if fs.rng.Chance(fs.faults.WritebackProb) {
		fs.writeback()
	}
	return body
}

// writeback makes one random un-synced change durable.
func (fs *StorageModel) writeback() {
	type change struct {
		d    *fdir
		name string
		ino  InodeID
	}
	var cands []change
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		for _, name := range d.names() {
			e := d.entries[name]
			if e.cur != e.durable {
				cands = append(cands, change{d: d, name: name})
			}
			if e.cur != 0 {
				if in := fs.inodes[e.cur]; in.dirty && !in.poisoned {
					cands = append(cands, change{ino: e.cur})
				}
			}
		}
	}
	if len(cands) == 0 {
		return
	}
	fs.count.Writebacks++
	fs.durGen++
	c := cands[fs.rng.Intn(len(cands))]
	if c.d != nil {
		e := c.d.entries[c.name]
		e.durable = e.cur
		c.d.setEntry(c.name, e)
		return
	}
	in := fs.inodes[c.ino]
	in.durable = in.volatile
	in.dirty = false
}

// Counters returns the fault counters so far.
func (fs *StorageModel) Counters() FSCounters { return fs.count }

// split returns the directory of path and the base name in it.
func (fs *StorageModel) split(path string) (*fdir, string, bool) {
	i := strings.LastIndexByte(path, '/')
	if i < 0 {
		return nil, "", false
	}
	d, ok := fs.dirs[path[:i]]
	if !ok || path[i+1:] == "" {
		return nil, "", false
	}
	return d, path[i+1:], true
}

// lookup returns the inode currently named path.
func (fs *StorageModel) lookup(path string) (*inode, bool) {
	d, name, ok := fs.split(path)
	if !ok {
		return nil, false
	}
	e := d.entries[name]
	if e.cur == 0 {
		return nil, false
	}
	return fs.inodes[e.cur], true
}

// setEntry stores e, dropping entries that are absent in both versions.
func (d *fdir) setEntry(name string, e dirent) {
	_, existed := d.entries[name]
	if e.cur == 0 && e.durable == 0 {
		delete(d.entries, name)
		if existed {
			d.sorted = nil
		}
		return
	}
	d.entries[name] = e
	if !existed {
		d.sorted = nil
	}
}

func (fs *StorageModel) write(path string, data []byte, fail bool) control.ErrCode {
	d, name, ok := fs.split(path)
	if !ok {
		return control.ErrNotFound
	}
	content := bytes.Clone(data)
	if content == nil {
		content = []byte{}
	}
	if fail {
		switch fs.rng.Intn(3) {
		case 0: // nothing happened
			return control.ErrIO
		case 1: // created / truncated, nothing written
			content = []byte{}
		default: // a prefix was written
			content = content[:fs.rng.Intn(len(content)+1)]
		}
	}
	e := d.entries[name]
	if e.cur == 0 {
		fs.nextIno++
		e.cur = fs.nextIno
		fs.inodes[e.cur] = &inode{}
		d.setEntry(name, e)
	}
	in := fs.inodes[e.cur]
	in.volatile = content
	in.dirty = !bytes.Equal(in.volatile, in.durable)
	in.poisoned = false
	if fail {
		return control.ErrIO
	}
	return control.OK
}

func (fs *StorageModel) fsync(path string, fail bool) control.ErrCode {
	in, ok := fs.lookup(path)
	if !ok {
		return control.ErrNotFound
	}
	if fail {
		if fs.faults.FsyncGate && in.dirty {
			in.poisoned = true // the kernel drops the dirty pages' error state
		}
		return control.ErrIO
	}
	if in.poisoned {
		return control.OK // fsyncgate: the lie
	}
	in.durable = in.volatile
	in.dirty = false
	fs.durGen++
	return control.OK
}

func (fs *StorageModel) rename(from, to string, fail bool) control.ErrCode {
	fd, fname, ok1 := fs.split(from)
	td, tname, ok2 := fs.split(to)
	if !ok1 || !ok2 || fd.entries[fname].cur == 0 {
		return control.ErrNotFound
	}
	if fail && fs.rng.Intn(2) == 0 {
		return control.ErrIO // not applied
	}
	if from != to {
		src := fd.entries[fname]
		ino := src.cur
		src.cur = 0
		fd.setEntry(fname, src)
		dst := td.entries[tname]
		dst.cur = ino
		td.setEntry(tname, dst)
	}
	if fail {
		return control.ErrIO // applied, but reported as failed
	}
	return control.OK
}

func (fs *StorageModel) fsyncDir(dir string, fail bool) control.ErrCode {
	d, ok := fs.dirs[dir]
	if !ok {
		return control.ErrNotFound
	}
	if fail {
		return control.ErrIO
	}
	for _, name := range d.names() {
		e := d.entries[name]
		e.durable = e.cur
		d.setEntry(name, e)
	}
	fs.durGen++
	return control.OK
}

func (fs *StorageModel) read(path string, fail bool) ([]byte, control.ErrCode) {
	in, ok := fs.lookup(path)
	if !ok {
		return nil, control.ErrNotFound
	}
	if fail {
		return nil, control.ErrIO
	}
	data := bytes.Clone(in.volatile)
	if data == nil {
		data = []byte{}
	}
	if len(data) > 0 && !fs.quiet && fs.rng.Chance(fs.faults.ReadCorruptProb) {
		bit := fs.rng.Intn(len(data) * 8)
		data[bit/8] ^= 1 << (bit % 8)
		fs.count.ReadCorruptions++
	}
	return data, control.OK
}

func (fs *StorageModel) unlink(path string, fail bool) control.ErrCode {
	d, name, ok := fs.split(path)
	if !ok || d.entries[name].cur == 0 {
		return control.ErrNotFound
	}
	if fail && fs.rng.Intn(2) == 0 {
		return control.ErrIO // not applied
	}
	e := d.entries[name]
	e.cur = 0
	d.setEntry(name, e)
	if fail {
		return control.ErrIO
	}
	return control.OK
}

func (fs *StorageModel) list(dir string, fail bool) ([]string, control.ErrCode) {
	d, ok := fs.dirs[dir]
	if !ok {
		return nil, control.ErrNotFound
	}
	if fail {
		return nil, control.ErrIO
	}
	var names []string
	for _, name := range d.names() {
		if d.entries[name].cur != 0 {
			names = append(names, name)
		}
	}
	return names, control.OK
}

// names returns the directory's entry names, sorted. The slice is cached
// and must not be modified; it stays valid while the caller iterates even if
// entries are added or removed (a new slice is built next time).
func (d *fdir) names() []string {
	if d.sorted == nil {
		names := make([]string, 0, len(d.entries))
		for name := range d.entries { //detcheck:ok keys are sorted before use
			names = append(names, name)
		}
		slices.Sort(names)
		d.sorted = names
	}
	return d.sorted
}

// dirNames returns the directory paths, sorted.
func (fs *StorageModel) dirNames() []string { return fs.dirList }

// DurableGen changes whenever the durable view may have changed, so
// monitors (through GroundTruth) can skip unchanged steps.
func (fs *StorageModel) DurableGen() uint64 { return fs.durGen }

// CrashReport counts how the adversary resolved volatile state.
type CrashReport struct {
	Kept, Lost, Torn int
}

// Crash resolves all volatile state (see StorageModel) and returns what happened.
func (fs *StorageModel) Crash() CrashReport {
	var rep CrashReport
	fs.durGen++
	// 1. Directory entries: each un-synced change is kept or reverted.
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		for _, name := range d.names() {
			e := d.entries[name]
			if e.cur == e.durable {
				continue
			}
			if fs.rng.Intn(2) == 0 {
				e.durable = e.cur
				rep.Kept++
			} else {
				e.cur = e.durable
				rep.Lost++
			}
			d.setEntry(name, e)
		}
	}
	// 2. File contents of every inode that still has a name.
	var live []InodeID
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		for _, name := range d.names() {
			live = append(live, d.entries[name].cur)
		}
	}
	slices.Sort(live)
	live = slices.Compact(live)
	for _, id := range live {
		in := fs.inodes[id]
		switch {
		case in.poisoned:
			rep.Lost++ // fsyncgate: the "clean" pages never reach the disk
		case !in.dirty:
		case len(in.volatile) > 0 && fs.rng.Chance(fs.faults.TornWriteProb):
			in.durable = in.volatile[:fs.rng.Intn(len(in.volatile))] // a strict prefix
			rep.Torn++
		case fs.rng.Intn(2) == 0:
			in.durable = in.volatile
			rep.Kept++
		default:
			rep.Lost++
		}
		in.volatile = in.durable
		in.dirty = false
		in.poisoned = false
	}
	// 3. Forget inodes without a name.
	var dead []InodeID
	for id := range fs.inodes { //detcheck:ok only collects ids; deletion order is irrelevant
		if _, found := slices.BinarySearch(live, id); !found {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		delete(fs.inodes, id)
	}
	return rep
}

// SetQuiet stops (true) or resumes (false) injecting disturbances.
func (fs *StorageModel) SetQuiet(quiet bool) { fs.quiet = quiet }

// FileState is one path and its content in a Snapshot.
type FileState struct {
	Path string
	Data []byte
}

// Snapshot is a sorted view of every file, for invariants and logs. Data
// slices are shared with the filesystem and must not be modified.
type Snapshot struct {
	Files []FileState // sorted by Path
}

// Durable returns what would be on disk after a crash in which the adversary
// loses everything volatile: the durable entries pointing at durable content.
// "Acked implies durable" is checked against this view.
func (fs *StorageModel) Durable() Snapshot { return fs.snapshot(true) }

// Current returns the view reads see right now.
func (fs *StorageModel) Current() Snapshot { return fs.snapshot(false) }

func (fs *StorageModel) snapshot(durable bool) Snapshot {
	var snap Snapshot
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		for _, name := range d.names() {
			if data, ok := fs.file(d, name, durable); ok {
				snap.Files = append(snap.Files, FileState{Path: dn + "/" + name, Data: data})
			}
		}
	}
	return snap
}

// EachDurable calls fn for every file in the Durable view, sorted by path
// (the same order as Durable().Files). data is shared with the storage model
// and must not be modified.
func (fs *StorageModel) EachDurable(fn func(path string, data []byte)) {
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		for _, name := range d.names() {
			if data, ok := fs.file(d, name, true); ok {
				fn(dn+"/"+name, data)
			}
		}
	}
}

// DurableFile returns path's content in the Durable view.
func (fs *StorageModel) DurableFile(path string) ([]byte, bool) {
	d, name, ok := fs.split(path)
	if !ok {
		return nil, false
	}
	return fs.file(d, name, true)
}

// CurrentFile returns path's content in the Current view.
func (fs *StorageModel) CurrentFile(path string) ([]byte, bool) {
	d, name, ok := fs.split(path)
	if !ok {
		return nil, false
	}
	return fs.file(d, name, false)
}

func (fs *StorageModel) file(d *fdir, name string, durable bool) ([]byte, bool) {
	e, ok := d.entries[name]
	if !ok {
		return nil, false
	}
	if durable {
		if e.durable == 0 {
			return nil, false
		}
		return fs.inodes[e.durable].durable, true
	}
	if e.cur == 0 {
		return nil, false
	}
	return fs.inodes[e.cur].volatile, true
}

// Hash summarizes the full state (both versions), folded into the trace hash.
func (fs *StorageModel) Hash() uint64 {
	h := uint64(fs.nextIno)
	for _, dn := range fs.dirNames() {
		d := fs.dirs[dn]
		h = Fold(h, hashString(dn))
		for _, name := range d.names() {
			e := d.entries[name]
			h = Fold(h, hashString(name))
			h = Fold(h, uint64(e.cur))
			h = Fold(h, uint64(e.durable))
			h = fs.hashInode(h, e.cur)
			if e.durable != e.cur {
				h = fs.hashInode(h, e.durable)
			}
		}
	}
	return h
}

func (fs *StorageModel) hashInode(h uint64, id InodeID) uint64 {
	in, ok := fs.inodes[id]
	if !ok {
		return Fold(h, 0)
	}
	h = Fold(h, uint64(len(in.durable))<<32|uint64(format.Checksum(in.durable)))
	h = Fold(h, uint64(len(in.volatile))<<32|uint64(format.Checksum(in.volatile)))
	var flags uint64
	if in.dirty {
		flags |= 1
	}
	if in.poisoned {
		flags |= 2
	}
	return Fold(h, flags)
}
