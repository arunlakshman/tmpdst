package sim

import "manifester/toys3/core"

// FSFaults configures the fake filesystem's adversary. Zero value: an honest,
// instant filesystem that still loses un-synced changes on crash.
type FSFaults struct {
	// EIOProb is the per-operation probability that an operation fails with
	// ErrIO. What a failed operation did is itself chosen adversarially (see
	// FakeFS.Land).
	EIOProb float64
	// TornWriteProb is the probability, per file with un-fsynced content at a
	// crash, that the file survives holding a random prefix of its volatile
	// content (instead of all of it or none of it).
	TornWriteProb float64
	// Latency is the [min, max] delay from issuing an action to its effect
	// landing; ReplyDelay is the further [min, max] delay until the completion
	// is delivered. A crash in between leaves the effect applied but unacked.
	Latency    [2]core.Duration
	ReplyDelay [2]core.Duration
	// FsyncGate models Linux after a failed fsync: the dirty data is marked
	// clean, later fsyncs of that file return OK, and the data still never
	// reaches disk. Only a fresh FSWrite clears it.
	FsyncGate bool
}

// FSPlan is decided when an action is issued, so the decision consumes RNG
// draws in issue order (deterministic) regardless of when it lands.
type FSPlan struct {
	Fail  bool          // complete with ErrIO
	Land  core.Duration // issue → effect
	Reply core.Duration // effect → completion delivered
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
}

// FakeFS is an in-memory filesystem with crash semantics. Every file and every
// directory entry has a durable and a volatile (current) version:
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
// durable everywhere. A correct server survives every such outcome.
//
// Directories are fixed at construction (core.ObjDir and core.TmpDir of the
// root); there is no mkdir.
type FakeFS struct {
	rng     *Rand
	faults  FSFaults
	quiet   bool // no new faults (QuietTail); crash resolution still applies
	dirs    map[string]*fdir
	inodes  map[InodeID]*inode
	nextIno InodeID
}

// NewFakeFS returns an empty filesystem with the given (durable) directories.
func NewFakeFS(r *Rand, faults FSFaults, dirs ...string) *FakeFS {
	panic("toys3/sim: NewFakeFS unimplemented (phase 2)")
}

// Plan draws the fault decision and latencies for a filesystem action.
func (fs *FakeFS) Plan(a core.Action) FSPlan {
	panic("toys3/sim: FakeFS.Plan unimplemented (phase 2)")
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
func (fs *FakeFS) Land(a core.Action, p FSPlan) core.EventBody {
	panic("toys3/sim: FakeFS.Land unimplemented (phase 2)")
}

// CrashReport counts how the adversary resolved volatile state.
type CrashReport struct {
	Kept, Lost, Torn int
}

// Crash resolves all volatile state (see FakeFS) and returns what happened.
func (fs *FakeFS) Crash() CrashReport {
	panic("toys3/sim: FakeFS.Crash unimplemented (phase 2)")
}

// SetQuiet stops (true) or resumes (false) fault injection.
func (fs *FakeFS) SetQuiet(quiet bool) { fs.quiet = quiet }

// FileState is one path and its content in a Snapshot.
type FileState struct {
	Path string
	Data []byte
}

// Snapshot is a sorted view of every file, for invariants and logs.
type Snapshot struct {
	Files []FileState // sorted by Path
}

// Durable returns what would be on disk after a crash in which the adversary
// loses everything volatile: the durable entries pointing at durable content.
// "Acked implies durable" is checked against this view.
func (fs *FakeFS) Durable() Snapshot {
	panic("toys3/sim: FakeFS.Durable unimplemented (phase 2)")
}

// Current returns the view reads see right now.
func (fs *FakeFS) Current() Snapshot {
	panic("toys3/sim: FakeFS.Current unimplemented (phase 2)")
}

// Hash summarizes the full state (both versions), folded into the trace hash.
func (fs *FakeFS) Hash() uint64 {
	panic("toys3/sim: FakeFS.Hash unimplemented (phase 2)")
}
