# Chapter 11: The Storage Model

> *"The disk is not your friend. It is a sullen, adversarial witness that will testify to whatever it last wrote down, and it writes things down when it feels like it."*

Chapter 4 explained, in terms of a real POSIX system, how bytes become durable: the page cache, `fsync` on files and on directories, atomic `rename`, torn writes. This chapter is about turning that understanding into **code that lies to the server in every way a real disk might**, deterministically, from a seed. `toys3/sim/storagemodel.go` is 651 lines, and it is the reason the simulator can catch durability bugs at all. Without it, a crash would be a non-event: every write would survive, every rename would stick, and the server's careful fsync protocol would be indistinguishable from no protocol at all.

The type is `sim.StorageModel`. Until the refactor of Chapter 3a it was called `FakeFS`, and its configuration `FSFaults`; the new names come from the control-theory reading of the system. In a feedback loop the thing being controlled is what control textbooks call the **plant**; in toys3 it is the disk with its page cache, and the code calls it **storage**. A software-in-the-loop testbench replaces real storage with a **storage model**. The inputs that act on storage without the controller's consent (an EIO, a torn write, a flipped bit, the kernel's background writeback, the fsyncgate lie, a power cut) are **disturbances**. The word "fake" undersold the thing: a test fake is usually a *simplified* working implementation, and this one is deliberately *more* hostile than the real thing.

The design goal is stated in one sentence of the type's doc comment: **"A correct controller survives every such outcome."** (`toys3/sim/storagemodel.go:102`). The doc goes on to name the storage model's role for the monitors: "Durable() is the true state of storage, against which the monitors check the controller's claims (Committed telemetry)" (`toys3/sim/storagemodel.go:103-104`). The StorageModel is not a model of ext4 or XFS. It is a model of *what POSIX permits*, resolved by an adversary. Anything the server is not entitled to assume, the StorageModel may do.

---

## 11.1 The two-version idea

Everything in the StorageModel rests on one idea: **every piece of state exists in two versions**, the one you can see now and the one that survives a crash.

- Every **file** (inode) has `volatile` content (what reads return now) and `durable` content (what is on the platter).
- Every **directory entry** (name → inode) has a `cur` version (what lookups see now) and a `durable` version (what the directory block on disk says).

The operations move state between versions in exactly the ways POSIX promises:

```
                FSWrite                    FSFsync(file)
   (nothing) ─────────────►  volatile  ─────────────────►  durable      (file content)

                create/rename/unlink       FSFsyncDir(dir)
   (nothing) ─────────────►    cur     ─────────────────►  durable      (directory entry)

   reads and listings see:  volatile + cur
   a crash leaves:          durable + durable, after the adversary resolves
                            every un-synced difference independently
```

And a crash is the moment of truth: each difference between the two versions is resolved, one at a time, by the RNG. The new state is then "all durable": `cur == durable` for every entry and `volatile == durable` for every inode.

This two-version model is, in miniature, what Pillai et al. call the "persistence properties" of a filesystem: which operations are atomic, and which orderings are preserved across a crash. The StorageModel chooses the *weakest* reasonable answers on purpose: content and directory entries are persisted independently, no ordering between them is promised without an explicit fsync, and a write is not atomic across a crash.

## 11.2 The knobs: Disturbances

```go
// toys3/sim/storagemodel.go:12-49
type Disturbances struct {
	EIOProb float64
	TornWriteProb float64
	DeadTime    [2]control.Duration
	FeedbackDelay [2]control.Duration
	ReadCorruptProb float64
	WritebackProb float64
	FsyncGate bool
}
```

(Comments elided; each field's comment is quoted where the behaviour is discussed below.)

The struct mixes two kinds of parameter, and its doc comment says so: "the storage model: its dynamics (dead time and feedback delay) and the disturbances it injects" (`toys3/sim/storagemodel.go:12-16`). `DeadTime` and `FeedbackDelay` are *dynamics*: every real storage device has them, even a perfect one, and they are what lets operations overlap and complete out of order. The other five are *disturbances* in the strict sense: exogenous inputs that make storage misbehave, which the controller can neither command nor prevent. Power loss is a disturbance too, but it is injected by the testbench (`Config.CrashProb`, Chapter 10) because it has to be coordinated with the queue and the oracle.

| Field | Default (`BaseConfig`) | What it does |
|---|---|---|
| `EIOProb` | 0.01 (swarm: 0.002 / 0.01 / 0.01 / 0.03) | Per-operation chance of completing with `ErrIO`. What the failed op *did* is chosen adversarially. |
| `TornWriteProb` | 0.3 | Per dirty file at a crash: chance it survives holding a random strict prefix of its volatile content. |
| `DeadTime` | 20 µs .. 1.5 ms | Dead time: issue → effect lands. (Was `Latency`.) |
| `FeedbackDelay` | 0 .. 500 µs | Feedback delay: effect lands → completion delivered. A crash in between: applied but unacked. (Was `ReplyDelay`.) |
| `ReadCorruptProb` | 0.005 | Per `FSRead`: one bit flipped in the returned bytes; the stored data is unchanged. |
| `WritebackProb` | 0.05 | After each landed action: make one random un-synced change durable. |
| `FsyncGate` | true | After a failed fsync on a dirty file, later fsyncs lie. |

The defaults are in `toys3/sim/config.go:162-170`; the per-seed swarm variation of `EIOProb` is at `toys3/sim/config.go:143` (Chapter 12).

The zero value is documented as "an honest, instant filesystem that still loses un-synced changes on crash" (`toys3/sim/storagemodel.go:12-16`). That is worth dwelling on: even with *every* fault knob at zero, the crash model is still adversarial. Un-synced changes are kept or lost by coin flip. The knobs add *more* hostility on top of that baseline, but the baseline is already enough to catch most protocol bugs (mutants a, b and c in Chapter 15 need nothing more).

## 11.3 ActuationPlan: deciding the future at issue time

```go
// toys3/sim/storagemodel.go:51-58
// ActuationPlan is decided when an actuation is issued, so the decision
// consumes RNG draws in issue order (deterministic) regardless of when it
// lands.
type ActuationPlan struct {
	Fail          bool             // complete with ErrIO
	DeadTime      control.Duration // issue → effect (the actuation lands)
	FeedbackDelay control.Duration // effect → completion delivered
}
```

```go
// toys3/sim/storagemodel.go:145-153
// Plan draws the fault decision, dead time and feedback delay for a
// filesystem action.
func (fs *StorageModel) Plan(a control.Action) ActuationPlan {
	var p ActuationPlan
	p.Fail = !fs.quiet && fs.rng.Chance(fs.faults.EIOProb)
	p.DeadTime = control.Duration(fs.rng.Between(int64(fs.faults.DeadTime[0]), int64(fs.faults.DeadTime[1])))
	p.FeedbackDelay = control.Duration(fs.rng.Between(int64(fs.faults.FeedbackDelay[0]), int64(fs.faults.FeedbackDelay[1])))
	return p
}
```

The simulator calls `Plan` when the server *issues* an FS action (`toys3/sim/simulator.go:188-189`, Chapter 10) and stores the plan in the `KindFSLand` queue entry. Three draws (or two, when the storage model is quiet, i.e. in the Settling phase: `!fs.quiet &&` short-circuits the EIO coin), always in the order the server issued its actions. The plan's two durations were called `Land` and `Reply` in the first edition; `Reply` in particular was easy to confuse with `control.Reply`, the controller's answer to a client, which has nothing to do with it.

Why decide at issue rather than at land? Because landing order is itself a product of random dead times. If the fault coin were tossed at land time, then the order of RNG draws would depend on the order of landings, which depends on earlier draws. That is still deterministic, strictly speaking, but it makes the run fragile: a change to the dead-time ranges would reshuffle *which* operations fail, not just *when* they finish. Deciding at issue time pins "the 47th FS action of this run fails" to the action itself.

One consequence, noted in Chapter 10 §10.14: a failure planned during Chaos stays planned even if the action lands in Settling.

Note what `Plan` does *not* decide: *how* a failed operation fails (applied or not, which prefix was written). Those are decided at land time by further draws, because they depend on the filesystem state at that moment (how long the data is, whether the source exists).

## 11.4 The data structures

### inode

```go
// toys3/sim/storagemodel.go:60-71
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
```

The separation of inodes from names is essential and is the source of the most interesting bug in this book (§11.13). A rename does not copy data; it moves a name. If a crash then keeps the new name *and* reverts the removal of the old one, both names point to the same inode: a hard link that nobody created on purpose. On a real filesystem the same thing is conceivable (rename is two directory changes, and if they are in different directories and neither is fsynced, each could independently persist or not on a filesystem without cross-directory atomicity); in the StorageModel it is routine.

`dirty` is a cached `!bytes.Equal(volatile, durable)`. `poisoned` is the fsyncgate state (§11.6).

### dirent

```go
// toys3/sim/storagemodel.go:73-77
// dirent is one name in a directory, in two versions. 0 means absent.
type dirent struct {
	cur     InodeID // visible now (after creates/renames/unlinks)
	durable InodeID // what survives a crash (after FSFsyncDir)
}
```

InodeID 0 means "no file". So a dirent can encode four situations:

| `cur` | `durable` | Meaning |
|---|---|---|
| 0 | 0 | Absent everywhere (such entries are deleted from the map, see `setEntry`) |
| X | 0 | Created (or renamed in) but the directory was not fsynced |
| 0 | X | Unlinked (or renamed away) but the directory was not fsynced |
| X | Y | Present; if X ≠ Y, replaced by a rename that was not fsynced |

### fdir

```go
// toys3/sim/storagemodel.go:79-84
// fdir is a directory. Iterate names in sorted order only (detcheck bans
// ranging over maps; determinism requires it anyway).
type fdir struct {
	entries map[string]dirent
	sorted  []string // cached sorted names; nil when stale
}
```

The entries are in a Go map for O(1) lookup by name, but **every iteration goes through `names()`**, which returns a cached sorted slice:

```go
// toys3/sim/storagemodel.go:441-454
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
```

This is the canonical way to iterate a map deterministically in Go: collect the keys (in random order), sort them, iterate the sorted slice. `detcheck` (Chapter 8) flags every `range` over a map; the `//detcheck:ok` comment on line 447 is the suppression, with the mandatory reason. Every crash resolution, every writeback candidate list, every snapshot and every hash goes through `names()`, so the RNG is always consumed in name order.

The cache is invalidated by `setEntry` only when the *set of names* changes, not when an entry's versions change:

```go
// toys3/sim/storagemodel.go:274-288
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
```

The comment on `names()` adds a subtle guarantee: "it stays valid while the caller iterates even if entries are added or removed (a new slice is built next time)". `Crash` relies on this: it iterates `d.names()` while calling `setEntry`, which may delete entries and nil out the cache, but the slice being iterated is not modified, only replaced for the next caller.

### StorageModel

```go
// toys3/sim/storagemodel.go:108-118
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
```

Directories are fixed at construction (`NewStorageModel`, lines 128-143): the simulator passes `/data/obj` and `/data/tmp`, both durable and empty. "there is no mkdir" (lines 106-107): the controller has no mkdir action either, so this is a faithful mirror of the server's capabilities. Inode numbers come from `nextIno`, starting at 1 so that 0 can mean "absent".

Paths are split by `split` (lines 248-259) at the last `/` into a known directory and a non-empty base name; an unknown directory or empty name yields "not ok", which every operation turns into `ErrNotFound`. `lookup` (lines 261-272) resolves a path to the inode its `cur` entry names.

## 11.5 Land: applying an action

```go
// toys3/sim/storagemodel.go:155-204
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
	...
	switch a := a.(type) {
	case control.FSWrite:
		err = fs.write(a.Path, a.Data, p.Fail)
		body = control.FSWriteDone{IO: a.IO, Err: err}
	...
	}
	if err == control.ErrIO {
		fs.count.EIOs++
	}
	if fs.rng.Chance(fs.faults.WritebackProb) {
		fs.writeback()
	}
	return body
}
```

The doc comment's table is the contract for failed operations. Note the last line: **"Missing paths complete with ErrNotFound (never by chance)."** If the path does not exist, the operation returns ENOENT even if the plan said fail. ENOENT is information the server acts on (404 for GET and DELETE, a COPY of an absent source), so it must be truthful; EIO is reserved for "something went wrong and you cannot know what". `TestStorageModelEIOMayOrMayNotApply` checks this explicitly: a planned failure on a missing path returns ENOENT (`toys3/sim/storagemodel_test.go:151-154`).

After every landed action, a writeback coin is tossed (§11.8). Note the counters: `Landed` becomes `fsops` in the stats line, `EIOs` becomes `eio`.

Now each operation.

### 11.5.1 write

```go
// toys3/sim/storagemodel.go:290-324
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
```

This models `open(O_WRONLY|O_CREAT|O_TRUNC)` + `write` + `close`, which is exactly what production does (`toys3/prod/executor.go:76-86`).

- **`bytes.Clone`**: the StorageModel never keeps a reference to the server's slice. Events and actions are supposed to be immutable once sent (Chapter 3), but the StorageModel does not trust that. `TestStorageModelNeverAliasesHandedOutSlices` (`toys3/sim/storagemodel_test.go:173-187`) mutates the input after the write and the output after a read, and checks the stored content is unaffected.
- **Failure modes**, chosen uniformly: (0) nothing happened at all, not even the create; (1) the file was created or truncated but no data written; (2) a prefix was written, possibly all of it (`Intn(len+1)` includes the full length). All three return EIO. The server's response (Chapter 7) must be correct for all three: it replies 500 and leaves the temp file for recovery to clean up.
- **Create if absent**: a new inode and a dirent with `cur` set. The dirent's `durable` stays as it was (usually 0), so the *name* is not durable until the directory is fsynced, and the *content* is not durable until the file is fsynced. Two independent fsyncs are needed, which is the whole point of Chapter 4.
- **If the name already exists, the existing inode is overwritten.** Read that again, because it is the hinge of §11.13. `O_TRUNC` does not create a new file; it truncates the file the name currently points at. If that inode also has another name (a hard link), the other name sees the new content too. The StorageModel models this faithfully: `in := fs.inodes[e.cur]` and then `in.volatile = content`.
- **`dirty`** is recomputed: writing identical bytes to a synced file leaves it clean.
- **`poisoned = false`**: a fresh write clears the fsyncgate state (§11.6). This mirrors Linux, where new dirty pages get a fresh chance to be written back.

### 11.5.2 fsync

```go
// toys3/sim/storagemodel.go:326-344
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
```

A successful fsync copies volatile to durable. It makes the *content* durable and nothing else: not the name. A failed fsync makes nothing durable, and with `FsyncGate`, poisons a dirty inode. A poisoned inode's later fsyncs return OK without doing anything. That is "the lie" (§11.6).

Note that a failed fsync *without* `FsyncGate` (or on a clean inode) leaves the inode dirty, and a retry can succeed. That is the behaviour applications historically assumed, and the one Linux did not provide.

### 11.5.3 rename

```go
// toys3/sim/storagemodel.go:346-368
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
```

The visible effect of a rename is two `cur` changes: the source name's `cur` becomes 0, the destination name's `cur` becomes the moved inode (replacing whatever it named; the replaced inode keeps existing as long as some `durable` still names it, which matters for crash resolution). Neither `durable` changes. **Atomicity is modelled for visibility only**: no reader can see a state where the destination is half-replaced, because the change is a single assignment. **Durability is per-entry**: the two halves of the rename can independently persist or not at a crash.

A failed rename is applied or not with equal probability, and in both cases returns EIO. This is what makes the server's "rename EIO → 500 + Trip" rule necessary (Chapter 7): after an EIO from rename, the server cannot know whether the object is now visible under the new name.

`from == to` is a no-op, as in POSIX (`rename(2)`: "If oldpath and newpath are existing hard links referring to the same file, then rename() does nothing"). The server never issues such a rename (temp names and object names live in different directories), but the StorageModel handles it correctly anyway.

### 11.5.4 fsyncDir

```go
// toys3/sim/storagemodel.go:370-385
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
```

Every entry in the directory gets `durable = cur`. Entries that are now absent in both versions are deleted by `setEntry`. Note that this makes **every** pending change in the directory durable, not just the one the caller cares about. That is the real semantics: `fsync` on a directory flushes the directory, not one entry. The server benefits from this in one place (the `FSFsyncDir(obj/)` of one PUT persists every rename into `obj/` that has landed so far), and a careless server could become accidentally correct because another request's fsync covered for it. The simulator explores both: sometimes the other request's fsync lands first, sometimes not.

A failed directory fsync makes nothing durable. There is no poisoning for directories; FsyncGate only models the file-data case.

Crucially, **fsyncing a directory does nothing for file contents**. `obj/k` can have a durable name pointing to an inode whose durable content is empty. That is precisely the state mutant b produces (Chapter 15), and the `ObjNeverTorn` invariant catches it the moment the directory fsync lands:

```
#42     t=6.871157ms   inc=1 deliver     FSRenameDone{io11 OK} => FSFsyncDir{io14 /data/obj}
#43     t=7.182477ms   inc=1 fs-land     land FSRename{io13 /data/tmp/0000000000000009 -> /data/obj/k0} -> FSRenameDone{io13 OK}
#44     t=7.233120ms   inc=1 fs-land     land FSFsyncDir{io14 /data/obj} -> FSFsyncDirDone{io14 OK}
#44     t=7.233120ms   inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k0 is torn or corrupt (0 bytes)
#44     t=7.233120ms   inc=1 crash       FAILURE obj-never-torn: durable /data/obj/k2 is torn or corrupt (0 bytes)
```

No crash was needed. The durable view after #44 *already* contains two zero-byte objects, because the directory was synced and the files were not. Notice also that the directory fsync for `k2`'s PUT made `k0`'s rename durable too (io13 landed just before io14): one fsync, two entries.

### 11.5.5 read

```go
// toys3/sim/storagemodel.go:387-405
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
```

Reads see `volatile`, through `cur`. A read failure returns EIO with no data and no effect. And with probability `ReadCorruptProb`, one uniformly random bit of the *returned copy* is flipped; the stored data is untouched (it was cloned first). This models a transient media, controller or bus error: the next read of the same file is fine.

Why does this fault exist? Because without it, **the server's CRC check would be dead code**. A correct protocol never puts a torn or corrupt file under `obj/`, so every read of an object decodes cleanly, so the "corrupt → 500" branch never runs, so mutant e (skip the CRC check) would be indistinguishable from the correct server. `DESIGN.md:174-177` makes this point, and `TestCorruptionUnreachableWithoutBitFlips` (`toys3/sim/sim_test.go:120-133`) proves the first half: with `ReadCorruptProb = 0`, 200 seeds never produce a single `CorruptionDetected` telemetry. (While writing this book, mutant e was also run for 1,000 seeds with read corruption disabled: it was never caught.)

Seed 18 shows the fault reaching the server and the server doing the right thing:

```
#656    t=153.982439ms inc=7 fs-land     land FSRead{io87 /data/obj/k3} -> FSReadDone{io87 OK raw(29 bytes)}
#657    t=154.136154ms inc=7 deliver     FSReadDone{io87 OK raw(29 bytes)} => Emit{CorruptionDetected{"k3"}} Reply{c98 500}
```

`raw(29 bytes)` is how `describePayload` (`toys3/sim/log.go:29-44`) renders bytes that do not decode as an object: 16 bytes of header plus 13 of payload, one bit wrong. The server emits `CorruptionDetected` and replies 500. Under mutant e, the same seed at the same step serves the 13 corrupt bytes with a 200, and two invariants fire (Chapter 15).

Corruption is gated on `!fs.quiet` at *land* time, so it stops exactly when Chaos ends. Also note `len(data) > 0`: an empty file cannot have a bit flipped, and `Intn(0)` would panic.

A subtle statistic from the 200-seed run in the preface: `readcorrupt=71 corruptdet=70`. One flipped read did not produce a `CorruptionDetected` telemetry. The most likely explanation is the land/deliver split (Chapter 10 §10.8): the read landed (and was counted), and the server crashed before the completion was delivered.

### 11.5.6 unlink and list

```go
// toys3/sim/storagemodel.go:407-422
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
```

Unlink sets `cur = 0`; the name stays durable until the directory is fsynced. Like rename, a failed unlink is applied or not with equal probability. The inode is not freed: it stays in `fs.inodes` until a crash garbage-collects unnamed inodes (§11.9, phase 3). That is necessary, because a crash may *revert* the unlink, and the inode must still be there to come back.

```go
// toys3/sim/storagemodel.go:424-439
func (fs *StorageModel) list(dir string, fail bool) ([]string, control.ErrCode) {
	...
	var names []string
	for _, name := range d.names() {
		if d.entries[name].cur != 0 {
			names = append(names, name)
		}
	}
	return names, control.OK
}
```

Listing returns the names that are currently visible, sorted (matching the contract in `toys3/control/storage.go:144` and production's `os.ReadDir`, which also sorts). Names that are unlinked but not yet durably so are invisible, as they would be on a real system.

## 11.6 FsyncGate: the lie

In 2018, PostgreSQL developers discovered that their assumption about `fsync` failures on Linux was wrong. They assumed that if `fsync` returned EIO, the dirty data was still dirty, and a later `fsync` would retry writing it. On Linux, after a writeback error, the kernel could mark the affected pages clean (and, depending on kernel version and timing, report the error to only one of the file descriptors that asked). A later `fsync` would return success, because there was nothing dirty left to write. The data was simply gone. The episode became known as "fsyncgate".

The StorageModel models the essential consequence in three lines:

```go
// toys3/sim/storagemodel.go:331-339
	if fail {
		if fs.faults.FsyncGate && in.dirty {
			in.poisoned = true // the kernel drops the dirty pages' error state
		}
		return control.ErrIO
	}
	if in.poisoned {
		return control.OK // fsyncgate: the lie
	}
```

and in the crash resolution:

```go
// toys3/sim/storagemodel.go:503-504
		case in.poisoned:
			rep.Lost++ // fsyncgate: the "clean" pages never reach the disk
```

A poisoned inode's volatile content will never become durable by fsync, and at a crash it reverts to whatever was durable before. Writeback also skips poisoned inodes (§11.8). Only a fresh `FSWrite` clears the poison.

`TestStorageModelFsyncGateLies` (`toys3/sim/storagemodel_test.go:157-171`) is the whole story in fifteen lines:

```go
	fs := newTestFS(1, Disturbances{FsyncGate: true})
	do(t, fs, control.FSWrite{IO: 1, Path: tObj + "/k", Data: []byte("data")})
	do(t, fs, control.FSFsyncDir{IO: 2, Dir: tObj})
	if err := errOf(fs.Land(control.FSFsync{IO: 3, Path: tObj + "/k"}, ActuationPlan{Fail: true})); err != control.ErrIO {
		t.Fatal("want EIO")
	}
	if err := do(t, fs, control.FSFsync{IO: 4, Path: tObj + "/k"}); err != control.OK {
		t.Fatal("the retry should claim success")
	}
	fs.Crash()
	if got, _ := fs.CurrentFile(tObj + "/k"); len(got) != 0 {
		t.Fatalf("fsyncgate: data reached disk anyway: %q", got)
	}
```

Write, make the name durable, fsync fails, retry "succeeds", crash: the file exists (its name was durable) and is empty. A server that "handles" the EIO by retrying the fsync would ack data that is gone. This is why the server trips on any fsync error (Chapter 7), and why mutant d (keep going after the error) is caught at seed 1 by `obj-never-torn`: it renames the poisoned temp file into `obj/`, fsyncs the directory, and the durable view now holds an empty object.

## 11.7 What the StorageModel does not model

Before the crash machinery, a list of deliberate omissions, so you know what the adversary *cannot* do:

- **No mkdir, no directory renames.** The two directories exist and are durable from the start.
- **No interleaved torn writes.** A torn file holds a *prefix of the new content*. Real disks can tear at sector or page granularity and leave a mixture of old and new blocks (for an overwrite in place), or new blocks with garbage between them. toys3 never overwrites a file in place (it always writes a fresh temp file), so for the correct server a prefix is a reasonable model; Chapter 17 lists "interleaved torn writes" as a model extension.
- **No metadata corruption, no lost directories, no misdirected writes.** The StorageModel never corrupts *durable* state on its own; read corruption is transient.
- **No space exhaustion (ENOSPC), no permissions, no partial reads.** Every failure is EIO or ENOENT.
- **No ordering promises beyond fsync.** This one is *more* adversarial than real filesystems, see §11.14.

## 11.8 Background writeback

```go
// toys3/sim/storagemodel.go:206-243
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
```

Real kernels do not wait for `fsync` to write things back. Dirty pages are flushed by background threads after a timeout, journaled metadata is committed every few seconds, and memory pressure forces writeback at arbitrary times. So **un-synced changes become durable on their own**, at times the application does not control. A model in which un-synced changes only ever become durable "by luck at a crash" misses a whole category of states: the ones where something became durable *too early*, before the application had finished a multi-step protocol.

After each landed action, with probability `WritebackProb` (5%), one candidate change is chosen uniformly and made durable. The candidates are: every directory entry whose `cur` differs from `durable`, and every dirty, unpoisoned inode reachable through a current name. Two details:

- **An inode with two current names is listed twice** (the loop appends a candidate per name), so it is twice as likely to be chosen. This is a harmless bias; real writeback does not care about names at all.
- Inodes that are only reachable via a `durable` name (unlinked but not yet durably) are not candidates for content writeback, though their dirent change is.

`DESIGN.md:168-173` explains why this fault "turned out to matter":

> Without it, a temp file's name is durable only if a crash happens to keep it. With it, the name can already be durable when the file is renamed into `obj/`, so a later crash brings it back as a second hard link to the committed object.

That second hard link is the subject of §11.13, and Chapter 15 quantifies the effect on mutant f: without writeback (and without swarm) it took **429** seeds to catch; with writeback alone, **36**.

## 11.9 Crash: three phases

```go
// toys3/sim/storagemodel.go:463-530
type CrashReport struct {
	Kept, Lost, Torn int
}

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
```

### Phase 1: directory entries

Directories in sorted order, names in sorted order. Every entry with an un-synced change gets a fair coin: **keep** (the change is now durable) or **revert** (the change is lost). After phase 1, `cur == durable` for every entry, and entries absent in both are deleted.

Each entry is resolved **independently**. This is the crucial adversarial choice. A rename that moved a name from `tmp/` to `obj/` produced two entry changes, and they are resolved separately:

```
 Before crash:    tmp/t  {cur: 0, durable: I}      obj/k {cur: I, durable: 0}
                  (rename landed; neither directory fsynced; the temp name had been made durable)

 Coins:           tmp/t: keep  /  revert           obj/k: keep  /  revert

 Outcomes:        tmp/t gone,  obj/k = I     → rename persisted          ("present")
                  tmp/t = I,   obj/k gone    → rename lost               ("absent")
                  tmp/t = I,   obj/k = I     → BOTH NAMES: hard link     ("both")
                  tmp/t gone,  obj/k gone    → NEITHER: inode I unnamed, garbage collected
```

Real filesystems that journal metadata typically commit a rename as one transaction, so on them the "both" and "neither" outcomes are not expected after a crash (§11.14). The StorageModel allows them because POSIX does not forbid them, and because a protocol that is correct under independent resolution is correct under any coarser one.

### Phase 2: file contents

Now the set of **live inodes** is computed: every inode named by some entry (after phase 1, `cur == durable`, so there is one name per entry to look at). Sorted and deduplicated, so an inode with two names is resolved once. For each live inode:

1. **Poisoned** (fsyncgate): its content stays at whatever was durable. Counted as lost.
2. **Clean**: nothing to do.
3. **Torn**, with probability `TornWriteProb` if there is any volatile content: the durable content becomes a **strict prefix** of the volatile content, `volatile[:Intn(len)]`, which can be anywhere from 0 bytes to `len-1` bytes. Never the full content: a "torn" write that happens to be complete is not torn.
4. Otherwise a fair coin: **keep** all of the volatile content, or **lose** it (revert to the old durable content, which is empty for a never-synced file).

Then `volatile = durable`, clean, unpoisoned.

```
 dirty inode, volatile = "HDR|payload-v2", durable = "HDR|payload-v1"

   ┌─ poisoned ─────────► durable stays "HDR|payload-v1"           (Lost)
   ├─ torn (p=0.3) ─────► durable = prefix of "HDR|payload-v2"      (Torn)  e.g. "HDR|pay"
   ├─ coin heads ───────► durable = "HDR|payload-v2"                (Kept)
   └─ coin tails ───────► durable stays "HDR|payload-v1"            (Lost)
```

Note that the torn prefix is a prefix of the *new* content, not a mixture with the old (§11.7).

### Phase 3: garbage collection

Inodes that no entry names are deleted. They were kept alive until now precisely so that phase 1 could revert an unlink or a rename and bring them back. The map iteration on line 521 is suppressed for detcheck with a reason: it only collects ids, and deleting map keys in any order produces the same map.

### The report and the stats

`CrashReport` counts kept, lost and torn changes across both phases. The simulator adds them to `KeptChanges`, `LostChanges` and `TornFiles` and folds them into the trace (Chapter 10 §10.10). In the event log, every crash line shows them: `CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries`. `TestCoverage` requires all three to be non-zero across 200 seeds (`toys3/sim/sim_test.go:99-101`): a crash model that never tears a file or never keeps an un-synced change is not exploring.

### RNG consumption at a crash

Every un-synced dirent draws one `Intn(2)`. Every dirty, unpoisoned live inode draws a `Chance` (if `TornWriteProb > 0` and the file is non-empty), and then either a torn-length `Intn` or a keep/lose `Intn(2)`. All in sorted order. A crash with many pending changes consumes many draws, which shifts everything after it; another reason a seed's identity is "this exact universe", not "these parameters".

## 11.10 Views: Durable, Current, and DurableGen

```go
// toys3/sim/storagemodel.go:547-553
// Durable returns what would be on disk after a crash in which the adversary
// loses everything volatile: the durable entries pointing at durable content.
// "Acked implies durable" is checked against this view.
func (fs *StorageModel) Durable() Snapshot { return fs.snapshot(true) }

// Current returns the view reads see right now.
func (fs *StorageModel) Current() Snapshot { return fs.snapshot(false) }
```

```go
// toys3/sim/storagemodel.go:600-615
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
```

The **durable view** is the pessimistic future: durable names pointing at durable content. It is exactly what a crash would leave if the adversary lost *every* un-synced change. Because every crash outcome keeps a superset of it (some un-synced changes may be kept, but nothing durable is ever lost), checking an invariant against the durable view is a check against the worst case, without having to crash. Two invariants use it (Chapter 14):

- **`AckedIsDurable`** looks up the committed key's file in the durable view the moment the server emits `Committed` (via `DurableFile`, lines 582-589, which the monitors reach through the `GroundTruth` interface of Chapter 14). If the rename or the file content is not durable yet, the server acked too early. This is how mutants a and c are caught at seed 1 in the first ~45 steps, without a single crash.
- **`ObjNeverTorn`** decodes every durable file under `obj/`, iterating them with `EachDurable` (`toys3/sim/storagemodel.go:568-579`), which visits the durable view in sorted path order without building a `Snapshot`.

Neither invariant sees the `StorageModel` itself. They see a three-method interface, `GroundTruth` (`DurableFile`, `EachDurable`, `DurableGen`; `toys3/sim/invariant.go:37-51`), which `*StorageModel` implements. In the vocabulary of Chapter 3a this is an *anti-corruption layer* between the verification context and the storage model: the monitors may read the true durable state of storage and nothing else. In particular they cannot call `Current()`, the volatile view, which no real observer of a disk could see either. Before the refactor, `View.FS()` handed every invariant the whole `*FakeFS`.

The phrase "a superset of it" needs a qualification. Phase 1 either reverts `cur` to `durable` or replaces `durable` with the newer `cur`. Phase 2 either keeps `durable` or replaces it with newer content, and the torn case replaces it with a *prefix* of the newer content. So a crash never resurrects anything older than the durable view, but it can replace durable state with newer un-synced state, including a torn prefix of a file that was overwritten in place. The durable view is therefore precisely "what the disk holds if nothing un-synced survives". That is what the invariants need: in a correct server, every file under `obj/` was fully fsynced before it got its name and is never written again (overwrites go through a fresh temp inode), so nothing can tear it.

`DurableGen` (lines 459-461) is a counter bumped whenever the durable view may have changed: a successful fsync, a successful directory fsync, a writeback, and a crash. Writes, renames, unlinks and failed fsyncs never change the durable view, so they do not bump it. `ObjNeverTorn` remembers the last generation it checked and skips steps where nothing durable changed (`toys3/sim/invariant.go:169-174`), turning an O(files) check per step into one that runs only after the few operations that matter.

## 11.11 Hash

```go
// toys3/sim/storagemodel.go:617-652
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
	if in.dirty { flags |= 1 }
	if in.poisoned { flags |= 2 }
	return Fold(h, flags)
}
```

Folded into the trace after every step (Chapter 10 §10.5). It covers both versions of every entry and every reachable inode's both versions (as length plus CRC-32, rather than every byte, for speed) and flags. The inode *numbers* are included too (`nextIno`, `e.cur`, `e.durable`), so two runs that produce the same file contents through different sequences of creates would hash differently. That is intended: determinism means *the same history*, not merely the same outcome. Note that the hash does not include inodes reachable only through nothing (there are none outside a crash's phase 3) or the fault counters.

## 11.12 The StorageModel tests, one by one

`toys3/sim/storagemodel_test.go` tests the StorageModel in isolation. Each test pins one sentence of the model. Helpers: `newTestFS(seed, faults)` builds a StorageModel with directories `/d/obj` and `/d/tmp`; `do` lands an action with an empty plan (no failure); `errOf` extracts the error code from a completion.

**`TestStorageModelUnsyncedWriteMayVanish`** (lines 45-67). Over 64 seeds: write `obj/a` with no fsync of anything, check it is visible now but absent from the durable view, crash, and count whether it survived. Both outcomes must occur. This is the baseline adversary: an un-synced create is a coin flip (two coin flips, actually: one for the name, one for the content).

**`TestStorageModelRenameWithoutDirFsyncMayVanish`** (lines 69-93). Over 128 seeds: write a temp file, fsync it, fsync `tmp/` (so the temp *name* is durable), rename it to `obj/k`, and crash without fsyncing `obj/`. All three outcomes must occur: only the new name ("present"), neither visible under `obj/` ("absent"), and **both names** ("both"). That third outcome is the hard-link resurrection of §11.13, tested directly. The test's "absent" bucket lumps together two outcomes: the rename was lost (the temp name survives, `obj/k` does not), and "neither" (both entry changes resolved against the file, so the inode has no name left and is garbage-collected).

**`TestStorageModelFullySyncedSurvives`** (lines 95-115). Over 100 seeds, with `TornWriteProb: 1` (every dirty file would tear): write, fsync, rename, fsync `obj/`, fsync `tmp/`, then crash. The file must survive intact under its new name and the temp name must be gone. This is the *positive* half of the contract: the correct protocol defeats even the maximum-hostility adversary.

**`TestStorageModelTornPrefix`** (lines 117-132). Write `obj/k`, fsync only the directory (name durable, bytes not), crash with `TornWriteProb: 1`. The file must exist (its name was durable), the report must say `Torn == 1`, and the content must be a *strict* prefix of what was written.

**`TestStorageModelEIOMayOrMayNotApply`** (lines 134-155). Over 64 seeds: a failed rename must sometimes have applied and sometimes not. And a planned failure on a missing path returns ENOENT, not EIO.

**`TestStorageModelFsyncGateLies`** (lines 157-171). Covered in §11.6.

**`TestStorageModelNeverAliasesHandedOutSlices`** (lines 173-187). Writes copy their input; reads return copies.

Notice the shape of the first, second and fifth tests: **"over N seeds, every outcome must occur"**. A fault model is only useful if it actually produces the outcomes it claims to. Testing that an adversary is adversarial, rather than merely that it is *allowed* to be, is a habit worth stealing.

## 11.13 Worked scenario: the resurrected temp file

This is the most instructive interaction in the whole simulator, and it appears in a *passing* run of the correct server. Then we will see how removing one recovery step turns it into silent data corruption.

### The harmless version (seed 1, correct server)

At step #150, client request 23 starts a PUT of `k1`. The server's IOID counter is at 49, so the temp file is `tmp/0000000000000031` (0x31 = 49; `TmpPath` names temp files by the IOID of their write in 16 hex digits, `toys3/format/layout.go:73-79`):

```
#150    t=35.285919ms  inc=1 deliver     Request{c23 PUT "k1" value-of-23} => FSWrite{io49 /data/tmp/0000000000000031 obj(value-of-23)}
#154    t=35.592579ms  inc=1 fs-land     land FSWrite{io49 /data/tmp/0000000000000031 obj(value-of-23)} -> FSWriteDone{io49 OK}
#155    t=35.792963ms  inc=1 deliver     FSWriteDone{io49 OK} => FSFsync{io51 /data/tmp/0000000000000031}
#156    t=36.356855ms  inc=1 fs-land     land FSFsync{io51 /data/tmp/0000000000000031} -> FSFsyncDone{io51 OK}
#159    t=36.788933ms  inc=1 deliver     FSFsyncDone{io51 OK} => FSRename{io53 /data/tmp/0000000000000031 -> /data/obj/k1}
#160    t=37.150944ms  inc=1 fs-land     land FSRename{io53 /data/tmp/0000000000000031 -> /data/obj/k1} -> FSRenameDone{io53 OK}
#161    t=37.524170ms  inc=1 deliver     FSRenameDone{io53 OK} => FSFsyncDir{io54 /data/obj}
#164    t=38.023766ms  inc=1 fs-land     land FSFsyncDir{io54 /data/obj} -> FSFsyncDirDone{io54 OK}
```

The textbook protocol: write, fsync file, rename, fsync `obj/`. Committed and acked shortly after. Now track the directory entries for inode *I* (the temp file's inode):

```
 after #154 (write lands):     tmp/…31 {cur: I, durable: 0}        I: volatile=obj(v23), durable=∅
 after #156 (fsync):                                               I: durable=obj(v23)
 some land between #154 and #160: writeback picks the tmp/…31
                               dirent change:  tmp/…31 {cur: I, durable: I}
 after #160 (rename lands):    tmp/…31 {cur: 0, durable: I}        obj/k1 {cur: I, durable: old}
 after #164 (fsync obj/):                                          obj/k1 {cur: I, durable: I}
```

The server never fsyncs `tmp/` after a rename. Why would it? The temp name is garbage once the rename has happened. So the removal of `tmp/…31` stays un-synced. The writeback step is inferred, not logged: for the name to come back after the crash, the entry's `durable` must have been *I*; the only fsync of `tmp/` in incarnation 1 happened during recovery at #10, long before io49; and a writeback *after* the rename would have persisted the removal instead. So a background writeback (5% per landed action) persisted the temp name somewhere between the write landing at #154 and the rename landing at #160.

Seventy steps later, at #233, the server trips after an fsync error, and the crash resolves every pending change. The `tmp/…31` entry has `cur: 0, durable: I`, a pending removal. The coin says *revert*. The name comes back, pointing at inode *I*, which is also the durable content of `obj/k1`. **Two names, one inode.** Incarnation 2's recovery lists `tmp/`:

```
#245    t=56.901846ms  inc=2 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]}
#246    t=57.400810ms  inc=2 deliver     FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]} => FSUnlink{io2 /data/tmp/0000000000000031} FSUnlink{io3 /data/tmp/000000000000004b} FSUnlink{io4 /data/tmp/000000000000004c}
```

`…31` is back from the dead. Recovery unlinks it, which is completely safe: unlinking one name of a hard link does not touch the inode's content or its other name. (`…4b` and `…4c` are ordinary leftovers of the PUTs interrupted by the trip.) In this run the unlink of `…31` happens to fail with EIO at #251, the server trips again, and incarnation 3 finds `…31` still there and removes it successfully at #268. Belt, braces, and a second belt.

This is a beautiful example of why **recovery must clean up temp files**, and why "clean up" means "unlink every name in `tmp/`", not "unlink names that look incomplete". The server cannot tell, from a name, whether a temp file is an interrupted write or an alias of a committed object. It does not need to: unlinking is safe in both cases. What it must *never* do is write to an existing temp name.

### The harmful version (seed 20, mutant f)

Mutant f (`toys3/sim/mutants_test.go:303-312`) makes recovery skip the cleanup: it rewrites every successful `FSListDone` to have no names, so the server lists `tmp/`, sees nothing, fsyncs `tmp/`, and starts serving. Temp files accumulate. In seed 20's stats line: `tempsrm=0`.

The other ingredient is in `TmpPath`'s doc comment: temp names are "unique within an incarnation, and recovery clears tmp/ before a new incarnation writes anything, so they never collide" (`toys3/format/layout.go:73-75`). IOIDs restart at 1 in every incarnation (the counter lives in memory). So `tmp/0000000000000004` is the temp name of the fourth IO of *every* incarnation. With recovery doing its job, that is fine. Without it, the new incarnation's `FSWrite` of `tmp/…04` lands on an *existing* name, and `write` (§11.5.1) truncates and overwrites **the existing inode**.

Here is seed 20 under mutant f. Incarnation 12 boots; `tmp/` holds ten leftovers, including `…04`; recovery ignores them:

```
#1468   t=220.791845ms inc=12 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000004 0000000000000005 000000000000000b ...]}
#1469   t=220.947361ms inc=12 deliver     FSListDone{io1 OK [0000000000000004 ...]} => FSFsyncDir{io2 /data/tmp}
#1472   t=222.258660ms inc=12 deliver     FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSWrite{io3 /data/tmp/0000000000000003 obj(value-of-220)} FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-221)} ...
```

(The log line for #1469 shows the *real* listing because the simulator logs the event before the mutant rewrites it; the server inside the mutant saw an empty list.)

Every one of those leftovers is durable: a crash leaves `cur == durable` for every surviving entry, so `tmp/…04 → J` (some old inode *J*) is a durable name from the moment incarnation 12 boots. (The mutant's recovery still issues `FSFsyncDir(tmp)`, but here it has nothing left to persist.)

Now c221's PUT of `k5`:

```
#1473   t=222.322148ms inc=12 fs-land     land FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-221)} -> FSWriteDone{io4 OK}
#1474   t=222.422691ms inc=12 deliver     FSWriteDone{io4 OK} => FSFsync{io7 /data/tmp/0000000000000004}
#1480   t=223.075460ms inc=12 fs-land     land FSFsync{io7 /data/tmp/0000000000000004} -> FSFsyncDone{io7 OK}
#1483   t=223.344205ms inc=12 deliver     FSFsyncDone{io7 OK} => FSRename{io11 /data/tmp/0000000000000004 -> /data/obj/k5}
#1489   t=223.664941ms inc=12 fs-land     land FSRename{io11 /data/tmp/0000000000000004 -> /data/obj/k5} -> FSRenameDone{io11 OK}
#1497   t=225.171211ms inc=12 deliver     FSFsyncDirDone{io14 OK} => Emit{Committed{c221 "k5" crc=e23c4709}} Reply{c221 200}
```

The write at #1473 went into the existing inode *J* (truncate + overwrite). Fsync, rename, fsync `obj/`, commit. Now:

```
 tmp/…04  {cur: 0, durable: J}     ◄── removal not durable (tmp/ never fsynced after rename)
 obj/k5   {cur: J, durable: J}     J: durable = obj(value-of-221)
```

At #1518, an injected crash. Among the three lost changes, the `tmp/…04` removal reverts:

```
#1518   t=227.534394ms inc=12 crash       CRASH (injected crash): kept=0 lost=3 torn=0, dropped 3 queued entries
```

```
 tmp/…04  {cur: J, durable: J}     ◄── resurrected: alias of obj/k5
 obj/k5   {cur: J, durable: J}
```

Incarnation 13 boots, lists `tmp/` (the same ten names, `…04` among them), ignores them, and starts serving. Client 231 PUTs `k25`. Its first write is IO 4:

```
#1530   t=232.481415ms inc=13 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000004 ...]}
#1534   t=234.445114ms inc=13 deliver     FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSRead{io3 /data/obj/k1} FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-231)} FSWrite{io5 /data/tmp/0000000000000005 obj(value-of-232)}
#1537   t=235.004392ms inc=13 fs-land     land FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-231)} -> FSWriteDone{io4 OK}
#1546   t=237.159940ms inc=13 fs-land     land FSRename{io10 /data/tmp/0000000000000004 -> /data/obj/k25} -> FSRenameDone{io10 OK}
```

The write at #1537 truncates and overwrites inode *J*. Inode *J* is `obj/k5`. The committed object for `k5`, acked to client 221 twelve milliseconds earlier, now holds `value-of-231`. Then the rename moves the `…04` name to `obj/k25`, so *J* is now named by both `obj/k5` and `obj/k25`. Twenty milliseconds later:

```
#1669   t=255.595778ms inc=13 deliver     Request{c248 GET "k5"} => FSRead{io52 /data/obj/k5}
#1678   t=256.741365ms inc=13 fs-land     land FSRead{io52 /data/obj/k5} -> FSReadDone{io52 OK obj(value-of-231)}
#1679   t=256.780566ms inc=13 deliver     FSReadDone{io52 OK obj(value-of-231)} => Reply{c248 200 value-of-231}
```

A GET of `k5` returns the value PUT to `k25`. The object passes its CRC (it is a perfectly valid object, just the wrong one), so the server cannot tell. The oracle can: request 231 was a PUT of `k25`, so no sequential history puts its value under `k5`. At the next quiescent point:

```
FAIL linearizable step 1750: no linearization of 1 ops from any of 1 possible start states {k5=221}; window:
  req 248 client 3 GET "k5" [471..475 @255595778..256780566] -> 200 value-of-231
```

The oracle's carried state says `k5` was certainly `value-of-221` at the start of the window, and the only op on `k5` in the window is a GET that returned `value-of-231`. No linearization exists. Chapter 13 explains how the oracle arrives at "1 possible start state", and Chapter 15 tells the rest of mutant f's story, including the ablation that shows how much writeback and swarm testing mattered.

Notice how many independent ingredients had to line up:

1. a leftover temp file with a name that a later incarnation would reuse (IO 4 is reused by every incarnation);
2. that name being durable at the moment it is renamed away (here because it survived an earlier crash; in other seeds via background writeback, as in seed 1 above);
3. a PUT reusing the name and committing, so the temp inode becomes an object;
4. a crash before anything made the temp name's removal durable, with the coin reverting it;
5. a second PUT, in a later incarnation, reusing the same IO number;
6. a later GET of the first key, before anything else overwrote it.

No human-written test would assemble that sequence. A simulator with a hostile disk and a few thousand crashes per second assembles it in 20 seeds.

## 11.14 How this compares with real filesystems

The StorageModel is deliberately *more* adversarial than common real filesystems in some respects, and less in others.

**More adversarial:**

- **Cross-directory renames are not atomic across a crash.** On ext4 and XFS, directory operations are journaled, and a rename is committed as one transaction: after a crash you see the old state or the new state, not both names or neither. The StorageModel resolves the two halves independently. A protocol that survives the StorageModel survives the journaled case, because the journaled outcomes are a subset.
- **No implicit ordering between data and metadata.** ext4's default `data=ordered` mode writes a file's data blocks before committing the metadata that references them, and ext4 has heuristics (the `auto_da_alloc` behaviour) that flush data early when a file is replaced via rename or truncate, to protect applications that skip `fsync`. The StorageModel promises none of this: a durable name can point at non-durable content (mutant b's zero-byte objects). This is the POSIX-guaranteed floor, and it is what other filesystems, other mount options, and other operating systems may give you.
- **Every un-synced change is a coin flip.** Real systems are usually "mostly kept" or "mostly lost" depending on how long ago the change was and when the last journal commit happened. A uniform coin explores the space more evenly.

**Less adversarial:**

- No interleaved or block-granular tearing, no corruption of durable data at rest, no misdirected or lost writes, no ENOSPC, no directory corruption (§11.7).
- No reordering of *successful* fsyncs: once `fsync` returns OK (and the inode is not poisoned), the data is durable forever. Real disks with volatile write caches and no flush support can violate this; that is a hardware lie the StorageModel does not model.

The ALICE study (Pillai et al., OSDI 2014) is the definitive treatment of how real filesystems differ in these "persistence properties" and how real applications (databases, version control systems, key-value stores) accidentally depend on the stronger ones. Its central lesson is the StorageModel's design principle: **program against the weakest guarantees you might get, and test against an adversary that gives you exactly those.**

## Key takeaways

- Every inode has `volatile` and `durable` content; every directory entry has `cur` and `durable` versions. Fsync of a file persists content; fsync of a directory persists *all* of that directory's entry changes; nothing else persists anything, except background writeback.
- Fault decisions are planned at issue time so RNG consumption follows issue order. How a failed op fails (applied or not, which prefix) is decided at land time. Missing paths always return ENOENT, never EIO.
- A write to an existing name overwrites the **existing inode**. Combined with crash-resurrected temp names, that turns a skipped cleanup into silent corruption of a committed object.
- A crash resolves directory entries first (each kept or reverted independently), then the contents of live inodes (poisoned → lost; else torn prefix, kept, or lost), then garbage-collects unnamed inodes.
- FsyncGate models Linux's post-error behaviour: a failed fsync can poison a dirty inode, after which fsyncs lie and the data never reaches disk.
- Read corruption exists to make the CRC check reachable; without it, the CRC is dead code in a correct server.
- The durable view is the worst-case crash outcome; `AckedIsDurable` and `ObjNeverTorn` check against it without crashing. `DurableGen` lets them skip steps where nothing durable changed.
- The StorageModel is more hostile than journaled filesystems on purpose. Surviving it implies surviving them.

## Exercises

1. **Run the StorageModel tests and read them.** `go test ./toys3/sim -run TestStorageModel -v`. For each test, write one sentence of the model it pins down.
2. **Make renames atomic.** Change `Crash` phase 1 so that, when a `tmp/` entry's pending removal and an `obj/` entry's pending creation involve the same inode, they are resolved with *one* coin. (You will need to pair them up; a first approximation is to resolve every pending dirent with the same coin per crash.) Predict: does mutant f become harder to catch? Run `go test ./toys3/sim -run 'TestCatchesBugs/f' -v` before and after. Revert.
3. **Turn off writeback.** Set `WritebackProb: 0` in `BaseConfig` (`toys3/sim/config.go:166`). Predict the first-catch seed for mutant f, then run `go test ./toys3/sim -run 'TestCatchesBugs/f' -v`. (When this book was written, swarm with writeback found it at seed 20; swarm without writeback at seed 24; Chapter 15 has the full ablation.) Revert.
4. **Remove the lie.** Set `FsyncGate: false` in `BaseConfig`. Predict which mutants are caught later or not at all. Run `TestCatchesBugs`. Then explain, using §11.6, why the real server's behaviour (trip on any fsync error) is still correct when the lie is off.
5. **A gentler tear.** Change the torn case in phase 2 to keep the *old* durable content plus a prefix of the new content's tail (a crude "interleaved" tear for overwrites). Does any test in the package fail? Does any mutant's first-catch seed change? Why is this change nearly invisible to a correct toys3 server? (Hint: does toys3 ever overwrite a durable file in place?)
6. **Count resurrections.** Add a counter to `Crash` for entries in `tmp/` whose pending removal was reverted while their inode is also named under `obj/` (an alias). Run 200 seeds of the correct server and print the total. Then run the same 200 seeds with mutant f's factory. Are aliases common? How often do they end in a failure?
7. **ENOSPC.** Sketch (on paper or in code) a new fault: `FSWrite` fails with a new `ErrNoSpace` code. What would need to change in `control.ErrCode`, the server's error table (Chapter 7) and the oracle? Is a write that fails with ENOSPC "applied or not"?

## Further reading

- Thanumalayan Sankaranarayana Pillai, Vijay Chidambaram, Ramnatthan Alagappan, Samer Al-Kiswany, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau, *"All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications"*, OSDI 2014. Introduces ALICE and the persistence-property framework this chapter borrows from.
- Dan Luu, *"Files are hard"* (2015). A tour of the research on filesystem crash consistency and error handling, and of how often applications get it wrong.
- The PostgreSQL "fsyncgate" discussion (pgsql-hackers, 2018, thread "Linux fsync() error reporting"), and Jonathan Corbet's LWN coverage, *"PostgreSQL's fsync() surprise"* (2018).
- Anthony Rebello et al., *"Can Applications Recover from fsync Failures?"*, USENIX ATC 2020. A systematic study of what happens after fsync errors across filesystems and applications.
- TigerBeetle's storage fault model (in the VOPR / simulator sources and docs), which goes further than toys3: misdirected writes, latent sector errors, and corruption of data at rest.
- The sled project's notes on simulation and crash testing (sled.rs).
