# Chapter 5: The controller, part 1: operating modes, recovery, and queuing

> *"Memory is disposable."* (`toys3/control/doc.go:107`)

We now open the controller, `control.Controller`, and read it. It is the S3 server's state machine: in this book "the server" and "the controller" name the same thing, the first from the outside (it serves S3 requests) and the second from the inside (it controls a disk; Chapter 3a). This chapter covers the controller's state, how `Step` dispatches an event, the operating modes of one incarnation (startup, recovering, auto, tripped), and the recovery protocol that runs on every boot. Chapter 6 covers the request protocols (PUT, GET, DELETE, COPY) and the key interlock, and Chapter 7 the error policy and the trip.

The single most important idea of this chapter is in the epigraph. The server keeps **nothing** across a crash. Every boot starts from an empty `Controller` struct, and everything the server needs to know about the world it rediscovers from disk. This is loop.md's rule 9 ("Put all durable state in the store... anything in memory is treated as disposable") applied to its logical extreme, and it is what makes crash testing tractable: there is no in-memory state to get out of sync with the disk, because there is no in-memory state that survives.

---

## 5.1 The shape of the code

Before the refactor, the server was one 532-line file, `core/server.go`. It is now three files in package `control`, split along the seams that were already there:

```
toys3/control/controller.go   356 lines
    lines   5- 11   Config
    lines  13- 72   private enums (mode, opKind, reqStep), the request record, interlocked
    lines  74-146   Controller struct, New, Step, newIO, trip
    lines 148-356   requests: onRequest, permitted, startPermitted,    ◄── Chapters 6 and 7
                    start, issue, finish, commit, failTrip, onRequestIO
toys3/control/recovery.go     102 lines
    lines   5- 15   recStep
    lines  17-102   onStarted, onRecoveryDone, fsyncTmpDir, indexIO    ◄── this chapter
toys3/control/hash.go          56 lines
    lines   5- 32   StateHash                                          ◄── Chapter 8
    lines  34- 56   the FNV-1a hasher
```

The split is a pure move: no statement changed, and the golden test of Chapter 8 (§8.12) proved it (the trace hash of every one of 550 seeds stayed bit-identical). It is worth noticing *why* these were the seams. Recovery has its own enum (`recStep`), its own fields (`rec`, `recIO`, `recRemoved`) and its own event handler (`onRecoveryDone`); it talks to the rest of the controller only through `mode`, `newIO`, `trip` and `startPermitted`. The hasher is a commodity algorithm that only `StateHash` uses, and `StateHash` exists for one customer, the simulator. Neither belongs in the file that holds the control law.

There are no maps anywhere in these files. The comment on the struct explains why (`toys3/control/controller.go:84-86`): "This is O(in-flight²) per step, which is fine for a toy and keeps the state small and obviously deterministic (no maps at all)." Iteration order over a Go map is randomized on purpose (Chapter 8), so a map in the state machine is a determinism hazard that has to be managed. A slice has an order you chose. For a server that rarely has more than a handful of requests in flight, the quadratic scans cost nothing and buy simplicity.

## 5.2 Config

```go
// toys3/control/controller.go:5-11
// Config is the controller's static configuration.
type Config struct {
	// Dir is the data root; obj/ and tmp/ live under it and must already exist.
	Dir string
	// MaxObjectSize bounds PUT payloads (400 above it). 0 means no limit.
	MaxObjectSize int
}
```

Two fields. `Dir` is the root under which `obj/` and `tmp/` live (Chapter 4). In simulation it is `/data` (`toys3/sim/config.go:173`); in production it is the absolute path given by `-dir` (`toys3/cmd/toys3/main.go:32-36`). `MaxObjectSize` is 64 bytes in simulation, which keeps values small and logs readable, and 64 MiB by default in production (`toys3/cmd/toys3/main.go:29`).

Config is *static*: it is passed to the factory on every boot and never changes. It is not hashed by `StateHash` (Chapter 8) because it cannot differ between two runs of one seed.

## 5.3 The private enums

Four small enums describe where the controller and each request are. They are `uint8` types, which keeps them cheap to hash and easy to read in a debugger.

### mode: the operating mode of the incarnation

```go
// toys3/control/controller.go:13-23
// mode is the controller's operating mode within one incarnation: startup,
// recovering, automatic (closed-loop serving), or tripped. The numeric values
// are folded into StateHash: append new modes, never reorder them.
type mode uint8

const (
	modeStartup    mode = iota // before Started; requests are queued
	modeRecovering             // listing and unlinking temp files; requests are queued
	modeAuto                   // normal operation
	modeTripped                // Trip was returned; every later event is ignored
)
```

Industrial controllers have *operating modes*. A pump controller in a plant is typically in one of Hand, Off or Auto: Auto means the controller is closing the loop by itself, reading its sensors and driving its actuators to track the setpoint. When a safety condition trips it, it goes to a Tripped mode and stays there until someone resets it. toys3's four modes map onto that vocabulary one for one:

| Mode | Meaning | Before the refactor |
|---|---|---|
| `modeStartup` | power is on, but the controller has not been told to start; requests queue | `phaseBooting` |
| `modeRecovering` | establishing a known initial condition (an empty `tmp/`); requests queue | `phaseRecovering` |
| `modeAuto` | closed-loop operation: requests are served | `phaseServing` |
| `modeTripped` | a `Trip` was returned; the controller ignores everything until the runtime replaces it | `phaseHalted` |

Why rename "phase" to "mode"? Two reasons. First, "mode" is the control-system word for exactly this concept, and it lines up with the other renames (Trip, interlock, permissive). Second, it frees the word "phase" for the *testbench's* timeline: a simulation run has a Chaos phase and then a Settling phase (Chapter 10), and before the refactor the same word meant both "where is the server in its lifecycle" and "where is the run in its schedule". The comment above the enum carries a warning that matters more than any name: *the numeric values are folded into `StateHash`*. Renaming a constant is free; reordering the `iota` would change every trace hash (Chapter 8).

A note on recovery as a mode. A control engineer might call it *homing*: at power-up a machine tool drives each axis to a reference switch to establish a known position before it accepts any work. Recovery is the same idea (establish a known initial condition: no leftover temp files) and the same rule (accept no work until it is done). The code keeps the storage word, "recovering", because that is what every storage engineer will search for.

### recStep: where recovery is

```go
// toys3/control/recovery.go:8-15
// recStep is where recovery is (only meaningful while modeRecovering).
type recStep uint8

const (
	recListing     recStep = iota // waiting for FSListDone(tmp)
	recUnlinking                  // waiting for the FSUnlinkDone of every leftover
	recFsyncingTmp                // waiting for FSFsyncDirDone(tmp)
)
```

### opKind and reqStep: what a request is and where it is

```go
// toys3/control/controller.go:25-48
// opKind is the kind of a client request. Hashed by StateHash: append only.
type opKind uint8

const (
	kindPut opKind = iota
	kindGet
	kindDelete
	kindCopy
)

// reqStep is the protocol step a request is on. Each step except
// stepWaiting has exactly one filesystem action outstanding (request.io), and
// names the completion it waits for. Hashed by StateHash: append only.
type reqStep uint8

const (
	stepWaiting    reqStep = iota // start permissive false: an earlier request holds one of its keys (or recovery runs)
	stepReading                   // GET: FSRead(obj/key); COPY: FSRead(obj/src)
	stepWritingTmp                // PUT/COPY: FSWrite(tmp)
	stepFsyncTmp                  // PUT/COPY: FSFsync(tmp)
	stepRenaming                  // PUT/COPY: FSRename(tmp → obj/key)
	stepFsyncObj                  // PUT/COPY/DELETE: FSFsyncDir(obj)
	stepUnlinking                 // DELETE: FSUnlink(obj/key)
)
```

The comment on `reqStep` states a structural invariant worth memorising: **every non-waiting request has exactly one filesystem action outstanding**, recorded in `request.io`. Requests are strictly sequential internally. Concurrency comes from having many requests, not from a request issuing several actions at once. This is what makes "which request does this completion belong to?" a simple scan.

## 5.4 The request record

```go
// toys3/control/controller.go:50-61
// request is one accepted client request and its progress.
type request struct {
	seq    uint64 // arrival order; defines every per-key FIFO queue
	client ClientReq
	kind   opKind
	key    string // the key read or written (Dst for COPY)
	src    string // COPY only
	data   []byte // PUT payload; for COPY, the src payload once read
	step   reqStep
	io     IOID   // the outstanding FS action; 0 while stepWaiting
	tmp    string // temp file path, once FSWrite was issued
}
```

Field by field:

- `seq` is the arrival order within this incarnation, assigned from `Controller.nextSeq` when the request is accepted. It defines every per-key FIFO queue (Chapter 6).
- `client` is the runtime's correlation ID for the reply (a `ClientReq`, `toys3/control/types.go:17-20`). The server echoes it on the `Reply` and never interprets it.
- `kind` is the operation.
- `key` is "the key this request writes or reads", which for COPY is the **destination**. This choice means the PUT path, which COPY reuses, can always use `r.key` as the rename target.
- `src` is used only by COPY.
- `data` is the PUT payload, and for a COPY it is filled in with the source payload once the read completes (`toys3/control/controller.go:296`).
- `step` and `io` are the protocol position and the one outstanding I/O.
- `tmp` is the temp file path, recorded when `FSWrite` is issued so that later steps (`FSFsync`, `FSRename`) can name it.

Two helpers describe which keys a request touches:

```go
// toys3/control/controller.go:63-72
// touches reports whether r reads or writes key.
func (r *request) touches(key string) bool {
	return r.key == key || (r.kind == kindCopy && r.src == key)
}

// interlocked reports whether a and b share a key, so they must not run at
// the same time (the key interlock).
func interlocked(a, b *request) bool {
	return a.touches(b.key) || (b.kind == kindCopy && a.touches(b.src))
}
```

Notice that `interlocked` does not distinguish reads from writes. Two GETs on the same key conflict and are serialized. A reader–writer lock would let them run concurrently, and it would be correct, but it would complicate both the code and the reasoning, and the toy does not need the throughput. You can see this in the seed-1 log: a GET of `k2` and a COPY reading `k2` never overlap.

## 5.5 The Controller struct

```go
// toys3/control/controller.go:87-101
type Controller struct {
	env  Env
	cfg  Config
	mode mode

	nextIO  IOID   // last IOID issued
	nextSeq uint64 // last arrival sequence issued

	// Recovery bookkeeping (modeRecovering only).
	rec        recStep
	recIO      []IOID // outstanding recovery IOs (one, or one per leftover temp)
	recRemoved int    // temps unlinked successfully

	reqs []*request // accepted, unfinished requests, ascending seq
}
```

- `env` is the injected source of randomness and buggify (`toys3/control/env.go`). **The server never uses it.** There is no call to `s.env` anywhere in `controller.go`; the test environment even says so in a comment ("the server does not draw randomness today", `toys3/control/controller_test.go:14`). The field exists so that buggify points can be added later without changing the factory signature (Chapter 17 suggests some).
- `cfg` is the static configuration.
- `mode` is the operating mode (§5.3).
- `nextIO` is the last IOID issued. IOIDs start at 1 in every incarnation (the zero value of `nextIO` is 0, and `newIO` increments before returning). Because the simulator drops every completion addressed to a dead incarnation (Chapter 10), reusing IOIDs across incarnations is safe: an `FSWriteDone{io7}` from incarnation 1 can never reach incarnation 2.
- `nextSeq` is the last arrival sequence issued.
- `rec`, `recIO`, `recRemoved` hold recovery's progress. `recIO` is a slice of the outstanding recovery IOs: one during listing and tmp-fsync, one per leftover temp during unlinking.
- `reqs` holds every accepted, unfinished request in ascending `seq` order. It is both the set of in-flight requests and, implicitly, every per-key queue.

That is the *entire* state of the server. Fourteen-ish words of scalars and two slices. Compare that to how much state a typical server carries in caches, connection pools and background goroutines, and you start to see why it is so easy to hash, replay and throw away.

## 5.6 New and the factory

```go
// toys3/control/controller.go:103-106
// New returns a server for one boot. It does nothing until Started.
func New(env Env, cfg Config) *Controller {
	return &Controller{env: env, cfg: cfg, mode: modeStartup}
}
```

`New` performs no I/O and issues no actions. A freshly built server is inert until it receives `Started`. The runtime wraps it in a `control.Factory` (`toys3/control/machine.go:22-28`):

```go
type Factory func(env Env, cfg Config) Machine

// NewMachine is the Factory for the real server.
func NewMachine(env Env, cfg Config) Machine { return New(env, cfg) }
```

Runtimes call the factory on first boot and after every crash. The simulator does so in `restart` (`toys3/sim/simulator.go:310-326`), which builds a new machine, marks the server alive, and schedules the `Started` delivery for the same instant. The production loop calls it once per process (`toys3/prod/loop.go:62`), because in production a "crash" ends the process and a supervisor starts a new one.

Taking a factory instead of calling `New` directly is what lets the simulator test deliberately broken servers: each mutant in `toys3/sim/mutants_test.go` is just another `Factory` that wraps `control.New` (Chapter 15).

## 5.7 Step: the dispatcher

Here is the one public method that matters:

```go
// toys3/control/controller.go:108-133
// Step consumes one event and returns the actions to execute.
func (s *Controller) Step(ev Event) []Action {
	if s.mode == modeTripped {
		return nil
	}
	switch b := ev.Body.(type) {
	case Started:
		return s.onStarted()
	case Request:
		return s.onRequest(b)
	}
	fb, ok := ev.Body.(Feedback)
	if !ok {
		return nil
	}
	io := fb.ID()
	if s.mode == modeRecovering && indexIO(s.recIO, io) >= 0 {
		return s.onRecoveryDone(io, ev.Body)
	}
	for _, r := range s.reqs {
		if r.step != stepWaiting && r.io == io {
			return s.onRequestIO(r, ev.Body)
		}
	}
	return nil // unknown or stale IOID: ignore
}
```

Read it as a decision tree:

```
Step(ev)
 ├─ tripped?                         → nil            (a tripped incarnation is deaf)
 ├─ Started                         → onStarted
 ├─ Request                         → onRequest      (validate, enqueue, maybe start)
 ├─ not Feedback (an FS completion) → nil            (defensive; nothing else exists)
 ├─ recovering and io ∈ recIO       → onRecoveryDone
 ├─ io matches a running request    → onRequestIO
 └─ otherwise                       → nil            (unknown or stale IOID)
```

Several properties are worth calling out.

**It never blocks and never does I/O.** It looks at memory, changes memory, and returns a slice of plain values. In production this takes microseconds.

**`ev.At` is ignored.** The server does not use time at all. Every wait in toys3 is a wait for an FS completion, so there are no timers and no timeouts (`DESIGN.md:86-87`: "No timers are needed: every wait is on an FS completion"). The `At` field is still stamped on every event by the runtime, so a future version that needs deadlines can read it without any change to the interfaces.

**Unknown completions are ignored, not panicked on.** A completion whose IOID matches no outstanding operation, or matches one but is of the wrong kind, does nothing. This is rule 8 of loop.md ("Give every request a correlation ID so late or duplicate completions can be recognized and ignored"). In the current simulator, stale completions cannot actually arrive (they are dropped with their incarnation), but a server that tolerates them is robust against a runtime that does deliver them, and `TestStaleIOIgnored` pins that behaviour down (§5.12).

**The tripped check comes first.** Once a `Trip` has been returned, the incarnation is untrusted (Chapter 7). It ignores everything, including a `Request`, which therefore never gets a `Reply` from this incarnation. That is fine because the runtime kills the incarnation right after executing the `Trip`: the simulator crashes it on the same step (`toys3/sim/simulator.go:200-207`), and the production loop returns `ErrTripped` from `Run`, after which `main` exits (`toys3/cmd/toys3/main.go:48-56`).

### Feedback.ID() and newIO

`Step` does not need a helper to find the IOID of a completion: it asks the completion. Every filesystem completion implements the `Feedback` interface of the Storage port (Chapter 3, §3.5), whose one method is `ID() IOID`. The single type assertion `ev.Body.(Feedback)` both answers "is this a completion at all?" and, when it is, yields the IOID.

Before the refactor this was a helper, `ioOf`, a seven-case type switch over the completion types. The switch and the interface are equivalent here, and the refactor's golden test (Chapter 8) confirms that replacing one with the other changed no run, but the interface has two advantages. It is *declared* next to the types, so adding an eighth completion type without an `ID()` method fails to compile at the assertion `var _ Feedback = FSWriteDone{}` pattern in `storage.go` rather than silently being ignored by a switch somewhere else. And the same concept, with the same name, is used by every consumer: the controller, the simulator and both test harnesses.

```go
// toys3/control/controller.go:135-138
func (s *Controller) newIO() IOID {
	s.nextIO++
	return s.nextIO
}
```

`newIO` is the only place IOIDs are allocated. It increments first, so the first IOID of every incarnation is 1, and 0 can mean "none" (as in `request.io` while waiting).

### trip

```go
// toys3/control/controller.go:140-146
// trip fail-stops: nothing after it in this incarnation is trusted. The
// runtime executes the actions before it (here: the 500, if any) and then
// kills the incarnation.
func (s *Controller) trip(cause string, before ...Action) []Action {
	s.mode = modeTripped
	return append(before, Trip{Cause: cause})
}
```

`trip` sets the mode to `modeTripped` and returns the given actions followed by `Trip`. `Trip` is therefore always the last action of its step, a property the simulator enforces as a protocol check (`toys3/sim/simulator.go:201-204`: "Trip is not the last action of its step"). Recovery calls `trip` with no `before` actions; request errors call it with a 500 `Reply` (Chapter 7).

## 5.8 The modes of one incarnation

Putting the modes together:

```
                 New()
                   │
                   ▼
          ┌─────────────────┐   Request → queued (no actions)
          │  modeStartup    │◄──────────┐
          └────────┬────────┘───────────┘
                   │ Started → FSList(tmp)
                   ▼
          ┌─────────────────┐   Request → queued (no actions)
          │ modeRecovering  │◄──────────┐
          │  recListing     │───────────┘
          │  recUnlinking   │
          │  recFsyncingTmp │
          └──┬──────────┬───┘
   recovery  │          │ FSFsyncDirDone(tmp) OK
   error     │          ▼  → Emit{Recovered}, start queued requests
             │ ┌─────────────────┐   Request → validate; run if at head of its queues
             │ │  modeAuto       │◄──────────┐
             │ └────────┬────────┘───────────┘
             │          │ error after obj/ may have changed
             ▼          ▼  → [Reply 500], Trip
          ┌─────────────────┐
          │  modeTripped    │   every event → nil
          └─────────────────┘
                   │
                   ▼   runtime kills the incarnation; a new one starts at New()
```

A second `Started` within one incarnation is ignored (`onStarted` checks `s.mode != modeStartup`). There is no transition out of `modeTripped` and no transition back to recovering: recovery runs exactly once per incarnation, and a failed recovery is retried by tripping and starting a *new* incarnation from scratch.

## 5.9 Why requests are queued during startup

`modeStartup`'s comment says "before Started; requests are queued". You might think that can never happen: surely the runtime delivers `Started` first? In production it does: `Loop.Run` steps `Started` before it reads anything from the inbox (`toys3/prod/loop.go:62-66`). But in simulation it is a real possibility, and it is a nice example of how the simulator explores orders you would not think to test.

When the simulator restarts the server, it schedules the `Started` delivery at the current instant and wakes any parked clients (`toys3/sim/simulator.go:322-325`):

```go
	s.schedule(Entry{At: s.now, Kind: KindDeliver, Inc: s.inc, Event: control.Started{}})
	if !s.quiescing {
		s.wakeParked()
	}
```

`wakeParked` schedules each client's wake-up at `now + Think()`, and think time is drawn from `[0, 1ms]` (`toys3/sim/config.go:159`), so it can be exactly zero. A client wake at the same instant as the `Started` delivery is ordered against it by the seeded random `Tie` (Chapter 10). If the wake goes first, the client issues its request, and the request's delivery is scheduled at that same instant, again with a random tie. So a `Request` can reach a server that has not yet seen `Started`. The server queues it, exactly as it would during recovery, and starts it once recovery is done.

This is the sort of corner that an implementer, reasoning about "the" order of events at boot, would never write a test for. The simulator simply produces it, sometimes, and the server has to be correct anyway.

## 5.10 Recovery, line by line

Recovery has one goal: **make `tmp/` empty, durably, before serving.** After a crash, `tmp/` can contain:

- temp files of PUTs and COPYs that were in progress (written, perhaps fsynced, not yet renamed),
- temp files whose `FSWrite` failed partway (mutations that replied 500, `toys3/control/controller.go:304-307`),
- resurrected temp names that are hard-link aliases of committed objects (Chapter 4, §4.4).

None of these mean anything to a new incarnation, and the last kind is actively dangerous if a temp name is ever reused (Chapter 15, mutant f). `obj/` needs no recovery at all: "obj/ only ever holds complete files" (`toys3/control/doc.go:112-114`).

The recovery protocol is three steps (`toys3/control/doc.go:105-114`):

```
Started ──► FSList(tmp) ──► FSUnlink(each leftover) ──► FSFsyncDir(tmp) ──► Emit{Recovered}, serve
```

### onStarted

```go
// toys3/control/recovery.go:17-26
func (s *Controller) onStarted() []Action {
	if s.mode != modeStartup {
		return nil // a second Started in one incarnation: ignore
	}
	s.mode = modeRecovering
	s.rec = recListing
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSList{IO: io, Dir: format.TmpDir(s.cfg.Dir)}}
}
```

Move to recovering, allocate IOID 1, record it as the one outstanding recovery IO, and ask for a listing of `tmp/`. This is why every incarnation's log begins with `FSList{io1 /data/tmp}`.

### onRecoveryDone: listing

```go
// toys3/control/recovery.go:28-51
// onRecoveryDone advances recovery. Any error trips: recovery must finish
// before serving, and restarting retries it from scratch.
func (s *Controller) onRecoveryDone(io IOID, body EventBody) []Action {
	switch s.rec {
	case recListing:
		d, ok := body.(FSListDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.trip("recovery: list tmp: " + d.Err.String())
		}
		s.recIO = nil
		var acts []Action
		for _, name := range d.Names {
			uio := s.newIO()
			s.recIO = append(s.recIO, uio)
			acts = append(acts, FSUnlink{IO: uio, Path: format.TmpDir(s.cfg.Dir) + "/" + name})
		}
		if len(acts) == 0 {
			return s.fsyncTmpDir()
		}
		s.rec = recUnlinking
		return acts
```

- The type assertion `body.(FSListDone)` guards against a completion of the wrong kind that happens to carry the right IOID. It is ignored like a stale one.
- An error trips. There is nothing sensible to do if you cannot even list `tmp/`; the next boot will try again.
- On success, issue **one `FSUnlink` per leftover, all at once**. They run concurrently; the server waits for all of them. `recIO` becomes the list of their IOIDs.
- `FSListDone.Names` are base names, sorted ascending (`toys3/control/storage.go:144`). Both runtimes sort: the StorageModel lists from its sorted name cache (`toys3/sim/storagemodel.go:424-439`) and `os.ReadDir` returns entries sorted by name (`toys3/prod/executor.go:61`). Sorted output is not cosmetic: it makes the order of the unlink actions, and therefore of their IOIDs, a deterministic function of the directory contents.
- If there were no leftovers, skip straight to the tmp fsync.

### onRecoveryDone: unlinking

```go
// toys3/control/recovery.go:53-70
	case recUnlinking:
		d, ok := body.(FSUnlinkDone)
		if !ok {
			return nil
		}
		switch d.Err {
		case OK:
			s.recRemoved++
		case ErrNotFound: // already gone: fine
		default:
			return s.trip("recovery: unlink temp: " + d.Err.String())
		}
		i := indexIO(s.recIO, io)
		s.recIO = append(s.recIO[:i], s.recIO[i+1:]...)
		if len(s.recIO) > 0 {
			return nil
		}
		return s.fsyncTmpDir()
```

- `ErrNotFound` is tolerated: the goal is "the name is gone", and it is. (In the current simulator nothing else deletes temp files during recovery, so this case is rare, but it is the right semantics and costs nothing.)
- `ErrIO` trips. A failed unlink may or may not have taken effect, and recovery must not declare `tmp/` clean on a maybe.
- Each completion removes its IOID from `recIO`. The `indexIO` lookup cannot return -1 here, because `Step` only routes to `onRecoveryDone` when the IOID is in `recIO`.
- When the last unlink completes, move on to the fsync. Completions may arrive in any order; the code does not care.

### onRecoveryDone: fsyncing tmp/

```go
// toys3/control/recovery.go:72-93
	case recFsyncingTmp:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.trip("recovery: fsync tmp dir: " + d.Err.String())
		}
		s.mode = modeAuto
		s.recIO = nil
		acts := []Action{Emit{Telemetry: Recovered{TempsRemoved: s.recRemoved}}}
		return append(acts, s.startPermitted()...)
	}
	return nil
}

func (s *Controller) fsyncTmpDir() []Action {
	s.rec = recFsyncingTmp
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSFsyncDir{IO: io, Dir: format.TmpDir(s.cfg.Dir)}}
}
```

The tmp fsync makes the unlinks durable. Its success is the moment the incarnation starts serving: the mode flips to `modeAuto`, the `Recovered` telemetry is emitted (with the number of temps actually removed), and `startPermitted` starts every request that queued up during booting and recovery, in arrival order (Chapter 6 explains `startPermitted` and `permitted`).

The telemetry is emitted *before* the first request actions in the same slice. The simulator's liveness invariant uses it: at the end of a run, the current incarnation must have emitted `Recovered` (`toys3/sim/invariant.go:220-228`).

Note that the fsync is issued **even when nothing was removed** (`toys3/control/doc.go:108`: "fsyncs the tmp directory (even if nothing was removed)"). §5.14 looks at whether this step is actually necessary.

## 5.11 Recovery in a real run

Here is recovery happening in seed 1 of the default configuration. Incarnation 1 has just tripped because a temp-file fsync failed (an EIO, Chapter 7):

```
#233 t=53.112270ms inc=1 deliver FSFsyncDone{io78 EIO} => Reply{c30 500} Trip{fsync temp: EIO}
#233 t=53.112270ms inc=1 crash   CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
#235 t=55.879722ms inc=2 restart boot
#236 t=55.879722ms inc=2 deliver Started => FSList{io1 /data/tmp}
#238 t=56.016744ms inc=2 deliver Request{c31 PUT "k0" value-of-31}
#240 t=56.034093ms inc=2 deliver Request{c32 COPY "k1" -> "k0"}
#242 t=56.246986ms inc=2 deliver Request{c33 GET "k2"}
#244 t=56.308317ms inc=2 deliver Request{c34 PUT "k1" value-of-34}
#245 t=56.901846ms inc=2 fs-land land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]}
#246 t=57.400810ms inc=2 deliver FSListDone{io1 OK [0000000000000031 000000000000004b 000000000000004c]} => FSUnlink{io2 /data/tmp/0000000000000031} FSUnlink{io3 /data/tmp/000000000000004b} FSUnlink{io4 /data/tmp/000000000000004c}
#247 t=57.617612ms inc=2 fs-land land FSUnlink{io3 /data/tmp/000000000000004b} -> FSUnlinkDone{io3 OK}
#248 t=57.816081ms inc=2 deliver FSUnlinkDone{io3 OK}
#249 t=58.485027ms inc=2 fs-land land FSUnlink{io4 /data/tmp/000000000000004c} -> FSUnlinkDone{io4 OK}
#250 t=58.656857ms inc=2 deliver FSUnlinkDone{io4 OK}
#251 t=58.892693ms inc=2 fs-land land FSUnlink{io2 /data/tmp/0000000000000031} -> FSUnlinkDone{io2 EIO}
#252 t=59.063797ms inc=2 deliver FSUnlinkDone{io2 EIO} => Trip{recovery: unlink temp: EIO}
#252 t=59.063797ms inc=2 crash   CRASH (trip: recovery: unlink temp: EIO): kept=1 lost=1 torn=0, dropped 0 queued entries
```

Walk through it:

- **#236**: the new incarnation's first step. `Started` produces `FSList{io1}`. IOIDs have restarted at 1.
- **#238–#244**: four client requests arrive while the listing is in flight. Each produces no actions: they are queued in `reqs` with `stepWaiting`.
- **#245–#246**: the listing returns three names. Two (`...4b`, `...4c`) are the temp files of the PUTs that were in flight when incarnation 1 tripped: `io75` and `io76` (0x4b = 75, 0x4c = 76). The third, `...31`, is the resurrected temp name from Chapter 4: IOID 49 of incarnation 1, renamed into `obj/k1` at step 160, whose removal from `tmp/` was never made durable. It is now a hard-link alias of `obj/k1`. Recovery does not know or care; it unlinks all three at once.
- **#247–#250**: two unlinks succeed, and produce no actions because another unlink is still outstanding.
- **#251–#252**: the third unlink fails with EIO. Recovery trips. No `Recovered` telemetry, no requests served. The four queued requests will be marked abandoned by the simulator (they were invoked but never replied to).

Then incarnation 3 tries again:

```
#253 t=60.075667ms inc=3 restart boot
#254 t=60.075667ms inc=3 deliver Started => FSList{io1 /data/tmp}
#256 t=60.298237ms inc=3 deliver Request{c35 PUT "k0" value-of-35}
#258 t=60.371480ms inc=3 deliver Request{c36 GET "k1"}
#260 t=60.531762ms inc=3 deliver Request{c37 GET "k2"}
#261 t=60.885632ms inc=3 fs-land land FSList{io1 /data/tmp} -> FSListDone{io1 OK [0000000000000031 000000000000004c]}
#262 t=60.991441ms inc=3 deliver FSListDone{io1 OK [0000000000000031 000000000000004c]} => FSUnlink{io2 /data/tmp/0000000000000031} FSUnlink{io3 /data/tmp/000000000000004c}
#264 t=60.999885ms inc=3 deliver Request{c38 GET "k2"}
#265 t=61.112270ms inc=0 quiesce quiesce
#266 t=61.138867ms inc=3 fs-land land FSUnlink{io3 /data/tmp/000000000000004c} -> FSUnlinkDone{io3 OK}
#267 t=61.388093ms inc=3 deliver FSUnlinkDone{io3 OK}
#268 t=61.904926ms inc=3 fs-land land FSUnlink{io2 /data/tmp/0000000000000031} -> FSUnlinkDone{io2 OK}
#269 t=62.293568ms inc=3 deliver FSUnlinkDone{io2 OK} => FSFsyncDir{io4 /data/tmp}
#270 t=63.465781ms inc=3 fs-land land FSFsyncDir{io4 /data/tmp} -> FSFsyncDirDone{io4 OK}
#271 t=63.590127ms inc=3 deliver FSFsyncDirDone{io4 OK} => Emit{Recovered{temps=2}} FSWrite{io5 /data/tmp/0000000000000005 obj(value-of-35)} FSRead{io6 /data/obj/k1} FSRead{io7 /data/obj/k2}
#272 t=63.801526ms inc=3 fs-land land FSRead{io6 /data/obj/k1} -> FSReadDone{io6 OK obj(value-of-23)}
#273 t=63.912692ms inc=3 deliver FSReadDone{io6 OK obj(value-of-23)} => Reply{c36 200 value-of-23}
```

Things to notice:

- The listing now has **two** names, not three. `...4b` was unlinked successfully by incarnation 2, and that unlink survived the crash (either a writeback made it durable, or the crash-time coin kept it: the crash report says `kept=1 lost=1`). `...4c` was also unlinked by incarnation 2, but that change was lost at the crash, so the name is back. Recovery is **idempotent**: it does not care how much of the previous attempt survived; it lists whatever is there and removes it.
- `...31` is still there, because incarnation 2's unlink of it failed (the StorageModel chose "EIO, not applied", or "applied" and then the crash reverted it).
- **#271**: the fsync of `tmp/` succeeds, and in one step the server emits `Recovered{temps=2}` and starts three queued requests: c35's PUT (an `FSWrite` to `tmp/0000000000000005`, a name that is safe to reuse now that `tmp/` is durably empty), c36's GET of `k1`, and c37's GET of `k2`. c38's GET of `k2` stays queued behind c37's (same key).
- **#272–#273**: c36 reads `obj/k1` and gets `value-of-23`. That is the object c23 committed at step 167 of incarnation 1, through the very temp name `...31` that recovery just unlinked. Unlinking the alias did not touch the object, exactly as Chapter 4 predicted.

## 5.12 The unit tests for recovery

`toys3/control/controller_test.go` drives the server by hand through a `harness` (`toys3/control/controller_test.go:21-145`): an in-memory map of files, a list of pending FS actions, and helpers to complete them one at a time, in order. Every FS change is immediately "durable" in this harness; there are no crash semantics. That is fine for testing the server's *logic*: what actions it emits, in what order, in response to which completions. Crash behaviour is the simulator's job.

The harness's `describe` function renders actions compactly so that tests can assert exact sequences as strings, replacing temp names with `tmp/*` because their hex digits are noise.

### TestRecovery

```go
// toys3/control/controller_test.go:311-330
func TestRecovery(t *testing.T) {
	h := newHarness(t)
	h.files[root+"/tmp/0000000000000007"] = []byte("junk")
	h.files[root+"/tmp/0000000000000009"] = []byte("junk")
	h.files[format.ObjPath(root, "k")] = format.EncodeObject([]byte("kept"))

	expect(t, "Started", h.step(Started{}), "list /d/tmp")
	expect(t, "request during recovery", h.step(Request{Client: 1, Op: Get{Key: "k"}}), "")
	expect(t, "second request", h.step(Request{Client: 2, Op: Put{Key: "j", Data: []byte("x")}}), "")
	expect(t, "list done", h.completeNext(), "unlink tmp/*; unlink tmp/*")
	expect(t, "unlink 1", h.completeNext(), "")
	expect(t, "unlink 2", h.completeNext(), "fsyncdir /d/tmp")
	expect(t, "fsync tmp", h.completeNext(), "recovered 2; read /d/obj/k; write tmp/*")
	h.drain()
	...
}
```

This test is a compressed version of the trace above, and it pins down five behaviours:

1. `Started` produces exactly one action, a listing of `tmp/`.
2. Requests during recovery produce **nothing**: no reply, no FS action. They are queued, not rejected.
3. The listing produces one unlink per leftover, all in the same step.
4. The first unlink completion produces nothing; the last one produces the tmp fsync.
5. The fsync completion produces, in order: the `Recovered` telemetry with the right count, then the queued requests started in arrival order (the GET's read, then the PUT's write).

The final loop checks that the leftover files are actually gone from the harness's map.

### TestRecoveryEmptyAndErrors

```go
// toys3/control/controller_test.go:332-347
func TestRecoveryEmptyAndErrors(t *testing.T) {
	h := newHarness(t)
	h.step(Started{})
	expect(t, "empty list", h.completeNext(), "fsyncdir /d/tmp")
	expect(t, "done", h.completeNext(), "recovered 0")

	h = newHarness(t)
	h.fail = func(a Action) ErrCode {
		if _, ok := a.(FSList); ok {
			return ErrIO
		}
		return OK
	}
	h.step(Started{})
	expect(t, "list error", h.completeNext(), "trip")
}
```

Two edge cases. An empty `tmp/` still gets its fsync and produces `Recovered{0}`. A failed listing trips with no reply (there is no request to reply to).

### TestStaleIOIgnored

This test is about the dispatcher, and it is the most instructive of the three:

```go
// toys3/control/controller_test.go:410-432
func TestStaleIOIgnored(t *testing.T) {
	h := newHarness(t)
	h.boot()
	h.step(Request{Client: 1, Op: Put{Key: "k", Data: []byte("v")}})
	before := h.s.StateHash()
	writeIO, _ := ioOfAction(h.pending[0])
	for _, b := range []EventBody{
		FSWriteDone{IO: 9999},                // unknown IOID
		FSFsyncDone{IO: writeIO},             // right IOID, wrong completion kind
		FSListDone{IO: 1},                    // recovery's IOID, long finished
		FSFsyncDirDone{IO: 2, Err: ErrIO},    // recovery's fsync, long finished
		FSReadDone{IO: writeIO + 1, Err: OK}, // not yet issued
	} {
		if acts := h.step(b); len(acts) != 0 {
			t.Fatalf("%#v: got actions %s", b, describe(acts))
		}
	}
	if h.s.StateHash() != before {
		t.Fatal("stale completion changed state")
	}
	expect(t, "real completion", h.drain(), "fsync tmp/*; rename tmp/* /d/obj/k; fsyncdir /d/obj; committed c1 k del=false; reply c1 200")
	expect(t, "second Started ignored", h.step(Started{}), "")
}
```

Five bogus completions are delivered while a PUT is waiting for its write:

| Event | Why it must be ignored | Which line ignores it |
|---|---|---|
| `FSWriteDone{9999}` | no such IOID | falls through the request loop, `controller.go:132` |
| `FSFsyncDone{writeIO}` | right IOID, wrong kind | `onRequestIO`'s type assertion, `controller.go:300-303` |
| `FSListDone{1}` | recovery's IOID, but recovery is over | `s.mode == modeRecovering` is false, then no request has io 1 |
| `FSFsyncDirDone{2, EIO}` | same, and an *error* that would trip if accepted | same |
| `FSReadDone{writeIO+1}` | an IOID that has not been issued yet | no request has it |

The fourth case is the dangerous one. If the dispatcher had not checked the mode, a late error for recovery's tmp fsync could trip a healthy, serving server. The test also checks that the **state hash** is unchanged (Chapter 8): "ignored" must mean "no effect at all", not just "no actions". Then the real completions drive the PUT to its 200, and a second `Started` is ignored.

## 5.13 What the server does not do at boot

It is as instructive to list what recovery omits:

- **It does not scan `obj/`.** There is no index to rebuild, no checksum sweep, no repair. Objects are self-contained files, and the protocol guarantees they are complete. A corrupt object (bit rot) is discovered lazily, on read, and reported as a 500 with a `CorruptionDetected` telemetry (Chapter 6).
- **It does not try to finish interrupted PUTs.** A PUT that was interrupted before its `FSFsyncDir(obj/)` completed was never acked (it was abandoned by the crash, or it replied 500). Its client has no expectation. Whether its rename landed is up to the crash; the oracle accepts either outcome (Chapter 13).
- **It does not remember anything about the previous incarnation.** It does not know how many incarnations there were, what IOIDs they used, or what they were doing. This is what "memory is disposable" means in practice.

This minimalism is a design choice with teeth: every piece of recovery logic that does not exist is a piece that cannot be wrong.

## 5.14 An open question: is the tmp fsync necessary?

The recovery protocol fsyncs `tmp/` after unlinking the leftovers. The comment says it does so "even if nothing was removed". Is that step load-bearing?

While writing this book, a mutant was built (outside the repository, in a scratch program that reuses the `skipper` wrapper from `mutants_test.go`) that removes exactly this step: it intercepts `FSFsyncDir(tmp)` and feeds the inner server an immediate OK instead of issuing it.

```go
func mutSkipTmpFsyncDir(env control.Env, cfg control.Config) control.Machine {
	return &skipper{inner: control.New(env, cfg), skip: func(a control.Action) bool {
		d, ok := a.(control.FSFsyncDir)
		return ok && d.Dir == format.TmpDir(cfg.Dir)
	}}
}
```

Run against seeds 1 to 2,000 with all default invariants: **not caught once.**

Here is an argument for why, under the current model, that is the right answer rather than a weakness of the invariants:

1. Without the fsync, recovery's unlinks may be volatile: a temp name `t` has `cur = ∅` but `durable = I` (some old inode).
2. The new incarnation may then write a temp file of the same name `t` (IOIDs restart at 1). In the StorageModel, `write` to a name whose `cur` is empty **creates a fresh inode** (`toys3/sim/storagemodel.go:309-315`): it does not resurrect `I`. So the write cannot overwrite whatever `I` is (possibly a committed object's inode).
3. If the server then crashes, the un-synced unlink may be reverted and `t` may again name `I`. But that is just another leftover, and the *next* recovery removes it before any write.

So the only dangerous thing, writing through a name that aliases a committed object, cannot happen within the incarnation, durable unlink or not. The fsync protects against a hazard the protocol already rules out by other means.

But "not caught" is not "not needed", and there are reasons to keep it:

- On a real filesystem, the `O_CREAT|O_TRUNC` open that `OSExecutor` uses (`toys3/prod/executor.go:77`) on a name that is *visibly* absent also creates a fresh inode, so the argument carries over to POSIX. But it relies on the visible state and the durable state agreeing about *which* names exist when the write happens. The fsync makes that agreement trivial instead of argued.
- It makes `Recovered` mean something simple and strong: "`tmp/` is durably empty". Simple postconditions are easier to build on (for example, a future version that keeps something meaningful in `tmp/`).
- It costs one fsync per boot.

Treat this as the first of several places in the book where DST is used not just to find bugs, but to **find out which parts of a design are actually doing work**. Chapter 15 returns to the idea; the exercises below ask you to try to break the argument.

## Key takeaways

- The server's entire state is a handful of scalars and two slices (`reqs`, `recIO`). No maps, no time, no randomness. It is rebuilt from nothing on every boot.
- `Step` is a dispatcher: tripped → ignore; `Started` → recovery; `Request` → validate and queue; FS completion → route by IOID to recovery or to the one request waiting on it; anything else → ignore. Wrong-kind and stale completions are ignored without changing state.
- An incarnation goes through the operating modes startup → recovering → auto → (tripped). Requests that arrive before auto mode are queued, not rejected. The startup case is reachable in simulation because `Started` and a client's request can share an instant.
- Recovery is: list `tmp/`, unlink everything in it concurrently (ENOENT is fine), fsync `tmp/`, emit `Recovered`, start queued requests in arrival order. Any error trips, and the next incarnation retries from scratch. Recovery is idempotent.
- Recovery deliberately does nothing to `obj/`, because the write protocol guarantees `obj/` only ever holds complete files.
- Under the current model, the recovery fsync of `tmp/` is not caught as necessary by 2,000 seeds; there is an argument why, and reasons to keep it anyway.

## Exercises

1. **Trace a boot by hand.** Using only `controller.go`, write the exact action list the server returns for: `Started`; `Request{c1 GET k}`; `FSListDone{io1, [a b]}`; `FSUnlinkDone{io3}`; `FSUnlinkDone{io2, ENOENT}`; `FSFsyncDirDone{io4}`. Then encode it as a test in the style of `TestRecovery` (in a scratch file you delete afterwards) and run `go test ./toys3/control -run YourTest -v`.
2. **Make the tripped server talk.** Change `Step` so that a tripped server replies 503 to new requests instead of ignoring them. Predict: does any controller unit test fail? Does any simulation seed fail (`go test ./toys3/sim -run TestSeeds -seeds=50`)? Which simulator check would object, and why? (Hint: read `toys3/sim/simulator.go:200-207` and ask whether a tripped incarnation ever receives another event.) Revert.
3. **Recovery without the type assertion.** Remove the `d, ok := body.(FSUnlinkDone); if !ok { return nil }` guard in the `recUnlinking` case and replace it with an unchecked assertion. Which existing test, if any, catches this? Write one that does. (Hint: what can a recovery IOID collide with?)
4. **Try to break §5.14's argument.** Build the "skip tmp fsync" mutant yourself (copy `skipper` and its helpers out of `toys3/sim/mutants_test.go` into a small program in a scratch module that `replace`s `manifester` with this repository, or add it temporarily to `mutants_test.go` and to the `mutants` list). Run 500 seeds. Then look for a change to the *StorageModel* (not the server) that is still faithful to POSIX and under which the mutant *is* caught. If you find one, decide whether the StorageModel should adopt it.
5. **Make recovery partial.** Change the `recUnlinking` case so that an `ErrIO` is treated like `ErrNotFound` (skip it, keep going). Predict which mutant from Chapter 15 this most resembles and which invariant will catch it, then run 500 seeds (`go test ./toys3/sim -run 'TestSeeds$' -seeds=500`). Did your prediction hold? If it was not caught, explain why a single leftover is less dangerous than skipping cleanup entirely.
6. **Find a boot-time race.** Write a small program (or a temporary test in `toys3/sim`) that runs seeds with `Config.Log = true` and searches `Result.Log` for a `Request` delivered to an incarnation before that incarnation's `Started`. How many of the first 200 seeds contain one? (This tells you how often the `modeStartup` queuing path is exercised.)

## Further reading

- George Candea and Armando Fox, *"Crash-Only Software"*, HotOS 2003. The argument that the only way to stop a component should be to crash it, and the only way to start it should be to recover. toys3's "memory is disposable, recovery on every boot" is crash-only design.
- `loop.md`, rules 8 and 9 (correlation IDs; all durable state in the store).
- Pierre Zemb, *The Moonpool Sim Book*, on process lifecycles and incarnations; summarized in `docs/lessons.md`, lessons 4 and 5.
- Pillai et al., *"All File Systems Are Not Created Equal"*, OSDI 2014, for the recovery assumptions real applications make about what survives a crash.
