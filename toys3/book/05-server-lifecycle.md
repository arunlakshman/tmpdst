# Chapter 5: The server, part 1: lifecycle, recovery, and queuing

> *"Memory is disposable."* (`toys3/core/doc.go:102`)

We now open `toys3/core/server.go` and read it. This chapter covers the top half of the file: the server's state, how `Step` dispatches an event, the lifecycle of one incarnation (booting, recovering, serving, halted), and the recovery protocol that runs on every boot. Chapter 6 covers the request protocols (PUT, GET, DELETE, COPY) and the per-key queues, and Chapter 7 the error policy.

The single most important idea of this chapter is in the epigraph. The server keeps **nothing** across a crash. Every boot starts from an empty `Server` struct, and everything the server needs to know about the world it rediscovers from disk. This is loop.md's rule 9 ("Put all durable state in the store... anything in memory is treated as disposable") applied to its logical extreme, and it is what makes crash testing tractable: there is no in-memory state to get out of sync with the disk, because there is no in-memory state that survives.

---

## 5.1 The shape of the file

`server.go` is 532 lines, organised in five sections:

```
lines   1-  9   Config
lines  11- 76   private enums and the request record
lines  78-166   Server struct, New, Step, dispatch helpers, halt
lines 168-255   recovery                        ◄── this chapter
lines 257-477   requests                        ◄── Chapters 6 and 7
lines 479-532   hashing                         ◄── Chapter 8
```

There are no maps anywhere in it. The comment on the struct explains why (`toys3/core/server.go:84-86`): "This is O(in-flight²) per step, which is fine for a toy and keeps the state small and obviously deterministic (no maps at all)." Iteration order over a Go map is randomized on purpose (Chapter 8), so a map in the state machine is a determinism hazard that has to be managed. A slice has an order you chose. For a server that rarely has more than a handful of requests in flight, the quadratic scans cost nothing and buy simplicity.

## 5.2 Config

```go
// toys3/core/server.go:3-9
// Config is the server's static configuration.
type Config struct {
	// Dir is the data root; obj/ and tmp/ live under it and must already exist.
	Dir string
	// MaxObjectSize bounds PUT payloads (400 above it). 0 means no limit.
	MaxObjectSize int
}
```

Two fields. `Dir` is the root under which `obj/` and `tmp/` live (Chapter 4). In simulation it is `/data` (`toys3/sim/config.go:171`); in production it is the absolute path given by `-dir` (`toys3/cmd/toys3/main.go:32-36`). `MaxObjectSize` is 64 bytes in simulation, which keeps values small and logs readable, and 64 MiB by default in production (`toys3/cmd/toys3/main.go:29`).

Config is *static*: it is passed to the factory on every boot and never changes. It is not hashed by `StateHash` (Chapter 8) because it cannot differ between two runs of one seed.

## 5.3 The private enums

Four small enums describe where the server and each request are. They are `uint8` types, which keeps them cheap to hash and easy to read in a debugger.

### phase: where the incarnation is

```go
// toys3/core/server.go:11-19
// phase is the server's lifecycle stage within one incarnation.
type phase uint8

const (
	phaseBooting    phase = iota // before Started; requests are queued
	phaseRecovering              // listing and unlinking temp files; requests are queued
	phaseServing                 // normal operation
	phaseHalted                  // Halt was returned; every later event is ignored
)
```

### recStep: where recovery is

```go
// toys3/core/server.go:21-28
// recStep is where recovery is (only meaningful while phaseRecovering).
type recStep uint8

const (
	recListing     recStep = iota // waiting for FSListDone(tmp)
	recUnlinking                  // waiting for the FSUnlinkDone of every leftover
	recFsyncingTmp                // waiting for FSFsyncDirDone(tmp)
)
```

### opKind and reqStep: what a request is and where it is

```go
// toys3/core/server.go:30-53
type opKind uint8

const (
	kindPut opKind = iota
	kindGet
	kindDelete
	kindCopy
)

// reqStep is the protocol step a request is on. Each step except
// stepWaiting has exactly one filesystem action outstanding (request.io), and
// names the completion it waits for.
type reqStep uint8

const (
	stepWaiting    reqStep = iota // queued behind an earlier request on one of its keys (or recovery)
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
// toys3/core/server.go:55-66
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

- `seq` is the arrival order within this incarnation, assigned from `Server.nextSeq` when the request is accepted. It defines every per-key FIFO queue (Chapter 6).
- `client` is the runtime's correlation ID for the reply (a `ClientReq`, `toys3/core/types.go:17-20`). The server echoes it on the `Reply` and never interprets it.
- `kind` is the operation.
- `key` is "the key this request writes or reads", which for COPY is the **destination**. This choice means the PUT path, which COPY reuses, can always use `r.key` as the rename target.
- `src` is used only by COPY.
- `data` is the PUT payload, and for a COPY it is filled in with the source payload once the read completes (`toys3/core/server.go:417`).
- `step` and `io` are the protocol position and the one outstanding I/O.
- `tmp` is the temp file path, recorded when `FSWrite` is issued so that later steps (`FSFsync`, `FSRename`) can name it.

Two helpers describe which keys a request touches:

```go
// toys3/core/server.go:68-76
// touches reports whether r reads or writes key.
func (r *request) touches(key string) bool {
	return r.key == key || (r.kind == kindCopy && r.src == key)
}

// conflicts reports whether a and b share a key.
func conflicts(a, b *request) bool {
	return a.touches(b.key) || (b.kind == kindCopy && a.touches(b.src))
}
```

Notice that `conflicts` does not distinguish reads from writes. Two GETs on the same key conflict and are serialized. A reader–writer lock would let them run concurrently, and it would be correct, but it would complicate both the code and the reasoning, and the toy does not need the throughput. You can see this in the seed-1 log: a GET of `k2` and a COPY reading `k2` never overlap.

## 5.5 The Server struct

```go
// toys3/core/server.go:87-101
type Server struct {
	env   Env
	cfg   Config
	phase phase

	nextIO  IOID   // last IOID issued
	nextSeq uint64 // last arrival sequence issued

	// Recovery bookkeeping (phaseRecovering only).
	rec        recStep
	recIO      []IOID // outstanding recovery IOs (one, or one per leftover temp)
	recRemoved int    // temps unlinked successfully

	reqs []*request // accepted, unfinished requests, ascending seq
}
```

- `env` is the injected source of randomness and buggify (`toys3/core/env.go`). **The server never uses it.** There is no call to `s.env` anywhere in `server.go`; the test environment even says so in a comment ("the server does not draw randomness today", `toys3/core/server_test.go:12`). The field exists so that buggify points can be added later without changing the factory signature (Chapter 17 suggests some).
- `cfg` is the static configuration.
- `phase` is the lifecycle stage.
- `nextIO` is the last IOID issued. IOIDs start at 1 in every incarnation (the zero value of `nextIO` is 0, and `newIO` increments before returning). Because the simulator drops every completion addressed to a dead incarnation (Chapter 10), reusing IOIDs across incarnations is safe: an `FSWriteDone{io7}` from incarnation 1 can never reach incarnation 2.
- `nextSeq` is the last arrival sequence issued.
- `rec`, `recIO`, `recRemoved` hold recovery's progress. `recIO` is a slice of the outstanding recovery IOs: one during listing and tmp-fsync, one per leftover temp during unlinking.
- `reqs` holds every accepted, unfinished request in ascending `seq` order. It is both the set of in-flight requests and, implicitly, every per-key queue.

That is the *entire* state of the server. Fourteen-ish words of scalars and two slices. Compare that to how much state a typical server carries in caches, connection pools and background goroutines, and you start to see why it is so easy to hash, replay and throw away.

## 5.6 New and the factory

```go
// toys3/core/server.go:103-106
// New returns a server for one boot. It does nothing until Started.
func New(env Env, cfg Config) *Server {
	return &Server{env: env, cfg: cfg, phase: phaseBooting}
}
```

`New` performs no I/O and issues no actions. A freshly built server is inert until it receives `Started`. The runtime wraps it in a `core.Factory` (`toys3/core/machine.go:22-28`):

```go
type Factory func(env Env, cfg Config) Machine

// NewMachine is the Factory for the real server.
func NewMachine(env Env, cfg Config) Machine { return New(env, cfg) }
```

Runtimes call the factory on first boot and after every crash. The simulator does so in `restart` (`toys3/sim/simulator.go:309-325`), which builds a new machine, marks the server alive, and schedules the `Started` delivery for the same instant. The production loop calls it once per process (`toys3/prod/loop.go:62`), because in production a "crash" ends the process and a supervisor starts a new one.

Taking a factory instead of calling `New` directly is what lets the simulator test deliberately broken servers: each mutant in `toys3/sim/mutants_test.go` is just another `Factory` that wraps `core.New` (Chapter 15).

## 5.7 Step: the dispatcher

Here is the one public method that matters:

```go
// toys3/core/server.go:108-132
// Step consumes one event and returns the actions to execute.
func (s *Server) Step(ev Event) []Action {
	if s.phase == phaseHalted {
		return nil
	}
	switch b := ev.Body.(type) {
	case Started:
		return s.onStarted()
	case Request:
		return s.onRequest(b)
	}
	io, ok := ioOf(ev.Body)
	if !ok {
		return nil
	}
	if s.phase == phaseRecovering && indexIO(s.recIO, io) >= 0 {
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
 ├─ halted?                         → nil            (a halted incarnation is deaf)
 ├─ Started                         → onStarted
 ├─ Request                         → onRequest      (validate, enqueue, maybe start)
 ├─ not an FS completion            → nil            (defensive; nothing else exists)
 ├─ recovering and io ∈ recIO       → onRecoveryDone
 ├─ io matches a running request    → onRequestIO
 └─ otherwise                       → nil            (unknown or stale IOID)
```

Several properties are worth calling out.

**It never blocks and never does I/O.** It looks at memory, changes memory, and returns a slice of plain values. In production this takes microseconds.

**`ev.At` is ignored.** The server does not use time at all. Every wait in toys3 is a wait for an FS completion, so there are no timers and no timeouts (`DESIGN.md:31-32`: "No timers are needed: every wait is on an FS completion"). The `At` field is still stamped on every event by the runtime, so a future version that needs deadlines can read it without any change to the interfaces.

**Unknown completions are ignored, not panicked on.** A completion whose IOID matches no outstanding operation, or matches one but is of the wrong kind, does nothing. This is rule 8 of loop.md ("Give every request a correlation ID so late or duplicate completions can be recognized and ignored"). In the current simulator, stale completions cannot actually arrive (they are dropped with their incarnation), but a server that tolerates them is robust against a runtime that does deliver them, and `TestStaleIOIgnored` pins that behaviour down (§5.12).

**The halted check comes first.** Once a `Halt` has been returned, the incarnation is untrusted (Chapter 7). It ignores everything, including a `Request`, which therefore never gets a `Reply` from this incarnation. That is fine because the runtime kills the incarnation right after executing the `Halt`: the simulator crashes it on the same step (`toys3/sim/simulator.go:199-206`), and the production loop returns `ErrHalted` from `Run`, after which `main` exits (`toys3/cmd/toys3/main.go:48-56`).

### ioOf and newIO

```go
// toys3/core/server.go:134-158
// ioOf extracts the IOID of a filesystem completion.
func ioOf(b EventBody) (IOID, bool) {
	switch d := b.(type) {
	case FSWriteDone:
		return d.IO, true
	...
	case FSListDone:
		return d.IO, true
	}
	return 0, false
}

func (s *Server) newIO() IOID {
	s.nextIO++
	return s.nextIO
}
```

`ioOf` is a type switch over the closed set of completions. Because `EventBody` is a closed interface (an unexported marker method `isEvent()`, `toys3/core/event.go:12`), the set of cases is known and fixed; Chapter 3 discusses why the vocabulary is closed. `newIO` is the only place IOIDs are allocated. It increments first, so the first IOID of every incarnation is 1, and 0 can mean "none" (as in `request.io` while waiting).

### halt

```go
// toys3/core/server.go:160-166
// halt fail-stops: nothing after it in this incarnation is trusted. The
// runtime executes the actions before it (here: the 500, if any) and then
// kills the incarnation.
func (s *Server) halt(reason string, before ...Action) []Action {
	s.phase = phaseHalted
	return append(before, Halt{Reason: reason})
}
```

`halt` sets the phase and returns the given actions followed by `Halt`. `Halt` is therefore always the last action of its step, a property the simulator enforces as a protocol check (`toys3/sim/simulator.go:200-203`: "Halt is not the last action of its step"). Recovery calls `halt` with no `before` actions; request errors call it with a 500 `Reply` (Chapter 7).

## 5.8 The lifecycle of one incarnation

Putting the phases together:

```
                 New()
                   │
                   ▼
          ┌─────────────────┐   Request → queued (no actions)
          │  phaseBooting   │◄──────────┐
          └────────┬────────┘───────────┘
                   │ Started → FSList(tmp)
                   ▼
          ┌─────────────────┐   Request → queued (no actions)
          │ phaseRecovering │◄──────────┐
          │  recListing     │───────────┘
          │  recUnlinking   │
          │  recFsyncingTmp │
          └──┬──────────┬───┘
   recovery  │          │ FSFsyncDirDone(tmp) OK
   error     │          ▼  → Emit{Recovered}, start queued requests
             │ ┌─────────────────┐   Request → validate; run if at head of its queues
             │ │  phaseServing   │◄──────────┐
             │ └────────┬────────┘───────────┘
             │          │ error after obj/ may have changed
             ▼          ▼  → [Reply 500], Halt
          ┌─────────────────┐
          │  phaseHalted    │   every event → nil
          └─────────────────┘
                   │
                   ▼   runtime kills the incarnation; a new one starts at New()
```

A second `Started` within one incarnation is ignored (`onStarted` checks `s.phase != phaseBooting`). There is no transition out of `phaseHalted` and no transition back to recovering: recovery runs exactly once per incarnation, and a failed recovery is retried by halting and starting a *new* incarnation from scratch.

## 5.9 Why requests are queued while booting

`phaseBooting`'s comment says "before Started; requests are queued". You might think that can never happen: surely the runtime delivers `Started` first? In production it does: `Loop.Run` steps `Started` before it reads anything from the inbox (`toys3/prod/loop.go:62-66`). But in simulation it is a real possibility, and it is a nice example of how the simulator explores orders you would not think to test.

When the simulator restarts the server, it schedules the `Started` delivery at the current instant and wakes any parked clients (`toys3/sim/simulator.go:321-324`):

```go
	s.schedule(Entry{At: s.now, Kind: KindDeliver, Inc: s.inc, Event: core.Started{}})
	if !s.quiescing {
		s.wakeParked()
	}
```

`wakeParked` schedules each client's wake-up at `now + Think()`, and think time is drawn from `[0, 1ms]` (`toys3/sim/config.go:157`), so it can be exactly zero. A client wake at the same instant as the `Started` delivery is ordered against it by the seeded random `Tie` (Chapter 10). If the wake goes first, the client issues its request, and the request's delivery is scheduled at that same instant, again with a random tie. So a `Request` can reach a server that has not yet seen `Started`. The server queues it, exactly as it would during recovery, and starts it once recovery is done.

This is the sort of corner that an implementer, reasoning about "the" order of events at boot, would never write a test for. The simulator simply produces it, sometimes, and the server has to be correct anyway.

## 5.10 Recovery, line by line

Recovery has one goal: **make `tmp/` empty, durably, before serving.** After a crash, `tmp/` can contain:

- temp files of PUTs and COPYs that were in progress (written, perhaps fsynced, not yet renamed),
- temp files whose `FSWrite` failed partway (mutations that replied 500, `toys3/core/server.go:425-428`),
- resurrected temp names that are hard-link aliases of committed objects (Chapter 4, §4.4).

None of these mean anything to a new incarnation, and the last kind is actively dangerous if a temp name is ever reused (Chapter 15, mutant f). `obj/` needs no recovery at all: "obj/ only ever holds complete files" (`toys3/core/doc.go:107-109`).

The recovery protocol is three steps (`toys3/core/doc.go:100-109`):

```
Started ──► FSList(tmp) ──► FSUnlink(each leftover) ──► FSFsyncDir(tmp) ──► Emit{Recovered}, serve
```

### onStarted

```go
// toys3/core/server.go:170-179
func (s *Server) onStarted() []Action {
	if s.phase != phaseBooting {
		return nil // a second Started in one incarnation: ignore
	}
	s.phase = phaseRecovering
	s.rec = recListing
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSList{IO: io, Dir: TmpDir(s.cfg.Dir)}}
}
```

Move to recovering, allocate IOID 1, record it as the one outstanding recovery IO, and ask for a listing of `tmp/`. This is why every incarnation's log begins with `FSList{io1 /data/tmp}`.

### onRecoveryDone: listing

```go
// toys3/core/server.go:181-205
// onRecoveryDone advances recovery. Any error halts: recovery must finish
// before serving, and restarting retries it from scratch.
func (s *Server) onRecoveryDone(io IOID, body EventBody) []Action {
	switch s.rec {
	case recListing:
		d, ok := body.(FSListDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.halt("recovery: list tmp: " + d.Err.String())
		}
		s.recIO = nil
		var acts []Action
		for _, name := range d.Names {
			uio := s.newIO()
			s.recIO = append(s.recIO, uio)
			acts = append(acts, FSUnlink{IO: uio, Path: TmpDir(s.cfg.Dir) + "/" + name})
		}
		if len(acts) == 0 {
			return s.fsyncTmpDir()
		}
		s.rec = recUnlinking
		return acts
```

- The type assertion `body.(FSListDone)` guards against a completion of the wrong kind that happens to carry the right IOID. It is ignored like a stale one.
- An error halts. There is nothing sensible to do if you cannot even list `tmp/`; the next boot will try again.
- On success, issue **one `FSUnlink` per leftover, all at once**. They run concurrently; the server waits for all of them. `recIO` becomes the list of their IOIDs.
- `FSListDone.Names` are base names, sorted ascending (`toys3/core/event.go:96`). Both runtimes sort: the FakeFS lists from its sorted name cache (`toys3/sim/fakefs.go:413-428`) and `os.ReadDir` returns entries sorted by name (`toys3/prod/executor.go:60`). Sorted output is not cosmetic: it makes the order of the unlink actions, and therefore of their IOIDs, a deterministic function of the directory contents.
- If there were no leftovers, skip straight to the tmp fsync.

### onRecoveryDone: unlinking

```go
// toys3/core/server.go:206-223
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
			return s.halt("recovery: unlink temp: " + d.Err.String())
		}
		i := indexIO(s.recIO, io)
		s.recIO = append(s.recIO[:i], s.recIO[i+1:]...)
		if len(s.recIO) > 0 {
			return nil
		}
		return s.fsyncTmpDir()
```

- `ErrNotFound` is tolerated: the goal is "the name is gone", and it is. (In the current simulator nothing else deletes temp files during recovery, so this case is rare, but it is the right semantics and costs nothing.)
- `ErrIO` halts. A failed unlink may or may not have taken effect, and recovery must not declare `tmp/` clean on a maybe.
- Each completion removes its IOID from `recIO`. The `indexIO` lookup cannot return -1 here, because `Step` only routes to `onRecoveryDone` when the IOID is in `recIO`.
- When the last unlink completes, move on to the fsync. Completions may arrive in any order; the code does not care.

### onRecoveryDone: fsyncing tmp/

```go
// toys3/core/server.go:225-246
	case recFsyncingTmp:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.halt("recovery: fsync tmp dir: " + d.Err.String())
		}
		s.phase = phaseServing
		s.recIO = nil
		acts := []Action{Emit{Fact: Recovered{TempsRemoved: s.recRemoved}}}
		return append(acts, s.startRunnable()...)
	}
	return nil
}

func (s *Server) fsyncTmpDir() []Action {
	s.rec = recFsyncingTmp
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSFsyncDir{IO: io, Dir: TmpDir(s.cfg.Dir)}}
}
```

The tmp fsync makes the unlinks durable. Its success is the moment the incarnation starts serving: the phase flips, the `Recovered` fact is emitted (with the number of temps actually removed), and `startRunnable` starts every request that queued up during booting and recovery, in arrival order (Chapter 6 explains `startRunnable` and `canRun`).

The fact is emitted *before* the first request actions in the same slice. The simulator's liveness invariant uses it: at the end of a run, the current incarnation must have emitted `Recovered` (`toys3/sim/invariant.go:196-204`).

Note that the fsync is issued **even when nothing was removed** (`toys3/core/doc.go:103`: "fsyncs the tmp directory (even if nothing was removed)"). §5.14 looks at whether this step is actually necessary.

## 5.11 Recovery in a real run

Here is recovery happening in seed 1 of the default configuration. Incarnation 1 has just halted because a temp-file fsync failed (an EIO, Chapter 7):

```
#233 t=53.112270ms inc=1 deliver FSFsyncDone{io78 EIO} => Reply{c30 500} Halt{fsync temp: EIO}
#233 t=53.112270ms inc=1 crash   CRASH (halt: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
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
#252 t=59.063797ms inc=2 deliver FSUnlinkDone{io2 EIO} => Halt{recovery: unlink temp: EIO}
#252 t=59.063797ms inc=2 crash   CRASH (halt: recovery: unlink temp: EIO): kept=1 lost=1 torn=0, dropped 0 queued entries
```

Walk through it:

- **#236**: the new incarnation's first step. `Started` produces `FSList{io1}`. IOIDs have restarted at 1.
- **#238–#244**: four client requests arrive while the listing is in flight. Each produces no actions: they are queued in `reqs` with `stepWaiting`.
- **#245–#246**: the listing returns three names. Two (`...4b`, `...4c`) are the temp files of the PUTs that were in flight when incarnation 1 halted: `io75` and `io76` (0x4b = 75, 0x4c = 76). The third, `...31`, is the resurrected temp name from Chapter 4: IOID 49 of incarnation 1, renamed into `obj/k1` at step 160, whose removal from `tmp/` was never made durable. It is now a hard-link alias of `obj/k1`. Recovery does not know or care; it unlinks all three at once.
- **#247–#250**: two unlinks succeed, and produce no actions because another unlink is still outstanding.
- **#251–#252**: the third unlink fails with EIO. Recovery halts. No `Recovered` fact, no requests served. The four queued requests will be marked abandoned by the simulator (they were invoked but never replied to).

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
- `...31` is still there, because incarnation 2's unlink of it failed (the FakeFS chose "EIO, not applied", or "applied" and then the crash reverted it).
- **#271**: the fsync of `tmp/` succeeds, and in one step the server emits `Recovered{temps=2}` and starts three queued requests: c35's PUT (an `FSWrite` to `tmp/0000000000000005`, a name that is safe to reuse now that `tmp/` is durably empty), c36's GET of `k1`, and c37's GET of `k2`. c38's GET of `k2` stays queued behind c37's (same key).
- **#272–#273**: c36 reads `obj/k1` and gets `value-of-23`. That is the object c23 committed at step 167 of incarnation 1, through the very temp name `...31` that recovery just unlinked. Unlinking the alias did not touch the object, exactly as Chapter 4 predicted.

## 5.12 The unit tests for recovery

`toys3/core/server_test.go` drives the server by hand through a `harness` (`toys3/core/server_test.go:19-143`): an in-memory map of files, a list of pending FS actions, and helpers to complete them one at a time, in order. Every FS change is immediately "durable" in this harness; there are no crash semantics. That is fine for testing the server's *logic*: what actions it emits, in what order, in response to which completions. Crash behaviour is the simulator's job.

The harness's `describe` function renders actions compactly so that tests can assert exact sequences as strings, replacing temp names with `tmp/*` because their hex digits are noise.

### TestRecovery

```go
// toys3/core/server_test.go:322-341
func TestRecovery(t *testing.T) {
	h := newHarness(t)
	h.files[root+"/tmp/0000000000000007"] = []byte("junk")
	h.files[root+"/tmp/0000000000000009"] = []byte("junk")
	h.files[ObjPath(root, "k")] = EncodeObject([]byte("kept"))

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
5. The fsync completion produces, in order: the `Recovered` fact with the right count, then the queued requests started in arrival order (the GET's read, then the PUT's write).

The final loop checks that the leftover files are actually gone from the harness's map.

### TestRecoveryEmptyAndErrors

```go
// toys3/core/server_test.go:343-358
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
	expect(t, "list error", h.completeNext(), "halt")
}
```

Two edge cases. An empty `tmp/` still gets its fsync and produces `Recovered{0}`. A failed listing halts with no reply (there is no request to reply to).

### TestStaleIOIgnored

This test is about the dispatcher, and it is the most instructive of the three:

```go
// toys3/core/server_test.go:421-443
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
| `FSWriteDone{9999}` | no such IOID | falls through the request loop, `server.go:131` |
| `FSFsyncDone{writeIO}` | right IOID, wrong kind | `onRequestIO`'s type assertion, `server.go:421-424` |
| `FSListDone{1}` | recovery's IOID, but recovery is over | `s.phase == phaseRecovering` is false, then no request has io 1 |
| `FSFsyncDirDone{2, EIO}` | same, and an *error* that would halt if accepted | same |
| `FSReadDone{writeIO+1}` | an IOID that has not been issued yet | no request has it |

The fourth case is the dangerous one. If the dispatcher had not checked the phase, a late error for recovery's tmp fsync could halt a healthy, serving server. The test also checks that the **state hash** is unchanged (Chapter 8): "ignored" must mean "no effect at all", not just "no actions". Then the real completions drive the PUT to its 200, and a second `Started` is ignored.

## 5.13 What the server does not do at boot

It is as instructive to list what recovery omits:

- **It does not scan `obj/`.** There is no index to rebuild, no checksum sweep, no repair. Objects are self-contained files, and the protocol guarantees they are complete. A corrupt object (bit rot) is discovered lazily, on read, and reported as a 500 with a `CorruptObject` fact (Chapter 6).
- **It does not try to finish interrupted PUTs.** A PUT that was interrupted before its `FSFsyncDir(obj/)` completed was never acked (it was abandoned by the crash, or it replied 500). Its client has no expectation. Whether its rename landed is up to the crash; the oracle accepts either outcome (Chapter 13).
- **It does not remember anything about the previous incarnation.** It does not know how many incarnations there were, what IOIDs they used, or what they were doing. This is what "memory is disposable" means in practice.

This minimalism is a design choice with teeth: every piece of recovery logic that does not exist is a piece that cannot be wrong.

## 5.14 An open question: is the tmp fsync necessary?

The recovery protocol fsyncs `tmp/` after unlinking the leftovers. The comment says it does so "even if nothing was removed". Is that step load-bearing?

While writing this book, a mutant was built (outside the repository, in a scratch program that reuses the `skipper` wrapper from `mutants_test.go`) that removes exactly this step: it intercepts `FSFsyncDir(tmp)` and feeds the inner server an immediate OK instead of issuing it.

```go
func mutSkipTmpFsyncDir(env core.Env, cfg core.Config) core.Machine {
	return &skipper{inner: core.New(env, cfg), skip: func(a core.Action) bool {
		d, ok := a.(core.FSFsyncDir)
		return ok && d.Dir == core.TmpDir(cfg.Dir)
	}}
}
```

Run against seeds 1 to 2,000 with all default invariants: **not caught once.**

Here is an argument for why, under the current model, that is the right answer rather than a weakness of the invariants:

1. Without the fsync, recovery's unlinks may be volatile: a temp name `t` has `cur = ∅` but `durable = I` (some old inode).
2. The new incarnation may then write a temp file of the same name `t` (IOIDs restart at 1). In the FakeFS, `write` to a name whose `cur` is empty **creates a fresh inode** (`toys3/sim/fakefs.go:298-304`): it does not resurrect `I`. So the write cannot overwrite whatever `I` is (possibly a committed object's inode).
3. If the server then crashes, the un-synced unlink may be reverted and `t` may again name `I`. But that is just another leftover, and the *next* recovery removes it before any write.

So the only dangerous thing, writing through a name that aliases a committed object, cannot happen within the incarnation, durable unlink or not. The fsync protects against a hazard the protocol already rules out by other means.

But "not caught" is not "not needed", and there are reasons to keep it:

- On a real filesystem, the `O_CREAT|O_TRUNC` open that `OSExecutor` uses (`toys3/prod/executor.go:76`) on a name that is *visibly* absent also creates a fresh inode, so the argument carries over to POSIX. But it relies on the visible state and the durable state agreeing about *which* names exist when the write happens. The fsync makes that agreement trivial instead of argued.
- It makes `Recovered` mean something simple and strong: "`tmp/` is durably empty". Simple postconditions are easier to build on (for example, a future version that keeps something meaningful in `tmp/`).
- It costs one fsync per boot.

Treat this as the first of several places in the book where DST is used not just to find bugs, but to **find out which parts of a design are actually doing work**. Chapter 15 returns to the idea; the exercises below ask you to try to break the argument.

## Key takeaways

- The server's entire state is a handful of scalars and two slices (`reqs`, `recIO`). No maps, no time, no randomness. It is rebuilt from nothing on every boot.
- `Step` is a dispatcher: halted → ignore; `Started` → recovery; `Request` → validate and queue; FS completion → route by IOID to recovery or to the one request waiting on it; anything else → ignore. Wrong-kind and stale completions are ignored without changing state.
- An incarnation goes booting → recovering → serving → (halted). Requests that arrive before serving are queued, not rejected. The booting case is reachable in simulation because `Started` and a client's request can share an instant.
- Recovery is: list `tmp/`, unlink everything in it concurrently (ENOENT is fine), fsync `tmp/`, emit `Recovered`, start queued requests in arrival order. Any error halts, and the next incarnation retries from scratch. Recovery is idempotent.
- Recovery deliberately does nothing to `obj/`, because the write protocol guarantees `obj/` only ever holds complete files.
- Under the current model, the recovery fsync of `tmp/` is not caught as necessary by 2,000 seeds; there is an argument why, and reasons to keep it anyway.

## Exercises

1. **Trace a boot by hand.** Using only `server.go`, write the exact action list the server returns for: `Started`; `Request{c1 GET k}`; `FSListDone{io1, [a b]}`; `FSUnlinkDone{io3}`; `FSUnlinkDone{io2, ENOENT}`; `FSFsyncDirDone{io4}`. Then encode it as a test in the style of `TestRecovery` (in a scratch file you delete afterwards) and run `go test ./toys3/core -run YourTest -v`.
2. **Make the halted server talk.** Change `Step` so that a halted server replies 503 to new requests instead of ignoring them. Predict: does any core unit test fail? Does any simulation seed fail (`go test ./toys3/sim -run TestSeeds -seeds=50`)? Which simulator check would object, and why? (Hint: read `toys3/sim/simulator.go:199-206` and ask whether a halted incarnation ever receives another event.) Revert.
3. **Recovery without the type assertion.** Remove the `d, ok := body.(FSUnlinkDone); if !ok { return nil }` guard in the `recUnlinking` case and replace it with an unchecked assertion. Which existing test, if any, catches this? Write one that does. (Hint: what can a recovery IOID collide with?)
4. **Try to break §5.14's argument.** Build the "skip tmp fsync" mutant yourself (copy `skipper` and its helpers out of `toys3/sim/mutants_test.go` into a small program in a scratch module that `replace`s `manifester` with this repository, or add it temporarily to `mutants_test.go` and to the `mutants` list). Run 500 seeds. Then look for a change to the *FakeFS* (not the server) that is still faithful to POSIX and under which the mutant *is* caught. If you find one, decide whether the FakeFS should adopt it.
5. **Make recovery partial.** Change the `recUnlinking` case so that an `ErrIO` is treated like `ErrNotFound` (skip it, keep going). Predict which mutant from Chapter 15 this most resembles and which invariant will catch it, then run 500 seeds (`go test ./toys3/sim -run 'TestSeeds$' -seeds=500`). Did your prediction hold? If it was not caught, explain why a single leftover is less dangerous than skipping cleanup entirely.
6. **Find a boot-time race.** Write a small program (or a temporary test in `toys3/sim`) that runs seeds with `Config.Log = true` and searches `Result.Log` for a `Request` delivered to an incarnation before that incarnation's `Started`. How many of the first 200 seeds contain one? (This tells you how often the `phaseBooting` queuing path is exercised.)

## Further reading

- George Candea and Armando Fox, *"Crash-Only Software"*, HotOS 2003. The argument that the only way to stop a component should be to crash it, and the only way to start it should be to recover. toys3's "memory is disposable, recovery on every boot" is crash-only design.
- `loop.md`, rules 8 and 9 (correlation IDs; all durable state in the store).
- Pierre Zemb, *The Moonpool Sim Book*, on process lifecycles and incarnations; summarized in `docs/lessons.md`, lessons 4 and 5.
- Pillai et al., *"All File Systems Are Not Created Equal"*, OSDI 2014, for the recovery assumptions real applications make about what survives a crash.
