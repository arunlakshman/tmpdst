# Chapter 2: The Architecture: `Step(Event) []Action`

> *"The best way to make I/O deterministic is not to do any."*

Chapter 1 named the three ingredients of DST: one thread of control, simulated I/O and time, and one seeded source of randomness. This chapter is about the *shape of code* that makes those ingredients cheap to provide. In toys3 that shape is a single method:

```go
// toys3/control/machine.go:3-7
// StateMachine is the shape of the server: Step is the only place state
// changes. It never blocks and never does I/O.
type StateMachine interface {
	Step(Event) []Action
}
```

Everything the server will ever know arrives as an `Event`. Everything the server will ever cause arrives back at the runtime as a list of `Action`s. Between the two there is no I/O, no clock, no goroutine, no channel, no lock. The rest of this chapter explains why that austere shape is worth its cost, what the naive alternatives look like and exactly where they break, and then walks through all fifteen rules of `loop.md`, showing for each one where toys3 follows it, and in a few places where it deliberately does something different.

---

## 2.1 The problem: logic that does I/O cannot be simulated

Start with the code most of us would write first. A PUT handler for an object store, in ordinary Go:

```go
// NOT toys3 code: the "obvious" version.
func (s *Store) Put(key string, data []byte) error {
	tmp := filepath.Join(s.dir, "tmp", newID())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(encode(data)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, filepath.Join(s.dir, "obj", escape(key))); err != nil {
		return err
	}
	return syncDir(filepath.Join(s.dir, "obj"))
}
```

This is correct-looking code. Each HTTP request would run it on its own goroutine, with a `sync.Mutex` per key to serialize same-key operations. What is wrong with it from a testing point of view?

1. **The I/O is welded to the logic.** You cannot run this function without a filesystem. To test what happens when `Sync` fails, you need a filesystem whose `Sync` fails.
2. **The concurrency is in the threads.** Two concurrent PUTs on different keys interleave however the Go scheduler and the kernel decide. You cannot ask "what if the second rename completes before the first fsync?" and get the same answer twice.
3. **Time is implicit.** Each syscall takes however long it takes. Nothing in the program says "this fsync took 1.2 ms and that read took 30 µs".
4. **A crash is not a value.** There is no point in the program where "the power went out here" can be expressed. If you kill the process, the page cache survives (a process kill is not a power loss); if you cut power to a VM, you cannot choose the instant.

The standard engineering response to point 1 is dependency injection: put the filesystem behind an interface. Let us follow that road and see where it ends.

## 2.2 Naive approach 1: mock the filesystem interface

```go
// NOT toys3 code.
type FS interface {
	WriteFile(path string, data []byte) error
	Fsync(path string) error
	Rename(from, to string) error
	FsyncDir(dir string) error
	ReadFile(path string) ([]byte, error)
}
```

Now `Put` calls `s.fs.WriteFile(...)` and a test can pass a fake `FS` whose `Fsync` returns `EIO`. This is real progress: error paths become testable. But look at what the interface *cannot say*.

**It cannot say "this happened, and nobody was told."** `Rename(from, to) error` is synchronous. The call returns once, with one error. In a real system the effect of a rename lands in the kernel's directory cache at one instant, and the return value reaches the caller at a later instant. If the process dies between those two instants, the rename *happened* but the caller *never learned of it*. `docs/lessons.md` puts this sharply:

> A synchronous `Put() error` cannot express "the PUT was applied at t1 and its response was lost at t2"; an engine can.
> (`docs/lessons.md`, §0)

You could try to fake it inside the mock ("apply the rename, then panic to simulate a crash"), but now the crash is a panic unwinding through your real code's stack, running its `defer`s, and the "dead" process's goroutines are still alive. A crash in a real machine stops *everything*, including the other forty goroutines halfway through their own syscalls.

**It cannot let a scheduler choose interleavings.** With a blocking interface, each request is a goroutine parked inside `fs.Fsync`. The *mock* can decide when to return, but the mock is called from many goroutines at once, so the order in which it is called is decided by the Go runtime. You can make the mock block on a channel and release goroutines one at a time in a seeded order, and people do build such things, but now your "deterministic" scheduler is fighting the Go runtime: `select` with several ready cases picks one at random (`docs/lessons.md` §0 again), goroutine start order is unspecified, and `runtime.Gosched` promises nothing.

**It cannot fold state into a hash.** Where is the server's state? It is spread across goroutine stacks (local variables in forty half-finished `Put` calls), mutexes, and the mock. To check determinism you want to hash "the entire state of the server" after every step. With goroutines, there is no "step", and the state is not a value you can hash.

## 2.3 Naive approach 2: make everything a goroutine but control the scheduler

The next idea is to keep the blocking style and replace the scheduler. In some languages this is genuinely viable: Rust's async ecosystem lets you supply your own executor, which is how frameworks like `madsim` and `turmoil` simulate tokio programs, and how Moonpool works. `docs/lessons.md` §0 explains why the repository's authors decided Go cannot do this:

> Go cannot do this. We cannot replace the goroutine scheduler, and `select` chooses randomly among ready cases. So in Go, library-level determinism needs sans-I/O state machines.

(There are partial workarounds in Go, such as running with `GOMAXPROCS=1` and carefully avoiding `select`, but they are fragile: any library that starts a goroutine or uses a `sync.Pool` can quietly break determinism. The alternative, used by Antithesis, is to make the *whole machine* deterministic with a deterministic hypervisor, which works on unmodified code but is a much bigger hammer.)

## 2.4 The sans-I/O pattern

The term "sans-I/O" comes from the Python networking community (the `sans-io` documentation project around protocol libraries such as `h11` and `h2`): a protocol implementation that never touches a socket. It consumes bytes that someone else read and produces bytes for someone else to write. Applied to a whole server, the pattern becomes:

```
         ┌──────────────── control.Controller (pure logic, no I/O) ───────────────┐
  Events  ───►  │            Step(event) ──► mutate state ──► []Action            │ ──► Actions
                └─────────────────────────────────────────────────────────────────┘
  Event  = Started | Request{Client, Put|Get|Delete|Copy}
         | FSWriteDone | FSFsyncDone | FSRenameDone | FSFsyncDirDone
         | FSReadDone | FSUnlinkDone | FSListDone            (each carries IOID, Err)
  Action = Reply{Client, Status, Body}
         | FSWrite | FSFsync | FSRename | FSFsyncDir | FSRead | FSUnlink | FSList
         | Trip{Cause} | Emit{Telemetry}
```

(This is `toys3/DESIGN.md:71-80`, redrawn.) The server does not *call* the filesystem. It *asks* for a filesystem operation by returning an `FSWrite` value, and it later *learns* the outcome when an `FSWriteDone` value is fed back in. Every one of the four problems from §2.1 dissolves:

1. **I/O is separated from logic.** `Step` can be called from a unit test with hand-made events (`toys3/control/controller_test.go` does exactly this, with no filesystem at all), from the simulator with completions produced by a plant model, and from production with completions produced by real `os` calls.
2. **Concurrency is in the data.** Many requests are in flight at once, but each in-flight request is a record (`*request`, `toys3/control/controller.go:51-61`) holding the IOID it is waiting for. The order in which completions arrive is decided by whoever calls `Step`, which in simulation is a seeded priority queue.
3. **Time is explicit.** Each `Event` carries `At` (`toys3/control/event.go:5-8`). The server never reads a clock.
4. **A crash is a value.** In the simulator, a crash is simply: stop calling `Step` on this server object, throw it away, drop every queued event addressed to it, resolve the plant model's volatile state, and later build a new server with the `Factory`. "Applied at t1, response lost at t2" is just two separate queue entries, an `fs-land` at t1 and a `deliver` at t2, with a crash scheduled in between (Chapter 10).

Here is the PUT protocol as the server experiences it, one step per completion:

```
Request{c7, Put k}    ──► Step ──► [FSWrite{io1, tmp/01, hdr+data}]
FSWriteDone{io1}      ──► Step ──► [FSFsync{io2, tmp/01}]
FSFsyncDone{io2}      ──► Step ──► [FSRename{io3, tmp/01 → obj/k}]
FSRenameDone{io3}     ──► Step ──► [FSFsyncDir{io4, obj}]
FSFsyncDirDone{io4}   ──► Step ──► [Emit{Committed}, Reply{c7, 200}]
```

(`toys3/DESIGN.md:89-95`.) And here is the same thing as it actually appeared in the event log of seed 1 (captured with the scratch tracer described in Chapter 16; trimmed to the lines that concern request `c6`):

```
#21  t=3.397177ms  inc=1 deliver  Request{c6 PUT "k2" value-of-6}
#26  t=4.822820ms  inc=1 deliver  FSReadDone{io5 ENOENT nil} => Reply{c4 404} FSUnlink{io6 /data/obj/k0} FSWrite{io7 /data/tmp/0000000000000007 obj(value-of-6)}
#33  t=6.109640ms  inc=1 fs-land  land FSWrite{io7 /data/tmp/0000000000000007 obj(value-of-6)} -> FSWriteDone{io7 OK}
#38  t=6.531543ms  inc=1 deliver  FSWriteDone{io7 OK} => FSFsync{io10 /data/tmp/0000000000000007}
#40  t=6.676498ms  inc=1 fs-land  land FSFsync{io10 /data/tmp/0000000000000007} -> FSFsyncDone{io10 OK}
#42  t=6.871157ms  inc=1 deliver  FSFsyncDone{io10 OK} => FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2}
#44  t=7.233120ms  inc=1 fs-land  land FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2} -> FSRenameDone{io12 OK}
#46  t=7.377477ms  inc=1 deliver  FSRenameDone{io12 OK} => FSFsyncDir{io14 /data/obj}
#49  t=8.030661ms  inc=1 fs-land  land FSFsyncDir{io14 /data/obj} -> FSFsyncDirDone{io14 OK}
#50  t=8.242306ms  inc=1 deliver  FSFsyncDirDone{io14 OK} => Emit{Committed{c6 "k2" crc=9b35c638}} Reply{c6 200} FSRead{io15 /data/obj/k2}
```

Three things to notice already:

- `c6` arrived at step 21 but its first action appeared only at step 26. It was **queued** behind an earlier COPY (`c4 COPY "k2" -> "k0"`) that touched `k2`. The per-key FIFO (Chapter 6) is pure bookkeeping inside `Step`; no lock was taken.
- The step that finished `c6` (step 50) also *started* the next request waiting on `k2` (`FSRead{io15 ...}`, a GET). One `Step` can emit actions belonging to several requests.
- Each FS action has two log lines: `fs-land` (the effect is applied to the fake disk) and `deliver` (the completion is handed to the server). Between them, a crash can happen. That gap is the entire "applied but not acknowledged" failure mode, made explicit.

## 2.5 Why Events and Actions must be plain data

`loop.md` rule 3 says "Make Events and Actions plain data: structs without channels, funcs or pointers into shared state." In toys3 every event body and every action is a small struct of integers, strings and byte slices. Why insist on this?

**Logging.** A plain struct can be printed. `sim.DescribeEvent` and `sim.DescribeAction` (`toys3/sim/log.go:47-100`) render every event and action into the log lines you saw above. A closure cannot be printed meaningfully; a channel prints as an address.

**Comparison and hashing.** A plain struct can be folded into a hash. The simulator does exactly this for every event delivered and every action returned (`hashEvent` and `hashAction`, `toys3/sim/trace.go:30-83`), so the trace hash covers not just the server's internal state but its entire input/output history. A pointer into shared state would hash differently on every run (addresses are not deterministic), and a func cannot be hashed at all.

**Replay.** A plain-data event can be stored and fed back in later, to the same server or to a different version of it. That makes regression tests out of logs, and it makes the "wrap the real server in a mutant" trick of Chapter 15 possible: a mutant receives the same plain event, rewrites it (for example, flips an `ErrIO` to `OK`), and passes it to the real server (`toys3/sim/mutants_test.go:131-195`).

**No hidden channels of influence.** If an Action contained a callback, the runtime would call it, and the callback could mutate server state *outside* `Step`. That breaks the one-thread-of-control ingredient. If an Event contained a pointer into a shared buffer, a goroutine elsewhere could change the bytes after the server had looked at them.

There is one subtlety: `[]byte` *is* a reference type in Go. `FSWrite.Data`, `FSReadDone.Data` and `Reply.Body` are slices, and two parties holding the same slice can see each other's writes. toys3 handles this by convention plus defensive copying at the boundaries:

```go
// toys3/control/doc.go:24-28
// Rules (enforced by tools/detcheck in CI):
//   - imports only from tools/detcheck/allow.txt (no time, os, math/rand, sync);
//   - no goroutines, channels, select, or package-level variables;
//   - never let map iteration order decide behavior;
//   - Events and Actions are plain data; byte slices are immutable once sent.
```

"Byte slices are immutable once sent" is a convention: detcheck cannot enforce it. The PlantModel backs it up by cloning on the way in and on the way out (`content := bytes.Clone(data)` at `toys3/sim/plantmodel.go:294`, `data := bytes.Clone(in.volatile)` at `toys3/sim/plantmodel.go:394`), and there is a test that the fake never aliases a slice it handed out (`TestPlantModelNeverAliasesHandedOutSlices`, `toys3/sim/plantmodel_test.go:173-187`). The oracle clones reply bodies before keeping them (`op.Body = slices.Clone(rep.Body)`, `toys3/sim/oracle.go:98`). The server itself aliases in one place, deliberately: `DecodeObject` returns a payload that aliases the file buffer (`toys3/format/object.go:27-30`), which is safe precisely because nobody writes to a received buffer.

## 2.6 What the shape costs

It would be dishonest to present the sans-I/O shape as free. It costs three things.

**Inversion of control.** The PUT handler from §2.1 was eleven lines of straight-line code. In toys3 the same protocol is spread over five cases of a `switch r.step` in `onRequestIO` (`toys3/control/controller.go:272-356`), plus `start`, `writeTmp`, `issue`, `finish` and `commit`. You are writing a state machine by hand. Every local variable of the straight-line version that must survive across an I/O becomes a field of `request` (`tmp`, `data`, `io`, `step`, `toys3/control/controller.go:51-61`). This is the same transformation an `async` compiler performs automatically; in Go you do it yourself.

**Explicit bookkeeping of what you are waiting for.** Each request records exactly one outstanding IOID, and `Step` must route each completion to its owner by searching (`toys3/control/controller.go:119-132`). Completions that match nothing (stale ones from before a restart, or duplicates) must be ignored rather than crash the server.

**Discipline in the runtime.** The production runtime must preserve the illusion: one goroutine calls `Step`, completions come back through one inbox, and actions are executed in order (`toys3/prod/loop.go:60-99`). If someone calls `Step` from two goroutines, every guarantee evaporates, and nothing in the type system prevents it.

In exchange you get a server that can run, unchanged, in a simulator that controls every input. For a storage system whose correctness depends on behaviour under crashes, that is a very good trade.

## 2.7 Factory, Machine, Hashable

Three small types in `toys3/control/machine.go` finish the architecture.

```go
// toys3/control/machine.go:9-30
// Hashable lets the simulator fold state into a running trace hash after every
// step. The same seed must produce the same hash sequence; a divergence means
// nondeterminism leaked in.
type Hashable interface {
	StateHash() uint64
}

// Machine is what a runtime drives: a hashable state machine.
type Machine interface {
	StateMachine
	Hashable
}

// Factory builds a fresh Machine for one boot. Runtimes call it on first boot
// and after every crash, then deliver Started. Taking a Factory (rather than
// calling New directly) lets the simulator run deliberately buggy variants.
type Factory func(env Env, cfg Config) Machine

// NewMachine is the Factory for the real server.
func NewMachine(env Env, cfg Config) Machine { return New(env, cfg) }

var _ Factory = NewMachine
```

- **`Hashable`** exists for determinism checking. After every server step the simulator folds `StateHash()` into its trace hash (`toys3/sim/simulator.go:184`). Chapter 8 dissects `Controller.StateHash` (`toys3/control/hash.go:7-32`).
- **`Machine`** is the pair. Runtimes depend on `control.Machine`, never on `*control.Controller`. That is what lets a mutant (which wraps a `*control.Controller` and rewrites its inputs or outputs) be dropped in without the runtime noticing.
- **`Factory`** encodes "memory is disposable". A crash is not "reset some fields"; it is "throw the object away and build a new one". Both runtimes take a factory: `prod.NewLoop(cfg, factory, exec, inboxSize)` (`toys3/prod/loop.go:39`) and `sim.Config.Factory` (`toys3/sim/config.go:35-37`), defaulting to `control.NewMachine` (`toys3/sim/simulator.go:43-45`). In the simulator, `restart` calls the factory on every boot (`toys3/sim/simulator.go:310-326`).

The line `var _ Factory = NewMachine` is a compile-time assertion that `NewMachine` has the `Factory` signature. You will see the same idiom (`var _ control.Env = Env{}`, `var _ View = (*Simulator)(nil)`) throughout the code. detcheck explicitly permits blank `_` package-level variables (`tools/detcheck/detcheck.go:142-144`) for exactly this reason.

## 2.8 The same `Step`, two runtimes

Here is the whole point of the architecture in one diagram. The box in the middle is byte-for-byte the same code in both worlds.

```
 PRODUCTION (toys3/prod, Chapter 9)                 SIMULATION (toys3/sim, Chapter 10)

 HTTP handler goroutines                            Workload (simulated clients)
   │ Submit(op) ─► inbox (chan)                       │ wake ─► schedule Deliver(Request)
   ▼                                                  ▼
 Loop.Run goroutine:                                Simulator.Run (one goroutine):
   at := time.Since(start)                            e := queue.Pop(); now = e.At
   acts := m.Step(Event{at, ev})   ◄── SAME ──►       acts := server.Step(Event{now, body})
   for a in acts:                                     for a in acts:
     Reply ─► waiting handler                           Reply ─► Oracle.Return, Workload.Complete
     FS*   ─► OSExecutor: goroutine + os call           FS*   ─► PlantModel.Plan ─► schedule fs-land
              ─► done ─► inbox                                     ─► Land ─► schedule deliver
     Emit  ─► log line                                  Emit  ─► telemetry timeline (invariants)
     Trip  ─► Run returns ErrTripped; exit               Trip  ─► crash ─► restart via Factory
```

In production, `Loop.Run` stamps `At` with a monotonic reading (`at := control.Time(time.Since(l.start))`, `toys3/prod/loop.go:65`). In simulation, `callStep` stamps it with the virtual clock (`s.server.Step(control.Event{At: s.now, Body: body})`, `toys3/sim/simulator.go:226`). The server cannot tell which world it is in.

## 2.9 The fifteen rules of `loop.md`, one by one

`loop.md` is a one-page design guide written for the larger Manifester project (a Kafka-compatible broker). toys3's design document says it "follows `../loop.md` strictly" (`toys3/DESIGN.md:7`). Let us check that claim rule by rule. Where toys3 deviates, the deviation is usually because toys3 is simpler than a broker (no network, no timers), and it is worth knowing which rules are load-bearing and which are about problems toys3 does not have.

### Rule 1: Separate logic from I/O

> **Separate logic from I/O.** Core packages (`broker`, `partition`, `groupcoord`) never import `net`, `os`, `time`, `math/rand`, or the AWS SDK. Enforce this in CI with a lint check such as `depguard`.
> (`loop.md:64-66`)

**In toys3:** `toys3/control` imports, outside tests, no standard-library package at all; its only import is `manifester/toys3/format`. The four standard packages the deterministic core needs live in the format package:

```go
// toys3/format/object.go:3-6
import (
	"encoding/binary"
	"hash/crc32"
)
```

```go
// toys3/format/layout.go:3-6
import (
	"strconv"
	"strings"
)
```

`controller.go`, `recovery.go` and `hash.go` import exactly one package, `manifester/toys3/format` (the on-disk contract). The enforcement is stronger than the rule asks for: instead of `depguard` with a *deny* list, the repository uses its own analyzer, `tools/detcheck`, with an *allow* list (`tools/detcheck/allow.txt`), which fails closed: a new import is rejected until someone adds it deliberately. The decision is recorded as decision I in `docs/decisions.md`. `scripts/ci-determinism.sh:12-20` runs it on every toys3 package except `prod` and the commands (today `toys3/control`, `toys3/format` and `toys3/sim`); the list is derived with `go list`, so a new pure package cannot be forgotten. Chapter 8 covers detcheck in detail, including an amusing bug it had with Go's generated test mains.

Note that the allow-list contains `hash/crc32` but not `hash/fnv`. That is why the controller carries its own four-line FNV-1a (`toys3/control/hash.go:34-56`, with the comment "hash/fnv is not on the determinism allow-list, and it is four lines").

### Rule 2: Use the `Step(Event) []Action` shape

> **Use the `Step(Event) []Action` shape** for every stateful component. State changes happen only inside `Step`.
> (`loop.md:67-68`)

**In toys3:** `control.StateMachine` (`toys3/control/machine.go:5-7`) and `Controller.Step` (`toys3/control/controller.go:109-133`). Every mutation of `Controller` fields happens in a function reachable only from `Step` (the constructor `New` sets the initial phase, and that is all). `StateHash` reads but never writes.

Note what is *not* a state machine in toys3: the simulated clients (`sim.Workload`) and the plant model (`sim.PlantModel`) are ordinary objects with methods, driven directly by the simulator. `docs/lessons.md` §10 argues that in the larger system the workload should itself be a sans-I/O machine, because it will speak Kafka bytes; toys3's workload never crashes and needs no runtime, so it does not bother (the comment at `toys3/sim/workload.go:42-43` says exactly that).

### Rule 3: Events and Actions are plain data

> **Make Events and Actions plain data:** structs without channels, funcs or pointers into shared state.
> (`loop.md:69-70`)

**In toys3:** see §2.5. The comment on `EventBody` restates the rule (`toys3/control/event.go:10-12`). The only reference types inside events and actions are `[]byte` and `[]string` (`FSListDone.Names`), governed by the "immutable once sent" convention, and the `Op` and `Telemetry` interface fields, whose dynamic values are themselves plain structs.

### Rule 4: Inject every source of nondeterminism through `Env`

> **Inject every source of nondeterminism** through a small `Env` interface: `Now()`, `Rand()`, `NewID()`. Never call `time.Now()` or `uuid.New()` directly.
> (`loop.md:71-72`)

**In toys3, with two deliberate deviations.** The `Env` interface has randomness and buggify, but **no `Now()` and no `NewID()`**:

```go
// toys3/control/env.go:3-8
// Env is injected into the server at construction. It is the only way
// nondeterminism enters the control. Time is NOT here: it arrives on each Event.
type Env interface {
	Rand
	Buggify
}
```

- **Time arrives on the event** instead of being pulled with `Now()`. This is decision 2 in `docs/decisions.md` ("No `Now()` in the core") and lesson 2 in `docs/lessons.md`. The advantage: within one `Step`, time is a constant. A `Now()` call could return different values at the top and bottom of the same step, and the runtime would have to make sure the clock never runs backwards across steps. Stamping on dequeue gives monotonicity for free (`toys3/control/event.go:3-4`: "so At never decreases across successive Steps of one incarnation").
- **IDs come from counters**, not from `NewID()`. The server assigns IOIDs with `s.nextIO++` (`toys3/control/controller.go:135-138`) and the runtime assigns `ClientReq` values (`toys3/control/types.go:17-20`). Counters are deterministic for free.

A surprise worth recording now (Chapter 3 returns to it): **the server never uses its `Env` at all.** `Controller.env` is stored (`toys3/control/controller.go:88`, `105`) and never read. There are no buggify call sites anywhere in `toys3/control`. The unit-test environment says so in a comment: "a fixed control.Env (the server does not draw randomness today)" (`toys3/control/controller_test.go:14`). The plumbing exists so that adding a buggify point later is a one-line change.

### Rule 5: Wrap external systems in narrow interfaces you own

> **Wrap external systems in narrow interfaces you own,** such as `ObjectStore{Put, Get, List, Delete}` ... Never let SDK types leak outside the adapter.
> (`loop.md:73-75`)

**In toys3:** the "narrow interface you own" is the **plant port**: the set of FS actions `FSWrite`, `FSFsync`, `FSRename`, `FSFsyncDir`, `FSRead`, `FSUnlink`, `FSList` (`toys3/control/plant.go:56-101`), their completions, and the two interfaces that name the groups, `Actuation` and `Feedback` (`toys3/control/plant.go:8-20`). The port is defined in package `control` and owned by the controller; the adapters conform to it. That ownership is the point of rule 5: the controller's language is not dictated by whichever filesystem API happens to implement it. Seven operations, each with whole-file semantics: there is no `open`, no file descriptor, no offset, no partial write. That narrowness is a design choice with consequences (Chapter 17 discusses what it cannot model, like interleaved torn writes within a file). Errors cross the boundary as a three-value `ErrCode` (`OK`, `ErrNotFound`, `ErrIO`, `toys3/control/plant.go:29-35`), never as a Go `error`; the production adapter does the classification in one place (`code`, `toys3/prod/executor.go:103-113`).

In production, the Go-level interface is `prod.Executor`:

```go
// toys3/prod/executor.go:14-20
// Executor performs filesystem actions. Execute must not block the loop: it
// starts the operation and, when it finishes, calls done exactly once with
// the matching completion (control.FSWriteDone for control.FSWrite, and so on),
// from any goroutine.
type Executor interface {
	Execute(a control.Action, done func(control.EventBody))
}
```

Note that this interface lives in `prod`, not in `control`. The simulator does *not* implement `Executor`; it interprets the same actions with `PlantModel.Plan` and `PlantModel.Land`. This matches `docs/lessons.md` §0: "The simulator does **not** implement them. It implements *engines* that own state, draw from the single seeded RNG, and schedule completions on one global queue."

### Rule 6: Make every I/O operation asynchronous

> **Make every I/O operation asynchronous:** an action goes out and a `...Done{id, result, err}` event comes back. Nothing in the logic ever blocks.
> (`loop.md:76-77`)

**In toys3:** every FS action has a matching completion: `FSWrite` → `FSWriteDone`, ..., `FSList` → `FSListDone` (`toys3/control/plant.go:103-149`). The `Action` doc spells out the contract: "Actions in one returned slice are started in order, but the filesystem actions among them may complete in any order" (`toys3/control/action.go:3-6`). In the seed-1 trace above, step 16 emitted two `FSRead`s (`io3` for `k2`, `io4` for `k0`) and `io4` landed first (step 17) even though it was issued second.

### Rule 7: Timers are actions too

> **Timers are actions too:** `SetTimer{id, d}` goes out and `TimerFired{id}` comes back.
> (`loop.md:78-79`)

**In toys3: not needed, and therefore absent.** There is no `SetTimer` action and no `TimerFired` event. `toys3/DESIGN.md:84-85` explains: "No timers are needed: every wait is on an FS completion." The server has no timeouts, no retries and no batching windows. If an FS operation never completes, the request never completes; that is a liveness bug, and the simulator would catch it with the `Liveness` invariant (Chapter 14), because the PlantModel always completes every operation that is not dropped by a crash.

Adding a timeout would be the first place this rule bites. Exercise 5 at the end of the chapter asks you to sketch it.

### Rule 8: Give every request a correlation ID

> **Give every request a correlation ID** so late or duplicate completions can be recognized and ignored. The simulation will deliver these.
> (`loop.md:80-81`)

**In toys3:** two kinds of correlation ID.

- `IOID` correlates an FS action with its completion (`toys3/control/plant.go:22-25`). Every completion is routed by IOID, and anything unrecognized is dropped:

```go
// toys3/control/controller.go:119-132
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
```

  Even a completion with the right IOID but the wrong *kind* is ignored: each step checks the body type with `d, ok := body.(FSWriteDone); if !ok { return nil }` (for example `toys3/control/controller.go:300-303`). `TestStaleIOIgnored` (`toys3/control/controller_test.go:410-432`) feeds five kinds of bogus completion and asserts the state hash is unchanged.
- `ClientReq` correlates a `Request` with its `Reply` (`toys3/control/types.go:17-20`). The server echoes it.

A subtle point: IOIDs are unique only *within one incarnation* (each new `Controller` starts `nextIO` at 0). That is safe because the runtime never delivers a completion to a later incarnation: the simulator drops every queued entry of a dead incarnation (`Queue.DropIncarnation`, `toys3/sim/queue.go:101-122`), and in production a tripped loop's `post` discards completions after `Run` returns (`toys3/prod/loop.go:101-108`). It also has a consequence for temp-file names, which are derived from IOIDs; Chapters 5 and 15 show what goes wrong when recovery does not clean up.

Does the simulation actually "deliver these" late or duplicate completions, as the rule says? Late, yes: an FS action's completion can be delivered long after others issued later. Duplicates, no: the PlantModel delivers each completion exactly once (or never, after a crash). Chapter 17 lists duplicate delivery as a possible extension.

### Rule 9: Put all durable state in the store

> **Put all durable state in the store.** A broker must fully rebuild from `ObjectStore` plus `MetaStore` after a crash, and anything in memory is treated as disposable.
> (`loop.md:82-84`)

**In toys3:** the "store" is the data directory. The server keeps no durable state anywhere else, and rebuilds nothing on boot except cleaning `tmp/`:

```go
// toys3/control/controller.go:74-75
// Controller is the toys3 state machine: the control law of the object
// server. It turns client requests and plant feedback (filesystem
```

Recovery (`onStarted` and `onRecoveryDone`, `toys3/control/recovery.go:17-86`) lists `tmp/`, unlinks everything, fsyncs `tmp/`, and starts serving. `obj/` needs no recovery because the protocol only ever renames complete, fsynced files into it (`toys3/control/doc.go:104-113`). Chapter 5 covers recovery in depth.

### Rule 10: Keep the production runtime thin

> **Keep the production runtime thin** (`cmd/broker`): accept on TCP → decode → inbox channel → `Step` → execute actions in goroutines → feed results back into the inbox. It shouldn't contain any business logic.
> (`loop.md:85-87`)

**In toys3:** `toys3/prod` is about 430 lines of non-test code (560 with its tests). `Loop.Run` (`toys3/prod/loop.go:60-75`) is a fifteen-line loop; `dispatch` (`toys3/prod/loop.go:78-99`) is a type switch with four cases; `OSExecutor.run` (`toys3/prod/executor.go:42-72`) maps each action to one or two `os` calls. The HTTP handler translates paths and headers into `control.Op` values (`ParseRequest`, `toys3/prod/http.go:81-107`) and does nothing else.

Is there *any* business logic in `prod`? Two borderline items: `ParseRequest` rejects malformed paths with 400 before the controller sees them, and `PrepareDir` creates `obj/` and `tmp/` (`toys3/prod/executor.go:115-125`), because the controller has no `mkdir` action. Both are input/environment adaptation rather than protocol logic. Chapter 9 walks through the whole package.

### Rule 11: Build the simulator as one goroutine with a priority queue

> **Build the simulator** (`sim/`) as one goroutine with a priority queue ordered by `(time, seq, rngTie)`, one `*rand.Rand` seeded from `-seed`, and a virtual clock.
> (`loop.md:88-90`)

**In toys3, with one deliberate change in the ordering.** The queue is a binary heap ordered by `(At, Tie, Seq)`, not `(time, seq, rngTie)`:

```go
// toys3/sim/queue.go:127-136
func (q *Queue) less(i, j int) bool {
	a, b := &q.items[i], &q.items[j]
	if a.At != b.At {
		return a.At < b.At
	}
	if a.Tie != b.Tie {
		return a.Tie < b.Tie
	}
	return a.Seq < b.Seq
}
```

The order matters. With `(time, seq, rngTie)`, the insertion sequence number would decide every same-instant pair, and the random tie would never be consulted (sequence numbers are unique). With `(At, Tie, Seq)`, the seeded random tie decides same-instant order, so different seeds explore different orders, and `Seq` is only a final tie-break in the astronomically unlikely case of two equal 64-bit ties. `TestQueueTieBreakDependsOnSeed` (`toys3/sim/queue_test.go:34-66`) checks that same-instant order actually varies across seeds. Record this as a small discrepancy between `loop.md` and the code; `toys3/DESIGN.md:134` gets it right.

The RNG is not `*rand.Rand` (which detcheck forbids) but `sim.Rand`, a hand-written SplitMix64 with a draw counter (`toys3/sim/rand.go`). It is seeded from `Config.Seed`, which `TestSeeds` takes from the `-seed` flag (`toys3/sim/sim_test.go:13`). The virtual clock is `Simulator.now`, set from each popped entry (`toys3/sim/simulator.go:94`). Chapter 10 covers all of this.

### Rule 12: Make the fakes adversarial

> **Make the fakes adversarial:** random latency, 503s, timeouts, a PUT that succeeds but whose response is lost, duplicated responses, reordering, network partitions between AZs, and crash plus restart at any event.
> (`loop.md:91-93`)

**In toys3** the "fake" is the plant model, `sim.PlantModel`, and its adversarial behaviours are configured by `sim.Disturbances`: in control vocabulary, a *disturbance* is an input that acts on the plant and that the controller cannot command. They are translated from object-store faults into filesystem faults (`toys3/sim/plantmodel.go:12-49`):

| `loop.md` fault | toys3 equivalent |
|---|---|
| random latency | dead time (command → effect) and feedback delay (effect → completion), drawn per op as `DeadTime` and `FeedbackDelay` (`PlantModel.Plan`, `toys3/sim/plantmodel.go:146-152`) |
| 503s | `EIOProb`: any op fails with `ErrIO`; a failed write may leave a prefix, a failed rename or unlink may or may not have applied |
| succeeds but response lost | a crash between `fs-land` and `deliver` (Chapter 10) |
| reordering | independent delays per op; completions arrive in any order |
| crash plus restart at any event | `CrashProb` after every server step and every landing (`maybeCrash`, `toys3/sim/simulator.go:266-270`), plus `Trip` |
| (filesystem-specific) | torn writes (`TornWriteProb`), un-synced loss, background writeback (`WritebackProb`), read bit flips (`ReadCorruptProb`), fsyncgate (`FsyncGate`) |

Not modelled: duplicated responses, timeouts (there are no timers to time out) and partitions (there is no network). Chapter 11 is entirely about the PlantModel.

### Rule 13: Check invariants after every step

> **Check invariants after every step, not just at the end.**
> (`loop.md:94-96`)

**In toys3:** `afterStep` runs every invariant's `AfterStep` after every simulator step (`toys3/sim/simulator.go:339-354`), and `AtEnd` once at the end (`toys3/sim/simulator.go:103-109`). The `Invariant` interface documents why:

```go
// toys3/sim/invariant.go:53-60
// Invariant is checked after every simulator step and once at the end of the
// run. Checking every step (not just at the end) reports a bug at the step
// that caused it, which is what makes the event log readable.
type Invariant interface {
	Name() string
	AfterStep(v View) []Failure
	AtEnd(v View) []Failure
}
```

Two invariants deserve a footnote: `Linearizable` only does real work at quiescent points (when no request is outstanding), because a linearizability check of a window with pending operations is ill-defined; and `ObjNeverTorn` skips steps where nothing durable changed (`toys3/sim/invariant.go:169-174`). Chapter 14 covers each invariant.

### Rule 14: Hash the state after every step; run each seed twice

> **Hash the state after every step** into a running trace hash, and run each seed twice in CI. If the hashes differ, nondeterminism has leaked in.
> (`loop.md:97-98`)

**In toys3:** the trace hash folds the step number, time, entry kind and incarnation before each step, every event and action, the server's `StateHash` after each delivery, and the PlantModel hash after every step (`toys3/sim/simulator.go:95-97`, `172-183`). `TestDeterminismCanary` runs seeds 1 to 12 twice each and compares trace hash, RNG draw count and step count (`toys3/sim/sim_test.go:24-42`); `scripts/ci-determinism.sh:36-45` runs it in CI; and the campaign runner replays every tenth seed with `-check-determinism` (`toys3/cmd/toys3sim/main.go:76-83`). Chapter 8.

### Rule 15: Make failures one command

> **Make failures one command:** `go test ./sim -seed=81237` reproduces the failure and prints the event log. In CI, run many seeds (for example 10k per PR), and keep failing seeds as regression tests.
> (`loop.md:99-101`)

**In toys3, mostly.** The command is `go test ./toys3/sim -run TestSeeds -seed=N -v`; it replays seed N with logging on and prints the full event log to stderr if any invariant fails (`toys3/sim/sim_test.go:47-59`). Every failure message carries its own repro command (`repro`, `toys3/sim/sim_test.go:17-19`; and `toys3/cmd/toys3sim/main.go:57`).

Two parts of the rule are not implemented: CI runs 200 seeds (the default of `-seeds`, `toys3/sim/sim_test.go:12`), not 10,000, as part of `go test ./...` in `.github/workflows/determinism.yml`; and there is no list of "failing seeds kept as regression tests". For a correct server that never fails, the second is moot, but if you find a real bug, Chapter 16 suggests how to pin its seed.

### Scorecard

```
rule  topic                              toys3
 1    logic/IO separation, CI lint       YES (stricter: allow-list detcheck)
 2    Step(Event) []Action               YES (server only; workload/plant model are plain objects)
 3    plain-data events/actions          YES (+ "byte slices immutable once sent" convention)
 4    Env for nondeterminism             PARTIAL by design: time on Event, IDs from counters;
                                         Env exists but the server never uses it
 5    narrow owned interfaces            YES (plant port: 7 actuations, 7 feedbacks, 3 ErrCodes)
 6    async I/O with ...Done events      YES
 7    timers as actions                  N/A (no timers needed)
 8    correlation IDs                    YES (IOID per incarnation, ClientReq); no duplicate injection
 9    durable state only in the store    YES (memory rebuilt from nothing)
10    thin production runtime            YES
11    one-goroutine sim, PQ, seeded RNG  YES, but ordered (At, Tie, Seq), not (time, seq, rngTie)
12    adversarial fakes                  YES (plant model + disturbances), no duplicates/partitions
13    invariants every step              YES
14    trace hash, run seeds twice        YES
15    one-command repro                  YES; 200 seeds in CI, no pinned regression seeds
```

## 2.10 One `Step`, read top to bottom

To close the chapter, read the dispatcher one more time with the architecture in mind:

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

- Line 110: once tripped, the server is deaf. A runtime should never deliver anything after `Trip`, but if it does, nothing happens.
- Lines 113-118: the two non-I/O events. `Started` begins recovery; `Request` is validated and queued.
- Lines 119-123: everything else must be **feedback from the plant**, an FS completion implementing `control.Feedback`, whose `ID()` is the IOID of the command it answers. Anything that is not is ignored (there is nothing else in the closed set, so this branch is defensive).
- Lines 124-126: recovery completions are routed to the recovery sub-machine.
- Lines 127-131: request completions are routed to the request waiting on that IOID. Note `r.step != stepWaiting`: a queued request's `io` field is 0 and must not match anything.
- Line 132: no owner, no effect.

Notice what is absent: `At` is never read. The server's behaviour does not depend on time at all. It is stamped on every event because the architecture requires it and because a future timeout would need it, but today toys3's correctness is entirely about *order*, not *time*. This is a useful thing to know when reading traces: the timestamps tell you what the simulator decided, not what the server computed.

## 2.11 The same shape, in control-theory terms

Everything in this chapter was argued from software-engineering first principles: separate logic from I/O, make I/O asynchronous, make the scheduler a seeded queue. There is an older discipline that arrived at the same shape for its own reasons, and toys3's names come from it.

A *feedback control loop* has a **controller** that computes commands, a **plant** (the physical process being controlled) that receives them through **actuators**, and **sensors** that report the plant's state back as **feedback**. The controller runs as a *discrete-time* update: at each sampling instant `k` it reads its inputs and computes `x[k+1] = f(x[k], u[k])` and its outputs. That is `Step(Event) []Action`, letter for letter: the controller's state is the `Controller` struct, the input `u[k]` is the event, `Event.At` is the sampling instant, and the outputs are the actions.

```
   requests (operator) ─────┐            disturbances: EIO, torn writes, bit flips,
                            ▼            writeback, power loss
                 ┌──────────────────────┐            │
                 │      CONTROLLER      │ actuations ▼
                 │  control.Controller  │──(FS*)────────►┌───────────────────────┐
                 │  Step(Event)[]Action │                │         PLANT         │
                 │                      │◄──(FS*Done)────│  disk + page cache    │
                 └──┬────────────┬──────┘  feedback      │  prod: OSExecutor     │
                    │            │                       │  sim:  PlantModel     │
     replies, or    │            │ telemetry             └───────────────────────┘
     Trip (fail-    ▼            ▼ (Committed, Recovered, CorruptionDetected)
     stop)       clients      log (prod) / monitors (sim)
```

Read the sans-I/O argument again with this picture: the controller "never does I/O" because no controller does; it commands actuators and reads feedback. The "land/deliver split" of §2.4 is the plant's **dead time** (command to effect) followed by its **feedback delay** (effect to report). A crash is a **disturbance**. And the simulator is what control engineers call a *software-in-the-loop* (SIL) testbench: the real controller code, closed around a *model* of the plant instead of the real one.

This is not a coat of paint. Each of those names now appears in the code (`control.Controller`, `control.Actuation`, `control.Feedback`, `sim.PlantModel`, `sim.Disturbances`, `DeadTime`, `FeedbackDelay`, `control.Trip`, `control.Telemetry`), and each one was chosen because the concept was already there, unnamed. Equally important are the places where a control name was considered and *rejected*: requests are not "setpoints" (a GET does not set anything), linearizability has nothing to do with linearization, and the CRC is not an "integrity sensor". Chapter 3a, after the vocabulary chapter, works through the whole domain model: subdomains, bounded contexts, the aggregate, the ports, and the metaphor, end to end, including where and why it stops.

## Key takeaways

- A synchronous I/O interface (even a mocked one) cannot express "the effect landed, then the process died before the caller learned of it". An asynchronous Action/Event vocabulary can, as two separate scheduled moments.
- In the sans-I/O shape, concurrency lives in data (records of in-flight requests keyed by IOID), not in threads. Whoever calls `Step` decides the interleaving.
- Events and Actions are plain data so they can be logged, hashed, compared, replayed and rewritten by mutants. Byte slices are a reference type, so "immutable once sent" is a convention backed by copying at the fake's boundary.
- `Factory` makes "memory is disposable" structural: a crash throws the object away.
- The vocabulary between the controller and the disk is a port the controller owns (`Actuation`, `Feedback`), with two adapters: the real `OSExecutor` and the simulated `PlantModel`.
- toys3 follows `loop.md` closely. Its deviations (time on events instead of `Now()`, counters instead of `NewID()`, no timers, `(At, Tie, Seq)` ordering) are each either simplifications or improvements.

## Exercises

1. **Read it as a state machine.** Take the eleven-line `Put` from §2.1 and list every local variable whose value must survive across a blocking call. Find the corresponding field of `request` in `toys3/control/controller.go:51-61`. Which field has no counterpart in the straight-line version, and why does the state machine need it? (Hint: `step`.)
2. **Prove stale completions are harmless.** Run `go test ./toys3/control -run TestStaleIOIgnored -v`. Then add a sixth case to the list at `toys3/control/controller_test.go:416-422` of your own invention (for example an `FSRenameDone` with the IOID of the outstanding write). Predict whether `StateHash` changes. Run it. Revert your change afterwards.
3. **Break rule 3 on purpose.** In a scratch copy of the repo, add a field `Callback func()` to `control.Reply` and run `go test ./toys3/sim -run TestDeterminismCanary`. Does anything fail? Now look at `hashAction` (`toys3/sim/trace.go:59-83`): would the callback contribute to the trace hash? What does this tell you about which rules are enforced by tooling and which only by convention?
4. **Same-instant order.** Run `go test ./toys3/sim -run TestQueueTieBreakDependsOnSeed -v`. Then, in a scratch copy, change `Queue.less` to compare `Seq` before `Tie` (the `loop.md` order). Run `go test ./toys3/sim -run 'TestQueue|TestSeeds' -seeds=50`. Which tests fail (there are two), and why? Would any mutant in `mutants_test.go` become harder to catch? (Think about which bugs depend on two events at the same virtual instant.)
5. **Design a timer.** Suppose toys3 should reply 503 to any request that has waited more than 50 ms of virtual time. Following rules 6 and 7, write down: the new Action, the new Event, which fields of `request` change, what `Step` does when the timer fires *after* the request already completed, and what `DropIncarnation` must do with timer entries. Do not implement it; Chapter 17 suggests it as a real extension.
6. **Find the clock.** `grep -n "At" toys3/control/controller.go`. Confirm the server never reads `ev.At`. Then find the two places where `At` is stamped (`toys3/prod/loop.go` and `toys3/sim/simulator.go`).

## Further reading

- The sans-I/O documentation project (https://sans-io.readthedocs.io), "Writing I/O-Free Protocol Implementations", and the `h11` HTTP/1.1 library as its canonical example.
- `docs/lessons.md` in this repository, §0 (why Go needs sans-I/O) and §10 (Process / Workload / Invariant roles).
- `docs/decisions.md` in this repository, especially decisions 1, 2 and I, and the threading-model diagrams.
- The `madsim` and `turmoil` projects (Rust), which take the opposite approach: keep blocking/async code and replace the executor and I/O layer. A useful contrast with the Go constraints in §2.3.
- Will Wilson, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014, for FoundationDB's Flow actor model, which plays the role that the explicit state machine plays here.
- Pierre Zemb, *The Moonpool Sim Book* (https://pierrez.github.io/moonpool/), the "provider pattern" chapters.
