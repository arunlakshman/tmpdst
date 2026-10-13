# Chapter 9: The Production Runtime

> *"The production runtime should be so boring that you would be embarrassed to find a bug in it."*

Everything so far has been about a state machine that never does anything. `control.Controller.Step` receives an Event and returns a slice of Actions; it never opens a file, never reads a clock, never starts a goroutine. That is what made it possible to run it inside a simulator. But a storage server that never stores anything is not much use. Somebody has to actually call `open(2)`, `write(2)`, `fsync(2)` and `rename(2)`, accept HTTP connections, and hand replies back to waiting clients.

That somebody is `toys3/prod`: about 430 lines of Go (including its doc comment) that turn the pure controller into a real HTTP object server. This chapter reads all of it. It is short, and it is meant to be. `loop.md` rule 10 says:

> **Keep the production runtime thin** (`cmd/broker`): accept on TCP → decode → inbox channel → `Step` → execute actions in goroutines → feed results back into the inbox. It shouldn't contain any business logic.

The reason is not aesthetic. The production runtime is the one part of the system that **the simulator never runs**. Every line in it is tested only by ordinary tests and by real use. So every line that can live in the controller instead (and be tested by millions of simulated crashes) should. What is left in `prod` should be a mechanical translation between the controller's vocabulary (Chapter 3) and the operating system's.

---

## 9.1 The shape

The package doc draws the whole runtime:

```go
// toys3/prod/doc.go:1-19
// Package prod is the production runtime for toys3: the thin shell that does
// the real I/O the controller (package control) only asks for. It contains no
// business logic: OSExecutor is the real plant adapter (actuations in,
// feedback out) and Handler is the anti-corruption layer from S3 REST to
// control.Op.
//
//	HTTP handlers ──Request──┐                   ┌──► goroutine: os.WriteFile ... ──┐
//	                         ▼                   │                                  │
//	                    ┌─────────┐  Step()  ┌───┴──────┐                           │
//	                    │  inbox  │ ───────► │ Executor │ ──► Reply → waiting handler
//	                    │ (chan)  │          └──────────┘                           │
//	                    └─────────┘ ◄────────── FS...Done completions ──────────────┘
//
// One goroutine (Loop.Run) owns the control.Controller and calls Step one event
// at a time: the event loop is the control loop. Actuations (filesystem
// actions) run concurrently in Executor goroutines and their feedback comes
// back through the inbox. This package may import os, net/http, time, etc.; it
// is deliberately not checked by detcheck and not run in simulation.
package prod
```

Four pieces, each in its own file:

| File | Piece | Role |
|---|---|---|
| `env.go` | `Env` | the production `control.Env`: real randomness, buggify always off |
| `executor.go` | `Executor`, `OSExecutor`, `PrepareDir` | performs FS actions with real `os` calls |
| `loop.go` | `Loop` | owns the machine, the inbox, and the map of waiting clients |
| `http.go` | `Handler`, `ParseRequest` | translates S3-style HTTP into `control.Op`s and replies back |

And one binary that wires them together: `toys3/cmd/toys3/main.go`.

Compare this with the simulator's main loop (Chapter 10). They have exactly the same structure, with different implementations of each box:

```
                   production (toys3/prod)                 simulation (toys3/sim)
  event source     HTTP handlers + executor goroutines     Queue of Entries (At, Tie, Seq)
  inbox            buffered Go channel                     the same Queue
  time stamp       time.Since(start), monotonic            virtual clock = Entry.At
  Step caller      Loop.Run goroutine                      Simulator.deliver
  FS actions       OSExecutor: goroutine + os.*            PlantModel.Plan + fs-land entry
  Reply            channel to a waiting HTTP handler       Oracle.Return + Workload.Complete
  Emit             log line                                telemetry timeline for invariants
  Trip             Run returns ErrTripped; process exits   crash + scheduled restart
  randomness       math/rand/v2                            the run's single sim.Rand
```

The controller cannot tell which column it is in. That is the entire point of the sans-I/O architecture (Chapter 2).

The refactor of Chapter 3a rewrote this package's doc comment in the domain model's terms, and each phrase names a role:

- **"OSExecutor is the real plant adapter (actuations in, feedback out)."** The controller owns the plant port (`Actuation`, `Feedback`, Chapter 3 §3.5); `OSExecutor` is one of its adapters, the one attached to a real disk. The simulator's `PlantModel` is the other. Both *conform* to the controller's vocabulary: neither introduces a word of its own.
- **"Handler is the anti-corruption layer from S3 REST to control.Op."** In DDD an anti-corruption layer translates a foreign model into yours so that the foreign one never leaks in. Nothing about HTTP (methods, headers, URL escaping, buckets) crosses into the controller; it receives `Put`, `Get`, `Delete` and `Copy` values (§9.6).
- **"The event loop is the control loop."** In control engineering a digital controller runs a loop: sample the inputs, compute, write the outputs, wait for the next sample. `Loop.Run` is exactly that: take the next event, stamp its sampling instant, `Step`, dispatch the actions. Here the two meanings of "loop" coincide, which is why `Loop` kept its name.

## 9.2 Env: the boring one

```go
// toys3/prod/env.go:9-16
// Env is the production control.Env: real randomness, buggify always off.
type Env struct{}

func (Env) Uint64() uint64          { return rand.Uint64() }
func (Env) Intn(n int) int          { return rand.IntN(n) }
func (Env) Maybe(control.Site) bool { return false }

var _ control.Env = Env{}
```

`rand` here is `math/rand/v2`, whose top-level functions are seeded randomly per process and safe for concurrent use. Production randomness does not need to be reproducible; it needs to be random.

`Maybe` always returns `false`. This is FoundationDB's buggify rule: rare-but-legal paths are taken *only* in simulation, never in production. As Chapter 3 noted, the toys3 server has no buggify sites today, so this method is never called. It is there so that the day someone adds one, production is safe by construction.

`Env{}` is an empty struct, so `var _ control.Env = Env{}` costs nothing: it is a compile-time check that `Env` still satisfies the interface if the interface changes.

## 9.3 The Executor: where the system calls live

### The contract

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

Three obligations, each one essential:

1. **Must not block the loop.** `Execute` is called from the loop goroutine, inside `dispatch`. If it did the `fsync` inline, every other request would wait behind it. The whole point of the async action/completion split (`loop.md` rule 6) is that the loop never waits.
2. **Exactly once.** The controller's request record waits for exactly one completion with its IOID (Chapter 6). Zero completions means a request that hangs forever. Two completions would be harmless in the toys3 core (the second is a stale IOID and is ignored, `toys3/control/controller.go:132`), but "exactly once" is the clean contract.
3. **The matching completion.** An `FSWrite` must complete with an `FSWriteDone` carrying the same IOID. The controller ignores a completion "of the wrong kind for the step the owner is on" (`toys3/control/doc.go:110-111`), so a mismatched type would also hang the request.

Making `Executor` an interface (rather than calling `OSExecutor` directly) buys one thing: tests can substitute a faulty executor. `prod_test.go` does exactly that to exercise `Trip` (§9.8).

### OSExecutor

```go
// toys3/prod/executor.go:22-39
// OSExecutor runs each action in its own goroutine with real os calls:
//
//	FSWrite     os.OpenFile(O_WRONLY|O_CREATE|O_TRUNC, 0o644) + Write + Close
//	FSFsync     os.Open + File.Sync + Close
//	FSRename    os.Rename
//	FSFsyncDir  os.Open(dir) + File.Sync + Close
//	FSRead      os.ReadFile
//	FSUnlink    os.Remove
//	FSList      os.ReadDir, names sorted
//
// Errors map to control.ErrCode: errors.Is(err, fs.ErrNotExist) → ErrNotFound,
// anything else → ErrIO.
type OSExecutor struct{}

// Execute implements Executor.
func (OSExecutor) Execute(a control.Action, done func(control.EventBody)) {
	go func() { done(run(a)) }()
}
```

One goroutine per action. This is the simplest possible non-blocking executor. Go goroutines are cheap enough that a toy server will not notice; a production-grade system would bound the number of concurrent filesystem operations (a worker pool, or simply a semaphore), because an unbounded burst of `fsync` calls on one disk does not get faster by being concurrent. Note that the *core* bounds the number of in-flight operations implicitly: at most one outstanding FS action per accepted, unfinished request (Chapter 6), and requests are bounded by the HTTP server's concurrency. So the runtime's lack of a bound is not a correctness problem, just a resource-management one.

The work itself is a type switch that mirrors the vocabulary one-to-one:

```go
// toys3/prod/executor.go:41-72
// run performs a synchronously and returns its completion.
func run(a control.Action) control.EventBody {
	switch a := a.(type) {
	case control.FSWrite:
		return control.FSWriteDone{IO: a.IO, Err: code(writeFile(a.Path, a.Data))}
	case control.FSFsync:
		return control.FSFsyncDone{IO: a.IO, Err: code(syncPath(a.Path))}
	case control.FSRename:
		return control.FSRenameDone{IO: a.IO, Err: code(os.Rename(osPath(a.From), osPath(a.To)))}
	case control.FSFsyncDir:
		return control.FSFsyncDirDone{IO: a.IO, Err: code(syncPath(a.Dir))}
	case control.FSRead:
		data, err := os.ReadFile(osPath(a.Path))
		if err != nil {
			return control.FSReadDone{IO: a.IO, Err: code(err)}
		}
		return control.FSReadDone{IO: a.IO, Data: data}
	case control.FSUnlink:
		return control.FSUnlinkDone{IO: a.IO, Err: code(os.Remove(osPath(a.Path)))}
	case control.FSList:
		entries, err := os.ReadDir(osPath(a.Dir)) // sorted by name
		if err != nil {
			return control.FSListDone{IO: a.IO, Err: code(err)}
		}
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		return control.FSListDone{IO: a.IO, Names: names}
	}
	panic("toys3/prod: OSExecutor got a non-filesystem action")
}
```

Read it against the action definitions in `toys3/control/action.go` (Chapter 3) and check each correspondence:

- **`FSWrite`** must "create Path, or truncate it if it exists, and write Data as the whole file content" (`toys3/control/plant.go:56-58`). That is exactly `O_WRONLY|O_CREATE|O_TRUNC`. It does **not** fsync; that is a separate action. The controller decides when durability is needed.
- **`FSFsync`** reopens the file by path and calls `Sync`. Opening read-only (`os.Open`) is sufficient: `fsync` flushes the *inode's* dirty data, not data written through a particular file descriptor, so any descriptor for the file works on Linux.
- **`FSFsyncDir`** is the same `syncPath` applied to a directory. Opening a directory with `os.Open` and calling `Sync` is how you fsync a directory in Go. This is the call that makes renames, creates and unlinks in that directory durable (Chapter 4).
- **`FSRename`** is `os.Rename`, which is `rename(2)`: atomically replaces the destination.
- **`FSRead`** returns the *whole raw file*, header included (`toys3/control/plant.go:130-131`). The CRC check is done by the controller, not here. If the executor validated the CRC, that logic would escape the simulator.
- **`FSUnlink`** is `os.Remove`, which is `unlink(2)` for a file.
- **`FSList`** returns base names in ascending order. `os.ReadDir` already sorts by file name. The controller relies on this sorting for determinism of recovery (`toys3/control/plant.go:144`): in simulation the PlantModel sorts too (`toys3/sim/plantmodel.go:423-438`), so recovery issues its unlinks in the same order in both worlds.

The panic at the end is for actions that are not filesystem actions. `Loop.dispatch` handles `Reply`, `Emit` and `Trip` itself and passes everything else to the executor, so if a new action type were added to the controller and forgotten here, the server would panic loudly rather than silently drop it.

A small asymmetry remains after the refactor: `Executor.Execute` still takes a plain `control.Action`, not a `control.Actuation`, so the type system does not say that only actuations reach it; the `default` panic says it at run time instead. Narrowing the parameter to `Actuation` would be a behaviour-neutral improvement (`dispatch` already routes every non-`Reply`, non-`Emit`, non-`Trip` action there), and a good small exercise.

Two small helpers do the system-call plumbing:

```go
// toys3/prod/executor.go:74-99
func osPath(p string) string { return filepath.FromSlash(p) }

func writeFile(path string, data []byte) error {
	f, err := os.OpenFile(osPath(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// syncPath fsyncs a file or a directory.
func syncPath(path string) error {
	f, err := os.Open(osPath(path))
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
```

Note the error handling on `Close`: the first error wins, but a `Close` error is not ignored when the operation itself succeeded. On some filesystems (notably network filesystems) `close` is where a deferred write error surfaces; dropping it would turn an I/O error into a silent success. This is the kind of detail that the simulator *cannot* check, because the simulator does not run this code. It has to be right by inspection.

On paths: the controller speaks slash-separated paths (`toys3/control/plant.go:51-54`), as plain data, so that event logs and hashes look the same on every OS. `osPath` converts at the boundary. On Unix, `filepath.FromSlash` is the identity.

On macOS, it is worth knowing that Go's `(*os.File).Sync` issues `fcntl(F_FULLFSYNC)` rather than plain `fsync`, because plain `fsync` on macOS does not ask the drive to flush its write cache. The toys3 code does not need to do anything special for this; it is one more reason to use `File.Sync` rather than a raw syscall.

### Mapping errors to the vocabulary

```go
// toys3/prod/executor.go:101-113
// code maps an os error to a control.ErrCode, logging real I/O errors (the core
// only sees the code).
func code(err error) control.ErrCode {
	switch {
	case err == nil:
		return control.OK
	case errors.Is(err, fs.ErrNotExist):
		return control.ErrNotFound
	default:
		log.Printf("toys3: io error: %v", err)
		return control.ErrIO
	}
}
```

The controller's error vocabulary has three values (`toys3/control/plant.go:31-35`): `OK`, `ErrNotFound` (ENOENT), and `ErrIO` ("EIO or anything else; the operation may or may not have taken effect"). `code` is the *only* place that classifies real errors, which is the pattern `docs/lessons.md` lesson 1 asks for ("Adapters classify SDK and HTTP errors ... in exactly one place").

The mapping is deliberately pessimistic. `EACCES`, `ENOSPC`, `EROFS`, `EIO`, `EINVAL`: everything other than "not found" is `ErrIO`, which the controller treats as "may or may not have taken effect" and, for any step at or after the first one that can touch `obj/`, answers with 500 and `Trip` (Chapter 7). A disk-full during the temp-file write is a plain 500 (`toys3/control/controller.go:304-308`); a disk-full during the directory fsync trips the server. That is conservative but correct: the controller cannot know whether `ENOSPC` from an `fsync` means the data is safe.

The original error message is logged here, because the controller only ever sees the code. That is the price of plain-data events: `control.ErrCode` is a `uint8`, comparable and hashable; a Go `error` is an interface value that is neither. Operators need the message; the state machine does not.

There is one subtle classification worth checking against the controller's expectations. For `FSRename`, `ErrNotFound` means "the rename source does not exist" (`toys3/control/plant.go:33`). `os.Rename` returns an error satisfying `fs.ErrNotExist` when the source is missing, so this maps correctly. The controller's `stepRenaming` handler treats *any* non-OK rename as `failTrip` (`toys3/control/controller.go:326-328`), so even a misclassification here would not cause a wrong 200.

### PrepareDir: the step the controller cannot take

```go
// toys3/prod/executor.go:115-125
// PrepareDir creates root/obj and root/tmp if missing and fsyncs root, so the
// directories are durable before the server's first Started (the controller
// has no mkdir action).
func PrepareDir(root string) error {
	for _, d := range []string{format.ObjDir(root), format.TmpDir(root)} {
		if err := os.MkdirAll(osPath(d), 0o755); err != nil {
			return err
		}
	}
	return syncPath(root)
}
```

The controller's vocabulary has no `FSMkdir`, and `control.Config.Dir`'s doc says `obj/` and `tmp/` "must already exist" (`toys3/control/controller.go:7`). The simulator's PlantModel creates both directories as durable at construction (`toys3/sim/plantmodel.go:128-143`). In production, `PrepareDir` runs once before the loop starts, creates them, and fsyncs `root` so that the two new directory entries survive a power cut.

This is a small piece of logic outside the controller, and therefore outside the simulator. It is tolerable because it is tiny, idempotent, and runs before anything else. It is also a known gap, discussed in §9.10: if `root` itself is newly created, its own entry in its parent directory is not fsynced.

## 9.4 The Loop: one goroutine owns the machine

### State

```go
// toys3/prod/loop.go:20-35
// Loop owns one control.Machine and is the only goroutine that calls Step.
type Loop struct {
	cfg     control.Config
	factory control.Factory
	exec    Executor
	inbox   chan control.EventBody
	start   time.Time     // At = time.Since(start), monotonic
	stopped chan struct{} // closed when Run returns

	// Log receives telemetry and lifecycle lines; nil means log.Default().
	Log *log.Logger

	mu      sync.Mutex
	nextReq control.ClientReq
	waiters map[control.ClientReq]chan control.Reply // one per in-flight HTTP request
}
```

Two kinds of state live here, with two different ownership rules:

- **The machine** (created inside `Run`) and the inbox's receive side belong exclusively to the `Run` goroutine. No lock is needed or used, because nothing else touches them. This is the production version of "one thread of control" (Chapter 1).
- **`nextReq` and `waiters`** are shared between HTTP handler goroutines (which register waiters in `Submit`) and the loop goroutine (which delivers replies in `dispatch`). They are protected by `mu`.

The inbox carries `control.EventBody` values, not `control.Event`s. The time stamp is added by the loop when the event is *dequeued*, not when it was produced. That matches the contract on `control.Event`: "The runtime stamps At when it dequeues the event, so At never decreases across successive Steps of one incarnation" (`toys3/control/event.go:3-4`). If producers stamped events, two handlers could stamp `t=5` and `t=4` and enqueue them in that order, and the controller would see time go backwards.

### Construction

```go
// toys3/prod/loop.go:37-49
// NewLoop returns a loop for one process lifetime. inboxSize bounds the
// number of queued events (backpressure on HTTP handlers and completions).
func NewLoop(cfg control.Config, factory control.Factory, exec Executor, inboxSize int) *Loop {
	return &Loop{
		cfg:     cfg,
		factory: factory,
		exec:    exec,
		inbox:   make(chan control.EventBody, inboxSize),
		start:   time.Now(),
		stopped: make(chan struct{}),
		waiters: map[control.ClientReq]chan control.Reply{},
	}
}
```

"For one process lifetime" is the key phrase. In the simulator, a crash destroys the machine and a restart calls the factory again with a new incarnation (Chapter 10). In production, a `Loop` *is* an incarnation: it builds one machine, runs it, and when the machine trips, the loop is done. Restarting means a new process (and so a new `Loop`). Taking a `control.Factory` rather than a `*control.Controller` keeps the shape identical to the simulator and lets tests plug in other machines.

`start` is `time.Now()`, which in Go carries a monotonic clock reading. `time.Since(l.start)` uses the monotonic reading, so `At` cannot jump backwards if the wall clock is adjusted (NTP step, manual change). The controller never interprets `At` as a wall time, and today it does not use `At` at all (it has no timers), but the contract is in place for when it does.

### Run

```go
// toys3/prod/loop.go:51-75
// Run builds the machine, delivers Started, and then steps every inbox event
// in order, dispatching the actions:
//
//	Reply     → the waiter registered by Submit (dropped if it gave up)
//	FS*       → exec.Execute, whose done callback posts to the inbox
//	Emit      → structured log line
//	Trip      → return an error; the process exits and a supervisor restarts it
//
// It returns when ctx is cancelled or on Trip. Run may be called once.
func (l *Loop) Run(ctx context.Context) error {
	defer close(l.stopped)
	m := l.factory(Env{}, l.cfg)
	var ev control.EventBody = control.Started{}
	for {
		at := control.Time(time.Since(l.start)) // monotonic clock reading
		if err := l.dispatch(m.Step(control.Event{At: at, Body: ev})); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev = <-l.inbox:
		}
	}
}
```

This is the `loop.md` event loop in eleven lines:

1. Build the machine with the production `Env`.
2. The first event is `Started`, delivered directly (not through the inbox), so it is guaranteed to be the first thing the machine sees. HTTP requests that arrive during recovery sit in the inbox, and when they are stepped the server queues them until recovery finishes (Chapter 5). Nothing is rejected.
3. Loop: stamp, step, dispatch, then block on the next inbox event or cancellation.

The `select` is the only place where nondeterminism enters the loop. If the context is cancelled while events are waiting, Go picks randomly between the ready cases, so the loop may process a few more events or stop immediately. That is fine in production and is exactly why `select` is banned in deterministic packages (Chapter 8).

`defer close(l.stopped)` signals every other goroutine that this incarnation is over. Two consumers care: `post` (which must stop delivering completions) and `Submit` (which must stop waiting for replies).

### dispatch

```go
// toys3/prod/loop.go:77-99
// dispatch executes the actions of one Step in order.
func (l *Loop) dispatch(acts []control.Action) error {
	for _, a := range acts {
		switch a := a.(type) {
		case control.Reply:
			l.mu.Lock()
			ch := l.waiters[a.Client]
			delete(l.waiters, a.Client)
			l.mu.Unlock()
			if ch != nil {
				ch <- a // buffered(1); exactly one reply per request
			}
		case control.Emit:
			l.logf("toys3: telemetry %T %+v", a.Telemetry, a.Telemetry)
		case control.Trip:
			l.logf("toys3: TRIP: %s", a.Cause)
			return fmt.Errorf("%w: %s", ErrTripped, a.Cause)
		default:
			l.exec.Execute(a, l.post)
		}
	}
	return nil
}
```

Actions are executed **in order**, which matters in two places:

- In the commit step, the controller returns `Emit{Committed}` then `Reply{200}` then possibly the first FS action of the next queued request (Chapter 6). The telemetry is logged before the client is answered.
- On an error that trips, the controller returns `Reply{500}` then `Trip` (`toys3/control/controller.go:264-268`). The 500 is handed to its waiter before `dispatch` returns `ErrTripped`. Anything after a `Trip` would never be executed, which is why the controller guarantees `Trip` is always last (and the simulator checks it, `toys3/sim/simulator.go:200-204`).

The `Reply` case never blocks the loop. The waiter channel is created with capacity 1 in `Submit`, and each `ClientReq` gets exactly one `Reply` from the controller, so the send always finds space. If the client gave up (its context was cancelled), its `Submit` has already removed the waiter, `ch` is nil, and the reply is dropped. That is the production version of "the operation completed but nobody was told", which the controller's semantics already permit: a mutation whose client disappeared may or may not have happened, exactly like an abandoned request in the simulator (Chapter 13).

`Emit`ted telemetry becomes log lines (`toys3: telemetry control.Committed {...}`). In simulation the same telemetry drives invariants (Chapter 14); here it is an operational breadcrumb trail, the historian of Chapter 3a. Production and the simulator are two subscribers to one published signal. You will see them in the real session below.

FS actions are handed to the executor with `l.post` as the completion callback. `Execute` returns immediately (it starts a goroutine), so dispatching a step's actions takes microseconds regardless of how slow the disk is.

### post: dropping completions for a dead incarnation

```go
// toys3/prod/loop.go:101-108
// post delivers a completion to the inbox, or drops it if Run has returned
// (a completion addressed to a dead incarnation).
func (l *Loop) post(b control.EventBody) {
	select {
	case l.inbox <- b:
	case <-l.stopped:
	}
}
```

Without the `stopped` case, an executor goroutine finishing after `Run` returned would block forever on a full inbox that nobody reads: a goroutine leak per in-flight operation at every trip. The comment ties this to the simulator's notion of incarnations: the simulator drops queue entries addressed to a dead incarnation (`toys3/sim/queue.go:101-122`, Chapter 10); production drops completions that arrive after the loop stopped. Same rule, different mechanism.

Note what `post` does *not* do: it does not undo the effect. The `rename` already happened on disk. "It happened, but nobody was told" is precisely the case the controller's recovery protocol and the simulator's land/deliver split are designed for.

The inbox is bounded (`inboxSize`), and `post` blocks while it is full. That is backpressure, as the `NewLoop` comment says. Note, though, that completions and new requests share one channel: a flood of HTTP requests can fill the inbox and delay the completions that would let requests *finish*. `docs/lessons.md` lesson 13 ("Control before data") explains why a serious system separates them; toys3 does not. It is listed among the gaps in §9.10 and Chapter 17.

### Submit: the client side

```go
// toys3/prod/loop.go:118-154
// Submit sends op to the server as a control.Request with a fresh ClientReq and
// waits for its Reply. If ctx ends first the reply is discarded on arrival;
// the operation itself may still complete (the client just never learns).
func (l *Loop) Submit(ctx context.Context, op control.Op) (control.Reply, error) {
	ch := make(chan control.Reply, 1)
	l.mu.Lock()
	l.nextReq++
	id := l.nextReq
	l.waiters[id] = ch
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.waiters, id)
		l.mu.Unlock()
	}()

	select {
	case l.inbox <- control.Request{Client: id, Op: op}:
	case <-ctx.Done():
		return control.Reply{}, ctx.Err()
	case <-l.stopped:
		return control.Reply{}, ErrStopped
	}
	select {
	case rep := <-ch:
		return rep, nil
	case <-ctx.Done():
		return control.Reply{}, ctx.Err()
	case <-l.stopped:
		select { // a Reply dispatched just before a Trip still counts
		case rep := <-ch:
			return rep, nil
		default:
			return control.Reply{}, ErrStopped
		}
	}
}
```

The ordering of operations here is careful and worth tracing:

1. **Register before sending.** The waiter is in the map *before* the request enters the inbox. If it were the other way round, the loop could step the request, reply instantly (a 400 for an invalid key is returned in the same `Step`), and find no waiter.
2. **Fresh, unique `ClientReq`.** `nextReq` is a per-`Loop` counter starting at 1. `control.ClientReq`'s doc requires uniqueness "within one runtime lifetime" (`toys3/control/types.go:17-20`); one `Loop` is one lifetime.
3. **Three-way select on send.** The inbox might be full (backpressure). The client might give up (`ctx`). The loop might be gone (`stopped`), in which case nobody will ever read the inbox and the handler must not hang.
4. **Three-way select on receive**, with a nested non-blocking receive in the `stopped` case.

That nested select handles a real race. Consider the tripping step: the controller returns `[Reply{c, 500}, Trip]`. `dispatch` sends the 500 on `ch` (buffered, succeeds), then returns `ErrTripped`, `Run` returns, and the deferred `close(l.stopped)` runs. Now the handler's outer select has two ready cases: `<-ch` and `<-l.stopped`. Go picks one at random. Without the nested select, half the time the client would get `ErrStopped` (mapped to 503) instead of the 500 the server actually sent. With it, a reply that was dispatched before the trip always wins. The comment says it in eight words.

Is the difference between 500 and 503 important? Both mean "unknown outcome" to a client. But they mean different things to an *operator*: a 500 says "this request hit an I/O error", a 503 says "the server was not running". Getting it right costs three lines.

5. **`defer delete`.** If the client gives up, its waiter is removed so the eventual reply is dropped rather than leaking a map entry.

## 9.5 The HTTP handler

### The S3 subset

```go
// toys3/prod/http.go:14-36
// CopySourceHeader names the source of a CopyObject, as in S3:
// "x-amz-copy-source: /srcbucket/srckey" (URL-escaped; leading slash optional).
const CopySourceHeader = "X-Amz-Copy-Source"

// ErrMethodNotAllowed is returned by ParseRequest for unsupported methods.
var ErrMethodNotAllowed = errors.New("method not allowed")

// Handler maps the S3 REST subset onto control.Request events:
//
//	PUT    /{bucket}/{key}                          → control.Put{Key, Data: body}
//	PUT    /{bucket}/{key} + x-amz-copy-source      → control.Copy{Src, Dst}
//	GET    /{bucket}/{key}                          → control.Get
//	DELETE /{bucket}/{key}                          → control.Delete
//
// Buckets are not first-class: the object key is "bucket/key", so there is one
// flat namespace and no CreateBucket. Replies map Status straight to the HTTP
// status code; a 200 GET writes Body. S3 returns 204 for DELETE; toys3 keeps
// 200 for simplicity.
type Handler struct {
	Loop *Loop
	// MaxBody bounds request bodies (http.MaxBytesReader); 0 means no limit.
	MaxBody int64
}
```

The mapping to S3 is cosmetic: S3's `CopyObject` is a `PUT` with an `x-amz-copy-source` header, so toys3 does the same. Buckets are flattened into the key (`"photos/cat.jpg"`), which `EscapeKey` later turns into the single file name `photos%2Fcat%2Ejpg` (Chapter 4). That is why the controller can treat keys as opaque strings.

### ServeHTTP

```go
// toys3/prod/http.go:38-76
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Method == http.MethodPut && r.Header.Get(CopySourceHeader) == "" {
		rd := io.Reader(r.Body)
		if h.MaxBody > 0 {
			rd = http.MaxBytesReader(w, r.Body, h.MaxBody)
		}
		b, err := io.ReadAll(rd)
		if err != nil {
			http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
			return
		}
		body = b
	}
	op, err := ParseRequest(r, body)
	if errors.Is(err, ErrMethodNotAllowed) {
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rep, err := h.Loop.Submit(r.Context(), op)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if rep.Status == control.StatusOK && rep.Body != nil {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(rep.Body)))
		w.WriteHeader(http.StatusOK)
		w.Write(rep.Body)
		return
	}
	w.WriteHeader(int(rep.Status))
}
```

The handler's responsibilities, in order:

1. **Read the body fully** for a plain PUT (the controller's `Request` doc says it is "a client request that has been fully received and decoded", `toys3/control/event.go:22-24`). Streaming is out of scope; an object is a byte slice.
2. **Bound the body** with `http.MaxBytesReader`. A client cannot make the server buffer an arbitrarily large body. An oversized body becomes a 400 here, before the controller ever sees it. (The controller has its own bound, `MaxObjectSize`, which also yields 400; `cmd/toys3` sets both to the same value, so in practice the handler's bound triggers first.)
3. **Parse** into a `control.Op`, mapping parse errors to 400 and unknown methods to 405 with an `Allow` header.
4. **Submit** and wait. A `Submit` error (client gone, loop stopped) becomes 503.
5. **Write the reply**: a 200 with a body for GET, otherwise just the status code. Notice that the controller's `Status` values are HTTP status codes already (`toys3/control/types.go:22-30`), so the mapping is a cast.

The `r.Context()` passed to `Submit` is cancelled when the client disconnects. So a client that hangs up mid-request frees its handler goroutine immediately; the operation itself carries on inside the controller and completes (or not), unobserved.

### ParseRequest and splitKey

```go
// toys3/prod/http.go:78-117
func ParseRequest(r *http.Request, body []byte) (control.Op, error) {
	key, err := splitKey(r.URL.Path)
	if err != nil {
		return nil, err
	}
	switch r.Method {
	case http.MethodGet:
		return control.Get{Key: key}, nil
	case http.MethodDelete:
		return control.Delete{Key: key}, nil
	case http.MethodPut:
		src := r.Header.Get(CopySourceHeader)
		if src == "" {
			return control.Put{Key: key, Data: body}, nil
		}
		unesc, err := url.PathUnescape(src)
		if err != nil {
			return nil, errors.New("bad " + CopySourceHeader + ": " + err.Error())
		}
		srcKey, err := splitKey(unesc)
		if err != nil {
			return nil, errors.New("bad " + CopySourceHeader + ": " + err.Error())
		}
		return control.Copy{Src: srcKey, Dst: key}, nil
	}
	return nil, ErrMethodNotAllowed
}

// splitKey turns "/bucket/key" (leading slash optional) into "bucket/key".
func splitKey(p string) (string, error) {
	p = strings.TrimPrefix(p, "/")
	bucket, key, ok := strings.Cut(p, "/")
	if !ok || bucket == "" || key == "" {
		return "", errors.New("path must be /{bucket}/{key}")
	}
	return p, nil
}
```

`ParseRequest` is exported and pure (no I/O beyond what is already in `r`), so it could be unit tested exhaustively; today it is covered by `TestEndToEnd`. Things worth noticing:

- `r.URL.Path` is the *decoded* path, so `/b/sp%20ace/x` becomes key `b/sp ace/x`. Keys may contain further slashes (`splitKey` only requires a non-empty bucket and a non-empty rest).
- The copy source is URL-unescaped explicitly, because headers are not decoded by `net/http`. A leading slash is optional (S3 accepts both).
- Validation of the *key itself* (non-empty, escaped name fits in 255 bytes) is **not** done here. It is done in the controller (`ValidKey`, `toys3/format/layout.go:58-62`), which replies 400 without touching anything. That keeps the rule inside the simulated world, where the workload's `Invalid` operations exercise it (Chapter 12).
- The path check happens *before* the method check. So `POST /onlybucket` gets 400, not 405. A minor wart, harmless.

## 9.6 Wiring it up: cmd/toys3

```go
// toys3/cmd/toys3/main.go:26-62
func main() {
	dir := flag.String("dir", "./toys3-data", "data directory (obj/ and tmp/ are created inside)")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	maxObj := flag.Int("max-object", 64<<20, "largest object in bytes (0 = unlimited)")
	flag.Parse()

	root, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatalf("toys3: %v", err)
	}
	root = filepath.ToSlash(root)
	log.Printf("toys3: dir=%s addr=%s", root, *addr)
	if err := prod.PrepareDir(root); err != nil {
		log.Fatalf("toys3: prepare %s: %v", root, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loop := prod.NewLoop(control.Config{Dir: root, MaxObjectSize: *maxObj}, control.NewMachine, prod.OSExecutor{}, 1024)
	srv := &http.Server{Addr: *addr, Handler: &prod.Handler{Loop: loop, MaxBody: int64(*maxObj)}}

	go func() {
		err := loop.Run(ctx)
		if errors.Is(err, prod.ErrTripped) {
			// Fail-stop: in-memory state is untrusted. A supervisor restarts us
			// and recovery runs on boot.
			log.Fatalf("toys3: exiting: %v", err)
		}
		srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("toys3: %v", err)
	}
	log.Printf("toys3: stopped")
}
```

The sequence:

1. Resolve `-dir` to an absolute, slash-separated path (the controller's path convention).
2. `PrepareDir`: create `obj/` and `tmp/` and fsync the root.
3. `signal.NotifyContext`: SIGINT or SIGTERM cancels `ctx`, which makes `Run` return `context.Canceled`, which makes the goroutine call `srv.Close()`, which makes `ListenAndServe` return `ErrServerClosed`, and the process logs "stopped" and exits 0. A graceful shutdown in four hops.
4. On `ErrTripped`, `log.Fatalf` exits the process with status 1 *immediately*. This is the fail-stop. There is no attempt to drain in-flight requests or to wait for executor goroutines; the in-memory state is untrusted by definition (Chapter 7), so the less the process does after deciding to trip, the better.

The inbox size is 1024 and the maximum object is 64 MiB by default. Note that the simulator uses `MaxObjectSize: 64` *bytes* (`toys3/sim/config.go:173`): the simulator's job is to explore protocol interleavings, not to move megabytes.

## 9.7 Try it

Build and run the server in a directory of your own choosing (do not reuse a directory another process is using; the server assumes it is the only writer):

```sh
go build -o /tmp/toys3bin ./toys3/cmd/toys3
/tmp/toys3bin -dir /tmp/my-toys3-data -addr 127.0.0.1:18080 &
```

Then drive it with `curl`. This is a real session recorded while writing this book (only the scratch path is shortened):

```
$ curl -s -i -X PUT --data-binary 'hello world' http://127.0.0.1:18080/photos/cat.jpg
HTTP/1.1 200 OK

$ curl -s -i http://127.0.0.1:18080/photos/cat.jpg
HTTP/1.1 200 OK
Content-Length: 11
Content-Type: application/octet-stream
Date: Mon, 12 Oct 2026 00:32:53 GMT

hello world

$ curl -s -i -X PUT -H 'x-amz-copy-source: /photos/cat.jpg' http://127.0.0.1:18080/backup/cat.jpg
HTTP/1.1 200 OK

$ curl -s -i -X DELETE http://127.0.0.1:18080/photos/cat.jpg
HTTP/1.1 200 OK

$ curl -s -i http://127.0.0.1:18080/photos/cat.jpg
HTTP/1.1 404 Not Found

$ curl -s -i -X POST http://127.0.0.1:18080/photos/cat.jpg
HTTP/1.1 405 Method Not Allowed
```

And the server's log for the same session (re-recorded at commit `a4018d5`, on port 18081; the first edition's log said `toys3: fact core.Committed ...`, because the type was then `core.Committed` and the line was labelled `fact`):

```
2026/10/13 06:41:32 toys3: dir=/…/data addr=127.0.0.1:18081
2026/10/13 06:41:32 toys3: telemetry control.Recovered {TempsRemoved:0}
2026/10/13 06:41:32 toys3: telemetry control.Committed {Client:1 Key:photos/cat.jpg Deleted:false Sum:222957957}
2026/10/13 06:41:32 toys3: telemetry control.Committed {Client:3 Key:backup/cat.jpg Deleted:false Sum:222957957}
2026/10/13 06:41:32 toys3: telemetry control.Committed {Client:4 Key:photos/cat.jpg Deleted:true Sum:0}
2026/10/13 06:41:32 toys3: stopped
```

Read the log against the session:

- `Recovered {TempsRemoved:0}` appears before any request is served: boot recovery listed `tmp/`, found nothing, fsynced `tmp/` and started serving (Chapter 5).
- `Client:1` is the PUT. `Client:2` was the GET, which emits no telemetry. `Client:3` is the COPY; note it has the *same* `Sum` as the PUT, because COPY writes identical bytes. `Client:4` is the DELETE. The second GET (`Client:5`, 404) and the POST (rejected in the handler, never submitted) emit nothing.
- `Sum:222957957` is `0x0d4a1185`, the CRC-32 (IEEE) of `hello world`.
- `stopped` is the graceful-shutdown path: the process was killed with SIGTERM.

Now look at the bytes on disk. After a second PUT and COPY run in a fresh directory, the object directory held:

```
$ ls -l /…/obj
-rw-r--r--  1 …  27 Oct 12 06:03 backup%2Fcat%2Ejpg
-rw-r--r--  1 …  27 Oct 12 06:03 photos%2Fcat%2Ejpg

$ od -A d -t x1 -c "/…/obj/photos%2Fcat%2Ejpg"
0000000    54  4f  59  33  85  11  4a  0d  0b  00  00  00  00  00  00  00
           T   O   Y   3 205 021   J  \r  \v  \0  \0  \0  \0  \0  \0  \0
0000016    68  65  6c  6c  6f  20  77  6f  72  6c  64
           h   e   l   l   o       w   o   r   l   d
0000027
```

Byte by byte, against the format in `toys3/format/doc.go:19-25`:

```
offset  bytes                       field
0       54 4f 59 33                 magic "TOY3"
4       85 11 4a 0d                 CRC-32 0x0d4a1185, little endian  (= 222957957, the Sum above)
8       0b 00 00 00 00 00 00 00     payload length 11, little endian
16      68 65 6c 6c 6f 20 77 6f 72 6c 64    "hello world"
```

27 bytes: a 16-byte header plus 11 bytes of payload. The file names show `EscapeKey` at work: `/` became `%2F` and `.` became `%2E`, because only ASCII letters, digits, `-` and `_` pass through (`toys3/format/layout.go:12-30`). And `tmp/` is empty: every temp file was renamed into `obj/`.

Try a few more things yourself:

```sh
# A key with a space; the URL path is decoded before it becomes a key.
curl -s -o /dev/null -w '%{http_code}\n' -X PUT --data-binary 'x' 'http://127.0.0.1:18080/b/sp%20ace/x'
ls /tmp/my-toys3-data/obj      # b%2Fsp%20ace%2Fx

# A path with no key part: rejected by the handler (400), never reaches the controller.
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18080/onlybucket

# Leave junk in tmp/, restart, and watch recovery remove it.
kill %1; echo junk > /tmp/my-toys3-data/tmp/00000000000000ff
/tmp/toys3bin -dir /tmp/my-toys3-data -addr 127.0.0.1:18080 &   # logs Recovered {TempsRemoved:1}
```

## 9.8 How prod is tested

The simulator never runs `prod`. So `prod` has its own tests, in `toys3/prod/prod_test.go`. They are integration tests against a real temporary directory and an `httptest.Server`.

### TestEndToEnd

```go
// toys3/prod/prod_test.go:71-108 (abridged)
func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	s := startServer(t, dir, OSExecutor{})
	s.expect("PUT", "/b/k1", []byte("hello"), nil, 200, "")
	s.expect("GET", "/b/k1", nil, nil, 200, "hello")
	s.expect("PUT", "/b/k2", nil, map[string]string{CopySourceHeader: "/b/k1"}, 200, "")
	...
	s.expect("PUT", "/b/k3", nil, map[string]string{CopySourceHeader: "b/nope"}, 404, "")
	s.expect("PUT", "/b/sp%20ace/x", []byte("nested"), nil, 200, "")
	s.expect("GET", "/onlybucket", nil, nil, 400, "")
	s.expect("POST", "/b/k1", nil, nil, 405, "")
	if err := s.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}

	// A crash mid-PUT leaves a temp file behind.
	leftover := filepath.Join(dir, "tmp", "00000000000000ff")
	...
	s = startServer(t, dir, OSExecutor{})
	defer s.stop()
	// Requests queue behind recovery, so this reply proves recovery finished.
	s.expect("GET", "/b/k2", nil, nil, 200, "hello")
	...
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("leftover temp file survived recovery: %v", err)
	}
```

It covers every operation and status code, the copy-source header with and without its leading slash, URL-escaped keys, the 400 and 405 paths, graceful stop (`context.Canceled`), and a restart that recovers a planted temp file. The comment "Requests queue behind recovery, so this reply proves recovery finished" uses the controller's queuing semantics as a synchronization barrier. That is neat: no sleeps.

What it is *not*: a crash test. `s.stop()` cancels the context; it does not lose the page cache. Planting a temp file simulates the *aftermath* of a crash, not the crash itself. Real power-loss testing of `prod` would need a VM or a block-device fault injector (Chapter 17).

### TestTripStopsLoop

```go
// toys3/prod/prod_test.go:110-130
// failFsync wraps OSExecutor and fails every file fsync with EIO.
type failFsync struct{ OSExecutor }

func (f failFsync) Execute(a control.Action, done func(control.EventBody)) {
	if a, ok := a.(control.FSFsync); ok {
		go done(control.FSFsyncDone{IO: a.IO, Err: control.ErrIO})
		return
	}
	f.OSExecutor.Execute(a, done)
}

func TestTripStopsLoop(t *testing.T) {
	s := startServer(t, t.TempDir(), failFsync{})
	s.expect("PUT", "/b/k", []byte("v"), nil, 500, "")
	if err := <-s.done; !errors.Is(err, ErrTripped) {
		t.Fatalf("Run returned %v, want ErrTripped", err)
	}
	s.done <- nil // let stop() return
	s.expect("GET", "/b/k", nil, nil, 503, "")
	s.stop()
}
```

This is where the `Executor` interface earns its keep: a three-line wrapper makes every file fsync fail. The test checks the end-to-end fail-stop path: the PUT gets its 500 (not a 503, thanks to the nested select in `Submit`), `Run` returns `ErrTripped`, and a later request gets 503 because the loop has stopped. Note that `failFsync` still calls `done` from a goroutine (`go done(...)`), honouring the "must not block the loop" contract even in a test double.

### What nothing tests

Be explicit about the gaps. The following are not exercised by the simulator, and only partly or not at all by `prod_test.go`:

- **Error classification in `code`.** No test forces a real `ENOENT` versus `EIO` versus `ENOSPC` through `OSExecutor`.
- **`Close` errors** in `writeFile` and `syncPath`.
- **Actual durability.** No test cuts power. `fsync` is called; whether the platform honours it is taken on trust.
- **Concurrency of the executor under load.** Many goroutines doing I/O at once, completions arriving in arbitrary order. The *controller*'s handling of arbitrary completion order is tested heavily by the simulator (its queue explores orders); the runtime's plumbing is simple enough to trust.
- **The inbox under backpressure**, and `Submit`'s cancellation paths.

That list is short because the runtime is short. That is the payoff of rule 10.

## 9.9 Concurrency in production, and why the controller does not care

In production, many things happen at once:

```
 HTTP goroutine A ─ Submit(PUT k1) ─┐
 HTTP goroutine B ─ Submit(GET k2) ─┤                     ┌── goroutine: write tmp/0..07 ──┐
 HTTP goroutine C ─ Submit(PUT k1) ─┼─► inbox ─► Run ─────┼── goroutine: read obj/k2 ───────┤
 executor goroutines ── post(...) ──┘    (one at a time)  └── ...                          │
                    ▲                                                                      │
                    └──────────────────── completions, in whatever order the disk finishes ┘
```

The completions arrive in whatever order the kernel finishes them. A read of `k2` may complete before a write of `k1` issued earlier. Two PUTs on *different* keys interleave their write/fsync/rename/fsync-dir steps freely. None of this is a problem, because:

- **`Step` runs one event at a time.** There are no data races in the controller, no locks, no partial updates visible to another step.
- **Same-key requests are serialized by the controller**, not by the runtime. The second `PUT k1` from goroutine C is accepted into `reqs` but does not start until the first `PUT k1` has replied (Chapter 6). The runtime does not need to know anything about keys.
- **Every completion carries its IOID**, so arrival order does not matter. The controller finds the owning request by IOID (`toys3/control/controller.go:127-131`).

The simulator explores completion orders deliberately: each FS action gets a random dead time (until it lands) and a random feedback delay (until its completion is delivered) (`toys3/sim/plantmodel.go:145-152`), and same-instant ties are broken by a seeded random draw (Chapter 10). So the space of orders that production can produce is (a superset of what is) explored in simulation, which is why we can trust the controller under real concurrency without running the real concurrency.

## 9.10 Gaps between the model and production

The simulator's model of the world and the production runtime disagree in a few places. None is a bug in the toy's stated scope, but each is a lesson about where DST's guarantees end.

**1. No supervisor is included.** `main.go` says "A supervisor restarts us and recovery runs on boot." There is no supervisor in the repository. Under systemd you would use `Restart=on-failure`; under Kubernetes the pod restarts. Until something restarts the process, a tripped server is simply down. In the simulator, restart is automatic after a random delay of 100 µs to 5 ms (`toys3/sim/config.go:172`).

**2. A production Trip is a process exit, not a power loss.** This is the important one. The simulator treats `Trip` exactly like a crash: the PlantModel resolves all volatile state at that instant, as if the power went out (`toys3/sim/simulator.go:200-207`, `275-306`). In production, `log.Fatalf` kills the process, but **the kernel's page cache and directory cache survive**. After a restart, recovery and subsequent reads see the *volatile* view of the filesystem, not the durable one.

Consider the scenario from Chapter 1 against the *correct* server in production: `FSFsyncDir(obj/)` fails, the server replies 500 and trips. The supervisor restarts it within a second. Recovery lists and cleans `tmp/`, fsyncs `tmp/`, and starts serving. A `GET` of the key reads the renamed file, which is still visible through the kernel's caches, and returns `200 v2`. Then the power fails, the un-persisted rename is lost, and the key reads back as `v1`. That is the very "observed then vanished" history that Trip was supposed to prevent (Chapter 7), and the simulator cannot see it, because in the simulator a Trip *is* a power loss, which resolves the volatile state before anyone can read it.

The defence would have to be in the controller's recovery protocol, since only the controller is simulated: for example, recovery could `FSFsyncDir(obj/)` (and fsync the object files it might serve) before emitting `Recovered`, so that whatever is visible at boot is also durable, or trip again if it cannot be made so. After a real fsync error on Linux, even that is not a complete answer for file *contents* (the "fsyncgate" behaviour means a later fsync may report success for pages that were already dropped; Chapter 11 models this as `FsyncGate`). To make the simulator expose the gap, it would need to model a trip as "process exit without power loss" (volatile state survives) as a distinct event from a crash. Chapter 17 (Part I, §2) lists this gap. It is still open at commit `a4018d5`: the refactor renamed `Halt` to `Trip` and, by design, changed no behaviour, so this finding ("Halt after restart" in the first edition, "Trip after restart" now) is exactly as true as it was. Note that the control vocabulary makes the gap easier to state: the simulator models a *trip* as if it were a *crash* (a power-loss disturbance), and in production it is not one.

**3. In-flight operations at a Trip.** When the process exits, executor goroutines die mid-syscall. From the kernel's perspective, the syscall either completed or did not; `rename` is atomic, `write` may be partial. That matches the simulator's "landed but not delivered" model reasonably well. In tests (where `Run` returns instead of the process exiting), those goroutines run to completion after the loop stopped, and `post` drops their completions. Their *effects* stay. This is again the "it happened, but nobody was told" case.

**4. A tripping 500 may never reach the client.** `dispatch` hands the 500 to the waiter's channel, then `Run` returns, then `log.Fatalf` calls `os.Exit`. The HTTP handler goroutine may or may not have written the 500 to the socket by then. The client may see a connection reset instead. Semantically that is fine: both a 500 and a dropped connection mean "unknown outcome". But it means the careful Reply-before-Trip logic in `Submit` only matters in tests, where the process does not exit.

**5. `PrepareDir` does not fsync the parent of `root`.** If `-dir` names a directory that does not exist yet, `MkdirAll` creates it, and `syncPath(root)` fsyncs `root` (making `obj` and `tmp` durable inside it), but nothing fsyncs `root`'s parent. A power cut right after first start could lose `root` entirely. On a second start, `PrepareDir` would recreate it, so the only consequence is losing objects written in that first window. Real deployments usually create the data directory ahead of time, which sidesteps the issue.

**6. One inbox for control and data.** Completions and new requests share the inbox channel. Under overload, HTTP handlers can fill the inbox and delay the completions that would free resources. `docs/lessons.md` lesson 13 and `docs/decisions.md` decision G describe the fix used in the bigger design: separate control and data inboxes, control first.

**7. Real filesystems are not the PlantModel.** The PlantModel is in some ways *more* adversarial than ext4 or APFS: for example, it can resolve the two directory-entry changes of one cross-directory rename independently at a crash, leaving both names or neither (Chapter 11), whereas a journaling filesystem typically commits a rename atomically. In other ways it is *less* adversarial: it has no metadata-only journaling quirks, no delayed allocation, no reordering between files beyond what its per-entry resolution allows. The protocol is designed for the more adversarial model, which is the safe direction.

## Key takeaways

- `toys3/prod` is a thin, mechanical translation between the controller's vocabulary and the OS. It contains no business logic, because it is the one part of the system the simulator never runs.
- `Loop.Run` is the only goroutine that calls `Step`. It stamps `At` at dequeue time from a monotonic clock, dispatches actions in order, and returns on `Trip` or cancellation.
- The `Executor` contract (non-blocking, exactly one matching completion) is what lets the controller keep many FS operations in flight while staying single-threaded.
- `code()` is the single place that classifies real errors; it is pessimistic (anything but ENOENT is `ErrIO`), and the controller's error policy does the rest.
- `post` drops completions after the loop stops (the production analogue of dropping a dead incarnation's queue entries); `Submit` registers before sending and lets a reply dispatched before a Trip win over the stop signal.
- The biggest model gap: a production Trip is a process exit, not a power loss, so the page cache survives into the next incarnation. The simulator, which treats Trip as power loss, cannot see the consequences.

## Exercises

1. **Run it and read the telemetry.** Build `cmd/toys3`, run it with your own `-dir` and a free port, and replay the curl session from §9.7. Compare your log lines with the ones above. Then `od -A d -t x1 -c` one object file and compute its CRC with a tool of your choice (`python3 -c 'import zlib; print(hex(zlib.crc32(b"hello world")))'`). Kill the server when done.
2. **Recovery in production.** With the server stopped, create two files in `<dir>/tmp/`, one of them named like a real temp file (`000000000000002a`) and one not (`junk`). Start the server and read the `Recovered` telemetry. Predict `TempsRemoved` first. Does recovery care what the names look like? Find the line in `onRecoveryDone` that answers this.
3. **Fail-stop by hand.** Write a variant of `TestTripStopsLoop` (in a scratch copy of the package, or as an extra test you delete afterwards) whose executor fails `FSFsyncDir` instead of `FSFsync`. Predict the HTTP status of the PUT and the error `Run` returns. Then make it fail `FSRead` instead. Predict again; explain the difference using the error table in `toys3/control/doc.go:69-78`.
4. **The Reply-versus-Trip race.** Temporarily replace the nested `select` in `Submit` (`toys3/prod/loop.go:146-152`) with a plain `return control.Reply{}, ErrStopped`. Run `go test ./toys3/prod -run TestTripStopsLoop -count=200`. Does it ever fail? How often? Explain the failure rate from Go's `select` semantics. Revert.
5. **Model gap, on paper.** Write out, step by step, the production scenario of §9.10 point 2 (fsync-dir error, trip, restart, GET, power loss). Then propose a change to `onStarted`/`onRecoveryDone` that closes the directory half of the gap, and predict whether the simulator's existing invariants would accept your change (they should: it only adds an fsync). Implement it in a scratch branch and run `go test ./toys3/control ./toys3/sim -count=1`. Which unit tests in `controller_test.go` need updating, and why (hint: they assert exact action sequences)?
6. **Concurrency without fear.** Using `curl` in a shell loop (or `xargs -P 16`), fire 100 concurrent PUTs at the same key with different bodies, then GET it. Is the result one of the bodies you sent? Which one, and can you predict it? Relate your answer to the per-key FIFO in Chapter 6 and to the order in which requests enter the inbox.
7. **Grep for business logic.** Read every line of `toys3/prod` and list any decision it makes that depends on the *meaning* of a request (not just its encoding). You should find very few. For each, argue whether it belongs in the controller.

## Further reading

- `loop.md` (rules 6, 8 and 10) and `docs/decisions.md` (the "Threading model, Production" section) in this repository: the design this runtime implements, and the bigger version it is a rehearsal for.
- Brett Cannon et al., *"Network protocols, sans I/O"* (https://sans-io.readthedocs.io). The pattern behind keeping the runtime thin and the logic pure.
- The Go standard library documentation for `os.File.Sync`, `os.Rename`, `os.ReadDir`, `net/http.MaxBytesReader` and `signal.NotifyContext`.
- Jonathan Corbet, *"PostgreSQL's fsync() surprise"*, LWN.net, 2018. Why "trip and restart" does not by itself make a page cache trustworthy again (relevant to §9.10, point 2).
- George Candea and Armando Fox, *"Crash-Only Software"*, HotOS 2003. The philosophy behind fail-stop plus recovery-on-boot as the only shutdown path.
- Thanumalayan Sankaranarayana Pillai et al., *"All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications"*, OSDI 2014. On how real filesystems differ in which crash states they allow (relevant to §9.10, point 7).
