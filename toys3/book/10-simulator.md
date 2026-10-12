# Chapter 10: The Simulator

> *"A simulator is a universe with exactly one law of physics: the next thing that happens is whatever is at the front of the queue."*

Chapters 2 through 9 were about the thing being tested: a server that never does I/O, only describes it. This chapter is about the thing doing the testing. `toys3/sim/simulator.go` is 409 lines long, and it is the beating heart of everything that follows. The fake filesystem (Chapter 11), the workload (Chapter 12), the oracle (Chapter 13) and the invariants (Chapter 14) are all plugged into it, and all of them are driven by the one loop described here.

We will go through it in the order you would build it: the queue first (because the queue *is* time), then the `Simulator` struct, then `New`, `Run`, and every handler. Along the way we will look at real event logs produced by seed 1 and see how each piece of code shows up as a line in the log.

---

## 10.1 The main loop in eleven lines

The package doc of `toys3/sim` contains the whole simulator in pseudocode. It is worth memorizing:

```go
// toys3/sim/doc.go:9-25
// Main loop, on one goroutine:
//
//	e := queue.Pop(); clock = e.At
//	if e is addressed to a dead incarnation: drop it
//	switch e.Kind:
//	  deliver  → actions := server.Step(Event{clock, e.Event}); trace ← fold(StateHash)
//	             for a in actions:
//	               FS*   → plan := fs.Plan(a); schedule fsLand at clock+plan.Land
//	               Reply → oracle.Return; workload.Complete; schedule clientWake
//	               Emit  → record fact          Halt → crash now
//	             during Chaos: crash now with probability CrashProb
//	  fsLand   → body := fs.Land(a, plan); schedule deliver(body) at clock+plan.Reply
//	             during Chaos: crash now with probability CrashProb
//	  clientWake → req := workload.Issue; oracle.Invoke; schedule deliver(Request)
//	  crash    → fs.Crash(); queue.DropIncarnation; oracle.Crash; schedule restart
//	  restart  → inc++; server = Factory(...); schedule deliver(Started)
//	run invariants (per-step checks); the first failure stops the run
```

Everything in this chapter is an elaboration of these lines. Notice three things before we start.

First, **there is exactly one source of "what happens next"**: the queue. Clients, the filesystem, the server, crashes and restarts all communicate by scheduling queue entries. Nobody calls anybody "later"; they schedule an entry for later.

Second, **filesystem operations take two queue entries**, not one: a *land* (the effect is applied to the FakeFS) and a *deliver* (the completion event reaches the server). The gap between them is where "it happened, but nobody was told" lives. This is the single most important modelling decision in the simulator, and §10.7 is devoted to it.

Third, **invariants run after every step**, and the first failure stops the run. The step at which a failure is reported is therefore (for the eager invariants) the step at which the bug became observable, which is what makes the event log readable.

## 10.2 The queue is time

### 10.2.1 Entries

Every item in the queue is an `Entry`:

```go
// toys3/sim/queue.go:47-59
// Entry is one item in the global queue. Plain data, like events.
type Entry struct {
	At   core.Time
	Tie  uint64 // drawn from the run's Rand at Schedule time; orders same-At entries
	Seq  uint64 // assigned by Schedule; final tie-break, makes the order total
	Kind EntryKind
	Inc  Incarnation // for KindDeliver and KindFSLand; 0 otherwise

	Event  core.EventBody // KindDeliver
	Action core.Action    // KindFSLand: the FS action to apply
	Plan   FSPlan         // KindFSLand: fault and latency decided at issue time
	Client int            // KindClientWake
}
```

It is a tagged union written as a flat struct: `Kind` says which of the payload fields is meaningful. `At`, `Tie` and `Seq` are the ordering key. `Inc` ties server-bound entries to one boot of the server. Like Events and Actions in the core (Chapter 3), an `Entry` is plain data: no pointers into shared state, no closures. You could print the whole queue at any moment and it would be a complete description of the future.

The kinds:

```go
// toys3/sim/queue.go:14-23
const (
	KindDeliver    EntryKind = iota // deliver Event to the server (Inc must be current)
	KindFSLand                      // a filesystem action takes effect in the FakeFS
	KindClientWake                  // an idle client issues its next request
	KindCrash                       // kill the server now
	KindRestart                     // boot a new incarnation and deliver Started
	KindQuiesce                     // clients stop issuing until every request has replied
	KindQuiet                       // Chaos ends: no new faults or crashes (QuietTail begins)
	KindTailEnd                     // QuietTail ends: clients stop for good; the queue drains
)
```

Each kind has a `String()` (`toys3/sim/queue.go:25-45`) that appears in the event log's fourth column: `deliver`, `fs-land`, `client-wake`, `crash`, `restart`, `quiesce`, `quiet`, `tail-end`.

A small surprise worth noting now: **nothing in the simulator ever schedules a `KindCrash` entry.** Crashes are injected synchronously (`maybeCrash`, §10.9) or triggered by `Halt`, never via the queue. The handler for `KindCrash` exists (`toys3/sim/simulator.go:147-150`) and would work, but the only place the constant is used outside the handler is as the `Kind` of log entries for crashes and failures (§10.12). It is a hook for a future "scheduled crash at time T" feature, not a live path.

### 10.2.2 Ordering: (At, Tie, Seq)

```go
// toys3/sim/queue.go:61-68
// Queue is the global priority queue ordered by (At, Tie, Seq): time first,
// then a seeded random tie-break so same-instant events are explored in
// different orders across seeds, then insertion order so the order is total.
// A binary heap over a slice is enough.
type Queue struct {
	items []Entry
	seq   uint64
}
```

```go
// toys3/sim/queue.go:125-134
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

Three keys, three jobs:

1. **`At`** is virtual time. Earlier things happen first. This is the only key that has a physical meaning.
2. **`Tie`** is a random 64-bit number drawn when the entry is scheduled. When two entries are scheduled for the same instant, `Tie` decides which runs first. Because `Tie` comes from the run's single seeded RNG, the order is deterministic for a given seed, but *different seeds explore different orders*. This matters a lot in toys3: a client wake and a request delivery are frequently scheduled at the same instant (`wake` schedules the `Request` delivery at `s.now`, §10.8), and so is a restart and its `Started` delivery.
3. **`Seq`** is an insertion counter. Two entries with the same `At` and the same `Tie` (a 1-in-2^64 event, but possible) are ordered by insertion. Its real job is to make the order **total**: without it, the heap's internal layout could decide between equal keys, and a heap's layout depends on the exact history of pushes and pops. A total order means the pop sequence is a pure function of the set of entries, not of how the heap happened to arrange them.

A note on documentation drift: `loop.md` rule 11 says the simulator should use "a priority queue ordered by `(time, seq, rngTie)`". The code uses `(At, Tie, Seq)`: the random tie comes *before* the sequence number. The code's order is the useful one. With `(time, seq, rngTie)`, `seq` would always break same-instant ties by insertion order and the random tie would never be consulted (sequence numbers are unique), so same-instant events would always run in FIFO order and no seed would ever explore the other interleaving. `DESIGN.md` (line 80, "Queue ordered by (At, seeded Tie, Seq)") matches the code.

### 10.2.3 Schedule, Pop, and clamping

```go
// toys3/sim/queue.go:70-81
// Schedule enqueues e, assigning Tie (from r) and Seq. Scheduling in the past
// clamps At to now.
func (q *Queue) Schedule(r *Rand, now core.Time, e Entry) {
	if e.At < now {
		e.At = now
	}
	e.Tie = r.Uint64()
	q.seq++
	e.Seq = q.seq
	q.items = append(q.items, e)
	q.up(len(q.items) - 1)
}
```

Two details. First, every `Schedule` call consumes exactly one RNG draw. This means that *scheduling* is part of the deterministic history: if a code change adds one extra `Schedule` call, every subsequent random decision in the run shifts by one draw, and the run diverges completely from the old run of the same seed. That is fine and expected; it is why the canary in Chapter 8 compares two runs of the *same* code.

Second, the clamp: an entry scheduled in the past is moved to `now`. In practice nothing in the simulator schedules into the past (all delays are non-negative), so the clamp is a safety net that guarantees time never goes backwards. `TestQueueClampsPastAndDropsIncarnation` (`toys3/sim/queue_test.go:68-93`) schedules an entry at `At: 5` with `now = 100` and asserts it pops at 100.

`Pop` (`toys3/sim/queue.go:84-97`) is a textbook binary-heap pop: take the root, move the last element to the root, zero the vacated slot (so the garbage collector does not keep a stale `Event` alive), shrink, sift down.

### 10.2.4 DropIncarnation

```go
// toys3/sim/queue.go:99-120
// DropIncarnation removes every entry addressed to inc (KindDeliver and
// KindFSLand). Called on crash: un-landed FS operations never happen and
// pending completions are lost. It returns how many entries were dropped.
func (q *Queue) DropIncarnation(inc Incarnation) int {
	kept := q.items[:0]
	dropped := 0
	for _, e := range q.items {
		if (e.Kind == KindDeliver || e.Kind == KindFSLand) && e.Inc == inc {
			dropped++
			continue
		}
		kept = append(kept, e)
	}
	for i := len(kept); i < len(q.items); i++ {
		q.items[i] = Entry{}
	}
	q.items = kept
	for i := len(q.items)/2 - 1; i >= 0; i-- {
		q.down(i)
	}
	return dropped
}
```

This is the mechanical meaning of a crash in the queue. Two kinds of entries die with the incarnation:

- **`KindFSLand`** entries: filesystem actions the server issued that have not yet taken effect. In a power cut, an I/O request still sitting in the kernel's queue simply never happens.
- **`KindDeliver`** entries: completions (and requests, and the `Started` event) addressed to the dead server. Nobody is there to receive them.

Client wakes, quiesce markers, phase markers and restarts survive: clients and the passage of time do not care that the server died.

The implementation filters in place (the `kept := q.items[:0]` idiom reuses the backing array), zeroes the tail, and then rebuilds the heap bottom-up with `down` from the last internal node to the root. That is Floyd's O(n) heap construction. Filtering destroys the heap property, so the rebuild is necessary; pushing the survivors one by one would also work but would be O(n log n).

Note that **filtering preserves each surviving entry's `Tie` and `Seq`**, so the relative order of the survivors does not change. A crash removes futures; it does not reshuffle the remaining ones.

### 10.2.5 The queue tests

`toys3/sim/queue_test.go` has three tests, each checking one promise:

- `TestQueueOrdersByTimeThenTieThenSeq` (lines 9-32) schedules eight entries with times `{50, 10, 30, 10, 20, 10, 40, 30}` and checks that pops are non-decreasing in `At` and, within an instant, in `Tie`.
- `TestQueueTieBreakDependsOnSeed` (lines 34-66) schedules six entries at the same instant and checks two things: the same seed always produces the same order, and *some* seed in 2..19 produces a different order from seed 1. The second half is the important one. A tie-break that never varies is a tie-break that never explores.
- `TestQueueClampsPastAndDropsIncarnation` (lines 68-93) checks the clamp and that `DropIncarnation(1)` removes exactly the `KindDeliver` and `KindFSLand` entries of incarnation 1, keeping incarnation 2's delivery, the client wake and the restart.

## 10.3 The Simulator struct

```go
// toys3/sim/simulator.go:9-36
type Simulator struct {
	cfg  Config
	rng  *Rand
	now  core.Time
	step int
	q    Queue

	fs     *FakeFS
	server core.Machine // nil while crashed
	inc    Incarnation  // current (or last) incarnation; 0 before first boot
	alive  bool
	quiet  bool

	workload *Workload
	oracle   *Oracle
	facts    []TimedFact

	env       *env
	parked    []bool // idle clients waiting for a restart or the end of a quiesce
	quiescing bool   // clients hold new requests until every request replied
	stopped   bool   // QuietTail is over: clients issue nothing more
	halted    bool   // a failure stops the run

	trace uint64 // running trace hash
	res   Result
}
```

Field by field:

| Field | Meaning |
|---|---|
| `cfg` | The run's `Config` (Chapter 12 covers `DefaultConfig`/`BaseConfig`), with defaults filled in by `New`. |
| `rng` | The one `*Rand`. Shared by the queue, the FakeFS, the workload and the server's `env`. |
| `now` | Virtual time: the `At` of the entry being processed. |
| `step` | Count of entries popped and processed so far. Log lines are numbered by it. |
| `q` | The global queue. |
| `fs` | The FakeFS (Chapter 11). Survives crashes: it *is* the disk. |
| `server` | The current `core.Machine`, or `nil` while crashed. Built by `cfg.Factory`. |
| `inc` | Current incarnation number. 0 before the first boot, 1 after it, +1 per restart. |
| `alive` | True between a restart and the next crash. |
| `quiet` | True once Chaos is over (QuietTail and after). |
| `workload` | The simulated clients (Chapter 12). Survives crashes. |
| `oracle` | The history recorder and linearizability checker (Chapter 13). |
| `facts` | Every `Emit{Fact}` the server ever produced, stamped with step, time and incarnation. Invariants read this timeline. |
| `env` | The `core.Env` handed to each server incarnation. |
| `parked` | One flag per client: idle and waiting to be woken (by a restart or by the end of a quiesce). |
| `quiescing` | Clients hold new requests until every outstanding request has replied. |
| `stopped` | QuietTail is over; clients issue nothing more. |
| `halted` | A failure was recorded; the run stops. Note the name: this is the *simulator* halting, unrelated to the server's `core.Halt` action. |
| `trace` | The running trace hash (Chapter 8). |
| `res` | The `Result` being accumulated. |

Everything here is owned by the one goroutine that calls `Run`. There are no locks, no channels, no atomics. `detcheck` would reject them if there were (Chapter 8).

Look at what is *not* a field: there is no list of "in-flight FS operations" and no map of "outstanding requests". In-flight FS operations live only as `KindFSLand` and `KindDeliver` entries in the queue. Outstanding requests live in the workload (which client is busy) and the oracle (which history ops are pending). The queue is the simulator's only notion of the future.

## 10.4 New: building a universe

```go
// toys3/sim/simulator.go:38-76
func New(cfg Config) *Simulator {
	if cfg.Factory == nil {
		cfg.Factory = core.NewMachine
	}
	if cfg.Invariants == nil {
		cfg.Invariants = DefaultInvariants()
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 2_000_000
	}
	if cfg.Server.Dir == "" {
		cfg.Server.Dir = "/data"
	}
	rng := NewRand(cfg.Seed)
	s := &Simulator{
		cfg:      cfg,
		rng:      rng,
		fs:       NewFakeFS(rng, cfg.FS, core.ObjDir(cfg.Server.Dir), core.TmpDir(cfg.Server.Dir)),
		workload: NewWorkload(cfg.Workload, rng),
		oracle:   NewOracle(),
		env:      &env{rng: rng, buggify: true},
		trace:    cfg.Seed,
	}
	s.parked = make([]bool, s.workload.Clients())
	s.res.Seed = cfg.Seed
	s.schedule(Entry{At: 0, Kind: KindRestart})
	for c := range s.parked {
		s.schedule(Entry{At: core.Time(s.workload.Think()), Kind: KindClientWake, Client: c})
	}
	if cfg.Workload.QuiesceEvery > 0 {
		s.schedule(Entry{At: core.Time(cfg.Workload.QuiesceEvery), Kind: KindQuiesce})
	}
	s.schedule(Entry{At: core.Time(cfg.Chaos), Kind: KindQuiet})
	s.schedule(Entry{At: core.Time(cfg.Chaos + cfg.QuietTail), Kind: KindTailEnd})
	return s
}
```

Walk through it:

1. **Defaults.** No factory means the real server (`core.NewMachine`). No invariants means all five (`DefaultInvariants`, Chapter 14). `Dir` defaults to `/data`, which is why every path in the logs starts with `/data/`.
2. **One RNG.** `NewRand(cfg.Seed)` is created once and passed by pointer to the FakeFS, the workload and the `env`. There is no second generator. (`DefaultConfig` *does* use a second, separate `Rand` to choose the swarm parameters, but that happens before `New` and does not touch the run's stream: `toys3/sim/config.go:137`. Chapter 12 explains why.)
3. **The FakeFS starts with exactly two directories**, `obj/` and `tmp/`, both durable and empty. This matches what `prod.PrepareDir` guarantees in production (`toys3/prod/executor.go:114-124`): the core has no mkdir action, so the runtime must create both directories durably before the first `Started`.
4. **The trace hash starts at the seed.** Two different seeds therefore have different traces from step zero, even if their first steps happen to be identical.
5. **The initial schedule**, in this order (each `schedule` draws one `Tie` from the RNG):
   - `KindRestart` at t=0: the first boot.
   - One `KindClientWake` per client at a random think time in `[Think[0], Think[1]]` (0 to 1 ms by default).
   - The first `KindQuiesce` at `QuiesceEvery` (8 ms by default).
   - `KindQuiet` at `Chaos` (300 ms): the end of fault injection.
   - `KindTailEnd` at `Chaos + QuietTail` (360 ms): clients stop for good.

So the entire future of the run, at construction, is five-ish entries. Everything else is created dynamically as those entries are processed.

The first lines of seed 1's log show this playing out:

```
#1      t=0.000000ms   inc=1 restart     boot
#2      t=0.000000ms   inc=1 deliver     Started => FSList{io1 /data/tmp}
#4      t=0.444359ms   inc=1 deliver     Request{c1 GET "k2"}
#6      t=0.523067ms   inc=1 deliver     Request{c2 GET "k0"}
#7      t=0.665229ms   inc=1 fs-land     land FSList{io1 /data/tmp} -> FSListDone{io1 OK []}
#9      t=0.745782ms   inc=1 deliver     Request{c3 PUT "" value-of-3} => Reply{c3 400}
#10     t=0.748746ms   inc=1 deliver     FSListDone{io1 OK []} => FSFsyncDir{io2 /data/tmp}
#12     t=0.762895ms   inc=1 deliver     Request{c4 COPY "k2" -> "k0"}
#14     t=1.142951ms   inc=1 deliver     Request{c5 DELETE "k0"}
#15     t=1.573379ms   inc=1 fs-land     land FSFsyncDir{io2 /data/tmp} -> FSFsyncDirDone{io2 OK}
#16     t=1.947497ms   inc=1 deliver     FSFsyncDirDone{io2 OK} => Emit{Recovered{temps=0}} FSRead{io3 /data/obj/k2} FSRead{io4 /data/obj/k0}
```

Step numbers skip: #3, #5, #8, #11 and #13 are missing. Those are the `client-wake` steps. A wake does not append a log entry (§10.8), but it is still a step: it increments `s.step`, folds into the trace, and runs the invariants. You can read the gaps: #3 is client 0 waking and issuing `c1` (delivered at #4 at the same instant), #5 is the next client issuing `c2`, and so on. Notice also that requests `c1`, `c2`, `c4` and `c5` arrive *during recovery* (between #2 and #16) and produce no actions: the server queues them (Chapter 5), and they all start at #16, right after `Recovered`. `c3` is the exception: an invalid empty key gets an immediate `400` without entering the queue (`toys3/core/server.go:263-265`).

## 10.5 Run: the loop

```go
// toys3/sim/simulator.go:80-123
func (s *Simulator) Run() Result {
	for !s.halted {
		e, ok := s.q.Pop()
		if !ok {
			break
		}
		if s.step >= s.cfg.MaxSteps {
			s.fail(Failure{Invariant: "max-steps", Msg: fmt.Sprintf("run did not drain within %d steps", s.cfg.MaxSteps)})
			break
		}
		s.step++
		s.now = e.At
		s.trace = Fold(Fold(Fold(Fold(s.trace, uint64(s.step)), uint64(s.now)), uint64(e.Kind)), uint64(e.Inc))
		s.handle(e)
		s.trace = Fold(s.trace, s.fs.Hash())
		if s.halted {
			break
		}
		s.afterStep()
	}
	if !s.halted {
		for _, inv := range s.cfg.Invariants {
			for _, f := range inv.AtEnd(s) {
				s.addFailure(inv.Name(), f)
			}
		}
	}
	s.res.Steps = s.step
	s.res.EndTime = s.now
	s.res.TraceHash = s.trace
	s.res.RandCalls = s.rng.Calls()
	fc := s.fs.Counters()
	s.res.Stats.FSOps = fc.Landed
	...
	return s.res
}
```

Line by line:

- **Termination.** The loop runs until the queue is empty or a failure halts the simulation. A correct run always drains: after `KindTailEnd`, clients stop issuing, so once every in-flight request has completed there is nothing left to schedule. `Pop` returns `ok=false` and the loop exits.
- **`MaxSteps`.** A run that never drains (a livelock, a server that keeps scheduling work forever, a client that retries forever) would loop until the heat death of the laptop. After 2,000,000 steps the run fails with invariant `"max-steps"`. Note the check happens *after* popping and *before* incrementing: the run executes exactly `MaxSteps` entries and fails on the attempt to execute one more. A typical passing seed takes 1,000 to 3,000 steps (seed 1: 1,628), so the limit is three orders of magnitude of headroom.
- **Advance time.** `s.now = e.At`. This is the entire implementation of virtual time. There is no clock that ticks; time is whatever the current entry says it is. If the next entry is 5 ms away, time jumps 5 ms in one assignment. That is why a run covering 360 ms of simulated time finishes in a few milliseconds of wall time.
- **Fold the step header into the trace**: the step number, the time, the entry kind and the incarnation. Every step contributes these four numbers, so two runs agree on their trace hash only if they process the same kinds of entries at the same times in the same incarnations in the same order.
- **`handle(e)`**: dispatch on kind (§10.6). This is where the server steps, the FakeFS changes, clients act, and crashes happen. Several handlers fold more into the trace.
- **Fold the FakeFS hash.** After every step, the entire filesystem state (both durable and volatile versions of everything, Chapter 11 §11.11) is folded in. This catches a class of nondeterminism the server's own `StateHash` would miss: a difference in what the *disk* looks like.
- **`afterStep()`**: end a quiesce if possible, then run every invariant's `AfterStep` (§10.10).

When the loop ends without a failure, each invariant's `AtEnd` runs once. Then the `Result` is filled in: steps, end time, trace hash, number of RNG draws, and the stats gathered from the FakeFS counters and the oracle.

`RandCalls` deserves a mention. The determinism canary (`toys3/sim/sim_test.go:24-42`) compares three numbers between two runs of a seed: `TraceHash`, `RandCalls` and `Steps`. `RandCalls` is cheap, robust, and tells you *where* to look: if two runs agree on steps but disagree on draws, some code path drew a different number of random values, which narrows the search enormously.

## 10.6 handle: one switch, eight kinds

```go
// toys3/sim/simulator.go:125-167
func (s *Simulator) handle(e Entry) {
	switch e.Kind {
	case KindDeliver:
		if e.Inc != s.inc || !s.alive {
			s.note(e, "dropped: dead incarnation")
			return
		}
		s.deliver(e.Event)
	case KindFSLand:
		if e.Inc != s.inc || !s.alive {
			s.note(e, "dropped: dead incarnation")
			return
		}
		body := s.fs.Land(e.Action, e.Plan)
		s.trace = hashEvent(Fold(s.trace, 1), body)
		if s.cfg.Log {
			s.note(e, "land "+DescribeAction(e.Action)+" -> "+DescribeEvent(body))
		}
		s.schedule(Entry{At: s.now + core.Time(e.Plan.Reply), Kind: KindDeliver, Inc: s.inc, Event: body})
		s.maybeCrash()
	case KindClientWake:
		s.wake(e.Client)
	case KindCrash:
		if s.alive {
			s.crash("scheduled crash")
		}
	case KindRestart:
		s.restart()
	case KindQuiesce:
		if !s.stopped {
			s.quiescing = true
			s.note(e, "quiesce")
		}
	case KindQuiet:
		s.quiet = true
		s.fs.SetQuiet(true)
		s.env.buggify = false
		s.note(e, "chaos over: no more faults or crashes")
	case KindTailEnd:
		s.stopped = true
		s.note(e, "quiet tail over: clients stop")
	}
}
```

### The "dead incarnation" guard is defensive

Both server-bound kinds first check `e.Inc != s.inc || !s.alive`. In the current code this branch is **never taken**. Here is why: at every crash, `DropIncarnation(s.inc)` removes every `KindDeliver` and `KindFSLand` entry of the dying incarnation (§10.9). After the crash, `alive` is false, and nothing schedules server-bound entries while the server is down: `wake` parks clients instead of issuing requests (§10.8), and FS lands and deliveries are only scheduled from inside `deliver` and the `KindFSLand` handler, both of which only run while alive. When the server restarts, `inc` increments and new entries carry the new number.

So the guard is a belt to `DropIncarnation`'s braces. It costs nothing, and it would catch a future change that, say, scheduled a completion from somewhere that did not know about crashes. It also documents intent: "an entry for a dead incarnation is meaningless". If you ever see `dropped: dead incarnation` in a log, something about that reasoning has changed and you should find out what.

### The phase markers

`KindQuiesce`, `KindQuiet` and `KindTailEnd` are not about the server at all; they change the *simulator's* behaviour:

- **`KindQuiesce`** sets `quiescing = true` (unless clients are already stopped). From then on, idle clients park instead of issuing. When every outstanding request has replied, `afterStep` ends the quiesce and schedules the next one (§10.10). Quiescent points split the history into windows the oracle can check independently (Chapter 13). In seed 1 there are 27 quiesce lines; the first few are at 8.000 ms, 21.09 ms, 35.37 ms, 49.67 ms and 61.11 ms. The first is exactly `QuiesceEvery`; each later one is 8 ms after the previous quiesce *ended* (for example, the quiesce at 8.000 ms ended around 13.09 ms, and the next began at 21.09 ms).
- **`KindQuiet`** ends Chaos. Three switches flip: the simulator stops injecting crashes (`maybeCrash` checks `!s.quiet`), the FakeFS stops injecting new faults (`SetQuiet(true)`: no new EIO plans, no read bit-flips), and buggify turns off. Seed 1's log shows it at exactly 300 ms: `#1257 t=300.000000ms inc=0 quiet chaos over: no more faults or crashes`. Note `inc=0`: phase entries are not addressed to an incarnation.
- **`KindTailEnd`** sets `stopped = true`. Clients park and never wake again. The queue drains: in-flight FS operations land and deliver, the last replies go out, and the run ends.

## 10.7 deliver: stepping the server

This is where the server under test actually runs.

```go
// toys3/sim/simulator.go:169-216
func (s *Simulator) deliver(body core.EventBody) {
	acts, panicMsg := s.callStep(body)
	s.trace = hashEvent(s.trace, body)
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindDeliver, Event: body, Actions: acts})
	}
	if panicMsg != "" {
		s.fail(Failure{Invariant: "panic", Msg: "server panicked in Step(" + DescribeEvent(body) + "): " + panicMsg})
		return
	}
	for _, a := range acts {
		s.trace = hashAction(s.trace, a)
	}
	s.trace = Fold(s.trace, s.server.StateHash())
	for i, a := range acts {
		switch x := a.(type) {
		case core.FSWrite, core.FSFsync, core.FSRename, core.FSFsyncDir, core.FSRead, core.FSUnlink, core.FSList:
			plan := s.fs.Plan(a)
			s.schedule(Entry{At: s.now + core.Time(plan.Land), Kind: KindFSLand, Inc: s.inc, Action: a, Plan: plan})
		case core.Reply:
			s.reply(x)
		case core.Emit:
			s.facts = append(s.facts, TimedFact{Step: s.step, At: s.now, Inc: s.inc, Fact: x.Fact})
			switch f := x.Fact.(type) {
			case core.CorruptObject:
				s.res.Stats.CorruptObjects++
			case core.Recovered:
				s.res.Stats.TempsRemoved += f.TempsRemoved
			}
		case core.Halt:
			if i != len(acts)-1 {
				s.fail(Failure{Invariant: "protocol", Msg: "Halt is not the last action of its step"})
				return
			}
			s.res.Stats.Halts++
			s.crash("halt: " + x.Reason)
			return
		default:
			s.fail(Failure{Invariant: "protocol", Msg: fmt.Sprintf("unknown action %T", a)})
			return
		}
		if s.halted {
			return
		}
	}
	s.maybeCrash()
}
```

### 10.7.1 callStep and panics

```go
// toys3/sim/simulator.go:218-226
func (s *Simulator) callStep(body core.EventBody) (acts []core.Action, panicMsg string) {
	defer func() {
		if r := recover(); r != nil {
			acts, panicMsg = nil, fmt.Sprint(r)
		}
	}()
	return s.server.Step(core.Event{At: s.now, Body: body}), ""
}
```

The server is stepped with `At` set to the virtual clock. That is all the time it ever sees.

If `Step` panics (an index out of range, a nil dereference, a failed type assertion), the panic is recovered and becomes a `Failure` with invariant `"panic"`. This is why `Run`'s doc comment says "it never panics on server misbehavior" (`toys3/sim/config.go:194-196`). A campaign runner that executes thousands of seeds in parallel goroutines must not be taken down by one seed's panic, and the panic must arrive with a seed and a repro command like any other failure. `build` (`toys3/sim/simulator.go:327-334`) does the same for a panicking `Factory`.

### 10.7.2 Folding the step into the trace

Before dispatching any action, `deliver` folds three things into the trace: the event (`hashEvent`), every action (`hashAction`), and the server's `StateHash()`. The fold functions live in `toys3/sim/describe.go:154-223`; each event and action type gets a distinct tag (10 to 19 for events, 30 to 40 for actions) and its relevant fields. Chapter 8 discusses why hashing both the inputs/outputs and the internal state is stronger than either alone.

Note the panic path returns before `s.server.StateHash()` is folded: after a panic the server's state is suspect, and the run is over anyway.

### 10.7.3 Dispatching actions, in order

Actions are processed in the order the server returned them. This matters, because the core's contract (`toys3/core/action.go:3-6`) says "Actions in one returned slice are started in order", and several behaviours depend on it:

- **Filesystem actions** are not executed now. `fs.Plan(a)` decides, *at issue time*, whether the operation will fail and how long it will take to land and to reply (Chapter 11 §11.3), and a `KindFSLand` entry is scheduled at `now + plan.Land`. Deciding at issue time means the RNG is consumed in issue order, independent of landing order, which keeps the run deterministic regardless of how latencies shuffle completions.
- **`Reply`** goes to the oracle and the workload immediately (§10.8).
- **`Emit`** appends to the fact timeline with the current step, time and incarnation. Two facts also update stats: `CorruptObject` (coverage for the CRC path) and `Recovered` (sum of temps removed).
- **`Halt`** must be the last action of the slice. If it is not, that is a `"protocol"` failure: the core's contract (`toys3/core/doc.go:79-82`) says Halt is always last, and a runtime cannot know what to do with actions after a fail-stop. If it is last, the simulator counts it and crashes the server *right now*, with all the preceding actions already processed. That ordering is what gives the server's "500 then Halt" its meaning: the 500 reply has already reached the oracle when the crash happens.
- **Anything else** is a `"protocol"` failure. The `Action` interface is closed (Chapter 3), so this can only happen if a mutant or a future change invents a new action type without teaching the simulator about it.

After each action, `if s.halted { return }`: if dispatching a `Reply` triggered a failure (a reply to an unknown request), processing stops.

Finally, if nothing halted, `maybeCrash()` gives the injected-crash coin a chance (§10.9).

### 10.7.4 Where the reply goes

```go
// toys3/sim/simulator.go:228-237
func (s *Simulator) reply(r core.Reply) {
	okOracle := s.oracle.Return(s.now, r)
	client, okClient := s.workload.Complete(r)
	if !okOracle || !okClient {
		s.fail(Failure{Invariant: "protocol", Msg: fmt.Sprintf("reply to unknown or already answered request c%d", r.Client)})
		return
	}
	s.res.Stats.ByStatus[statusIndex(r.Status)]++
	s.schedule(Entry{At: s.now + core.Time(s.workload.Think()), Kind: KindClientWake, Client: client})
}
```

The oracle records the return (time, status, body) and the workload marks the client idle. If either does not recognize the request, the server replied twice or replied to a request that was never made: a protocol violation. Otherwise the reply is counted by status, and the client is scheduled to wake after a think time.

## 10.8 Land, then deliver: "it happened, but nobody was told"

Here is the modelling decision that makes the simulator honest about I/O.

A real `rename(2)` does not happen at the instant the program calls it, and the program does not learn its result at the instant it happens. There is a window between "the kernel applied the change" and "the program's thread was scheduled again with the return value". If the power fails in that window, the change happened (it may even be durable, if something else flushed it) but the program never learned. After reboot, the new incarnation has no record of the operation at all.

The simulator models this with two entries:

```
 server.Step ──► [FSRename{io53}]
                      │ Plan: Land=+0.36ms, Reply=+0.37ms, Fail=false
                      ▼
 t0          schedule KindFSLand  at t0+Land
 t0+Land     ── fs-land ──►  FakeFS.Land(FSRename)  (the effect happens HERE)
                      │      schedule KindDeliver(FSRenameDone{io53}) at +Reply
                      │      maybeCrash()  ◄── a crash here: effect applied, nobody told
                      ▼
 t0+Land+Reply ── deliver ──► server.Step(FSRenameDone{io53})
```

The `KindFSLand` handler (§10.6) applies the action to the FakeFS, folds the resulting completion into the trace, logs `land <action> -> <completion>`, schedules the completion's delivery `Plan.Reply` later, and then calls `maybeCrash()`. That last call is deliberate: a crash *immediately* after a landing is exactly the dangerous case.

Seed 1 contains a clean example. At step 824, an injected crash fires in the very step where an unlink lands:

```
#815    t=183.114311ms inc=6 deliver     FSFsyncDirDone{io3 OK} => Emit{Recovered{temps=1}} FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-113)} FSRead{io5 /data/obj/k1} FSUnlink{io6 /data/obj/k2}
#816    t=183.570211ms inc=6 fs-land     land FSRead{io5 /data/obj/k1} -> FSReadDone{io5 ENOENT nil}
#817    t=183.571660ms inc=6 deliver     FSReadDone{io5 ENOENT nil} => Reply{c115 404}
#819    t=183.660914ms inc=6 deliver     Request{c117 DELETE "k1"} => FSUnlink{io7 /data/obj/k1}
#820    t=183.767631ms inc=6 fs-land     land FSUnlink{io6 /data/obj/k2} -> FSUnlinkDone{io6 OK}
#821    t=183.788611ms inc=6 fs-land     land FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-113)} -> FSWriteDone{io4 OK}
#822    t=183.875466ms inc=6 deliver     FSWriteDone{io4 OK} => FSFsync{io8 /data/tmp/0000000000000004}
#823    t=184.097013ms inc=6 deliver     FSUnlinkDone{io6 OK} => FSFsyncDir{io9 /data/obj}
#824    t=184.456617ms inc=6 fs-land     land FSUnlink{io7 /data/obj/k1} -> FSUnlinkDone{io7 ENOENT}
#824    t=184.456617ms inc=6 crash       CRASH (injected crash): kept=1 lost=1 torn=1, dropped 3 queued entries
#825    t=186.110585ms inc=0 quiesce     quiesce
#826    t=188.682064ms inc=7 restart     boot
```

Two lines share step number #824: the `fs-land` that applied `FSUnlink{io7}`, and the `crash` that `maybeCrash()` injected right after it. The completion `FSUnlinkDone{io7 ENOENT}` had just been scheduled, and the crash dropped it along with two other queued entries (the in-flight `FSFsync{io8}` and `FSFsyncDir{io9}`, whose lands had not happened yet): `dropped 3 queued entries`. Client `c117`'s DELETE never got a reply; the oracle marks it *abandoned* (Chapter 13) and must allow it to have taken effect or not.

In this particular case the unlink found nothing (`ENOENT`), so no harm. But consider the unlink of `k2` two steps earlier at #820: it landed and its completion was delivered (#823), and the server issued `FSFsyncDir{io9}` to make it durable. That fsync never landed: it was among the dropped entries. So `DELETE k2` was visibly applied, never made durable, and the client (`c116`, issued at #811 in the same log) never heard back. The crash report `kept=1 lost=1 torn=1` shows the adversary resolving three un-synced changes: one directory or content change kept, one reverted, and one file left torn. Whether `obj/k2` came back depends on which of those it was; the oracle accepts either outcome, because an abandoned DELETE may or may not have happened. That is the land/deliver split doing its job: generating exactly the "maybe" outcomes a correct server must tolerate.

Without the split, a simulator would have to choose, for every crash, between "the operation did not happen" (only un-landed operations) and "the operation happened and was acknowledged" (only delivered completions). Real systems live in the third state as well, and the bugs live there too.

## 10.9 Clients: wake, park, quiesce

### wake

```go
// toys3/sim/simulator.go:239-253
func (s *Simulator) wake(c int) {
	if s.workload.Busy(c) {
		return
	}
	if s.stopped || s.quiescing || !s.alive {
		s.parked[c] = true
		return
	}
	req := s.workload.Issue(c, s.now)
	s.res.Stats.Requests++
	s.oracle.Invoke(s.now, c, req)
	s.trace = hashEvent(Fold(s.trace, 2), req)
	s.schedule(Entry{At: s.now, Kind: KindDeliver, Inc: s.inc, Event: req})
}
```

A client wakes up and decides what to do:

- **Busy?** Each client has at most one request outstanding (`toys3/sim/workload.go:19-21`). A stray wake for a busy client is ignored.
- **Can't issue?** If clients are stopped (after TailEnd), holding for a quiesce, or the server is down, the client *parks*: it records that it is idle and wants to be woken, and does nothing else. Parking is what makes "clients do not issue requests while the server is down" true (`toys3/sim/doc.go:30-31`).
- **Otherwise**, issue: the workload builds the next request (Chapter 12), the oracle records the invocation, the request is folded into the trace, and a `KindDeliver` of the `Request` is scheduled *at the current instant*, addressed to the current incarnation.

Two subtleties:

1. **Invocation is recorded at issue, not at delivery.** The comment on `HistOp.Invoke` says "when the Request was delivered" (`toys3/sim/oracle.go:27`), but `oracle.Invoke` is called here, in `wake`. The delivery is scheduled for the same virtual instant, so the *times* agree, but other same-instant entries may run in between (the `Tie` decides). For the linearizability check this is harmless and conservative: an earlier invocation point only widens the window in which the operation may take effect, which can only make the checker more permissive, never cause a false failure. (The oracle actually orders operations by logical sequence numbers, `InvSeq`/`RetSeq`, which are assigned here too; see Chapter 13.)
2. **The request can be dropped.** If a crash happens between the issue and the delivery (possible when other same-instant entries run first and one of them triggers a crash), `DropIncarnation` drops the request delivery and `oracle.Crash` marks it abandoned. The server never saw it. That is fine: an abandoned request may or may not have happened, and "never happened" is one of the options.

### wakeParked

```go
// toys3/sim/simulator.go:255-262
func (s *Simulator) wakeParked() {
	for c, p := range s.parked {
		if p {
			s.parked[c] = false
			s.schedule(Entry{At: s.now + core.Time(s.workload.Think()), Kind: KindClientWake, Client: c})
		}
	}
}
```

Called by `restart` (if not quiescing) and by `afterStep` when a quiesce ends (if alive). Each parked client is scheduled to wake after a fresh think time. `parked` is a slice indexed by client, iterated in index order: no map, no randomness except the think times, which are drawn in client order.

### afterStep and the end of a quiesce

```go
// toys3/sim/simulator.go:336-353
func (s *Simulator) afterStep() {
	if s.quiescing && s.workload.Idle() {
		s.quiescing = false
		if !s.stopped {
			s.schedule(Entry{At: s.now + core.Time(s.cfg.Workload.QuiesceEvery), Kind: KindQuiesce})
		}
		if s.alive {
			s.wakeParked()
		}
	}
	for _, inv := range s.cfg.Invariants {
		for _, f := range inv.AfterStep(s) {
			s.addFailure(inv.Name(), f)
		}
	}
}
```

After every step, if a quiesce is in progress and no client is busy, the quiesce ends: the next one is scheduled `QuiesceEvery` from *now*, and parked clients are woken (if the server is alive; if it is down, the restart will wake them).

Then every invariant's `AfterStep` runs. Note the order: the quiesce bookkeeping happens first, then invariants. And note that the `Linearizable` invariant does not look at `s.quiescing`; it asks the *oracle* whether it is quiescent (no op pending), which is the precise condition it needs (Chapter 14). A crash, which abandons every pending op, is therefore also a quiescent point for the oracle, even in the middle of what the simulator thinks of as a busy period.

## 10.10 Crashes

### maybeCrash

```go
// toys3/sim/simulator.go:264-269
func (s *Simulator) maybeCrash() {
	if s.alive && !s.quiet && s.rng.Chance(s.cfg.CrashProb) {
		s.crash("injected crash")
	}
}
```

Called after every server step (`deliver`) and after every FS landing. During Chaos, with probability `CrashProb` (0.001 to 0.01 depending on the seed's swarm draw, Chapter 12), the server dies right there.

The placement is the point. Crashes are injected only at moments when *something just happened*: the server just stepped (and may have sent a reply, issued writes, or changed in-memory state), or the disk just changed. Crashing at an arbitrary virtual time between events would be equivalent to crashing after the previous event, so these two hook points cover every distinct crash position.

A small detail about RNG consumption: `Chance(p)` returns `false` *without drawing* when `p == 0` (`toys3/sim/rand.go:48-49`), and the `&&` short-circuits, so no draw happens when the server is down or Chaos is over. Turning crashes off therefore changes the RNG stream. That is intended: a configuration change is a different universe.

### crash

```go
// toys3/sim/simulator.go:271-306
func (s *Simulator) crash(why string) {
	rep := s.fs.Crash()
	s.res.Stats.TornFiles += rep.Torn
	s.res.Stats.LostChanges += rep.Lost
	s.res.Stats.KeptChanges += rep.Kept
	dropped := s.q.DropIncarnation(s.inc)
	midPut := false
	for _, id := range s.workload.Crash() {
		s.res.Stats.Indetermined++
		if op := s.oracle.op(id); op != nil {
			switch op.Op.(type) {
			case core.Put, core.Copy:
				midPut = true
			}
			s.parked[op.Client] = true
		}
	}
	if midPut {
		s.res.Stats.CrashesMidPut++
	}
	s.oracle.Crash(s.now)
	s.alive = false
	s.server = nil
	s.res.Crashes++
	s.trace = Fold(Fold(Fold(Fold(s.trace, 3), uint64(rep.Kept)), uint64(rep.Lost)), uint64(rep.Torn))
	if s.cfg.Log { ... "CRASH (%s): kept=%d lost=%d torn=%d, dropped %d queued entries" ... }
	delay := s.rng.Between(int64(s.cfg.RestartDelay[0]), int64(s.cfg.RestartDelay[1]))
	s.schedule(Entry{At: s.now + core.Time(delay), Kind: KindRestart})
}
```

A crash is a power loss. In order:

1. **The disk resolves.** `fs.Crash()` decides, change by change, what survives (Chapter 11 §11.9) and reports how many changes were kept, lost and torn. These become `KeptChanges`, `LostChanges` and `TornFiles` in the stats.
2. **The queue forgets the dead incarnation**: un-landed FS operations and undelivered completions are dropped (§10.2.4).
3. **Clients learn nothing.** `workload.Crash()` marks every busy client idle and returns the abandoned request ids. Each counts as `Indetermined`. For each, the simulator looks up the pending history op *before* telling the oracle about the crash (order matters: `oracle.op` only finds ops that are still open, and `oracle.Crash` closes them all). If any abandoned op was a PUT or COPY, this crash counts as `CrashesMidPut`, one of the coverage numbers `TestCoverage` requires to be non-zero (`toys3/sim/sim_test.go:86-118`): a simulator that never crashes in the middle of a write is not testing durability. The client that owned each abandoned op is parked so the restart will wake it.
4. **The oracle records the crash**: every pending op becomes `Abandoned` with return time `now` (Chapter 13).
5. **The server is gone.** `alive = false`, `server = nil`. All of its memory (the request queue, the IOID counter, the recovery state) is discarded. This is "memory is disposable" (Chapter 5) made literal.
6. **Fold and log.** The crash report goes into the trace (tag 3), and the log line records why: `injected crash`, or `halt: <reason>`.
7. **Schedule the restart** after a random downtime in `RestartDelay` (100 µs to 5 ms by default).

A `Halt` takes exactly the same path (`deliver` calls `s.crash("halt: " + x.Reason)`). From the disk's point of view there is no difference between the server choosing to stop and the power failing; in both cases un-synced state is at the mercy of the adversary. `DESIGN.md:124-128` states this as the crash model, and Chapter 7 explains why it is the right model for fail-stop.

Seed 1 shows both kinds back to back:

```
#232    t=53.087606ms  inc=1 fs-land     land FSFsync{io78 /data/tmp/000000000000004c} -> FSFsyncDone{io78 EIO}
#233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => Reply{c30 500} Halt{fsync temp: EIO}
#233    t=53.112270ms  inc=1 crash       CRASH (halt: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
#235    t=55.879722ms  inc=2 restart     boot
```

The fsync of a temp file failed with EIO at #232 (a planned fault). Its completion was delivered at #233, the server replied 500 and halted (fsyncgate, Chapter 7), and in the same step the simulator crashed it: 2 un-synced changes kept, 2 lost, 1 queued entry dropped. 2.77 ms of downtime later, incarnation 2 booted.

## 10.11 Restart

```go
// toys3/sim/simulator.go:308-325
func (s *Simulator) restart() {
	s.inc++
	m, panicMsg := s.build()
	if panicMsg != "" {
		s.fail(Failure{Invariant: "panic", Msg: "Factory panicked: " + panicMsg})
		return
	}
	s.server = m
	s.alive = true
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindRestart, Note: "boot"})
	}
	s.schedule(Entry{At: s.now, Kind: KindDeliver, Inc: s.inc, Event: core.Started{}})
	if !s.quiescing {
		s.wakeParked()
	}
}
```

The incarnation number goes up, and a brand new server is built from the factory with the same `env` and `cfg.Server`. This is the only way server state crosses a crash: it does not. Everything the new server knows, it must learn from the disk, starting with the `Started` event (Chapter 5).

`Started` is scheduled at `s.now`, the same instant as the restart. Parked clients are woken after think times that can be zero (`Think` defaults to `[0, 1ms]`). So a client wake, and the `Request` delivery it schedules at the same instant, can tie with the `Started` delivery, and the random `Tie` decides which goes first. **A request can reach the server before `Started` does.** This is not a simulator bug; it is a faithful model of production, where an HTTP request can arrive before the loop has processed its first event. It is exactly why the server has a `phaseBooting` that queues requests rather than rejecting them (`toys3/core/server.go:15`, `toys3/core/server.go:288-291`). Seed 1's restarts happen to deliver `Started` first, but other seeds explore the other order, and the server must handle both.

If a quiesce is in progress, parked clients stay parked until the quiesce ends. Because a crash abandons every pending op, the workload is idle immediately after a crash; the quiesce therefore ends at the very next `afterStep`, but `afterStep` only wakes clients `if s.alive`, so during downtime the clients stay parked and the restart wakes them.

## 10.12 The env: randomness and buggify for the server

```go
// toys3/sim/simulator.go:398-409
type env struct {
	rng     *Rand
	buggify bool // false during QuietTail
}

func (e *env) Uint64() uint64       { return e.rng.Uint64() }
func (e *env) Intn(n int) int       { return e.rng.Intn(n) }
func (e *env) Maybe(core.Site) bool { return e.buggify && e.rng.Chance(0.05) }
```

The server's `core.Env` (Chapter 3) draws from the run's single RNG, so any random choice the server makes replays with the seed. `Maybe` implements buggify: during Chaos, a buggify site fires with 5% probability; after `KindQuiet`, `buggify` is false and `Maybe` returns false *without drawing* (the `&&` short-circuits).

The surprise, stated already in Chapter 3 and worth repeating: **the current server never calls its `env`.** `core.Server` stores it (`toys3/core/server.go:88`) and never reads it. There are no `Maybe` call sites anywhere in `toys3/core`. The interface is ready, the simulator side is implemented, and the switch-off in QuietTail is correct; the sites themselves are an exercise (Chapter 17 suggests some). This is also why `TestDeterminismCanary` would not notice if `env` were broken: nothing exercises it.

FoundationDB's buggify also differs in one respect worth knowing: each site is enabled or disabled *once per run*, and then fires with some probability on each evaluation. `docs/lessons.md` (§7) describes that two-level scheme for the larger project. toys3's `env.Maybe` is the simpler one-level version: every evaluation is an independent 5% coin.

## 10.13 Failures and logging

### fail and addFailure

```go
// toys3/sim/simulator.go:355-374
func (s *Simulator) addFailure(name string, f Failure) {
	if f.Invariant == "" {
		f.Invariant = name
	}
	s.fail(f)
}

func (s *Simulator) fail(f Failure) {
	if f.Step == 0 {
		f.Step, f.At = s.step, s.now
	}
	s.res.Failures = append(s.res.Failures, f)
	s.halted = true
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindCrash,
			Note: "FAILURE " + f.Invariant + ": " + f.Msg})
	}
}
```

The first failure halts the run ("after the first violation the rest of the history is noise"). But note what "first" means: `afterStep` loops over *all* invariants and all their failures for the current step before `Run` checks `s.halted` again. So **several failures can be recorded in the same step**. The mutants in Chapter 15 show this regularly. Mutant b at seed 1 produces two `obj-never-torn` failures at step 44 (both `obj/k0` and `obj/k2` are torn in the durable view), and mutant e at seed 18 is reported by both `no-corrupt-reads` and `linearizable` at step 657, because the corrupt GET completed the window the oracle was waiting on. Campaign summaries report `Failures[0]`, so the order of `DefaultInvariants()` (`toys3/sim/invariant.go:44-46`) decides which name you see first when several fire together.

A cosmetic quirk: failures are logged with `Kind: KindCrash`, so in the event log a failure line reads `crash FAILURE ...`:

```
#380    t=91.269593ms  inc=3 crash       FAILURE linearizable: no linearization of 1 ops ...
```

It is not a crash of the server; it is the simulator stopping.

### Log mode must not change the run

`Config.Log` turns on the event log (`Result.Log`). Every logging site in the simulator is guarded by `if s.cfg.Log`, and none of the guarded code touches the RNG, the queue, the trace or any state. That is a design rule, not a coincidence, and it is tested:

```go
// toys3/sim/sim_test.go:32-37
		// Log mode only records; it must not change the run.
		cfg := DefaultConfig(seed)
		cfg.Log = true
		if c := Run(cfg); c.TraceHash != a.TraceHash || len(c.Log) == 0 {
			t.Fatalf("seed %d: Log mode changed the trace (%x vs %x)", seed, c.TraceHash, a.TraceHash)
		}
```

Why this matters so much: the workflow for every failure is "the campaign found seed N without logging; replay seed N *with* logging to see what happened". If turning logging on changed the run, the replay would be a different universe and the bug might vanish. Heisenbugs are the very thing DST exists to eliminate; a simulator whose observation changes the outcome would reintroduce them. Imagine a well-meaning change that, in log mode only, called `describePayload` on a value that happened to draw a random number, or scheduled an extra debug entry. The canary would catch it immediately.

### Reading a log line

`LogEntry.String()` (`toys3/sim/describe.go:117-137`) renders:

```
#<step> t=<ms.µs-ns>ms inc=<n> <kind> [<event> => <actions>] [<note>]
```

`fmtTime` (`toys3/sim/describe.go:149-152`) prints virtual nanoseconds as milliseconds with six fractional digits, so `t=53.112270ms` is 53,112,270 ns. Deliveries print the event and the actions; lands, crashes, restarts and phase markers print a note.

## 10.14 The life of a run: phases

Putting the phase markers together gives the shape of every run:

```
 t=0                                     t=300ms              t=360ms
 │◄──────────────────── Chaos ───────────►│◄── QuietTail ──────►│◄── drain ──►│
 │ faults: EIO plans, read bit-flips      │ no new EIO plans    │ clients     │
 │ crashes: maybeCrash after every step   │ no injected crashes │ stopped     │
 │ buggify: on (5%)                       │ buggify off         │ in-flight   │
 │ clients issue; quiesce every ~8ms      │ clients issue       │ work lands, │
 │                                        │ (Liveness: no 500s  │ replies go  │
 │                                        │  for ops invoked    │ out, queue  │
 │                                        │  here)              │ empties     │
 ▼ KindRestart                            ▼ KindQuiet           ▼ KindTailEnd ▼ AtEnd invariants
```

Seed 1:

```
#1257   t=300.000000ms inc=0 quiet       chaos over: no more faults or crashes
#1258   t=300.106350ms inc=11 fs-land     land FSFsync{io57 /data/tmp/0000000000000036} -> FSFsyncDone{io57 OK}
...
#1595   t=360.000000ms inc=0 tail-end    quiet tail over: clients stop
...
#1626   t=371.313389ms inc=11 fs-land     land FSFsyncDir{io186 /data/obj} -> FSFsyncDirDone{io186 OK}
#1627   t=371.320483ms inc=11 deliver     FSFsyncDirDone{io186 OK} => Emit{Committed{c225 "k0" crc=bfa8d329}} Reply{c225 200}
```

The run ended at step 1628 (as reported by `steps=1628`) but the log's last line is #1627: step 1628 was the client wake scheduled by `c225`'s reply, which found `stopped == true`, parked the client, and wrote nothing to the log. After it, the queue was empty. The drain took 11 ms of virtual time past TailEnd, because a PUT that started just before 360 ms still had to go through write, fsync, rename and fsync-dir.

Seed 1 also shows the server running in incarnation 11 by the end: ten crashes, nine of them Halts.

### A subtlety: faults planned in Chaos can land in QuietTail

`FakeFS.Plan` decides `Fail` at *issue* time, using the quiet flag at that moment (`toys3/sim/fakefs.go:136-142`, Chapter 11). An FS action issued at 299.9 ms with a planned EIO lands after 300 ms, in QuietTail, and still fails. If it is an fsync, the server halts, and the simulator crashes it, during QuietTail. `maybeCrash` does not fire in QuietTail, but a `Halt` is not an injected crash; it is the server's own decision, and the simulator honours it.

Is this a problem for the Liveness invariant (Chapter 14), which requires no 500s in QuietTail? No, by construction. `Liveness` only flags 500s for ops *invoked* after QuietTail began (`toys3/sim/invariant.go:176-190`), and every FS action of such an op is planned in quiet mode and cannot fail. A leftover failure from Chaos can only hit an op that was invoked in Chaos. It can, however, cause a crash in QuietTail that abandons a QuietTail op; the run then needs the restart (at most 5 ms later) and the remaining tail to finish, which the 60 ms tail accommodates. Read corruption, by contrast, is decided at *land* time (`toys3/sim/fakefs.go:388`), so it stops exactly at 300 ms.

## 10.15 Why the simulator is shaped like this

It is worth stepping back and asking what properties the design buys.

- **Total order on events.** `(At, Tie, Seq)` plus one goroutine means there is exactly one "next thing". There are no races in the simulator, because there is nothing for anything to race with.
- **Every source of nondeterminism is a draw from one stream.** Ties, latencies, faults, crashes, crash resolution, workload choices, swarm parameters (from a separate but seeded stream), and the server's env. Grep the package for `rng` and you have the complete list.
- **The future is data.** The queue holds plain `Entry` values. A crash is "filter the queue", not "kill some goroutines and hope". This is what makes "drop every in-flight operation of a dead process" a ten-line function.
- **The server is a black box with a narrow interface.** The simulator calls `Factory`, `Step` and `StateHash`. It never reaches inside. Mutants (Chapter 15) exploit this: any `core.Machine` can be plugged in.
- **Checks run every step, at the step where the problem is first visible.** And the run stops there, so the end of the event log is the bug.
- **Observation does not perturb.** Log mode is pure recording, and a test enforces it.

## Key takeaways

- The queue *is* time. Entries are ordered by `(At, Tie, Seq)`: virtual time, then a seeded random tie-break that lets different seeds explore different same-instant orders, then insertion order to make the order total. (`loop.md` lists `(time, seq, rngTie)`, which would never consult the random tie; the code's order is the right one.)
- `Run` pops, advances `now`, folds the step header into the trace, handles the entry, folds the FakeFS hash, and runs invariants. A run ends when the queue drains; `MaxSteps` turns non-termination into a failure.
- FS actions are planned at issue, applied at *land*, and their completions *delivered* later. A crash between land and deliver gives "it happened, but nobody was told", the most important state a storage system must survive.
- A crash (injected or `Halt`) resolves the disk, drops the dead incarnation's queued entries, abandons outstanding requests, discards the server, and schedules a restart. A restart builds a fresh server and delivers `Started` at the same instant; requests may arrive first.
- Crashes are injected only right after a server step or an FS landing; those two hook points cover every distinct crash position.
- Several invariants can fail in the same step. Failures are logged with kind `crash`. Logging must never change the run, and a test enforces it.
- The server's `env` and the buggify switch are wired up, but the server has no buggify sites yet.

## Exercises

1. **Reproduce the step-824 crash.** Run `go test ./toys3/sim -run TestSeeds -seed=1 -v` and confirm the trace hash. That command only prints the event log on failure. To see the log of a passing seed, write a tiny test in a scratch copy of the package (or a separate module with a `replace` directive pointing at this repo) that runs `sim.Run` with `cfg.Log = true` and prints `sim.FormatLog(r.Log)`. Find steps #815 to #826 and confirm the excerpt in §10.8. Then find another `injected crash` in the log and work out, from the surrounding lines, which queued entries it dropped.
2. **Break the tie-break.** Change `less` in `toys3/sim/queue.go` to compare `Seq` before `Tie`. Predict: will `TestQueueTieBreakDependsOnSeed` fail? Will `TestDeterminismCanary` fail? Will any mutant in `TestCatchesBugs` become harder to catch? Run `go test ./toys3/sim -run 'TestQueue|TestDeterminismCanary|TestCatchesBugs' -v` and compare with your predictions. Revert.
3. **Remove the land/deliver split.** Make the `KindFSLand` handler deliver the completion immediately instead of scheduling it (call `s.deliver(body)` directly, keeping `maybeCrash` after it). Predict which mutants get harder or easier to catch, and why. Run `TestCatchesBugs` and look at the first-catch seeds. Revert.
4. **Crash only at lands.** Remove the `s.maybeCrash()` call at the end of `deliver`. Which crash positions can no longer be reached? Does `TestCoverage` still pass? Does any mutant's first-catch seed change?
5. **Prove the dead-incarnation guard is dead.** Add a counter to the `"dropped: dead incarnation"` branches and assert it is zero at the end of every run in `TestSeeds`. Run 200 seeds. Then argue in writing what change elsewhere in the simulator would make it non-zero.
6. **Requests before Started.** Write a test that runs seeds 1..200 with `Log` on and counts runs in which some `Request` delivery for incarnation *n* appears in the log before that incarnation's `Started` delivery. Is the count non-zero? If it is zero, explain which entries would have to tie, and check whether `Think`'s minimum of 0 makes it possible.
7. **Break log-mode purity on purpose.** In `deliver`, inside `if s.cfg.Log { ... }`, add `s.rng.Uint64()`. Run `go test ./toys3/sim -run TestDeterminismCanary`. Read the failure message. Revert.

## Further reading

- Will Wilson, *"Testing Distributed Systems w/ Deterministic Simulation"*, Strange Loop 2014. The FoundationDB simulator's event loop, virtual time, and simulated machine reboots.
- TigerBeetle's VOPR, documented in the TigerBeetle repository. Its packet simulator and storage simulator separate "the fault happened" from "the replica observed it" in a way closely analogous to land/deliver.
- The sled project's notes on simulation (sled.rs), which advocate driving the system from a single-threaded, seeded event loop and treating the network and disk as message queues.
- `docs/lessons.md` in this repository, §5 ("Explicit pending results; crashes resolve them") and §8 ("A run has explicit phases"), which are the design notes this simulator implements.
