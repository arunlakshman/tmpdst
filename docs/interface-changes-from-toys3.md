# Interface changes learned from toys3

toys3 is a small S3-like store built end to end with the same DST design as
this repo: a sans-I/O core, a simulator, a production runtime, oracle,
invariants, mutants, golden traces and reference specs (veritas). This document
lists the changes it implies for the root contracts in `core/`, `sim/` and
`sys/`. The short form of each accepted decision is a row in `decisions.md`
(rows 7–22). Evidence points into `toys3/` (code) and `toys3/book/` (chapters).

Status: **accepted, not yet applied to the Go files.**

---

## 1. core

### 1.1 Fail-stop: `Trip` (decision 7)

```go
// Trip asks the runtime to kill this incarnation: the machine's memory can no
// longer be trusted. It is the last action of its Step; the runtime starts the
// actions before it, then stops. Sim: same path as a crash (DropNode +
// Engine.OnCrash), counted separately as a trip. Prod: the loop returns
// ErrTripped and the process exits for its supervisor to restart.
type Trip struct{ Cause string }
```

- Why: toys3 mutants d/d2. Serving on after an error that leaves state
  untrustworthy loses acked data. Brokers need the same: a fence or epoch it
  cannot reconcile, a broken internal invariant, a commit outcome it cannot
  resolve.
- Evidence: `toys3/control/action.go:21-26`, `toys3/sim/simulator.go:200-207`,
  `toys3/prod/loop.go:91`, book ch7.
- The simulator fails a seed with a `Protocol` failure if `Trip` is not last.

### 1.2 Graceful stop: `Stop` and `Exit` (decision 8)

```go
// Stop asks the node to shut down gracefully by Deadline (node-local time):
// stop admitting, finish or hand off in-flight work, then emit Exit. Class: Control.
type Stop struct{ Deadline Time }

// Exit is the last action of a graceful shutdown. Like Trip it ends the
// incarnation, but it is not a fault: the sim counts it as a graceful stop.
type Exit struct{}
```

- If `Deadline` passes without `Exit`, the runtime kills the node. In sim this
  is a crash; in prod it is SIGKILL after the grace period.
- This gives `sim.Attrition.GracefulWeight` something to drive. Today it has no
  event behind it.
- Invariant to add: a node that emitted `Exit` has no acked-but-uncommitted
  produce.
- `core.ClassOf` gets a `Stop` row (Control).

### 1.3 The machine contract includes its hash (decision 9)

```go
type Machine interface {
	StateMachine
	Hashable
}

type Factory func(env Env, self NodeRef) Machine
```

- Why: today `Hashable` is optional, so a machine that lacks it silently drops
  out of the trace hash. `StateHash` also forces the author to say what the
  state is.
- Mutants are `Factory` decorators that delegate `StateHash` (toys3 ch15).
- `sim.Workload` embeds `core.Machine`.
- Evidence: `toys3/control/machine.go:16-25`.

### 1.4 I/O actions and completions are typed (decision 10)

```go
// IOAction is an Action that the outside world executes and answers with
// exactly one Completion carrying the same ReqID.
type IOAction interface {
	Action
	ReqID() ReqID
}

type Completion interface {
	EventBody
	ReqID() ReqID
}

var (
	_ IOAction   = ObjPut{}  // ObjGet, ObjDelete, ObjList, CPCall, Send, Dial
	_ Completion = ObjPutDone{} // ObjGetDone, ObjDeleteDone, ObjListDone, CPDone, SendDone, DialDone
)
```

- Add `DialDone{Req ReqID; Conn ConnID; Err ErrCode}`. `Dial` has no completion
  today.
- The method is `ReqID()` so it does not clash with the existing `Req` field.
- Rule (on `Step`): a Completion with an unknown, already-finished or
  wrong-kind ReqID is ignored with no state change. toys3 tests this as
  `TestStaleIOIgnored`, which checks that `StateHash` is unchanged.
- Rule (on `ReqID`): unique within one incarnation only. ReqIDs must never
  reach a durable name; object keys come from `Env.NewID()`. This is toys3
  mutant f: a reused temp name hard-linked a committed object.
- Evidence: `toys3/control/storage.go:8-24`, commit 10777a2.

### 1.5 Contract wording (decision 11)

These are doc changes only.

- `Step`: actions in one slice start in slice order. I/O among them may
  complete in any order.
- Exactly one completion per IOAction from a live incarnation.
  `LostResponse` surfaces as a completion (`ErrTimeout` + `MaybeApplied`), never
  as silence. A lost completion stalls the work queued behind it forever.
- Completions addressed to a dead incarnation are dropped.
- **Remove `ErrInterrupted`.** No live incarnation can ever receive it.
- `ErrNotFound`, `ErrConflict` and `ErrFenced` are always truthful. Engines may
  inject only `ErrTimeout`, `ErrUnavailable`, `ErrIO` and `ErrConnClosed`.
- `ErrCorrupt` is produced by the core's segment decoder, never by
  `sys.Classifier`.
- Enums that are folded into the hash (`ErrCode`, `Outcome`, `Class`, later
  `EntryKind`) are append-only and never reordered.
- Add `func (ErrCode) String() string` with frozen spellings.
- `Started`: the runtime guarantees it is the first Step of every incarnation.
  Control-first `InboxPolicy` already gives this in both prod and sim; the sim
  asserts it. toys3's sim could deliver a request before `Started` (book ch5
  §5.9); the shared policy rules that out here.

### 1.6 Facts are hash inputs (decision 12)

- `Fact.Name` and the field encoding feed the trace hash. Names are frozen
  string constants, past tense (`segment_committed`, `produce_acked`), and
  never derived from Go type names. Renaming one means re-baselining the
  golden file. (toys3 commits db1b837 and 91ab309; `telemetryHashKey`.)
- A fact may carry fields only a monitor needs, such as a checksum.
- **Ack facts go first:** `Emit{produce_acked}` comes in the same Step,
  immediately before the `Send` that carries the ack. This is what lets
  invariants check "acked ⇒ committed" on the same step, with no crash
  needed. In toys3 this caught mutants a and c at step ~46.

---

## 2. sim

### 2.1 Invariants see a View with ground truth (decision 13)

```go
type Invariant interface {
	Name() string
	Observe(v View) // after every step
	AtEnd(v View)   // after drain: liveness, final oracle window
}
// Reset is removed: Config.Invariants already builds fresh instances per seed.

type View interface {
	Step() uint64
	Now() Time
	Phase() Phase
	Alive(core.NodeRef) bool
	Timeline() Timeline
	History() History
	Objects() ObjectTruth
	CP() CPTruth
	Assert() core.Assertions // how invariants report
}

// Ground truth uses builtin types only (anti-corruption layer). Iteration is
// in sorted key order.
type ObjectTruth interface {
	Object(key string) ([]byte, bool)
	EachObject(fn func(key string, data []byte))
	Gen() uint64 // bumps when anything applied changes; lets checks skip steps
}

type CPTruth interface {
	Committed(tp core.TopicPartition) []core.BatchRange
	HighWatermark(tp core.TopicPartition) core.Offset
	Gen() uint64
}
```

- Why: in toys3 mutant b (skip fsync) is invisible to the linearizability
  oracle, because the CRC turns the loss into a 500 and a 500 promises nothing.
  Only the ground-truth checks `AckedIsDurable` and `ObjNeverTorn` catch it.
- Evidence: `toys3/sim/invariant.go:22-58`.

### 2.2 Engines (decision 14)

```go
type Engine interface {
	core.Hashable // folded into the trace hash after every step
	Name() string
	Handles(a core.IOAction) bool
	Execute(ctx EngineCtx, from core.NodeRef, a core.IOAction)
	OnInternal(ctx EngineCtx, body any)
	OnCrash(ctx EngineCtx, ref core.NodeRef)
	EnterRecovery()
}

type EngineCtx interface {
	// ... existing methods ...
	Assert() core.Assertions // fault coverage: "lost CP response happened"
}
```

- **Every fault coin and delay is drawn in `Execute`, at issue time.** How a
  fault resolves (applied or not) may be drawn when it lands. Draws therefore
  happen in issue order, and "op #47 fails" stays stable when delay ranges
  change.
- `EnterRecovery` affects actions issued after it. Plans already in flight stand.
- **Crash rule (differs from toys3):** `DropNode` drops only entries addressed
  to the node. Engine-internal *apply* entries survive. An S3 PUT or CP commit
  already on the wire can land after the broker dies. toys3 drops un-landed
  effects because its local disk dies with the process; that is wrong here.
- Fault structs:

```go
type ObjectStoreFaults struct {
	ApplyLatency   TwoSpeed // issue → effect (t1)
	RespondLatency TwoSpeed // effect → reply (t2)
	Unavailable503 float64
	Timeout        float64
	LostResponse   float64
	GetCorruptProb float64 // flip one bit in ObjGetDone.Data; stored object intact
	Outage         Outage
	CallerTimeout  core.Duration
}

// Outage is a correlated, sustained failure window (every op fails while it lasts).
type Outage struct {
	Prob float64
	Dur  [2]core.Duration
}
// ControlPlaneFaults gets the same latency split and Outage.
```

- `GetCorruptProb` is needed because without it the checksum path is dead
  code. toys3 mutant e went uncaught in 1,000 seeds and was caught at seed 18
  once bit flips were on.

### 2.3 History and the oracle are separate from the workload (decision 15)

```go
type OpOutcome uint8 // append-only

const (
	OpPending       OpOutcome = iota
	OpDefinite                // result known; effect inside [Invoke, Return]
	OpIndeterminate           // MaybeApplied: may take effect at ANY time after Invoke
	OpAbandoned               // client gave up and will not retry this op id
)

type HistOp struct {
	ID             core.ID       // the request
	Value          core.ID       // the write's identity, shared by its retries
	Client         core.NodeRef
	Op             any
	InvSeq, RetSeq uint64        // logical order; virtual time ties
	Invoke, Return Time
	Outcome        OpOutcome
	Result         any
}

type History interface {
	Invoke(client core.NodeRef, id, value core.ID, op any)
	Return(id core.ID, result any, o OpOutcome) bool // false: unknown or duplicate (protocol failure)
	Ops() []HistOp
}

type Workload interface {
	core.Machine
	Name() string
	Quiescent() bool // no ops in flight: the oracle may close a window here
}
// Workload.Check is removed. Linearizability is an Invariant that checks at
// quiescent windows (Config.QuiesceEvery) and in AtEnd.
```

- **Indeterminate ops are unbounded, Jepsen `:info` style.** toys3 bounds a
  failed op's effect to [Invoke, Return] only because every risky 500 trips in
  the same Step. Brokers do not trip on S3/CP timeouts, so a `MaybeApplied`
  produce may become visible at any later time, including after the client
  has moved on.
- Consequences for the oracle:
  - Indeterminate ops never close; they stay candidates in every later window.
  - Request id and write identity are separate, so idempotent-producer retries
    (same producerId/seq) are two requests with one `Value`. Seeing the value
    twice in a log is a duplicate-append violation.
  - Record payloads are self-describing: bytes read back name the write that
    produced them (toys3 `value.go`).
  - The search must stay bounded. When the candidate set of indeterminate ops
    exceeds the budget, widen (toys3 oracle `Budget`) and report
    `Inconclusive`, never a violation.

### 2.4 Per-seed results and failures (decision 16)

```go
type FailKind uint8 // append-only

const (
	FailViolation FailKind = iota
	FailInconclusive
	FailProtocol // Trip/Exit not last, unknown action, reply to unknown request, duplicate completion
	FailPanic    // recovered from Step / Factory
	FailMaxSteps
	FailNondeterminism
)

type Failure struct {
	Seed uint64
	Kind FailKind
	Prop core.Prop
	Step uint64
	At   Time
	Msg  string
}

type Counter struct {
	Name string
	N    uint64
} // ordered slice so golden rows survive renames

type SeedResult struct {
	Seed                   uint64
	Steps, RandCalls       uint64
	TraceHash              uint64
	Crashes, Trips, Exits  int
	Stats                  []Counter
	Failures               []Failure
	Log                    []LogEntry
}

type Report struct {
	Seeds          []SeedResult
	Always         []AssertionStat
	Coverage       []AssertionStat
	Nondeterminism []uint64
}

// AssertionStat gains FirstSeed, FirstStep uint64.
// Config gains MaxSteps uint64, Log bool, HaltOnFirst bool (default true in Replay).
// Config.CheckDeterminism becomes uint32: replay every Kth seed and compare
// (TraceHash, RandCalls, Steps).
```

- Contracts:
  - Logging and `Replay` must not change `TraceHash`.
  - Adjacent seeds must differ (canary).
  - Campaigns record and continue (lesson 11). Replay halts at the first
    failure.
- A golden file pins `(Steps, RandCalls, TraceHash, Stats)` per seed, plus each
  mutant's first-catching seed. That is how a refactor is proved
  behavior-neutral.

### 2.5 Randomness and swarm (decision 17, supersedes lessons §9 last bullet)

```go
type CountedRand interface {
	// ... existing ...
	Chance(p float64) bool        // draws nothing when p <= 0
	Between(lo, hi int64) int64   // inclusive lo, exclusive hi
	Derive(label string) CountedRand // independent stream from (seed, label)
}

// Swarm picks this seed's knobs from menus using rand.Derive("swarm"),
// never the run stream.
type Swarm func(seed uint64, r CountedRand, c *SeedConfig)
```

- Why: drawing swarm knobs or per-site buggify activation from the run stream
  means editing a menu, or adding the first buggify site, reshuffles every
  seed. In toys3 the swarm alone moved mutant f's first catch from seed 429
  to seed 24.
- Replay re-derives the config from the seed alone.

### 2.6 Scheduler (decision 18)

```go
type EntryKind uint8 // append-only: Deliver, EngineInternal, Crash, Restart, Stop, Phase, Quiesce

type Entry struct {
	At     Time
	Lane   Lane
	Kind   EntryKind
	Target Target
	Body   any
	Tie    uint64 // drawn at Schedule: exactly one draw per call
	Seq    uint64
}

// DropNode returns the number of entries dropped.
// Order: (At, Tie, Seq), with entries on one Lane at the same At kept in FIFO
// order.
```

- Fix `loop.md:89`. It says `(time, seq, rngTie)`, under which the random tie
  can never decide anything.
- Crashes, restarts, stops, phase changes and quiesce points are queue entries
  addressed to the simulator itself.
- Attrition gains `CrashProb float64`, evaluated after each node Step and each
  engine landing during Chaos.
- `Config.Scripted []ScriptedFault{At Time; Node core.NodeID; Kind EntryKind}`
  supports regression tests that need a crash at a specific step.
- Rename `Phases.QuietTail` to `Settling`. Liveness rule: no `ErrUnavailable`
  or timeout for ops *invoked* in Settling. Faults planned in Chaos may still
  land there. Add `Phases.MaxQuietLatency`.

---

## 3. sys

### 3.1 Executor, Sink, Classifier (decision 19)

```go
// Executor runs I/O only. The loop handles Emit, timers, Trip, Exit and
// panics on an unknown action. Exactly one Completion per IOAction, with the
// same ReqID, delivered to the Sink and never on the caller's goroutine.
type Executor interface {
	Execute(a core.IOAction)
}

type Sink interface {
	DeliverControl(body core.EventBody)
	DeliverData(body core.EventBody)
	// Closed is closed when the incarnation ends. Later deliveries are dropped,
	// never block, and never reach the next incarnation.
	Closed() <-chan struct{}
}

// Classify reduces err to plain data. It logs err; the core only sees the
// code. The Outcome depends on which operation failed, not only on err.
type Classifier interface {
	Classify(a core.IOAction, err error) (core.ErrCode, core.Outcome)
}
```

- The two inboxes (decisions G/H) stay. toys3's single FIFO, where completions
  wait behind ingress, is exactly the hazard lesson 13 warns about.
- The `done func(...)` callback shape from toys3 is not adopted.

### 3.2 Runtime loop (decision 20)

```go
var (
	ErrTripped = errors.New("tripped")
	ErrStopped = errors.New("stopped")
)

// Run delivers Started, then loops: InboxPolicy.Next, stamp At/Wall at
// dequeue, Step, dispatch actions in order. Returns ErrTripped on Trip, nil
// on Exit, ErrStopped on ctx done.
func Run(ctx context.Context, b Bundle, f core.Factory, self core.NodeRef) error
```

---

## 4. New packages (decisions 21, 22)

- **`veritas/`** (21). Executable specs: sequential, fault-free, short. Each
  doubles as the unit-test fake and implements the matching `sys` interface.
  - `veritas.ObjectStore`: a map.
  - `veritas.ControlPlane`: CommitSegment idempotent per SegmentKey.
  - `veritas.Partition`: an append-only log with a high watermark.
  - Runner: port toys3's generic lockstep runner and shrinker
    (`veritastest`: `Check[S, Op]` with New/Gen/Step/Invariant/Shrink/
    ShrinkAll/Format and a `TB` interface, detcheck-clean).
  - Conformance checks:
    - **C1**: broker with no faults vs `veritas.Partition`.
    - **C2**: each sim engine with no faults vs its veritas.
    - **C3**: `sys.ObjectStore` and `sys.ControlPlane` against MinIO/LocalStack
      vs veritas. This also covers `Classifier`; DST never executes `sys`.
    - **C4**: the Kafka oracle vs `veritas.Partition`.
  - The oracle must not import veritas; C4 is what keeps them independent.
- **`format/`** (22). Segment object layout (magic, CRC, length) and segment key
  naming, shared by the broker, sim invariants and veritas, under detcheck.
- **Mutants** (in `sim` tests): `Mutant{Name; Wrap func(core.Factory) core.Factory; Why string}`.
  - Pin a table of first-catch seeds and which check caught each mutant.
  - Mark invariants `BlackBox() bool` to show what clients alone can see.

---

## 5. Kept as is

- **Confirmed by toys3:**
  - sans-I/O `Step`, and time on the Event;
  - a Factory per boot;
  - `DropNode`/`OnCrash`;
  - lanes with a seeded tie;
  - a single counted stream;
  - the `Outcome` triple;
  - two inboxes;
  - `Env.Assertions` alongside external invariants;
  - one `Classifier`.
- **Unused by toys3 but kept:** `Scheduler.Cancel`, `Reseed`, `Fingerprint`,
  `MaybeP`, `Fault`, the `Runner` interface. toys3 is single-node with no
  timers, so it is not evidence against them.

## 6. Rejected

- **Porcupine as the checker of record.** It was reverted in toys3 (9cc93a1):
  its verdicts depend on wall-clock timeouts, it uses goroutines, and its state
  space blows up. No wall-clock-bounded checker goes on the detcheck allow-list.
- **Making the oracle depend on veritas.**
- **toys3's full package split, control-theory naming and done-callback
  Executor.**
