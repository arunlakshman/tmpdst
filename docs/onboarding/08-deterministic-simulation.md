# One seed replays a whole cluster

You give the simulator one seed. The simulator gives you one whole cluster history. The history has crashes. The history has partitions. The history has lost responses and corrupt reads. You run the seed again and you get the same history, step by step.

That property is the point of the design. A seed fails in a campaign at night. You replay the seed on your machine in the morning. You see the same steps in the same order. You do not guess which order the cluster took.

The price is real, and this chapter takes a view on the price. Production code gives up direct input and output. Production code gives up direct clocks and direct randomness. The simulator gives up parallel run time and runs in one goroutine. The trade is worth the price for this project. Order decides the hard bugs here. A crash at one step and a crash at the next step can give you two different clusters. If you cannot replay the step, you cannot prove the fix.

You find the design brief in `loop.md` at the repo root. You find the implementation in `sim/`. You find the engines in `sim/engines/`. You find two scenarios in `sim/kafka/` and `sim/echo/`. You find the driver in `cmd/manisim/`. If you read one file first, you read `loop.md`.

## The contract in `loop.md` makes one history per seed

`loop.md` states the deal in fifteen instructions. Six instructions carry the rest:

- One event loop runs each broker. A broker is a single-threaded state machine. The broker does no input and no output. The broker takes events and returns actions. The code defines `core.Machine` (`core/machine.go`) as `Step(Event) []Action` plus a mandatory `StateHash() uint64`. State changes only inside `Step`. A completion with an unknown request ID leaves state unchanged. A completion with a stale request ID leaves state unchanged.
- Events and actions are plain data. Events and actions hold no channels. Events and actions hold no funcs. Events and actions hold no pointers into shared state. Code can log, compare, hash, and replay each event and each action. Each input and output is asynchronous. An action goes out. A `...Done{req, result, err}` event comes back. A request ID joins the action and the event. Timers are actions too. `core.SetTimer` goes out. `core.TimerFired` comes back.
- `core.Env` (`core/env.go`) injects each source of nondeterminism. `core.Env` holds `Rand` (`Uint64`, `Intn`, `NewID`), `Buggify`, and `Assertions`. Time is not in `Env`. Time arrives on each `core.Event` (`At`, `Wall`).
- The stores hold all durable state. A broker rebuilds from the object store and the control plane after a crash. Memory is disposable. The phrase stateless broker means that rule here.
- The simulator is one goroutine. The simulator uses a priority queue ordered by `(time, rngTie, seq)`. The simulator uses one seeded RNG. The simulator uses a virtual clock. Instruction 11 sets this order. The tie sorts before `seq`, or the tie decides nothing.
- Instruction 12 requires the fakes to inject faults. Instruction 13 checks invariants after every step. Instruction 14 hashes state into a running trace hash and runs each seed twice. Instruction 15 gives one command for a failure.

`loop.md` gives you the shape. The code makes three choices sharper than the shape. The code uses a counted SplitMix64 stream instead of `*rand.Rand`. The code uses lanes as an explicit ordering domain. The code uses a three-phase run model. When `loop.md` and the code disagree, you trust the code. `loop.md` §15 writes `go test ./sim -seed=81237`. The real reproduction command is `go run ./cmd/manisim -seed …` (§8.11).

## The kernel in `sim/` holds no Kafka, S3, or TCP code

Package `sim` declares contracts only (`sim/doc.go`). The package declares one goroutine. The package declares one counted RNG. The package declares one global queue. The package declares an `Engine` for every external system. The package declares the `Process`, `Workload`, and `Invariant` roles (`sim/roles.go`). The kernel (`sim/kernel.go`, ~900 lines) implements the contracts.

The table gives you the map, and the map follows the package:

| File | What it owns |
|---|---|
| `sim/doc.go` | Package contract. The lane reservation rule |
| `sim/scheduler.go` | `Scheduler`, `Entry`, `Lane`, `Time` types |
| `sim/schedimpl.go` | The heap. Order `(At, Tie, Seq)`. Per-lane FIFO |
| `sim/rand.go` | `CountedRand` contract |
| `sim/randimpl.go` | SplitMix64, `Derive`, `TwoSpeed.Sample` |
| `sim/engine.go` | `Engine`, `EngineCtx`, all fault-knob structs |
| `sim/kernel.go` | `RunSeed`, boot, crash, restart, the main loop |
| `sim/run.go` | `Config`, `SeedConfig`, `Swarm`, `Phases`, results |
| `sim/runner.go` | `Runner.Run` (campaigns) and `Runner.Replay` |
| `sim/roles.go` | `Process`, `Workload`, `Invariant`, `View`, `History` |
| `sim/timeline.go` | `MemTimeline`. The append-only fact log |
| `sim/view.go` | The read-only `View` invariants receive |
| `sim/inboximpl.go` | Node inboxes (control, data) and the cost model |
| `sim/cost.go` | `CostModel` and `CostParams` contracts |
| `sim/hash.go`, `sim/tags.go` | FNV-1a trace hasher. Frozen type tags |
| `sim/assert.go` | Assertion bookkeeping and `Buggify` |

The kernel routes each action to one engine. The engine answers `Handles` for the action. If no engine handles an action, the seed fails with `FailProtocol/unrouted_action`. Engines schedule completions back into the queue. The same kernel drives the toy echo scenario and the real broker. The kernel holds no system detail, so the kernel needs no change when the system changes.

## The queue order `(At, Tie, Seq)` decides what the seed means

`sim/schedimpl.go` is a binary min-heap. The `less` function orders entries first by `At`. The `less` function orders entries next by `Tie`. The `less` function orders entries last by `Seq`. `Seq` is assignment order. Two rules make that order the right order.

- `Schedule` draws one RNG value per call. `Schedule` draws `e.Tie = rng.Uint64()`. `Schedule` draws even when code later overrides the tie. The draw count never depends on lane history. Each draw shifts all later draws, so the count stays independent of lane history.
- Each entry carries a `Lane` (`sim/scheduler.go`). A lane is an ordering domain, and a lane keeps one meaning in this chapter. If an entry joins a lane with a pending entry at the same `At`, the entry inherits that pending entry's tie (`laneGroup`). The pair falls through to `Seq`. `Seq` is call order, so the lane stays FIFO. The drawn ties order entries across lanes at the same instant. The seeded stream sets that cross-lane order. Without inheritance, two chunks of one TCP stream could land at the same time and reorder. Without the random tie, scheduling order would set the cross-lane order. The seeded stream gives each seed a new cross-lane order.

You schedule into the past and the scheduler clamps the entry to `Now()`. `Cancel` and `DropNode` mark heap items as dead. The kernel discards dead items at the top of the heap. Virtual time advances in `Pop`. Virtual time moves to the next entry. An idle simulated hour costs zero steps.

### Lanes keep one stream in order and keep engine lanes below `1<<62`

One rule in `sim/doc.go` and `sim/kernel.go` does more work than the rule gets credit for:

- Engine lanes are `1, 2, 3, …`. The kernel allocates each lane in first-use order. Engines never construct a `Lane`. Engines ask `EngineCtx.Lane(key)` with keys named `"<engine>/<whatever>"`. Netsim uses `net/<pair>/<dir>`. Fakeio uses `obj/<node>`. Allocation draws no randomness. First use decides the number. One key maps to one lane for one seed. Distinct keys never collide.
- Lanes at or above `laneReserved = 1<<62` belong to the kernel. The kernel lanes cover `laneSim` and per-node lanes `nodeLane(n, k)` = `laneReserved + 1 + idx<<8 + k`. `laneSim` covers phase changes, crashes, restarts, stops, and quiesce ticks. Per-node lanes use timers with `k=0` and wakeups with `k=1`. `EngineCtx.Lane` panics if engine space ever reaches `1<<62`. An engine invents a lane constant and the engine breaks the FIFO guarantee. The engine can also break another engine's lane.

Read the bound as a contract, not as a detail. Engine lanes live below `1<<62`. Kernel lanes live at `1<<62` and above. The two sets never meet.

## One counted stream makes each draw part of the history

`sim/randimpl.go` is a SplitMix64 stream with a draw counter (`Calls()`). The kernel builds the stream with `sim.NewRand`. All draws come from the one stream. All draws follow event order. Draws cover scheduler tie-breaks. Draws cover engine latencies and fault coins. Draws cover attrition rolls. Draws cover workload choices through `core.Env`. Draws cover incarnation IDs (`NewID`, two draws). `SeedResult.RandCalls` is a coordinate. Two runs of one seed consume different draw counts and the two runs have diverged. You see the divergence before you compare a hash.

`Derive(label)` returns an independent stream. The stream seeds from `mix64(seed ^ mix64(fnv1a(label)))`. `Derive` consumes nothing from the parent stream. Two consumers rely on that rule:

- `Swarm` draws per-seed knob selection from `Derive("swarm")` in `newRun`. You edit a swarm menu and the edit reshuffles no run stream.
- `Buggify` activation draws from `Derive("buggify/"+site)` (§8.7). You add a site and the site shifts no other draw.

Nothing else invents randomness. The code uses no `math/rand` global. The code uses no map iteration with unknown order. Engines sort with explicit code. Engines index with explicit code. The code uses no wall clock. Node clocks are `sched.Now()` plus a per-node skew. The kernel draws the skew at setup. The wall clock is the scripted `wallEpoch` (2023-11-14) plus virtual time.

You see the draw-count rule in many places. `Chance(p)` draws nothing when `p <= 0`. `Between(lo, hi)` draws nothing when `hi <= lo`. `TwoSpeed.Sample` skips the range draw when `Max <= Min`. An extra draw is a determinism bug. The extra draw changes each later draw. `Fingerprint()` probes stream state without a draw. You use `Fingerprint()` to find the step where two runs diverge.

## The kernel loop takes one step at a time and charges for the step

In this chapter, a step is one entry the kernel pops and the work the kernel does for that entry. `RunSeed(cfg, seed)` (`sim/kernel.go`) builds fresh engines for each seed. `RunSeed` builds fresh invariants, processes, and workloads for each seed. `RunSeed` boots processes before workloads. Broker refs reach workloads before the loop starts. `RunSeed` then runs `loop()`.

```text
for !halt:
    e := sched.Pop()                  # advances virtual Now to e.At
    foldEntry(e)                      # trace-hash the entry itself
    process(e)                        # deliver / engine / crash / phase …
    drain pending attrition rolls     # crash dice after steps & landings
    foldState()                       # trace-hash every StateHash
    for inv in invariants: inv.Observe(view)
```

The kernel dispatches each entry by kind. `EntryDeliver` pushes onto a node's inbox and pumps the inbox. `EntryEngineInternal` calls an engine's `OnInternal`. `EntryCrash`, `EntryRestart`, `EntryStop`, `EntryPhase`, and `EntryQuiesce` go to the simulator itself. `MaxSteps` (default 10,000,000) fails a seed with `FailMaxSteps`. A timer loop at one instant cannot hang CI.

The kernel does not step a node on delivery. Deliveries land in a two-class inbox (`sim/inboximpl.go`). The inbox holds a control class and a data class. `core.ClassOf` routes each delivery. `pump` reads the next item from `core.ControlFirst`. `pump` calls `Step` once. `pump` dispatches returned actions in slice order. `pump` then charges busy-time from the `CostModel` (`sim/cost.go`). The cost model uses `PerEvent`. The cost model uses `PerByte` for `BytesReceived`. The cost model uses seeded GC-like pause episodes. A busy node gets a wakeup entry at `BusyUntil`. The wakeup models a production loop goroutine with finite speed. Backpressure becomes seed-visible behaviour. Queueing becomes seed-visible behaviour. The kernel folds inbox lengths into the trace hash. The kernel folds `busy` into the trace hash.

### The run stops new damage before the run judges recovery

Each run has three phases (`sim/run.go: Phases`). The Kafka preset uses 30s chaos, 10s settling, and 5s settle (`sim/kafka/config.go`).

- In PhaseChaos, the run injects faults. Faults come from engine fault coins. Faults come from attrition. Faults come from buggify `Fault` sites.
- In PhaseSettling, the kernel calls `EnterRecovery` on every engine. Engines generate no new faults. Plans in flight land as drawn. The rule preserves damage that exists. The phase judges liveness.
- In PhaseSettle, the run drains remaining events to `tEnd`. The run then calls each invariant's `AtEnd` once.

The kernel schedules phase changes on `laneSim`. The Kafka workload is `PhaseAware`. A `PhaseAware` workload gets an `OnPhase` call. The kernel treats that call like a step. The kernel traces the call. The kernel dispatches the call. The kernel hashes the call.

## A crash throws the node away and the stores rebuild the node

`end()` in `sim/kernel.go` is the crash path. A hard crash takes this path. `Trip` takes this path. The graceful-stop deadline kill takes this path. The path has four parts, and the parts run in order:

- The kernel calls `sched.DropNode(ref)` and drops each pending entry for that incarnation. The kernel counts the entries as `dropped_by_crash`. A delivery to a dead incarnation remains and the kernel drops the delivery as stale at delivery time.
- The kernel calls `OnCrash(ref)` on every engine. Most engines do nothing in that call. Engine-internal entries have no node address. Engine-internal entries survive and can land after the broker dies. An object-store PUT apply step is one such entry. A control-plane commit is another such entry. An effect on the wire stands. Netsim is the exception. Netsim retires the dead node's connection ends and notifies peers.
- The kernel discards the machine, the inbox, and the timers. The kernel draws a recovery delay from `Attrition.RecoveryDelay`. An `EntryRestart` boots a new incarnation. The new incarnation gets a fresh incarnation ID from `rng.NewID()`. The new incarnation gets a fresh inbox. The kernel calls the factory again (`core.Factory(env, self)`). The broker rebuilds state from the object-store spec and the control-plane spec. The specs hold the durable state.
- The kernel pushes `core.Started{Self}` as a control-class event. The push makes `Started` the first `Step` of the new incarnation. If `Started` is not first, the kernel fails the seed with `started_first`.

Attrition (`kernel.go: attrition`) rolls `CrashProb` after each node step. Attrition rolls `CrashProb` after each engine landing. Attrition rolls during chaos. Attrition caps dead processes at `MaxDead` at one time. Attrition picks a hard crash or a graceful `core.Stop`. Attrition picks by `CrashWeight` and `GracefulWeight`. A graceful stop arms a kill at `now + stopGrace` (5s). A node `Exit`s first and the node drops the node kill entry. `Config.Scripted` adds crashes, stops, and restarts at fixed times. You use scripted faults when a test needs a fault at an exact point. A captured failure becomes a permanent test in that form.

## Faults draw in a fixed order, so one fault cannot move another fault

Faults come in two layers. The layers draw at different places, and both layers keep the draw order fixed.

Engine fault menus (`sim/engine.go`) are per-system knobs. The menus cover `ObjectStoreFaults`, `ControlPlaneFaults`, and `NetworkFaults`. Each menu has `TwoSpeed` latencies and probabilities. `TwoSpeed` gives a fast range and a rare slow tail. The shared store core, `sim/engines/internal/fakeio`, draws each coin at issue time in `Execute`. The shared store core draws each delay at issue time in `Execute`. The core draws in fixed order. The order is outage roll, 503, timeout, lost-response, apply latency, respond latency, and extra entropy. Draws happen in issue order. Draw order does not depend on landing order. The plan is one of four plans:

- `Unavailable`. A 503 gives this plan. An outage window gives this plan. The op is `NotApplied`. The engine delivers the error at respond time. Outages are correlated windows. The engine rolls an outage once. Each op fails until the window ends.
- `Timeout`. The caller gets `ErrTimeout/MaybeApplied` at the caller deadline. Code draws whether the op applies at landing. `OnInternal` uses a 0.5 coin for that draw. The caller cannot learn the result from the timeout alone.
- `Lost`. The op applies. The response never arrives. The caller cannot tell this case from a timeout. The case tests idempotency of retries.
- `Normal`. The op applies at `T1`. The engine responds at `T2`. The caller dies and the effect stands. The engine tells nobody.

Object-store code adds one case. `GetCorruptProb` flips one bit in the copy a GET returns. The stored object stays intact. The case tests the broker checksum path. Netsim adds the netsim fault families (§8.8).

Buggify (FoundationDB-style, `core/env.go`, implemented in `sim/assert.go`) is cooperative fault injection inside production code. Code calls `env.Buggify.Maybe(site)`, `MaybeP(site, p)`, or `Fault(site, p)`. Each stable `core.Site` gets activation for one run. Activation has probability 0.5 from the site derived stream. An active site fires with default probability 0.25 per call. `Fault` never fires after chaos. In production, the implementation returns false. The sites cost a predictable branch. The sites cost zero behaviour. You never interpolate values into a site string. A site string is a stable identity.

The swarm is `Swarm` in `sim/run.go`. Kafka defines the Kafka swarm in `sim/kafka/config.go`. The swarm re-rolls the menus for each seed from the derived swarm stream. The stream switches each fault knob off with probability 0.4. The stream scales each fault knob by ×0.5, ×1, ×2, or ×4 (capped at 0.4). Segment sizes come from profiles. Caps come from profiles. Profiles are `tiny`, `accumulate`, `defaults`, `wide`, and `mixed`. One seed gives one set of fault choices. A campaign runs many sets of fault choices. `Replay` re-derives the same configuration from the seed alone. A seed is a complete bug report for that reason.

## The engines supply the world, and the kernel supplies the order

All engines satisfy `sim.Engine` (`sim/engine.go`). The interface gives `Handles`, `Execute`, `OnInternal`, `OnCrash`, `EnterRecovery`, and `StateHash`. `Execute` never returns a completion in the same call. Each result is a scheduled entry. An engine with ground truth also implements `sim.ObjectTruth` or `sim.CPTruth`. The invariant `View` gets that truth through a type assertion (`sim/view.go`).

- `sim/engines/objstore` is a fake S3. The state core is the spec `veritas.ObjectStore`. The engine wraps the spec in `fakeio.Core` and adds the corrupt-GET hook. `Object` and `EachObject` (sorted keys) give `ObjectTruth`.
- `sim/engines/cpengine` is a fake control plane over `veritas.ControlPlane`. The engine adds `MaxCommitItems`. A `CommitSegment` needs more bounded-transaction items than the limit and the engine returns a refusal. The engine returns `ErrTooLarge` before the spec runs. The refusal changes nothing. The engine also gives `CPTruth` (`Committed`, `HighWatermark`).
- `sim/engines/logengine` is the log-backed control plane (`docs/sim-log-engine.md`). The engine uses an append-only slot log. The engine writes slots with if-absent puts. Two applying replicas run the log. One replica is the active writer. One replica is a hot standby. The log keeps `K` appends in flight. The log re-appends lost slots at a later time. The log fills holes with no-ops after `HoleAfter`. Slot numbers fence takeover and failback. The engine uses checkpoints. Trimming leaves tombstones. Replicas can stall, crash, and restart from a checkpoint. A slot-based idempotency window applies. The engine is an independent implementation. A reference replay of the log is the engine `CPTruth`. Code checks replica digests against that replay. `sim.LogParams.Bug` selects mutants. Mutants include writer-chosen offsets. Mutants include delete-instead-of-tombstone. A named property expects to catch each mutant.
- `sim/engines/netsim` gives TCP-like byte streams. A connection has two ends and two directions. Each direction has one lane. Each direction has monotone arrival times. Bytes never reorder. Chunks draw different latencies and bytes still never reorder. Sends chunk to `1..PartialIOMax` bytes. `SendDone` means accepted. `SendDone` does not mean delivered. The engine models half-close, abort, and pause and resume. Pause and resume backpressure is invisible to senders. The engine models per-pair permanent extra latency. The engine models clock skew. Fault families are partition, clog, random close, and black hole. A partition freezes in-flight bytes. The bytes deliver in order at heal. A black hole drops one direction without notice. Sends still succeed.

`sim/engines/conformance_test.go` and `sim/engines/internal/enginetest` hold engines to the engine contracts. The tests hold engines to the contracts independent of any scenario.

## Echo teaches the shape, and Kafka tests the real broker

`sim/echo` is the teaching scenario. The scenario has a minimal broker. A PUT stores one batch as a segment and then commits the segment. A GET looks the batch up and reads the batch back (`sim/echo/broker.go`). The scenario has four simple clients and four invariants. `AckedIsCommitted` checks one rule on the step a PUT is acked. The offset is committed at that step. The segment exists at that step. The segment decodes to the acked payload. `ReadYourAck` is another invariant. The mutants (`mutEarlyAck`, `mutAckOnMaybe`, `mutBadRead`) prove the invariants can fail. The scenario stays small on purpose.

`sim/kafka` is the real scenario. The scenario runs the production `broker` package as processes (3 brokers). Kafka clients over `kwire` and `kmsg` drive the scenario (`sim/kafka/workload.go`). The clients do input and output only through actions. The workload is a deterministic client model. A real client library cannot run inside the simulator. One request outstanding per client gives max-in-flight 1 per (producer, partition). That limit makes acked-offset ordering checkable.

- Producers (`sim/kafka/producer.go`) are at-least-once and not idempotent, with `maxAttempts = 3`. Record values encode record identity (`c<client>.b<batch>.r<record>|xxxx…`), so the oracle can match each committed record. Ambiguous error codes mark a batch as ambiguous. The codes are timeout, storage, unknown, and coordinator-not-available. A retry may commit the batch again. Duplicates are legal once per send. The workload mixes in deliberate bad batches. One batch in 40 claims a producer ID. The broker must reject that batch with `INVALID_REQUEST`. One batch in 40 is CRC-damaged after checksumming. The broker must reject that batch with `CORRUPT_MESSAGE`.
- Consumers (`sim/kafka/consumer.go`) list earliest offsets. Consumers then fetch all partitions from offset 0. Consumers track the next expected offset per partition. In `PhaseSettle`, consumers drain. Consumers stop at the first fetch that finds the end of every partition.
- History (`sim/roles.go`) is the client append-only op log. History records `Invoke` and `Return` with outcomes. `OpDefinite` puts the effect inside [invoke, return]. `OpIndeterminate` records a `MaybeApplied` result. The effect may take effect at any later time. The effect remains a candidate. The case is the Jepsen `:info` case. `OpAbandoned` is the third outcome. The workload records ops. Invariants judge the ops.

### The oracle checks each ack at the step the ack lands

`sim/kafka/oracle.go` is one invariant. The oracle owns the reconstructed logs. For each partition, the oracle rebuilds the committed record sequence. The oracle rebuilds the sequence from `View.CP()` ranges. The oracle rebuilds the sequence from `View.Objects()` segment contents. The oracle skips steps where neither `Gen()` moved. On every step, the oracle checks several rules:

- An acked range is committed in the control plane at the step the range was acked.
- Each committed record came from a produced batch.
- Records keep batch order.
- Records stay in the partition where the client produced the records.
- No committed record came from a batch with a definite rejection.
- A consumer never receives a corrupt batch.
- A consumer never receives an out-of-range batch.
- A consumer never reads past the high watermark.
- A consumer resumes where the consumer last read ended.
- A consumer sees each offset in order with the committed value.

At end (`AtEnd`), the oracle checks more rules. Acked records sit at the acked offsets. A client has acked batches that increase per partition. Abandoned batches were never committed. Each consumer caught up with the final log.

`sim/kafka/invariants.go` adds `liveness` and `faultCoverage`. `liveness` requires no op pending at end. An op still runs at `livenessGrace` = 6s into Settling. That op must end acked or successful. A late-landing chaos plan can excuse a late op. Segment-cap invariants are per-seed invariants from `InvariantsFor` (`sim/kafka/config.go`). Segment-timing invariants are per-seed invariants from `InvariantsFor`. Both kinds depend on the swarmed broker configuration.

### Assertions record a failure, and the run keeps going

Invariants report through `core.Assertions` (`sim/assert.go`). Methods are `Always`, `AlwaysOrUnreachable`, `Sometimes`, `Reachable`, and `Unreachable`. The methods use stable `core.Prop` strings. Code never interpolates values into those strings. A violated safety assertion records a `FailViolation`. The kernel records the first violation per property per seed. The run continues. One seed can show many bugs. `Runner.Run` aggregates results. Violated safety properties land in `Report.Always`. Coverage properties with no satisfaction land in `Report.Coverage`. `Config.ExpectReachable` declares paths the campaign must execute at least once. Kafka lists each fault family and each broker path. Examples are `"obj 503 injected"`, `"cp lost response injected"`, `"net: partition injected"`, and `"broker: segment sealed by batch cap"`. The campaign fails on any failing seed. The campaign fails on any `Always` violation. The campaign fails on any nondeterminism. The campaign fails on any coverage gap in a campaign of ≥100 seeds (`coverageMin` in `cmd/manisim/main.go`). A passing run never injects a partition and the run does not pass.

## The trace hash turns one changed step into one failed seed

After each popped entry, the kernel folds data into a single FNV-1a hash (`sim/hash.go`). The code writes the hash by hand. `hash/fnv` is off the deterministic allow-list. The kernel folds the entry (`At`, kind, lane, target, body). The kernel folds each node step (name, event, actions). The kernel then runs `foldState`. `foldState` folds each live node cached `StateHash`. `foldState` folds inbox lengths. `foldState` folds busy-until values. `foldState` folds each engine `StateHash`.

Types fold by frozen numeric tags (`sim/tags.go`). Events use 1-99. Actions use 101-199. CP ops use 201-299. CP responses use 301-399. Field values use 401-499. The code never uses `%T`. The code never uses reflection. You rename a Go type and the rename moves no hash. A type has no tag and the code panics. The panic stops the run. Tags are append-only. You change one tag and the change re-baselines each golden trace. The kernel caches `StateHash` results. The kernel recomputes a result after the kernel calls into that machine. The kernel recomputes a result after the kernel calls into that engine. `Config.VerifyHashes` can recompute every step. `Config.VerifyHashes` fails the seed if state changed outside a kernel call. The check catches a machine that a goroutine mutated in secret. The check catches a machine that a closure mutated in secret.

The canary is a re-run check. `Runner.Run` re-runs every `CheckDeterminism`-th seed. Kafka uses every 10th seed. The re-run flips `Log`. The re-run requires identical `(TraceHash, RandCalls, Steps)`. Logging must not change the hash. The kernel never folds log text. A mismatch is `FailNondeterminism`. The mismatch appears in `Report.Nondeterminism`. `RunSeed` runs one seed. `SeedResult` pins `(Steps, RandCalls, TraceHash, Stats)`. Golden files compare that tuple. `TestDeterminismCanary` exists in both `sim/` and `sim/kafka/`. `scripts/ci-determinism.sh` runs `TestDeterminismCanary`.

## Determinism is worth the price for this project

Determinism costs you three things you can name. You write brokers as state machines, and state machines take more design than direct calls. You draw each random choice from one stream, and one extra draw breaks the history. You hash state after each step, and the hash work runs even on passing seeds.

You get one thing back, and the thing pays for the three costs. You get a failure you can run again. A seed of 81237 fails once and you hold the whole order, the whole fault set, and the whole workload in one integer. You fix the broker. You run the seed again. The seed passes and the pass means the same history now ends clean. A test a reader cannot run again gives the reader a claim. A seed gives the reader a run.

## You run campaigns first, and you replay one seed after a failure

The driver is `cmd/manisim/main.go`. The flags (`parseOptions`) are the whole interface. The driver has no hidden config. The commands in this section are apparatus. You use the commands after you hold the model from the sections before this section.

```bash
# Campaign: defaults -seeds 100 -start 1 -scenario kafka -j <NumCPU>
go run ./cmd/manisim

# A bigger, verbose campaign over the log-backed control plane.
go run ./cmd/manisim -scenario kafka-log -seeds 1000 -start 5000 -v

# The echo smoke scenario, single shard.
go run ./cmd/manisim -scenario echo -seeds 20 -j 1
```

Scenarios are `kafka` (default), `kafka-log` (`kafka.ConfigLog`), and `echo`. Exit code is 1 on any failure. Failure covers a failing seed. Failure covers a violated `Always`. Failure covers nondeterminism. Failure covers a coverage gap in a ≥100-seed run. Exit code is 2 on usage errors. Campaigns shard seeds over `-j` goroutines in contiguous blocks (`cmd/manisim/merge.go: runSharded`). A seed result cannot depend on the shard that ran the seed. Each seed builds a world for that seed. Shard reports merge in shard order by the same rules as one `Run` (`mergeReports`). Sharding is safe for that reason.

Each failing seed prints a seed reproduction line. The form is this line:

```text
FAIL seed 81237: violation (kafka: acked range is committed in the control plane) step=… at=…: …
  repro: go run ./cmd/manisim -scenario kafka -seed 81237
```

If a campaign gives you a failing seed, you replay the seed in four steps:

1. Copy the repro line the campaign prints for the seed.
2. Run the repro line on your machine.
3. Read the step lines `Runner.Replay` prints for the seed.
4. Compare the replay `hash=` with the campaign `hash=` for the seed.

`-seed <n>` selects replay mode when `-seed` ≥ 0. `Runner.Replay` re-runs that one seed with `HaltOnFirst` and full logging. `Runner.Replay` prints one line per step. Each line gives `step`, virtual time, and text. `Runner.Replay` then prints the seed summary and the failure. The summary gives `steps`, `rand`, `hash`, `crashes`, `trips`, and `exits`. For a passing seed, `HaltOnFirst` changes no trace hash. For a passing seed, logging changes no trace hash. The replayed `hash=%016x` must match the campaign `-v` line for that seed. A mismatch means nondeterminism. A mismatch does not mean a flaky test.

You keep failing seeds. A seed and a scenario form a regression test. The test keeps the same input over time. `sim/kafka/c4_faults_test.go`, `sim/kafka/logmutants_test.go`, and the mutant tests use this pattern. The tests include scripted faults pinned at fixed times.

## Key takeaways

- One seed gives one complete history. Queue order draws from a single counted SplitMix64 stream (`sim/randimpl.go`). Fault coins draw from the stream. Latencies draw from the stream. Crashes draw from the stream. Workload choices draw from the stream. Draws follow event order. `Derive` side-streams keep swarm menus from reshuffling the run. `Derive` side-streams keep buggify activation from reshuffling the run.
- The global queue pops by `(At, Tie, Seq)`. Entries in one lane at one instant inherit a tie. Entries in one lane stay FIFO. Lanes ≥ `1<<62` are kernel-private. Engine lanes come from `EngineCtx.Lane(key)`.
- A crash discards state and rebuilds state. Pending deliveries to the dead incarnation drop. Engine effects on the wire stand. The factory reboots the process from the object-store spec and the control-plane spec. `Started` is the first step of the new incarnation.
- Invariants run after every step against ground truth. Invariants run against `View.Objects()` and `View.CP()`. Invariants run against the fact timeline. Invariants run against the op history. The Kafka oracle rule is "acked ⇒ committed, at that offset, with that value". The oracle checks the rule on every step. The oracle does not wait for the end.
- The trace hash uses frozen tags. The trace hash uses each `StateHash` on every step. The re-run canary makes nondeterminism a test failure. `go run ./cmd/manisim -scenario kafka -seed <n>` gives the full reproduction command.

## Checkpoint

<details>
<summary>1. Two entries are scheduled for the same virtual time, one on lane A and one on lane B, and later a third entry joins lane A at that time. What decides each pair's order, and why does the third entry not get its own random tie?</summary>

Across lanes, the seeded `Tie` from `Schedule` decides. Order is `(At, Tie, Seq)` in `sim/schedimpl.go`. The third entry inherits lane A pending tie for that instant. The pair in lane A falls through to `Seq`. `Seq` is call order. The lane stays FIFO. The third entry draws a tie too. `Schedule` draws once per call. The draw count stays independent of lane history.
</details>

<details>
<summary>2. Why would adding a new <code>Buggify</code> site, or a new swarm menu entry, be a determinism bug if activation drew from the run stream, and how does the code avoid it?</summary>

Each draw from the single run stream shifts all later draws. An unrelated edit would reshuffle each seed history. An unrelated edit would re-baseline each golden trace. Swarm choices draw from `rng.Derive("swarm")`. Buggify activation draws from `Derive("buggify/"+site)` (`sim/kernel.go`, `sim/assert.go`). `Derive` derives from the seed and the label alone. `Derive` consumes no draw from the parent stream.
</details>

<details>
<summary>3. A broker issues an object PUT and crashes before the PUT's apply entry lands. After restart, can the object exist even though the broker never saw <code>ObjPutDone</code>? Which code makes that so?</summary>

Yes. `DropNode` drops entries addressed to the dead incarnation. `DropNode` does not drop engine-internal entries. The PUT landing is an engine-internal entry in `sim/engines/internal/fakeio`. `OnCrash` is a no-op there. The plan applies at `T1`. The effect stands. No code tells the broker. The rebuilt broker must discover the object from the store. Durable state can live in the stores alone for this reason.
</details>

<details>
<summary>4. A campaign passes every seed but never injects a network partition. Does <code>manisim</code> call that success?</summary>

Not at campaign scale. `"net: partition injected"` is in Kafka's `ExpectReachable` list (`sim/kafka/invariants.go`). If no run reaches the path, the path appears in `Report.Coverage`. `cmd/manisim` fails campaigns of ≥100 seeds (`coverageMin`) with any coverage gap. Below that size, the run reports the gap but does not fail.
</details>

<details>
<summary>5. You replay a failing seed and the printed trace hash differs from the campaign run's hash for the same seed, before the failure step. What have you found, and what triple does the runner itself compare to catch it?</summary>

You have found nondeterminism in the run. A hidden draw can add nondeterminism. Map iteration can add nondeterminism. A goroutine can add nondeterminism. A wall clock can add nondeterminism. Replay must reproduce the campaign in full. `HaltOnFirst` cannot change the hash. Logging cannot change the hash. `Runner.Run` catches this case. `Runner.Run` re-runs every `CheckDeterminism`-th seed with `Log` flipped. `Runner.Run` compares `(TraceHash, RandCalls, Steps)` (`sim/runner.go`). A mismatch is `FailNondeterminism`.
</details>

## Try it

- You run the smoke scenario. You use `go run ./cmd/manisim -scenario echo -seeds 20 -v`. You note each seed `hash=`, `rand=`, and `steps=`. You run the scenario again. You confirm each line is identical.
- You replay one seed with the full log. You use `go run ./cmd/manisim -scenario echo -seed 3`. You find a crash (`sim_crashed`) in the log and the restart after the crash. You confirm the final `hash=` matches the `-v` campaign line for that seed.
- You run a short Kafka campaign. You use `go run ./cmd/manisim -scenario kafka -seeds 30 -v`. You replay any seed and check the hash again. You try `-j 1`. You confirm the per-seed lines do not change. Wall time can change.
- You break determinism on purpose in a scratch copy. You draw from the run RNG inside a `StateHash` method. Or you range over a map in an engine. You run `go test ./sim/kafka/ -run TestDeterminismCanary -count=1`. The canary catches a case a normal test run can miss.
- You pick any seed and replay the seed against the log control plane. You use `go run ./cmd/manisim -scenario kafka-log -seed 3`. The scenario changes the world for the same seed. The hash changes. The procedure does not change.

## Further reading

- `loop.md` gives the original fifteen-instruction design brief. You reread §11-§15 after the code. You spot where the implementation differs from the brief.
- `docs/lessons.md` gives the numbered lessons that code comments cite. Topics are single seeded stream, record-and-continue, phases, and swarm.
- `docs/decisions.md` gives the decision rows that `sim/` references. Topics cover lanes, tie order, fault families, and the log control plane.
- `docs/sim-log-engine.md` and `docs/log-cp-service.md` describe the log-backed control plane. `sim/engines/logengine` simulates that plane. The `kafka-log` scenario exercises that plane.
- `docs/flow-control.md` §4.1 gives the production behaviour that the cost model and two-class inbox in `sim/inboximpl.go` imitate.
- Tests give worked examples. `sim/kernel_test.go` covers crash, restart, lanes, and the canary. Other examples are `sim/kafka/kafka_test.go`, `sim/kafka/c4_faults_test.go`, `sim/echo/echo_test.go`, and `sim/engines/conformance_test.go`.
