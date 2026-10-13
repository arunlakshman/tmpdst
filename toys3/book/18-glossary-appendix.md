# Chapter 18: Glossary and Appendix

This chapter is reference material. The glossary defines every term the book uses with a specific meaning, with pointers into the code, including the domain-model and control-theory vocabulary of Chapter 3a. The appendices collect the tables you will want open while working: commands, a file index with key symbols and line numbers, the invariant and mutation tables cross-referenced to chapters, the error policy, the `Stats` fields, the log format, and a table of every name the domain-driven-design refactor changed.

Line numbers refer to commit `a4018d5`. Appendix G maps every name the refactor of Chapter 3a changed from its old spelling to its new one.

---

## Glossary

**Abandoned.** The `Outcome` of a request that was outstanding when the server crashed or tripped (`toys3/sim/history.go:20`). The client never learned what happened. For the oracle, an abandoned mutation may take effect at one instant in `[Invoke, crash]` or never; an abandoned read constrains nothing. Set by `Oracle.Crash` (`oracle.go:108-118`). Counted in `Stats.Indeterminate`. Chapters 10, 13.

**Acked-is-durable.** Invariant (b), checked eagerly by `AckedIsDurable` (`toys3/sim/invariant.go:79-85, 138-166`): on the step a `Committed` telemetry is emitted, the key's file in the plant's *durable* view (read through `GroundTruth`) must exist and carry the committed checksum (or be absent, for a delete). An estimate-versus-true-state check: the controller's claim against ground truth. Catches a missing fsync without waiting for a crash. Chapter 14.

**Action.** An output of `Step`: a request for the runtime to do something (`toys3/control/action.go:7`). A closed set: `Reply`, the seven filesystem commands (the `Actuation`s, `toys3/control/plant.go`), `Trip`, `Emit`. Plain data. Chapter 3.

**Actuation.** A command from the controller to the plant: one of the seven `FS*` actions, each implementing `control.Actuation` (an `Action` with an `ID() IOID`) (`toys3/control/plant.go:8-13`). Its result returns as the `Feedback` with the same ID. Chapters 3, 3a.

**ActuationPlan.** The fault decision, dead time and feedback delay for one actuation, drawn when it is *issued* (`toys3/sim/plantmodel.go:51-58`), so RNG consumption follows issue order. Fields `Fail`, `DeadTime`, `FeedbackDelay`. Was `FSPlan{Fail, Land, Reply}`. Chapter 11.

**Adversary.** The role the PlantModel plays at a crash: each un-synced change is resolved independently by the RNG, as if chosen by someone trying to break the server (`PlantModel.Crash`, `toys3/sim/plantmodel.go:468-529`). Chapter 11.

**Aggregate.** In DDD, a cluster of objects treated as one consistency unit. toys3 has one: the `Controller`, whose consistency boundary is one `Step`. The per-key queues are not aggregates but an invariant of it (the key interlock). Chapter 3a.

**Alias (resurrected temp).** A temp file name that a crash brings back as a second directory entry for the inode of a committed object, because the temp name was durable (often via writeback) and the rename's removal of it was not. Harmless if recovery unlinks it; catastrophic if a later write truncates it (mutant f). Chapters 11, 15.

**Allow-list.** `tools/detcheck/allow.txt`: the exact import paths the deterministic packages may import. Fail-closed: anything not listed is an error. A trailing `/...` allows subpackages. Chapter 8.

**Anti-corruption layer (ACL).** A translation layer that keeps a foreign model from leaking into yours. In toys3: `prod.Handler`/`ParseRequest` (S3 REST to `control.Op`) and `GroundTruth` (the monitors' only view of the plant model). Chapter 3a.

**Arrival order / seq.** Each accepted request gets `seq` from `Controller.nextSeq` (`toys3/control/controller.go:176-178`). Every per-key queue of the key interlock is ordered by it, which is why COPY's two-queue membership cannot deadlock. Not to be confused with `Entry.Seq` (queue) or `InvSeq`/`RetSeq` (oracle). Chapter 6.

**BaseConfig.** The fixed simulation configuration (`toys3/sim/config.go:148-175`): 300 ms Chaos, 60 ms Settling, 4 clients, 3 keys, the default fault probabilities. `DefaultConfig` is `BaseConfig` plus swarm variation. Chapter 12.

**Black-box invariant.** An invariant that judges only what clients observed, never the plant's ground truth: `NoCorruptReads`, `Linearizable`, `Liveness` (`BlackBoxInvariants`, `toys3/sim/invariant.go:70-72`). Contrast white-box. Chapters 14, 15.

**Bounded context.** A part of the code inside which every term has one meaning. toys3 has six: storage control, the plant, the object format, verification, the testbench, production. Chapter 3a.

**Budget (oracle).** The cap on memo entries in one WGL search, 2^20 by default (`toys3/sim/oracle.go:523-526`). Exceeding it is reported as a `linearizable` failure suggesting a smaller `QuiesceEvery`. Chapter 13.

**Buggify.** FoundationDB-style cooperative fault injection: code asks `env.Maybe(site)` whether to take a rare-but-legal path (`toys3/control/env.go:18-26`). The simulator says yes 5% of the time during Chaos (`toys3/sim/simulator.go:408`); production always says no. toys3's server has no buggify sites today. Chapters 3, 17.

**Canary (determinism).** `TestDeterminismCanary` (`toys3/sim/sim_test.go:24-42`): runs seeds 1–12 twice and compares trace hash, rand calls and steps; checks that logging does not perturb; checks that adjacent seeds differ. Run by CI. Chapters 8, 16.

**Chaos.** The first phase of a run (`Config.Chaos`, 300 ms by default) during which faults and crashes are injected. Ends at the `KindSettle` entry. Chapter 10.

**Checksum.** CRC-32 (IEEE) of an object's payload, stored in the header (`format.Checksum`, `toys3/format/object.go:15`). Also carried in `Committed.Sum`. Chapter 4.

**ClientReq.** A request id assigned by the runtime and echoed on the reply (`toys3/control/types.go:17-20`). In simulation, also the identity embedded in every value a PUT writes. Logged as `c17`. Chapter 3.

**Committed.** The telemetry (a domain event) emitted immediately before the 200 reply to a successful PUT, COPY or DELETE, at the moment the controller believes the mutation is durable (`toys3/control/telemetry.go:14-24`). Its `Sum` field exists only for monitors. Checked by acked-is-durable. Deliberately *not* renamed "Settled" (Chapter 3a §3a.10). Chapter 3.

**Completion.** A `...Done` event (`FSWriteDone`, etc.) that reports the result of a filesystem action, matched to it by `IOID`. In the domain model, *feedback* from the plant (`control.Feedback`). Chapter 3.

**Component.** In the oracle, a connected set of keys in one window: keys are linked if one op touches both (COPY) or a carried group contains both. Each component is searched independently (`Oracle.check`, `toys3/sim/oracle.go:363-454`). Chapter 13.

**Context map.** The diagram of bounded contexts and their relationships (shared kernel, conformist, customer/supplier, ACL, open host service, separate ways). Chapter 3a §3a.5.

**Controller.** `control.Controller`, the toys3 state machine: the control law that turns requests and plant feedback into actuations, replies, telemetry and trips (`toys3/control/controller.go:74-101`). "The server" in most of this book. Was `core.Server`. Chapters 3a, 5, 6.

**Correlation ID.** `loop.md` rule 8: every request carries an id so late or duplicate completions can be recognized. In toys3: `IOID` for FS actions, `ClientReq` for client requests. Chapter 2.

**CorruptionDetected.** The telemetry emitted when an object file fails its header, length or CRC check on read (`toys3/control/telemetry.go:32-38`); fault detection, in control terms. The reply is 500, not Trip. Counted in `Stats.CorruptionsDetected` (printed `corruptdet=`). Was `CorruptObject` (and still is, inside the frozen trace-hash key). Chapters 3, 7, 14.

**Crash.** A simulated power loss, a *disturbance* the controller cannot command: the plant model resolves volatile state, every queued entry of the dead incarnation is dropped, outstanding requests are abandoned, and a restart is scheduled (`Simulator.crash`, `toys3/sim/simulator.go:276-307`). A `Trip` takes the same code path but is a different concept (the controller's own action); `Stats.Crashes` counts both. Chapters 7, 10.

**CrashReport.** What `PlantModel.Crash` returns: counts of kept, lost and torn changes (`toys3/sim/plantmodel.go:463-465`). Logged on the crash line. Chapter 11.

**Current view.** What reads see now: `dirent.cur` pointing at `inode.volatile` (`PlantModel.Current`, `toys3/sim/plantmodel.go:552`). Not visible to monitors (they get only `GroundTruth`). Chapter 11.

**Dead time.** The delay between issuing an actuation and its effect landing on the plant (transport delay). `Disturbances.DeadTime`, 20 µs to 1.5 ms by default; drawn per op into `ActuationPlan.DeadTime`. Was `Latency`. Chapters 10, 11.

**DefaultConfig.** `BaseConfig` plus per-seed swarm choices of key count, client count, crash rate and EIO rate drawn from a stream separate from the run's (`toys3/sim/config.go:137-145`). What every test and campaign uses. Chapter 12.

**Definite.** The `Outcome` of a request that got 200, 400 or 404 (`toys3/sim/history.go:18`): it happened exactly as replied, at one instant in `[Invoke, Return]`. Chapter 13.

**Deliver.** `KindDeliver`: a queue entry that hands an event to the current server incarnation's `Step` (`toys3/sim/queue.go:17`). Distinct from fs-land. Chapter 10.

**detcheck.** The go/analysis checker in `tools/detcheck/detcheck.go` that enforces the determinism rules on `core/`, `sim/` and every toys3 package except `prod` and `cmd` (`toys3/control`, `toys3/format`, `toys3/sim`): import allow-list; no goroutines, channels, select, map range, reflect map iteration, or package-level variables (except error sentinels and `_`). Suppress with `//detcheck:ok <reason>`. Chapter 8.

**Dirent.** A directory entry in the PlantModel with two versions: `cur` (visible) and `durable` (survives a crash) (`toys3/sim/plantmodel.go:73-77`). 0 means absent. Chapter 11.

**Dirty.** An inode whose volatile content differs from its durable content (`toys3/sim/plantmodel.go:69`). Only dirty inodes can lose or tear content at a crash. Chapter 11.

**Disturbances.** `sim.Disturbances`: the plant model's configuration: its dynamics (`DeadTime`, `FeedbackDelay`) and the exogenous faults the controller cannot command (`EIOProb`, `TornWriteProb`, `ReadCorruptProb`, `WritebackProb`, `FsyncGate`) (`toys3/sim/plantmodel.go:13-49`). `Config.Disturbances`. Was `FSFaults` / `Config.FS`. Chapter 11.

**Domain event.** In DDD, a record of something that happened, named in the past tense. toys3's telemetry types are its domain events. Chapter 3a.

**DurableGen.** A counter bumped whenever durable state may have changed (`toys3/sim/plantmodel.go:458-460`), so `ObjNeverTorn` can skip steps where nothing durable changed. Chapter 14.

**Durable view.** What would survive a crash in which the adversary loses everything volatile: durable entries pointing at durable content (`PlantModel.Durable`, `toys3/sim/plantmodel.go:546-549`). The plant's *true state*; monitors read it through `GroundTruth`. Invariants (b) and (e) check it. Chapter 11.

**EIO / ErrIO.** The catch-all I/O error code: "the operation may or may not have taken effect" (`toys3/control/plant.go:34`). Injected per operation with `EIOProb`. Chapters 4, 7, 11.

**Emit.** The action that records a `Telemetry` (`toys3/control/action.go:30-32`). No effect on behaviour. Chapter 3.

**Entry.** One item in the simulator's queue: time, tie, seq, kind, incarnation and a payload (`toys3/sim/queue.go:49-61`). Plain data. Chapter 10.

**Env.** The interface through which nondeterminism enters the controller: `Rand` and `Buggify` (`toys3/control/env.go:5-8`). Time is *not* in it; it arrives on each event. The controller does not use it yet. Chapter 3.

**Estimate (state estimate).** `Oracle.estimate`: the set of reference-model states consistent with every reply so far, carried between windows as key groups (`toys3/sim/oracle.go:46-50`). Was `groups`. Chapter 13.

**Event.** An input to `Step`: `At` plus a `Body` from a closed set (`toys3/control/event.go:5-12`). Chapter 3.

**Factory.** `func(env Env, cfg Config) Machine`: builds a fresh server for one boot (`toys3/control/machine.go:22-25`). Taking a factory is what lets the simulator run mutants. Chapters 3, 15.

**Failed.** The `Outcome` of a request that got 500 (`toys3/sim/history.go:19`): a mutation may or may not have taken effect. Chapter 13.

**Fail-stop.** The policy of tripping (and recovering from disk) when in-memory state can no longer be trusted, rather than continuing to serve. Implemented with `Trip`. Chapter 7.

**Feedback.** The plant's answer to an actuation: one of the seven `FS*Done` completions, each implementing `control.Feedback` (an `EventBody` with an `ID() IOID`) (`toys3/control/plant.go:15-20`). `Step` routes feedback by `ID()`. Chapters 3, 3a.

**Feedback delay.** The delay between an actuation's effect landing on the plant and its completion being delivered to the controller (measurement delay). `Disturbances.FeedbackDelay`, 0 to 500 µs by default. A crash inside it is "it happened, but nobody was told". Was `ReplyDelay`. Chapters 10, 11.

**Fold.** The mixing function for the running trace hash (`toys3/sim/hash.go:7-11`). Chapter 8.

**fs-land.** `KindFSLand`: the queue entry at which an FS action's effect is applied to the plant model, one dead time after it was issued (`toys3/sim/queue.go:18`). Its completion is delivered one feedback delay later (`ActuationPlan.FeedbackDelay`). A crash in between gives "it happened, but nobody was told". Chapter 10.

**FsyncGate.** A PlantModel mode modelling Linux after a failed fsync: the inode is "poisoned", later fsyncs return OK, and the data never reaches disk until a fresh write (`toys3/sim/plantmodel.go:45-48, 315-333`). Named after the 2018 PostgreSQL incident. Chapters 7, 11.

**Golden test.** `TestGolden` (`toys3/sim/golden_test.go:73-101`): pins steps, random draws, trace hash, end time, failure count and every stat for seeds 1..500 (`DefaultConfig`) and 1..50 (`BaseConfig`), plus every mutant's first catch, against `testdata/golden-v1.txt`. Proved the DDD refactor behaviour-preserving. Chapter 8 §8.12.

**GroundTruth.** The anti-corruption interface through which monitors read the plant model's true durable state: `DurableFile`, `EachDurable`, `DurableGen` (`toys3/sim/invariant.go:37-51`). Returned by `View.GroundTruth()`; replaced `View.FS()`. Chapters 3a, 14.

**Group.** In the oracle, a set of keys whose possible states are carried jointly between windows (`toys3/sim/oracle.go:314-319`). The oracle's state estimate (`Oracle.estimate`) is a list of groups. Usually one key per group; COPY or correlated crash outcomes can tie keys together. Chapter 13.

**Incarnation.** One boot of the server, numbered from 1 (`toys3/sim/queue.go:5-11`). Every server-bound queue entry carries one; a crash drops all entries of the dead incarnation. Chapter 10.

**Indeterminate.** `Stats.Indeterminate` (printed `indet=`): requests outstanding at a crash, i.e. abandoned (`toys3/sim/simulator.go:283-284`). Was spelled `Indetermined`. Chapter 16.

**Inode.** A file's content in the plant model, independent of its names, with `durable`, `volatile`, `dirty` and `poisoned` (`toys3/sim/plantmodel.go:65-71`). Two names can point at one inode. Chapter 11.

**Invariant.** A check run after every simulator step and once at the end (`toys3/sim/invariant.go:56-60`). Five by default. Chapter 14.

**IOID.** Correlates a filesystem action with its completion; unique within one incarnation, assigned by `Controller.newIO` (`toys3/control/plant.go:22-25`, `controller.go:135-138`). Also names the temp file. Logged as `io42`. Chapter 3.

**Key interlock.** The controller's per-key FIFO discipline: two requests that share a key are *interlocked* (`interlocked`, `toys3/control/controller.go:68-72`) and may not run together; a request starts only when its start permissive holds. Chapter 6.

**Linearizable.** A history is linearizable if every operation can be assigned a single instant within its invocation-to-response interval such that the operations, executed sequentially in that order, produce the observed results. Invariant (a); checked by the `Oracle` (`toys3/sim/oracle.go`). Not to be confused with *linearization* in control theory (approximating a nonlinear system). Chapter 13.

**Liveness.** Invariant (d), "the closed loop settles": every request issued in Settling never gets a 500 (checked every step), and at the end the server is alive, its final incarnation recovered, and no op is pending (`toys3/sim/invariant.go:98-109, 199-235`). Chapter 14.

**LogEntry.** One line of the event log (`toys3/sim/config.go:185-194`), rendered by `LogEntry.String` (`toys3/sim/log.go:119-138`). Appendix F.

**Machine.** A hashable state machine: `StateMachine` + `Hashable` (`toys3/control/machine.go:16-20`). What a runtime drives. Chapter 3.

**Mode.** The controller's operating mode within one incarnation: `modeStartup`, `modeRecovering`, `modeAuto`, `modeTripped` (`toys3/control/controller.go:13-23`). Enum values are hashed; append only. Was `phase` (`phaseBooting`, `phaseRecovering`, `phaseServing`, `phaseHalted`). Chapter 5.

**Mutant.** A deliberately broken variant of the server, built by wrapping the real one so that exactly one protocol step is wrong (`toys3/sim/mutants_test.go`). Chapter 15.

**Oracle.** The component that records the client-visible history and checks it for linearizability against a sequential reference model (`toys3/sim/oracle.go:19-54`). In control terms a set-membership state observer (its state estimate is `Oracle.estimate`); the DST name was kept. Chapter 13.

**Phase.** Since the refactor, only the testbench's timeline: Chaos, then Settling (then the drain). The controller has *modes*, not phases. Chapter 10.

**Plant.** In control theory, the system being controlled. In toys3, the disk and its page cache: real in production (`prod.OSExecutor`), modelled in simulation (`sim.PlantModel`). Chapter 3a.

**PlantModel.** The plant model: the simulator's in-memory filesystem with crash semantics, standing in for the disk and its page cache (`toys3/sim/plantmodel.go:86-118`). Configured by `Disturbances`. Was `FakeFS`. Chapter 11.

**Plant port.** The controller-owned interface to the plant: `Actuation`, `Feedback`, the `FS*` and `FS*Done` types, `IOID`, `ErrCode` (`toys3/control/plant.go`). Adapters: `prod.OSExecutor`, `sim.PlantModel` (via the simulator), the unit-test harness. Chapters 3, 3a.

**Poisoned.** See FsyncGate.

**QuiesceEvery.** The interval at which clients pause until every request has replied, creating quiescent points that bound oracle windows (`toys3/sim/workload.go:29-33`). 8 ms by default. Chapter 12.

**Quiescent point.** A moment with no op pending. The oracle checks one window at each (and at each crash, which abandons everything pending). Chapter 13.

**Rand.** The run's single seeded random stream, SplitMix64 with a call counter (`toys3/sim/rand.go:10-61`). Chapter 8.

**Recovered.** The telemetry emitted once per boot when recovery finishes, carrying the number of temp files removed (`toys3/control/telemetry.go:26-30`). Chapter 5.

**Recovery.** On `Started`: list `tmp/`, unlink every leftover, fsync `tmp/`, emit `Recovered`, then start queued requests (`toys3/control/recovery.go:17-93`). Any error trips. Establishes a known initial condition (homing, in control terms); reconstitutes nothing. Chapter 5.

**Reference model.** The sequential specification the closed loop must reproduce: a map from key to writer. One state is a `refState` (`toys3/sim/oracle.go:164-167`; was `modelState`). Always qualify "model": reference model versus plant model. Chapter 13.

**Sample.** One telemetry value stamped by the simulator with its sample index (`Step`), time and incarnation (`toys3/sim/invariant.go:12-19`). `View.Telemetry()` returns them in order. Was `TimedFact`. Chapter 14.

**Sans-I/O.** The design pattern in which protocol logic consumes inputs and produces outputs as data, and never performs I/O itself. The `Step(Event) []Action` shape. Chapter 2.

**Seed.** The single number from which a run is derived: the run's RNG seed, and (XORed with a constant) the swarm configuration. Chapters 1, 12, 16.

**Self-describing value.** A PUT payload whose first 12 bytes encode the writer's `ClientReq` and length, and whose remaining bytes are a pure function of those (`MakeValue`, `toys3/sim/value.go:13-32`). Any bytes read back name their writer, or are rejected as garbage by `ValueWriter`. Chapter 12.

**Settling.** The phase after Chaos (`Config.Settling`, 60 ms by default): no new disturbances planned, no injected crashes, clients keep issuing, and the closed loop must settle: every outstanding request is answered and nothing issued in it gets a 500. Begins at `KindSettle`, ends at `KindSettlingEnd`. Was the *quiet tail* (`QuietTail`, `KindQuiet`, `KindTailEnd`). Chapters 10, 14.

**Shared kernel.** A small model shared, and changed together, by several contexts. In toys3: the object format (`toys3/format`) and the self-describing value encoding (`toys3/sim/value.go`). Chapter 3a.

**SIL testbench.** Software-in-the-loop: the real controller code run against a plant model, with a stimulus generator and monitors. What `toys3/sim` is (`toys3/sim/doc.go:1-4`). Chapters 3a, 10.

**Skipper.** The mutant wrapper that hides selected actions from the runtime and feeds the inner server an immediate OK completion instead (`toys3/sim/mutants_test.go:45-75`). Chapter 15.

**Start permissive.** The condition under which a request may start: no earlier unfinished request is interlocked with it (`permitted`, `toys3/control/controller.go:189-201`). `startPermitted` re-evaluates it for every waiting request. Was `canRun` / `startRunnable`. Chapter 6.

**StateHash.** FNV-1a over the controller's entire state in a fixed field order, with enum *values* (not names) (`toys3/control/hash.go:5-32`). Folded into the trace hash after every delivery. Chapter 8.

**Step.** (1) The server's single entry point, `Step(Event) []Action`. (2) One iteration of the simulator's main loop: one queue entry popped and handled (`Simulator.step`). Context disambiguates.

**Stimulus.** The testbench's input generator: the `Workload` and its self-describing values. Chapter 12.

**Swarm testing.** Varying the test configuration per seed (which features and faults are on, how many keys, etc.) rather than running every seed with one "balanced" configuration. Chapter 12.

**Telemetry.** The closed set of signals the controller publishes for supervisory monitoring, via `Emit`: `Committed`, `Recovered`, `CorruptionDetected` (`toys3/control/telemetry.go:3-12`). Domain events, named in the past tense, with any number of subscribers: the production log and the simulator's monitors and stats. No effect on behaviour. Was `Fact`. Chapters 3, 3a, 14.

**telemetryHashKey.** The frozen encoding of telemetry in the trace hash (`toys3/sim/trace.go:85-104`): it reproduces the pre-refactor rendering byte for byte (including the legacy spelling `CorruptObject{...}`), so renaming telemetry or changing the log never changes a trace hash. Pinned by `TestTelemetryHashKeyFrozen`. Chapter 8 §8.12.

**Tie.** A random number drawn when an entry is scheduled (`toys3/sim/queue.go:74-83`), used to order entries with the same `At`. Chapter 10.

**Torn write.** A file that a crash leaves holding a strict prefix of its un-synced content (`TornWriteProb`, `toys3/sim/plantmodel.go:505-507`). Chapters 4, 11.

**Trace hash.** The running hash of a run: step number, time, entry kind and incarnation, every event and action (telemetry through the frozen `telemetryHashKey`), the controller's `StateHash`, and the plant model hash, folded after every step (`toys3/sim/simulator.go:95-97, 173-184`). Two runs of one seed must agree; and a pure refactor must leave it unchanged. Chapter 8.

**Trip.** A safety trip: the action by which the controller fail-stops because it can no longer trust its own view of the plant (after an fsync error, or an error that may have left a change visible but not durable) (`toys3/control/action.go:21-26`). Always the last action of its step; the controller then enters `modeTripped` and ignores everything. In simulation it is processed as a crash; in production the process exits and a supervisor restarts it. Was `Halt{Reason}`; now `Trip{Cause}`. Chapter 7.

**Ubiquitous language.** In DDD, the vocabulary shared by the people and the code of a context: the identifiers, the docs and the conversation use the same words. toys3's comes partly from control theory. Chapter 3a.

**Unknown.** The oracle's widened value for a key: "present or absent, with any value" (`toys3/sim/oracle.go:265-269`). Any read is accepted and fixes the value. Chapter 13.

**Volatile.** The version of a file's content that reads see now (`inode.volatile`). Becomes durable only by fsync, writeback, or luck at a crash. Chapter 11.

**WGL.** Wing–Gong–Lowe: a depth-first search for a linearization, with Lowe's memoization of (set of linearized ops, reference-model state). `search.dfs` (`toys3/sim/oracle.go:547-606`). Chapter 13.

**White-box invariant.** An invariant that inspects ground truth the clients cannot see, here the plant's durable view through `GroundTruth`: `AckedIsDurable`, `ObjNeverTorn`. Chapter 14.

**Widening.** Replacing a too-large set of possible states with one state in which disagreeing keys are `unknown` (`widenStates`, `toys3/sim/oracle.go:456-478`). Sound, not complete. Counted in `Stats.Widenings`. Chapter 13.

**Window.** The ops between two quiescent points, checked together by the oracle. Chapter 13.

**Writeback.** The PlantModel's model of the kernel flushing one random un-synced change on its own (`WritebackProb`, `PlantModel.writeback`, `toys3/sim/plantmodel.go:205-242`). Chapter 11.

---

## Appendix A: Command cheat sheet

All commands are run from the repository root. Each was run while writing this book.

### Tests

```sh
go test ./toys3/control -v                               # controller unit tests (exact action sequences)
go test ./toys3/control -run TestErrorPolicy -v          # the error table, one subtest per row
go test ./toys3/prod -v                                  # end-to-end HTTP on a temp dir; Trip stops the loop
go test ./toys3/sim                                      # everything in the simulator package (~8 s)
go test ./toys3/sim -run TestSeeds -seed=132 -v          # replay one seed; event log printed on failure
go test ./toys3/sim -run 'TestSeeds$' -seeds=500 -v      # seeds 1..500, aggregate stats line
go test ./toys3/sim -run TestSeeds -duration=2m -v       # seeds 1,2,... for two minutes
go test ./toys3/sim -run TestDeterminismCanary -v        # same seed twice; log does not perturb
go test ./toys3/sim -run TestCoverage -v                 # every fault family fires in 200 seeds
go test ./toys3/sim -run TestCorruptionUnreachableWithoutBitFlips
go test ./toys3/sim -run TestCatchesBugs -v              # all seven mutants, all + black-box invariants
go test ./toys3/sim -run 'TestCatchesBugs/d2' -v         # one mutant
go test ./toys3/sim -run TestCatchesBugs -mutant-seeds=2000 -v
go test ./toys3/sim -run 'TestOracle' -v                 # hand-built histories
go test ./toys3/sim -run 'TestPlantModel' -v             # crash semantics of the plant model
go test ./toys3/sim -run 'TestGolden|TestTelemetryHashKeyFrozen' -v   # the behaviour pin (~3 s)
go test ./toys3/sim -run TestGolden -update-golden        # re-baseline: ONLY for a deliberate behaviour change
go test ./toys3/format -v                                # object format round trips and escaping
go test ./toys3/sim -run 'TestQueue' -v                  # queue ordering and dropping
go test ./tools/detcheck                                 # the analyzer's own test
```

### Campaigns

```sh
go run ./toys3/cmd/toys3sim -duration 10m -check-determinism
go run ./toys3/cmd/toys3sim -duration 15s -workers 2 -check-determinism
go run ./toys3/cmd/toys3sim -duration 30m -start-seed 1000000 -det-every 5 -max-failures 5
```

### Determinism gate

```sh
scripts/ci-determinism.sh                                    # detcheck + audits + canaries
go run ./tools/detcheck ./toys3/control/... ./toys3/format/... ./toys3/sim/...   # silent and exit 0 when clean
go run ./tools/detcheck ./toys3/prod/...                     # shows what detcheck rejects (expected)
scripts/audit-dep.sh github.com/foo/bar@v1.2.3               # audit a third-party module (network)
```

### Running the real server

```sh
go run ./toys3/cmd/toys3 -dir /tmp/toys3-data -addr 127.0.0.1:8080
curl -X PUT --data-binary 'hello world' http://127.0.0.1:8080/photos/cat.jpg       # 200
curl -i http://127.0.0.1:8080/photos/cat.jpg                                       # 200 hello world
curl -X PUT -H 'x-amz-copy-source: /photos/cat.jpg' http://127.0.0.1:8080/backup/cat.jpg   # 200
curl -X DELETE http://127.0.0.1:8080/photos/cat.jpg                                # 200
curl -i http://127.0.0.1:8080/photos/cat.jpg                                       # 404
curl -i -X POST http://127.0.0.1:8080/photos/cat.jpg                               # 405
od -A d -t x1 -c '/tmp/toys3-data/obj/backup%2Fcat%2Ejpg'                          # header + payload
```

### Bisecting and shrinking

```sh
git bisect start <bad> <good>
git bisect run sh -c 'go test ./toys3/sim -run TestSeeds -seed=4711 >/dev/null'
```

---

## Appendix B: File index

Line numbers are those of the declaration, at commit `a4018d5`.

### `toys3/control` (the controller; deterministic, detcheck-clean)

| File | Purpose | Key symbols (line) |
|---|---|---|
| `doc.go` | Package doc: the controller/plant loop, operating modes, durable write protocol, error table, key interlock, recovery | the whole file is documentation (1-113) |
| `types.go` | Controller scalars | `Time` 5, `Duration` 8, `ClientReq` 20, `Status` 23 (200/400/404/500, 25-30) |
| `event.go` | Inputs to `Step` (lifecycle and client) | `Event` 5, `EventBody` 12, `Started` 18, `Request` 25, `Op` 32, `Put` 35, `Get` 41, `Delete` 44, `Copy` 48 |
| `action.go` | Outputs of `Step` (client and supervisor) | `Action` 7, `Reply` 13, `Trip` 26, `Emit` 32 |
| `plant.go` | The plant port | `Actuation` 10, `Feedback` 17, `IOID` 25, `ErrCode` 29 (`OK`, `ErrNotFound`, `ErrIO` 31-35), `FSWrite` 59, `FSFsync` 66, `FSRename` 74, `FSFsyncDir` 80, `FSRead` 86, `FSUnlink` 92, `FSList` 98, `FSWriteDone` 106, `FSFsyncDone` 112, `FSRenameDone` 119, `FSFsyncDirDone` 125, `FSReadDone` 132, `FSUnlinkDone` 139, `FSListDone` 145, `ID()` methods 159-165 and 175-181 |
| `telemetry.go` | Published domain events | `Telemetry` 12, `Committed` 19, `Recovered` 28, `CorruptionDetected` 36 |
| `env.go` | Injected nondeterminism | `Env` 5, `Rand` 12, `Site` 19, `Buggify` 24 |
| `machine.go` | Shapes runtimes drive | `StateMachine` 5, `Hashable` 12, `Machine` 17, `Factory` 25, `NewMachine` 28 |
| `controller.go` | The control law | `Config` 6, `mode` 16, `opKind` 26, `reqStep` 38, `request` 51, `touches` 64, `interlocked` 70, `Controller` 87, `New` 104, `Step` 109, `newIO` 135, `trip` 143, `onRequest` 150, `badRequest` 185, `permitted` 191, `startPermitted` 205, `start` 216, `writeTmp` 229, `issue` 236, `finish` 244, `commit` 256, `failTrip` 266, `onRequestIO` 272 |
| `recovery.go` | Recovery | `recStep` 9, `onStarted` 17, `onRecoveryDone` 30, `fsyncTmpDir` 88, `indexIO` 95 |
| `hash.go` | State fingerprint | `StateHash` 7, `hasher` 36, `newHasher` 38 |
| `controller_test.go` | Hand-driven harness with exact expected action strings | `harness` 24, `newHarness` 33, `ioOfAction` 147, `describe` 156, `TestHappyPath` 211, `TestCommittedSum` 227, `TestNotFound` 241, `TestBadRequest` 252, `TestSameKeySerialized` 262, `TestDifferentKeysConcurrent` 280, `TestCopyHoldsBothKeys` 287, `TestCopySameKey` 302, `TestRecovery` 311, `TestRecoveryEmptyAndErrors` 332, `TestCorruptObject` 349, `TestErrorPolicy` 371, `TestStaleIOIgnored` 410, `TestStateHash` 434 |

### `toys3/format` (the on-disk contract; deterministic, detcheck-clean)

| File | Purpose | Key symbols (line) |
|---|---|---|
| `doc.go` | Byte layout of an object file; directory layout | (1-32) |
| `object.go` | One object file | `Magic` 10, `HeaderSize` 11, `Checksum` 15, `EncodeObject` 18, `DecodeObject` 30 |
| `layout.go` | Names and paths | `MaxNameLen` 10, `EscapeKey` 16, `UnescapeKey` 33, `isUnreserved` 54, `ValidKey` 60, `ObjDir` 65, `TmpDir` 68, `ObjPath` 71, `TmpPath` 76 |
| `format_test.go` | Round-trip, damage and escaping tests | `TestObjectRoundTrip` 9, `TestDecodeRejectsDamage` 22, `TestEscapeKey` 38, `TestValidKey` 61, `TestPaths` 68 |

### `toys3/sim` (the SIL testbench; deterministic, detcheck-clean)

| File | Context (Chapter 3a) | Purpose | Key symbols (line) |
|---|---|---|---|
| `doc.go` | — | The four contexts; main loop in pseudocode | (1-44) |
| `rand.go` | testbench | Seeded RNG | `Rand` 10, `NewRand` 16, `Uint64` 19, `Intn` 30, `Float64` 46, `Chance` 49, `Between` 52, `Calls` 61 |
| `hash.go` | testbench | Hash helpers | `Fold` 7, `hashString` 14, `hashBytes` 24 |
| `queue.go` | testbench | Global priority queue | `Incarnation` 11, `EntryKind` 14 (kinds 16-25), `Entry` 50, `Queue` 67, `Schedule` 74, `Pop` 86, `DropIncarnation` 104, `less` 127 |
| `config.go` | testbench | Run configuration and results | `Config` 10, `Result` 47, `Stats` 61, `Stats.Add` 83, `Stats.String` 106, `statusIndex` 116, `DefaultConfig` 137, `BaseConfig` 148, `Failure` 178, `LogEntry` 186, `Run` 198 |
| `simulator.go` | testbench | The main loop | `Simulator` 12, `New` 42, `Run` 83, `handle` 126, `deliver` 171, `callStep` 220, `reply` 229, `wake` 241, `wakeParked` 256, `maybeCrash` 266, `crash` 276, `restart` 310, `build` 328, `afterStep` 339, `fail` 365, `note` 378, View methods 386-395, `env` 401 |
| `trace.go` | testbench | Trace hashing | `hashOp` 15, `hashEvent` 30, `hashAction` 59, `telemetryHashKey` 91 |
| `log.go` | testbench | Log rendering | `DescribeOp` 12, `describePayload` 30, `DescribeEvent` 47, `DescribeAction` 72, `DescribeTelemetry` 103, `LogEntry.String` 119, `FormatLog` 141, `fmtTime` 151 |
| `plantmodel.go` | plant model | The adversarial filesystem | `Disturbances` 17, `ActuationPlan` 54, `InodeID` 63, `inode` 66, `dirent` 74, `fdir` 81, `PlantModel` 108, `FSCounters` 121, `NewPlantModel` 129, `Plan` 146, `Land` 165, `writeback` 206, `split` 248, `lookup` 261, `setEntry` 274, `write` 289, `fsync` 325, `rename` 345, `fsyncDir` 369, `read` 386, `unlink` 406, `list` 423, `names` 443, `DurableGen` 460, `CrashReport` 463, `Crash` 468, `SetQuiet` 532, `Snapshot` 542, `Durable` 549, `Current` 552, `EachDurable` 570, `DurableFile` 582, `CurrentFile` 591, `Hash` 617 |
| `workload.go` | stimulus | Simulated clients | `OpMix` 10, `WorkloadConfig` 17, `Workload` 49, `NewWorkload` 58, `Think` 85, `Issue` 92, `Complete` 123, `Crash` 135, `Idle` 147 |
| `value.go` | stimulus / verification (shared kernel) | Self-describing values | `ValueHeader` 15, `MakeValue` 19, `ValueWriter` 37 |
| `history.go` | verification | The client-visible history | `Outcome` 14, `HistOp` 24, `History` 45, `describeOp` 67 |
| `oracle.go` | verification | Linearizability checker | `Value` 14, `Oracle` 40, `OracleStats` 57, `NewOracle` 66, `Invoke` 76, `Return` 84, `Crash` 108, `CheckQuiescent` 142, `CheckFinal` 151, `refState` 167, `wop` 206, `apply` 214, `unknown` 269, `window` 274, `search` 304, `group` 316, `opKeys` 322, `check` 363, `widenStates` 459, `factor` 484, `dfs` 547, `memoKey` 608 |
| `invariant.go` | verification | Samples, the View, the ground-truth ACL and the five invariants | `Sample` 14, `View` 22, `GroundTruth` 42, `Invariant` 56, `DefaultInvariants` 63, `BlackBoxInvariants` 70, `NoCorruptReads` 77/112, `AckedIsDurable` 85/139, `ObjNeverTorn` 92/169, `Linearizable` 96/190, `Liveness` 105/200 |
| `sim_test.go` | tests | Drivers and canaries | flags 11-15, `repro` 17, `TestDeterminismCanary` 24, `TestSeeds` 46, `TestCoverage` 86, `TestCorruptionUnreachableWithoutBitFlips` 124 |
| `golden_test.go` | tests | The behaviour pin | `updateGolden` 21, `statsVector` 27, `goldenRow` 36, `goldenText` 41, `TestGolden` 73; data in `testdata/golden-v1.txt` (571 lines) |
| `trace_test.go` | tests | Pins the frozen telemetry hash key | `TestTelemetryHashKeyFrozen` 11 |
| `plantmodel_test.go` | tests | Plant model semantics | `newTestFS` 15, `TestPlantModelUnsyncedWriteMayVanish` 45, `TestPlantModelRenameWithoutDirFsyncMayVanish` 69, `TestPlantModelFullySyncedSurvives` 95, `TestPlantModelTornPrefix` 117, `TestPlantModelEIOMayOrMayNotApply` 134, `TestPlantModelFsyncGateLies` 157, `TestPlantModelNeverAliasesHandedOutSlices` 173 |
| `oracle_test.go` | tests | Hand-built histories | `TestOracleAcceptsLinearizableConcurrency` 53, `TestOracleRejectsReadNotYetPossible` 72, `TestOracleRejectsStaleRead` 85, `TestOracleRejectsAckedThenLost` 96, `TestOracleFailedMutationMayOrMayNotApply` 109, `TestOracleRejectsObservedThenVanished` 142, `TestOracleRejectsGarbageAndMustRespectRealTime` 158, `TestOracleCarriesUncertaintyAcrossWindows` 173, `TestOracleWidensInsteadOfGivingUp` 193 |
| `queue_test.go` | tests | Queue | `TestQueueOrdersByTimeThenTieThenSeq` 9, `TestQueueTieBreakDependsOnSeed` 34, `TestQueueClampsPastAndDropsIncarnation` 68 |
| `mutants_test.go` | tests | Broken servers | `fakeOK` 19, `truncAfterTrip` 36, `skipper` 49, `isObjFsyncDir` 79, `mutSkipDirFsync` 87, `mutSkipFileFsync` 92, `mutAckBeforeRename` 101, `noTrip` 113, `mutNoTripOnError` 121, `mutNoTripOnDirFsyncError` 127, `noTrip.Step` 131, `rewrite` 211, `restoreErr` 251, `ioOfAction` 269, `eventRewriter` 277, `mutSkipCRC` 291, `mutSkipTempCleanup` 304, `mutants` 319, `mutantSeeds` 329, `firstCatch` 333, `TestCatchesBugs` 349 |

### `toys3/prod` (production; not checked by detcheck)

| File | Purpose | Key symbols (line) |
|---|---|---|
| `doc.go` | Runtime diagram; the plant adapter and the ACL | (1-19) |
| `env.go` | Production `Env`: real randomness, buggify off | `Env` 10 |
| `executor.go` | The real plant adapter (`os` calls) | `Executor` 18, `OSExecutor` 34, `Execute` 37, `run` 42, `writeFile` 76, `syncPath` 89, `code` 103, `PrepareDir` 118 |
| `loop.go` | The event loop (the control loop) | `ErrTripped` 15, `ErrStopped` 18, `Loop` 21, `NewLoop` 39, `Run` 60, `dispatch` 78, `post` 103, `Submit` 121 |
| `http.go` | S3 subset over HTTP (the ACL) | `CopySourceHeader` 16, `ErrMethodNotAllowed` 19, `Handler` 32, `ServeHTTP` 39, `ParseRequest` 81, `splitKey` 110 |
| `prod_test.go` | End-to-end tests | `startServer` 26, `TestEndToEnd` 71, `failFsync` 111, `TestTripStopsLoop` 121 |

### `toys3/cmd`

| File | Purpose | Key symbols (line) |
|---|---|---|
| `toys3/main.go` | Server binary: `-dir`, `-addr`, `-max-object`; exits non-zero on Trip | `main` 26 |
| `toys3sim/main.go` | Parallel campaign runner | `main` 30, flags 31-36, shared state 39-48, `record` 53, workers 63-90, `progress` 92, exit 118-125 |

### Supporting files

| File | Purpose |
|---|---|
| `toys3/DESIGN.md` | The design document; domain model and vocabulary 22-67, invariant table 194-200, mutation table 245-253 |
| `toys3/REFACTOR-PROPOSAL.md` | The DDD and control-theory analysis: glossary with verdicts, context map, migration plan, implementation notes |
| `loop.md` | The 15 DST design rules |
| `tools/detcheck/detcheck.go` | The analyzer; test-main skip 95-99, file loop 106-184, `suppressions` 189 |
| `tools/detcheck/allow.txt` | Import allow-list |
| `tools/detcheck/testdata/src/a/a.go` | Analyzer test cases (`// want` comments) |
| `scripts/ci-determinism.sh` | CI gate; derived package list 12-17, SIGPIPE-safe canary detection 36-45 |
| `.github/workflows/determinism.yml` | vet, test, gate |

---

## Appendix C: Invariants and mutants, cross-referenced

### Invariants (`toys3/DESIGN.md:194-200`, `toys3/sim/invariant.go`)

| | Invariant | Type | Implementation | When checked | Chapters |
|---|---|---|---|---|---|
| a | Every 200 GET is linearizable against a per-key register model (COPY reads src and writes dst atomically) | black-box | `Linearizable` → `Oracle.CheckQuiescent` / `CheckFinal` | every quiescent point (incl. crashes) and end | 13, 14 |
| b | Acked mutations survive crashes unless superseded | black-box (via a) **and** white-box (`AckedIsDurable`) | oracle ("crashes erase nothing"); `AckedIsDurable` on the `Committed` step | quiescent points; eagerly every step | 13, 14 |
| c | No torn or corrupt bytes returned with 200 | black-box | `NoCorruptReads` | every step | 12, 14 |
| d | Every request replies while alive; the final boot recovers; no 500 for a request issued in Settling | black-box | `Liveness` | 500s every step; rest at end | 14 |
| e | Every file under `obj/` in the durable view is a complete object | white-box | `ObjNeverTorn` | whenever `DurableGen` changes | 11, 14 |

### Mutants (`toys3/DESIGN.md:245-253`, re-run for this book; all matched)

| Mutant | Broken step | Wrapper | Caught by (all invariants) | First seed | Black-box only | Chapters |
|---|---|---|---|---|---|---|
| a `a-skip-dir-fsync` | no `FSFsyncDir(obj/)` | `skipper` | acked-is-durable (step 46) | 1 | linearizable, 1 | 4, 15 |
| b `b-skip-file-fsync` | no `FSFsync(tmp)` | `skipper` | obj-never-torn (step 44) | 1 | liveness, 2 | 4, 15 |
| c `c-ack-before-rename` | reply 200, then rename and fsync | `skipper` with `deferReal` | acked-is-durable (step 42) | 1 | linearizable, 1 | 4, 15 |
| d `d-no-halt-on-error` | 500 without Trip after any post-write error | `noTrip` | obj-never-torn (step 241) | 1 | liveness, 20 | 7, 15 |
| d2 `d2-no-halt-on-dir-fsync-error` | same, only for failed `FSFsyncDir(obj/)` | `noTrip{dirOnly}` | linearizable (step 380) | 132 | linearizable, 132 | 7, 15 |
| e `e-skip-crc` | CRC not verified on read | `eventRewriter` | no-corrupt-reads (step 657) | 18 | no-corrupt-reads, 18 | 4, 14, 15 |
| f `f-skip-temp-cleanup` | recovery ignores leftover temps | `eventRewriter` | linearizable (step 1750) | 20 | linearizable, 20 | 5, 11, 15 |

Ablation for f (first catching seed; Chapter 17 Part II §13): fixed config without writeback 429, fixed with writeback 36, swarm without writeback 24, swarm with writeback 20. Mutant e without read corruption: not caught in 1,000 seeds.

---

## Appendix D: The error policy

From `toys3/control/doc.go:69-83` and `toys3/control/controller.go:272-356`; verified by `TestErrorPolicy` (`toys3/control/controller_test.go:371-408`) and `TestRecoveryEmptyAndErrors`.

| Step | ENOENT | EIO | Why | Code |
|---|---|---|---|---|
| `FSRead` (GET) | 404 | 500 | Nothing changed. | `controller.go:274-297` |
| `FSRead` (COPY src) | 404 | 500 | Nothing changed; dst untouched. | same |
| Read decodes badly | — | 500 + `CorruptionDetected` telemetry, no Trip | File already durable; restart would not fix it. | `controller.go:289-292` |
| `FSWrite(tmp)` | 500 | 500 | Only a temp file changed; next recovery deletes it. | `controller.go:299-309` |
| `FSFsync(tmp)` | 500 + Trip | 500 + Trip | fsyncgate: page cache untrustworthy. | `controller.go:311-319` |
| `FSRename(tmp→obj)` | 500 + Trip | 500 + Trip | May have applied, visibly but not durably. | `controller.go:321-329` |
| `FSUnlink(obj)` (DELETE) | 404 | 500 + Trip | May have applied, visibly but not durably. | `controller.go:331-343` |
| `FSFsyncDir(obj)` | 500 + Trip | 500 + Trip | fsyncgate, and the rename/unlink above is visible but not durable. | `controller.go:345-353` |
| Recovery `FSList(tmp)` | Trip | Trip | Recovery must finish before serving; next boot retries. | `recovery.go:32-39` |
| Recovery `FSUnlink(tmp/x)` | ignored (already gone) | Trip | | `recovery.go:53-64` |
| Recovery `FSFsyncDir(tmp)` | Trip | Trip | | `recovery.go:72-79` |

Rule of thumb: **any error at or after the first step that can touch `obj/` trips; errors before it are a plain 500.** `Trip` is always the last action of its step, preceded by the 500 reply if any; after it the server ignores every event (`controller.go:110-112`). Bad requests (invalid key, oversize PUT, unknown op) get 400 immediately and never enter the queue (`controller.go:150-175`).

Production mapping: `fs.ErrNotExist` → `ErrNotFound`; any other error → `ErrIO`, logged (`toys3/prod/executor.go:103-113`). A Trip makes `Loop.Run` return `ErrTripped`, and `cmd/toys3` exits via `log.Fatalf` for a supervisor to restart (`toys3/cmd/toys3/main.go:48-56`).

---

## Appendix E: `Stats` fields reference

Defined at `toys3/sim/config.go:61-80`, rendered by `Stats.String` (`config.go:106-113`) in this order.

| Printed | Field | Incremented at | Aggregation | Meaning |
|---|---|---|---|---|
| `reqs` | `Requests` | `simulator.go:250` (`wake`) | sum | Requests issued by clients |
| `200` `400` `404` `500` | `ByStatus[0..3]` | `simulator.go:236` (`reply`) | sum | Replies by status; any other status counts as 500 |
| `indet` | `Indeterminate` | `simulator.go:284` (`crash`) | sum | Requests abandoned by a crash |
| `fsops` | `FSOps` ← `FSCounters.Landed` | `plantmodel.go:166`; copied `simulator.go:115` | sum | FS actions that landed |
| `eio` | `EIOs` ← `FSCounters.EIOs` | `plantmodel.go:196-198` | sum | Landed actions completed with ErrIO |
| `crashes` | `Crashes` ← `Result.Crashes` | `simulator.go:299`; copied 121 | sum | All crashes, including Trips |
| `trips` | `Trips` | `simulator.go:205` | sum | Crashes requested by the server |
| `midput` | `CrashesMidPut` | `simulator.go:293-295` | sum | Crashes with a PUT or COPY outstanding |
| `torn` | `TornFiles` | `simulator.go:278` | sum | Files cut to a prefix by a crash |
| `lost` | `LostChanges` | `simulator.go:279` | sum | Un-synced changes discarded (incl. poisoned inodes) |
| `kept` | `KeptChanges` | `simulator.go:280` | sum | Un-synced changes a crash kept anyway |
| `readcorrupt` | `ReadCorruptions` ← `FSCounters` | `plantmodel.go:398-401`; copied `simulator.go:117` | sum | Reads returned with a flipped bit (counted at land) |
| `corruptdet` | `CorruptionsDetected` | `simulator.go:195-196` | sum | `CorruptionDetected` telemetry (counted at delivery) |
| `tempsrm` | `TempsRemoved` | `simulator.go:197-198` | sum | Temp files removed by recovery |
| `windows` | `Windows` ← `OracleStats` | `oracle.go:369`; copied `simulator.go:119` | sum | Linearizability windows checked |
| `maxwin` | `MaxWindowOps` | `oracle.go:370` | **max** | Ops in the largest window |
| `widened` | `Widenings` | `oracle.go:432, 480` | sum | Times the oracle widened keys to unknown |

Identities that hold for the correct server: `200 + 400 + 404 + 500 + indet == reqs`; `corruptdet ≤ readcorrupt`; `trips ≤ crashes`; `midput ≤ crashes`.

Not printed: `FSCounters.Writebacks` (`plantmodel.go:125`), `OracleStats.MaxVisited` and `MaxStates` (`oracle.go:60-61`). They are available programmatically.

---

## Appendix F: Queue entry kinds and the log format

### `EntryKind` (`toys3/sim/queue.go:16-25`)

| Kind | Printed | `Inc` set? | Scheduled by | Handled by | Logged? |
|---|---|---|---|---|---|
| `KindDeliver` | `deliver` | yes | `wake` (Request), `restart` (Started), fs-land (completion) | `deliver` → `Step` | yes: event `=>` actions |
| `KindFSLand` | `fs-land` | yes | `deliver` for each FS action | `PlantModel.Land`, schedules the completion | yes (Log mode): `land A -> E` |
| `KindClientWake` | `client-wake` | no | `New`, `reply`, `wakeParked` | `wake`: issue or park | no |
| `KindCrash` | `crash` | no | not scheduled in the current code; crashes are performed directly by `maybeCrash` and `Trip` | `crash` if alive | crash lines are written by `crash` itself |
| `KindRestart` | `restart` | no | `New` (t=0), `crash` | `restart` | yes: `boot` |
| `KindQuiesce` | `quiesce` | no | `New`, `afterStep` | sets `quiescing` | yes |
| `KindSettle` | `settle` | no | `New` at `Chaos` | stops faults and buggify | yes |
| `KindSettlingEnd` | `settling-end` | no | `New` at `Chaos+Settling` | clients stop | yes |

(No code path schedules a `KindCrash` entry today; the kind exists, and is handled, for a future "crash at time T" feature. Its other use is as the `Kind` of log entries written by `crash` and, confusingly, by `fail`.)

Queue order: `(At, Tie, Seq)` ascending (`queue.go:127-136`). Scheduling in the past clamps to now (`queue.go:75-77`). `DropIncarnation` removes `KindDeliver` and `KindFSLand` entries of one incarnation (`queue.go:104-122`).

### Log line format (`toys3/sim/log.go:119-138`)

```
#<step:-6> t=<fmtTime:-12> inc=<n> <kind:-11> [<DescribeEvent>] [=> <DescribeAction> ...] [<note>]
```

`fmtTime` prints `<ms>.<6-digit remainder>ms` (nanosecond resolution). Examples from seed 1:

```
#2      t=0.000000ms   inc=1 deliver     Started => FSList{io1 /data/tmp}
#44     t=7.233120ms   inc=1 fs-land     land FSRename{io12 /data/tmp/0000000000000007 -> /data/obj/k2} -> FSRenameDone{io12 OK}
#233    t=53.112270ms  inc=1 deliver     FSFsyncDone{io78 EIO} => Reply{c30 500} Trip{fsync temp: EIO}
#233    t=53.112270ms  inc=1 crash       CRASH (trip: fsync temp: EIO): kept=2 lost=2 torn=0, dropped 1 queued entries
#235    t=55.879722ms  inc=2 restart     boot
#1257   t=300.000000ms inc=0 settle      chaos over: settling (no new faults or crashes)
```

Rendering vocabulary (`log.go`):

| Text | Meaning |
|---|---|
| `c17` | `ClientReq` 17 |
| `io42` | `IOID` 42 of the current incarnation |
| `value-of-26` | bytes exactly equal to `MakeValue(26, len)` |
| `obj(value-of-26)` | an object file (valid header and CRC) whose payload is `value-of-26` |
| `obj(N bytes)` | a valid object file whose payload is not a workload value |
| `raw(N bytes)` | bytes that are neither a valid object nor a value (torn, bit-flipped) |
| `garbage(N bytes)` | in a history op: a body no PUT wrote |
| `nil` | no data (e.g. `FSReadDone` with ENOENT) |
| `Committed{c6 "k2" crc=9b35c638}` | the `Committed` telemetry, with the payload CRC in hex |
| `Committed{c56 delete "k0"}` | a delete's `Committed` telemetry |
| `CorruptionDetected{"k3"}` | the corruption telemetry (printed `CorruptObject{"k3"}` before the refactor) |
| `Trip{fsync temp: EIO}` | a safety trip and its cause (printed `Halt{...}` before the refactor) |
| `[102..104 @82642556..84991187]` | in a history op: `InvSeq..RetSeq` (logical) and `Invoke..Return` (virtual ns) |

---

## Appendix G: Old names → new names (the refactor of `9b65410..a4018d5`)

For readers with an older checkout, an older copy of this book, or old logs. The refactor changed no behaviour: every trace hash, step count, random-draw count and "caught at seed N" is the same before and after (Chapter 8, §8.12). Chapter 3a explains why each new name was chosen.

### Packages, files and paths

| Old | New | Notes |
|---|---|---|
| `toys3/core` (package `core`) | `toys3/control` (package `control`) | `core.X` → `control.X`, except the format symbols below |
| `core/server.go` | `control/controller.go`, `control/recovery.go`, `control/hash.go` | recovery and `StateHash` + hasher split out |
| `core/server_test.go` | `control/controller_test.go` | |
| `core/format.go` | `toys3/format/object.go`, `toys3/format/layout.go` | new package `format`; `core.ObjPath` → `format.ObjPath`, etc. |
| `core/doc.go` "On-disk layout" | `format/doc.go` | |
| `core/fact.go` | `control/telemetry.go` | |
| FS actions, completions, `IOID`, `ErrCode` in `action.go`, `event.go`, `types.go` | `control/plant.go` | plus the new `Actuation` and `Feedback` interfaces |
| — | `format/format_test.go` | new tests |
| `sim/describe.go` | `sim/log.go` (rendering) + `sim/trace.go` (hashing) | |
| `sim/fakefs.go`, `sim/fakefs_test.go` | `sim/plantmodel.go`, `sim/plantmodel_test.go` | |
| `Outcome`, `HistOp`, `History` in `oracle.go` | `sim/history.go` | |
| `MakeValue`, `ValueWriter`, `ValueHeader` in `workload.go` | `sim/value.go` | |
| — | `sim/golden_test.go`, `sim/testdata/golden-v1.txt`, `sim/trace_test.go` | new tests |

### Identifiers

| Old | New |
|---|---|
| `Server` | `Controller` |
| `phase`, `phaseBooting`, `phaseRecovering`, `phaseServing`, `phaseHalted` | `mode`, `modeStartup`, `modeRecovering`, `modeAuto`, `modeTripped` |
| `canRun(r)` | `permitted(r)` (the start permissive) |
| `conflicts(a, b)` | `interlocked(a, b)` (the key interlock) |
| `startRunnable()` | `startPermitted()` |
| `ioOf(body)`, the switch in `issue`, `ioOfAction` switches | `Feedback.ID()`, `Actuation.ID()` |
| `Halt{Reason}` | `Trip{Cause}` |
| `halt(...)`, `failHalt(...)` | `trip(...)`, `failTrip(...)` |
| `prod.ErrHalted` ("toys3: server halted") | `prod.ErrTripped` ("toys3: server tripped") |
| `Fact`, `Emit{Fact}` | `Telemetry`, `Emit{Telemetry}` |
| `CorruptObject` | `CorruptionDetected` |
| `TimedFact` | `Sample` |
| `View.Facts()` | `View.Telemetry()` |
| `View.FS()` (a `*FakeFS`) | `View.GroundTruth()` (a `GroundTruth` interface) |
| `DescribeFact` | `DescribeTelemetry` |
| (hash input `DescribeFact(f)`) | `telemetryHashKey(t)` (frozen) |
| `FakeFS`, `NewFakeFS` | `PlantModel`, `NewPlantModel` |
| `FSFaults`, `Config.FS` | `Disturbances`, `Config.Disturbances` |
| `FSPlan{Fail, Land, Reply}` | `ActuationPlan{Fail, DeadTime, FeedbackDelay}` |
| `FSFaults.Latency` | `Disturbances.DeadTime` |
| `FSFaults.ReplyDelay` | `Disturbances.FeedbackDelay` |
| `Config.QuietTail` | `Config.Settling` |
| `KindQuiet`, `KindTailEnd` | `KindSettle`, `KindSettlingEnd` (same numeric values) |
| `View.Quiet()`, `Simulator.quiet` | `View.Settling()`, `Simulator.settling` |
| `Simulator.fs`, `Simulator.facts` | `Simulator.plant`, `Simulator.samples` |
| `Liveness.quietFrom`, `quietSeen` | `Liveness.settlingFrom`, `settlingSeen` |
| `modelState` | `refState` |
| `Oracle.groups` | `Oracle.estimate` |
| `Stats.Halts` | `Stats.Trips` |
| `Stats.CorruptObjects` | `Stats.CorruptionsDetected` |
| `Stats.Indetermined` | `Stats.Indeterminate` |
| `noHalt`, `mutNoHaltOnError`, `mutNoHaltOnDirFsyncError`, `truncAfterHalt` | `noTrip`, `mutNoTripOnError`, `mutNoTripOnDirFsyncError`, `truncAfterTrip` |
| `TestHaltStopsLoop` | `TestTripStopsLoop` |
| `TestFakeFS*` | `TestPlantModel*` |
| `TestCorruptObjectUnreachableWithoutBitFlips` | `TestCorruptionUnreachableWithoutBitFlips` |
| `core.TmpPath(root string, id IOID)` | `format.TmpPath(root string, id uint64)` |
| — (new) | `PlantModel.EachDurable`, `GroundTruth`, `Actuation`, `Feedback` |

### Printed output

| Where | Old | New |
|---|---|---|
| `Stats.String()` | `halts=`, `corruptobj=` | `trips=`, `corruptdet=` |
| event log, action | `Halt{fsync temp: EIO}` | `Trip{fsync temp: EIO}` |
| event log, action | `Emit{CorruptObject{"k3"}}` | `Emit{CorruptionDetected{"k3"}}` |
| event log, crash line | `CRASH (halt: ...)` | `CRASH (trip: ...)` |
| event log, kind column | `quiet`, `tail-end` | `settle`, `settling-end` |
| event log, notes | `chaos over: no more faults or crashes`; `quiet tail over: clients stop` | `chaos over: settling (no new faults or crashes)`; `settling over: clients stop` |
| production log | `toys3: HALT: <reason>` | `toys3: TRIP: <cause>` |
| production log | `toys3: fact core.Committed {...}` | `toys3: telemetry control.Committed {...}` |
| `prod.ErrTripped` message | `toys3: server halted` | `toys3: server tripped` |

A five-line `sed` script built from the "event log" rows of this table turns any pre-refactor event log into the post-refactor one, byte for byte (Chapter 8 §8.12 shows it, and shows that seeds 1, 18, 20 and 132 match exactly).

### Deliberately unchanged

| Name | Why it was kept |
|---|---|
| mutant labels `d-no-halt-on-error`, `d2-no-halt-on-dir-fsync-error` | test names, quoted in the golden file, CI output and this book |
| `sim.Config.Server` | the controller's config inside the simulator config; renaming bought nothing |
| `Simulator.halted` | the *run* stopping after an invariant failure, which is not a controller trip |
| `Event`, `Action`, `Step`, `StateMachine`, `Machine` | the published language shared with `loop.md` |
| `Request`, `Op`, `Put`, `Get`, `Delete`, `Copy`, `Reply`, `Status` | S3 language; requests are not setpoints |
| `FSWrite` … `FSList`, `FS*Done` | POSIX language; "actuation" is the category |
| `Committed`, `Recovered` | already past tense; a commit is not a settling |
| `Oracle`, `Linearizable`, `Invariant` | DST and formal-methods terms (`Observer` would collide with GoF) |
| `Started`, `request`, `reqStep`, `KindFSLand`, `FSCounters` | proposed optional renames (`ColdStart`, `procedure`, `procStep`, `KindLand`, `Counters`) not taken |
| `Quiesce`, `QuiesceEvery`, `KindQuiesce`, `PlantModel.SetQuiet` | linearizability-checker terms; "a quiet plant" is a plant-side word |
| trace hashes, `RandCalls`, step counts, catch seeds | behaviour did not change |

## Key takeaways

- The glossary terms with the most precise and least obvious meanings are: *landed* vs *delivered*, *durable* vs *current/volatile*, *abandoned* vs *failed* vs *definite*, *incarnation*, and *widening*. If those five are sharp, the rest of the book reads easily.
- The appendices are designed to be kept open: A for commands, B for "where is that function", D when reasoning about errors, E when reading a campaign summary, F when reading a log, G when an old name or an old log gets in your way.

## Exercises

1. Without looking at the glossary, write one-sentence definitions of: fs-land, quiescent point, poisoned inode, widening, black-box invariant. Then compare.
2. Pick any five line references in Appendix B and check them with your editor. If any has drifted, you are on a newer commit; use `git log -p --follow` on the file to see what moved.
3. Using Appendix E, compute the identities for the 15-second campaign in Chapter 16. Do they hold?
4. Using Appendix F, write a `grep` or `awk` one-liner that extracts, from a seed's log, every crash line together with the line immediately before it. What fraction of crashes in seed 1 immediately follow an `fs-land`? Why is that fraction high? (Hint: where does `maybeCrash` get called?)
5. Add a `KindCrash` scheduler: a `Config.CrashAt []control.Duration` that schedules `KindCrash` entries at fixed virtual times. Use it to write a test that crashes exactly once, at a time you choose, during a PUT.
6. Using Appendix G, rewrite one paragraph of your own notes (or of a first-edition chapter) into the new vocabulary. Which renames changed only a word, and which changed what the sentence *claims*? (Hint: compare "the server halted" with "the controller tripped", and "the fake FS" with "the plant model's ground truth".)

## Further reading

- Maurice Herlihy and Jeannette Wing, *"Linearizability: A Correctness Condition for Concurrent Objects"*, ACM TOPLAS 1990.
- Jeannette Wing and Chun Gong, *"Testing and Verifying Concurrent Objects"*, Journal of Parallel and Distributed Computing, 1993.
- Gavin Lowe, *"Testing for Linearizability"*, Concurrency and Computation: Practice and Experience, 2017.
- Thanumalayan Sankaranarayana Pillai et al., *"All File Systems Are Not Created Equal"*, OSDI 2014.
- Guy Steele, Doug Lea and Christine Flood, *"Fast Splittable Pseudorandom Number Generators"*, OOPSLA 2014 (SplitMix64).
- Daniel Lemire, *"Fast Random Integer Generation in an Interval"*, ACM TOMACS 2019 (the bounded `Intn`).
