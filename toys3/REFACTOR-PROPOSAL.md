# toys3 refactor proposal: Domain-Driven Design with a control-theory vocabulary

Status: **proposal only.** No code or existing docs have been changed.
Baseline: branch `toys3` @ `0f0aad9`. Seed 1 is `steps=1628 crashes=10 trace=1ca97eb77a9044e1`, and the 7 mutants
are caught at seeds 1, 1, 1, 1, 132, 18 and 20 (details in §6).

**Summary.**

- toys3 already has an architecture that fits control theory: `Step(Event) []Action` is a
  discrete-time controller in a feedback loop around a plant (the disk).
- The refactor mostly *names* things that are already there. It moves one concept that is in the wrong
  place: the on-disk format leaves `core`.
- It separates the four bounded contexts that `toys3/sim` mixes today, first by file and naming and,
  optionally, by package.
- It does this with **no behaviour change**, and it keeps the trace hash
  bit-identical so the book's quoted hashes stay valid.

**Contents.**

1. Domain analysis
2. Ubiquitous language glossary
3. Context map
4. Tactical design
5. Package layout
6. Behaviour preservation
7. Migration plan
8. Open questions

Appendix: DDD scorecard.

---

## 1. Domain analysis

### 1.1 Subdomains

| Subdomain | Type | Why | Where today |
|---|---|---|---|
| **Durable object control** (the write protocol, per-key serialization, fail-stop, recovery) | **Core** | The system under test. It holds every interesting decision: the order of fsync and rename, when to Halt, and the interlock between keys. | `core/server.go`, `core/doc.go` |
| **Verification** (reference model, linearizability search, invariants, coverage) | **Core** (for *this* repo's purpose) | The project exists to learn DST. The oracle and the invariants are where the learning value is, and they are as carefully modelled as the server. | `sim/oracle.go`, `sim/invariant.go` |
| **Plant model** (fake filesystem with crash and fault semantics) | Supporting | It has to be realistic, and Chapter 17 shows its realism decides which bugs are found. But it models POSIX; it does not invent anything. | `sim/fakefs.go` |
| **Simulation testbench** (event queue, virtual clock, seeded RNG, incarnations, trace hash) | Supporting | It is necessary, but the same machinery would drive any `Step` machine. | `sim/simulator.go`, `queue.go`, `rand.go`, `hash.go`, `config.go` |
| **Object format** (header, CRC, key escaping, directory layout) | Supporting | It is a small, stable contract between incarnations and between the controller and the monitors. | `core/format.go` |
| **Stimulus** (workload, self-describing values) | Supporting | Its value encoding is what lets the oracle check reads exactly. | `sim/workload.go` |
| **Production runtime and transport** (event loop, OS executor, HTTP S3 subset) | Generic | A thin shell. "No business logic" is stated at `prod/doc.go:2`. | `prod/*` |
| PRNG, hashing, priority heap | Generic | Commodity algorithms (SplitMix64, FNV-1a, a binary heap). | `sim/rand.go`, `sim/hash.go`, `sim/queue.go`, `core/server.go:510-532` |

### 1.2 Bounded contexts

| # | Context | Responsibility | Language today | Code |
|---|---|---|---|---|
| A | **Storage Control** | Turns requests and plant feedback into actuations, replies and telemetry. Owns the durability protocol, the key interlocks, the trip policy and recovery. | `Server`, `Step`, `Event`, `Action`, `Request`, `Op`, `Reply`, `Halt`, `Emit`, `Fact`, `phase`, `request`, `reqStep`, `canRun`, `IOID` | `core/*` except `format.go` |
| B | **Durable Medium (Plant)** | The thing being controlled: files, directory entries, fsync, crash. It has two implementations, one real and one modelled. | `FSWrite`…`FSList`, `…Done`, `ErrCode`, `inode`, `dirent`, `durable/volatile`, `FakeFS`, `FSFaults`, `FSPlan`, `Land`, `Crash`, `OSExecutor` | vocabulary in `core/action.go:19-71` and `core/event.go:55-101`; model in `sim/fakefs.go`; real adapter in `prod/executor.go` |
| C | **Object Format** | Byte layout of an object file, key-to-name escaping, `obj/` and `tmp/` paths. The only thing that survives a crash. | `Magic`, `HeaderSize`, `Checksum`, `EncodeObject`, `DecodeObject`, `EscapeKey`, `ObjPath`, `TmpPath` | `core/format.go`, `core/doc.go:16-35` |
| D | **Verification** | Judges a run: the client-visible history against a sequential reference model (black box), and controller telemetry against plant ground truth (white box). | `Oracle`, `History`, `HistOp`, `Outcome`, `modelState`, `group`, `widen`, `Invariant`, `View`, `Failure`, `TimedFact`, `Linearizable`, `AckedIsDurable`, … | `sim/oracle.go`, `sim/invariant.go` |
| E | **Simulation Testbench** | Runs the controller against the plant model under a seeded schedule, injects crashes, and folds the trace hash. | `Simulator`, `Queue`, `Entry`, `EntryKind`, `Incarnation`, `Chaos`, `QuietTail`, `Quiesce`, `Rand`, `Fold`, `TraceHash`, `Stats`, `LogEntry` | `sim/simulator.go`, `queue.go`, `rand.go`, `hash.go`, `config.go`, `describe.go`, `workload.go` |
| F | **Production Runtime and Transport** | Real I/O, goroutines, and the HTTP mapping to S3. | `Loop`, `Submit`, `Executor`, `OSExecutor`, `Handler`, `ParseRequest`, `PrepareDir` | `prod/*`, `cmd/toys3` |

### 1.3 Where today's code mixes contexts (evidence)

| # | Mixing | Evidence | Consequence |
|---|---|---|---|
| M1 | **Format lives in the control package.** Context C is inside A, so everyone who only needs the format imports the whole controller. | `core/format.go` (all 118 lines). Users that need nothing else from it: `sim/simulator.go:58` (`core.ObjDir/TmpDir`), `sim/invariant.go:123,132,135,151,157` (`ObjPath`, `DecodeObject`, `Checksum`, `ObjDir`), `sim/describe.go:33` (`DecodeObject` in log rendering), `sim/fakefs.go:617-618` (`core.Checksum` in the plant hash), `sim/mutants_test.go` mutSkipCRC (`core.HeaderSize`, `core.Magic`), `prod/executor.go:118` (`core.ObjDir/TmpDir`) | Monitors and the plant model depend on the controller only to read bytes. The package doc spends 20 lines (`core/doc.go:16-35`) on byte offsets. |
| M2 | **Facts serve two masters**, observability (prod) and verification (sim), and the control package talks about the simulator. | `core/action.go:83-85` ("The simulator's invariants read facts; production logs them"); `core/fact.go:3-6` ("the simulator checks those beliefs against ground truth"); `core/fact.go:10-13,28-29`; `Committed.Sum` (`fact.go:18`) is read only by `AckedIsDurable` (`sim/invariant.go:135`); prod prints them generically (`prod/loop.go:89-90`, `%T %+v`); sim also turns them into coverage stats (`sim/simulator.go:193-198`) | This is legitimate: one published signal with two subscribers. But it has no name. "Fact" does not say "published for monitoring", and `CorruptObject` is a noun, not a past-tense event. |
| M3 | **Test seams are explained in terms of their one customer.** | `core/machine.go:9-14` (`Hashable`: "lets the simulator fold state"); `core/machine.go:22-25` (`Factory`: "lets the simulator run deliberately buggy variants"); `core/server.go:479-532` (FNV hasher inside the server file) | It works well. It is a Customer/Supplier relationship that should be named, and the hasher should be its own file. |
| M4 | **The plant's vocabulary is inside the control context and is decoded by type switches in 5 places.** | `core/server.go:134-153` (`ioOf`); `core/server.go:343-361` (`issue` re-switches every action type); `core/server_test.go:145-163` (`ioOfAction`); `sim/mutants_test.go` (`ioOfAction`, a copy); `sim/simulator.go:186` (lists all 7 FS action types); `prod/executor.go:41-71` | Adding an actuation means editing 5 or more switches. The *port* (actuation and feedback) is not a type. |
| M5 | **One `sim` package holds four contexts** (B, D, E, and stimulus). | `sim/doc.go:1-3` lists them all; `sim/invariant.go:20-32` (`View` exposes concrete `*FakeFS` and `*Oracle`); `sim/oracle.go:323` (oracle depends on the workload's `ValueWriter`); `sim/oracle.go:325,569,573` (oracle hard-codes the invariant name `"linearizable"`); `sim/describe.go` mixes log rendering (context E) with trace hashing (also E) and payload decoding (context C) | Nothing stops a monitor from reaching into plant internals or the queue. You cannot tell from imports which invariants are black-box. |
| M6 | **`Stats` is one struct fed by four contexts.** | `sim/config.go:59-78`: plant counters (`FSOps`, `EIOs`, `TornFiles`, `LostChanges`, `KeptChanges`, `ReadCorruptions`), oracle stats (`Windows`, `MaxWindowOps`, `Widenings`), controller telemetry (`CorruptObjects`, `TempsRemoved`), testbench counts (`Crashes`, `Halts`, `CrashesMidPut`, `Indetermined`); assembled at `sim/simulator.go:113-121` | This is fine for a learning tool. The glossary below gives each group a home, and §5 keeps the struct flat. |
| M7 | **The plant model knows the controller's layout.** | `sim/fakefs.go:96-97` ("Directories are fixed at construction (core.ObjDir and core.TmpDir of the root)"), wired at `sim/simulator.go:58` | Acceptable once the format is its own context (C is a shared kernel). |
| M8 | **The workload speaks the controller's language directly** (Conformist). | `sim/workload.go:93-119` builds `core.Request{Client, Op}`; `sim/oracle.go:110` takes a `core.Request` | Conformist is the right choice here. Name it; don't fix it. |
| M9 | **The testbench enforces a controller contract.** | "Halt is always the last action of its Step" is in `core/doc.go:79` but checked at `sim/simulator.go:199-203` | Fine, because a contract is checked at the port. Keep it, and mention it in the Trip docs. |
| M10 | **Small language drift**, from Chapter 17 Part II | `core/env.go:3-4` vs. the unused `env` (§1); `sim/oracle.go:27` `Invoke` comment (§4); `sim/simulator.go:370-373` failures logged as `KindCrash` (§5); `sim/queue.go:6-8` "dropped on Pop" (§16); `sim/queue.go:18` `KindCrash` never scheduled (§15); `sim/invariant.go:76-77` "every boot" (§3) | Comment-only fixes (§3, §4, §16) are behaviour-neutral and can be part of the refactor. The others change behaviour or the logs, and are out of scope (see Q5). |

---

## 2. Ubiquitous language glossary

### 2.1 The control-theory reading of toys3

```
     reference (client requests)                       disturbances (EIO, torn writes,
              │                                         bit flips, writeback, power loss)
              ▼                                                    │
     ┌──────────────────┐   actuation (FS commands)     ┌──────────▼──────────┐
     │   CONTROLLER     │ ─────────────────────────────►│        PLANT        │
     │ Step(Event)      │                               │  (disk + page cache)│
     │  []Action        │ ◄─────────────────────────────│                     │
     └──┬────────┬──────┘   feedback (FS…Done)          └─────────────────────┘
        │        │  dead time = actuation → effect; feedback delay = effect → completion
        │        └──► telemetry (Committed, Recovered, CorruptionDetected)
        └──► replies to the operator, or TRIP (fail-stop) when its own state is untrustworthy

  testbench (sim) = software-in-the-loop: real controller + plant model + stimulus + monitors
```

The fit is genuine in five places:

1. **Controller and plant.** The server never does I/O. It issues commands and reacts to feedback.
2. **Dead time and feedback delay.** These are exactly the testbench's `Latency` and `ReplyDelay`.
3. **Disturbances.** Faults are exogenous inputs the controller cannot command.
4. **Trip.** `Halt` is a safety trip: fail-stop to a safe state when the controller's own state can no
   longer be trusted.
5. **Interlock and start permissive.** The per-key FIFO is an interlock between conflicting procedures.

The fit is **forced** in three places:

- Requests as setpoints. A PUT is a setpoint change; a GET is not.
- Linearizability.
- The CRC.

The glossary keeps the plain name in those places.

### 2.2 Glossary

Verdicts:

- **RENAME**: recommended.
- **OPT**: optional; it fits, but the benefit is marginal.
- **KEEP**: the current name is better, or it belongs to a shared language with something outside toys3.

Hash-relevant rows are marked ⚠ (see §6).

#### Packages and architecture

| Current | Proposed | Verdict | Why |
|---|---|---|---|
| `toys3/core` | `toys3/control` | RENAME | The package *is* the control law, a discrete-time map from (state, input) to (state, outputs). "core" describes importance, not meaning. `control.Controller`, `control.Trip` and `control.Put` read naturally in Go. |
| `core/format.go` | `toys3/format` (new package) | RENAME/MOVE | There is no control analogue, and none is needed. It is the on-disk contract and keeps plain names (§5). |
| `toys3/sim` | `toys3/sim` | KEEP | It is a software-in-the-loop (SIL) testbench, and the docs should say so. But `bench` collides with `go test -bench`, and "sim" is the DST term. |
| `toys3/prod` | `toys3/prod` | KEEP | `runtime` would shadow the standard library package. It is the deployed loop, not hardware-in-the-loop. |
| `StateMachine`, `Event`, `EventBody`, `Action`, `Step` | same | KEEP | This is the published language shared with `../loop.md` and the Manifester broker. "Step" is already the control term for a discrete-time update x[k+1] = f(x[k], u[k]). ⚠ Name collision: `Step` is also the testbench's step counter (`Result.Steps`, `Failure.Step`) and the protocol step (`reqStep`). The `procStep` rename below keeps the protocol step distinct. |
| `Machine` (interface) | `Machine` | KEEP | It is the runtime contract that mutants decorate. A "machine" is also an FSM. |
| `Server` (struct) | `Controller` | RENAME | It computes actuations from feedback and references. "Server" is how the transport sees it, not what it is. |
| `New`, `NewMachine`, `Factory`, `Config` | `control.New`, `control.NewMachine`, `control.Factory`, `control.Config` | KEEP | They become control-qualified through the package name. |
| `Hashable`, `StateHash` | same | KEEP | There is no control analogue. It is a "state fingerprint". ⚠ The field order and the numeric enum values feed the hash. |
| `Env`, `Rand`, `Buggify`, `Site` | same | KEEP | Buggify is FoundationDB's term, and the DST community knows it. (`Env` is unused today; see Chapter 17 §1.) |
| `Time`, `Duration` | same | KEEP | `Event.At` is the **sampling instant**. Say so in the doc comment; don't rename the field. |

#### Controller internals (`core/server.go`)

| Current | Proposed | Verdict | Why |
|---|---|---|---|
| `phase` | `mode` | RENAME | Industrial controllers have operating modes (Hand/Off/Auto, Tripped). This also frees "phase" for the testbench's Chaos and Settling phases, and avoids a clash with ISA-88 "phase". ⚠ Keep the iota order. |
| `phaseBooting` | `modeStartup` | RENAME | Power applied, waiting for `Started`. |
| `phaseRecovering` | `modeRecovering` | RENAME (prefix only) | "Homing" (establishing a known initial condition at power-up) fits, but it is obscure. "Recovery" is the storage term. |
| `phaseServing` | `modeAuto` | RENAME | Closed-loop automatic operation, as in Hand/Off/Auto. |
| `phaseHalted` | `modeTripped` | RENAME | Follows from the `Halt` → `Trip` rename. |
| `request` (internal) | `procedure` | OPT | An ISA-88 or SFC procedure is an ordered sequence of steps, each waiting on a transition (here, a completion). `sequence` would clash with `seq`. |
| `reqStep`, `stepWaiting`…`stepUnlinking` | `procStep`, step constants unchanged | OPT | Follows `procedure`. ⚠ Keep the iota order. |
| `request.seq` | `seq` | KEEP | ⚠ Three different "seq" values exist: arrival seq, `Entry.Seq` and `InvSeq`/`RetSeq`. The docs should qualify them as *arrival* seq. |
| `canRun(r)` | `permitted(r)` | RENAME | **Start permissive** is the PLC term for "every condition required to start is true". Here the condition is "at the head of every key queue it touches". |
| `conflicts(a, b)` | `interlocked(a, b)` | RENAME | An **interlock** keeps two conflicting operations from running together. That is exactly the shared-key check. |
| `startRunnable()` | `startPermitted()` | RENAME | Starts every waiting procedure whose permissive is now true. |
| per-key FIFO queue (concept) | "key interlock" | doc term | Implicit queues are still the implementation (`server.go:81-86`). |
| `halt()`, `failHalt()` | `trip()`, `failTrip()` | RENAME | Follows `Trip`. |
| `ioOf()` and the switch in `issue()` | `Feedback.ID()` and `Actuation.ID()` methods | RENAME/SIMPLIFY | Removes M4's duplicated switches. |
| `touches`, `finish`, `commit`, `start`, `writeTmp`, `onRequest`, `onStarted`, `onRecoveryDone`, `recStep` | same | KEEP | These are protocol words, and clear ones. |

#### Events, actions, telemetry

| Current | Proposed | Verdict | Why |
|---|---|---|---|
| `Started` | `ColdStart` | OPT | A cold start retains no state; that is exactly the doc's "nothing in memory survived". The gain is small. `Started` appears in the book and logs about 100 times. |
| `Request`, `Op`, `Put`, `Get`, `Delete`, `Copy`, `Reply`, `Status`, `ClientReq` | same | KEEP | **Forced metaphor avoided.** PUT, DELETE and COPY are setpoint changes, but GET is a readback request, so "Setpoint" would be wrong for a quarter of the ops. These names are also the S3 published language the HTTP adapter translates to. |
| `FSWrite`, `FSFsync`, `FSRename`, `FSFsyncDir`, `FSRead`, `FSUnlink`, `FSList` | same names, plus a marker interface **`Actuation`** (`Action` + `ID() IOID`) | KEEP + NEW TYPE | These are **actuation commands** to the plant. The POSIX names are the plant's own language: toys3 conforms to POSIX and should not rename it. |
| `FSWriteDone`…`FSListDone` | same names, plus a marker interface **`Feedback`** (`EventBody` + `ID() IOID`) | KEEP + NEW TYPE | This is **feedback**: actuator readback, plus sensor data for Read and List. |
| `IOID` | `IOID` | KEEP | `ActuationID` would be more precise but longer, and IOID is known. |
| `ErrCode` (`OK`, `ErrNotFound`, `ErrIO`) | same | KEEP | It is the plant's status word. |
| `Halt{Reason}` | **`Trip{Cause}`** | RENAME | A **trip** (reactor SCRAM, a safety-instrumented-system trip) drives the system to a safe state when the controller's own state can no longer be trusted. Restart then re-arms it. This matches fsyncgate fail-stop exactly. ⚠ The Cause *text* is hashed, so keep the strings unchanged. |
| `Emit{Fact}` | `Emit{Telemetry}` | RENAME (field type) | "Emit" stays. |
| `Fact` (interface) | **`Telemetry`** | RENAME | Telemetry is the signals a controller publishes for supervisory monitoring, with any number of subscribers: the production log (a historian) and the testbench monitors. This names M2. |
| `Committed` | `Committed` | KEEP | "Settled" is tempting, but **settling is asymptotic convergence within a tolerance band**. A commit is a discrete, atomic durability point. "Acked implies durable" reads off `Committed`. |
| `Recovered` | `Recovered` | KEEP | See `modeRecovering`. |
| `CorruptObject` | **`CorruptionDetected`** | RENAME | This is fault detection (the FDI sense). It also fixes a DDD smell: a domain event should be past tense. ⚠ Its rendering is hashed (§6); the hash-stable approach handles it. |
| `Committed.Sum` | same | KEEP | Document it as "telemetry payload only monitors use". |

#### Testbench, plant model and stimulus (`sim`)

| Current | Proposed | Verdict | Why |
|---|---|---|---|
| `Simulator` | same | KEEP | Its doc says "SIL testbench". |
| `FakeFS`, `NewFakeFS` | **`PlantModel`**, `NewPlantModel` (`plant.Model` in the Full variant) | RENAME | In model-in-the-loop and software-in-the-loop testing, the **plant model** is the simulated stand-in for the real plant. ⚠ "Model" now means two things: the *plant model* and the oracle's *reference model*. Always qualify the word. |
| `FSFaults` | **`Disturbances`** | RENAME | **Disturbances** are exogenous inputs that act on the plant and that the controller cannot command: EIO, torn writes, read bit flips, writeback, fsyncgate. |
| `Config.FS` | `Config.Disturbances` | RENAME | |
| `FSFaults.Latency` | **`DeadTime`** | RENAME | **Dead time** (transport delay) is the gap between a command and its effect on the plant. |
| `FSFaults.ReplyDelay` | **`FeedbackDelay`** | RENAME | The **measurement delay** between the effect and the controller seeing it. The "landed but never told" window in Chapter 10 is exactly a crash inside this delay. |
| `FSPlan{Fail, Land, Reply}` | `ActuationPlan{Fail, DeadTime, FeedbackDelay}` | RENAME | |
| `Plan()`, `Land()`, `KindFSLand` | `Plan()`, `Land()`, `KindLand` | KEEP / OPT | "Land" is vivid and used throughout the book. Dropping "FS" is optional. |
| `FsyncGate`, `WritebackProb`, `TornWriteProb`, `EIOProb`, `ReadCorruptProb` | same | KEEP | These are plant-specific; each is a named disturbance. |
| `inode`, `dirent`, `durable`, `volatile`, `cur`, `poisoned` | same | KEEP | The plant's internal language. In control terms, `Durable()` is the **true state** that the controller's `Committed` estimate is compared against. |
| `FSCounters` | `Counters` | OPT | |
| `Crash`, `CrashProb`, `CrashReport`, `maybeCrash` | same | KEEP | A crash is a **power-loss disturbance**, which is not the same thing as a Trip. Keep the DST term. |
| `Chaos` | `Chaos` | KEEP | Chaos engineering is widely known. In control terms it is the "disturbed / transient regime". |
| `QuietTail` | **`Settling`** | RENAME | After disturbances stop, the closed loop must **settle**: every outstanding request gets a reply (no residual transient) and no request issued in this window gets a 500 (**zero steady-state error**). This also removes one of three near-homonyms: quiet, quiesce and quiescent. |
| `KindQuiet` | `KindSettle` | RENAME | Settling begins. ⚠ Keep the numeric value; `Kind` is folded into the hash. |
| `KindTailEnd` | `KindSettlingEnd` | RENAME | The stimulus horizon ends. ⚠ Same as above. |
| `View.Quiet()`, `Simulator.quiet` | `View.Settling()`, `settling` | RENAME | `PlantModel.SetQuiet` stays: "a quiet plant gets no new disturbances" is a plant-side term. |
| `Quiesce`, `QuiesceEvery`, `KindQuiesce`, `Quiescent` | same | KEEP | These are linearizability-checker terms. |
| `Incarnation`, `KindDeliver`, `KindClientWake`, `KindCrash`, `KindRestart` | same | KEEP | DST terms. |
| `Queue`, `Entry`, `EntryKind`, `Tie`, `Seq` | same | KEEP | Generic scheduler. |
| `Rand`, `Fold`, `TraceHash`, `RandCalls` | same | KEEP | Generic DST terms. |
| `Workload`, `Think`, `OpMix`, `Issue` | same | KEEP | Document it as the **stimulus generator**. "Reference generator" is forced, because GETs are not references. |
| `MakeValue`, `ValueWriter`, `ValueHeader` | same, in their own file `value.go` | KEEP | A shared kernel between stimulus and verification. |
| `TimedFact` | **`Sample`** | RENAME | A **sample** is a telemetry value stamped with its sample index (`Step`), time (`At`) and incarnation. |
| `View.Facts()` | `View.Telemetry() []Sample` | RENAME | |
| `DescribeFact` | `DescribeTelemetry` | RENAME | ⚠ It is used by the hash today, so §6 freezes the hash input first. |
| `Stats.Halts` | `Stats.Trips` | RENAME | The printed key changes from `halts=` to `trips=` (Q6). |
| `Stats.CorruptObjects` | `Stats.CorruptionsDetected` | RENAME | The printed key changes from `corruptobj=` to `corruptdet=` (Q6). |
| `Stats.Indetermined` | `Stats.Indeterminate` | RENAME (spelling) | Plain English fix. |
| `Stats.TornFiles`, `LostChanges`, `KeptChanges`, `ReadCorruptions`, `EIOs`, `FSOps` | same | KEEP | Group them in the struct under a `// plant disturbances` comment. |
| `Stats.Windows`, `MaxWindowOps`, `Widenings` | same | KEEP | Group them under `// verification`. |
| `Failure`, `LogEntry`, `Result`, `DefaultConfig`, `BaseConfig` | same | KEEP | `Violation` would be marginally better than `Failure`; it is not worth the churn. |

#### Verification (`sim/oracle.go`, `sim/invariant.go`)

| Current | Proposed | Verdict | Why |
|---|---|---|---|
| `Oracle` | `Oracle` (alternative: `Observer`) | KEEP (Q4) | It *is* a **set-membership state observer**. It reconstructs the set of possible reference-model states from outputs only, carries that set across windows, and over-approximates when the set grows (interval-observer style). An empty set means a fault is detected. The fit is remarkable, but "Observer" collides with the GoF Observer pattern (pub/sub), and "test oracle" is the DST term. The proposal keeps the type name and uses observer language *inside* it. |
| `modelState` | `refState` | RENAME (internal) | The sequential specification is a **reference model**, the model whose behaviour the closed loop should track (as in MRAC). |
| `Oracle.groups` | `Oracle.estimate` | RENAME (internal) | The current **state estimate**: a set of possible states, factored per key. |
| `widenStates`, "widening", `unknown` | same | KEEP | Abstract-interpretation terms with no exact control equivalent. |
| `History`, `HistOp`, `Outcome` (`Pending`, `Definite`, `Failed`, `Abandoned`) | same | KEEP | Linearizability literature. "Trajectory" would be forced. |
| `Linearizable` (invariant, `"linearizable"`) | same | **KEEP, collision flagged** | In control theory, **linearization** means approximating a nonlinear system around an operating point. In docs, always write "linearizability (concurrency)". Never coin a control-flavoured name containing "linear". |
| `Invariant`, `DefaultInvariants`, `BlackBoxInvariants` | same | KEEP | The formal-methods term. Docs can call them "monitors". `Interlock` would be wrong: interlocks *prevent* problems, invariants *detect* them. |
| `AckedIsDurable` | same | KEEP | Document it as the **estimate-vs-true-state check**: the controller's claimed state (`Committed`) against the plant's true durable state. |
| `ObjNeverTorn`, `NoCorruptReads` | same | KEEP | |
| `Liveness` | same | KEEP | It is the formal term, and precise. Its docs should say "settles within the Settling window". |
| `View` | same | KEEP | In the Full variant it narrows to a `GroundTruth` anti-corruption interface (§4.5). |
| CRC, `Checksum`, `Magic`, `HeaderSize`, `EncodeObject`, `DecodeObject`, `EscapeKey`, `ValidKey`, `ObjPath`, `TmpPath` | `format.*`, same identifiers | KEEP | **Forced metaphor avoided.** "Parity" or "integrity sensor" would only obscure. |

#### Production runtime (`prod`)

| Current | Proposed | Verdict | Why |
|---|---|---|---|
| `Loop` | `Loop` | KEEP, collision noted | Here the event loop *is* the control loop (sample, compute, actuate), so the double meaning is aligned rather than confusing. `ControlLoop` would be redundant. |
| `Executor`, `OSExecutor` | same | KEEP | It is the plant I/O adapter (actuator driver plus readback). `Actuator` would describe only half of it. |
| `ErrHalted` | `ErrTripped` | RENAME | Follows `Trip`. |
| `Handler`, `ParseRequest`, `Submit`, `PrepareDir`, `Env`, `ErrStopped` | same | KEEP | `Handler` is the anti-corruption layer from S3 REST to `control.Op`. |
| `inboxSize` (backpressure) | same | KEEP | Doc-only term: **saturation**. When the inbox is full, `Submit` blocks. |

### 2.3 Name collisions and double meanings to watch

| Term | Meanings | Resolution |
|---|---|---|
| linearizability / linearization | Concurrency correctness / small-signal approximation | Keep `Linearizable`. Never use "linearize" for anything else. |
| model | Plant model (`PlantModel`) / reference model (oracle spec) | Always qualify the word, in code (`refState`) and in prose. |
| step | `Step()` / sim step counter / protocol step / "step response" | `procStep` for the protocol step. Prose says "sim step #N". |
| phase | Server lifecycle / Chaos and Settling / ISA-88 phase | The server uses `mode`. "Phase" is reserved for the testbench timeline. |
| loop | Event loop / control loop / `loop.md` | Aligned in this design. |
| quiet / quiesce / quiescent | No faults / clients pause / no pending ops | QuietTail becomes `Settling`. The other two keep their linearizability meanings. |
| observer | State observer / GoF Observer | That is why `Oracle` is kept (Q4). |
| trip / crash | Self-initiated fail-stop / external power loss | `Stats.Crashes` counts both today (`config.go:68`). Keep that, and document "Crashes includes Trips". |
| bench | Testbench / `go test -bench` | Don't name a package `bench`. |
| seq | Arrival seq / `Entry.Seq` / `InvSeq`, `RetSeq` | Prose qualifies each one. |

---

## 3. Context map

```
                                 S3 clients
                                     │ HTTP
                       ┌─────────────▼──────────────┐
                       │ F1 TRANSPORT  prod/http.go │  generic
                       └─────────────┬──────────────┘
                                     │ ACL: S3 REST ─► control.Op
                       ┌─────────────▼──────────────┐
                       │ F2 PROD RUNTIME  prod/     │  generic
                       │ Loop · OSExecutor · Env    │──── conformist to POSIX (OS kernel = upstream)
                       └───┬──────────────────▲─────┘
           Step(Event)     │ CONFORMIST       │ Feedback
           []Action        │ (implements the  │
                           │  actuation PL)   │
   ╔═══════════════════════▼══════════════════╧══════════════════════════╗
   ║ A  STORAGE CONTROL   control/                    CORE               ║
   ║    Controller aggregate · key interlocks · trip · recovery          ║
   ║    OPEN HOST SERVICE + PUBLISHED LANGUAGE:                          ║
   ║      Step(Event) []Action  (shared with ../loop.md / Manifester)    ║
   ║      Actuation / Feedback   (outbound port: the plant interface)    ║
   ║      Telemetry              (outbound signal: any subscriber)       ║
   ╚══════╤═══════════════════════════╤══════════════════════════════════╝
          │ uses (upstream)           │ CUSTOMER/SUPPLIER (Hashable, Factory exist for the bench)
          ▼                           ▼
   ┌───────────────┐       ┌────────────────────────┐  owns   ┌─────────────────────────┐
   │ C OBJECT      │◄──SK──│ E TESTBENCH  sim/      │────────►│ B PLANT MODEL           │
   │   FORMAT      │       │ Simulator·Queue·Rand   │         │ sim/plantmodel.go       │
   │   format/     │       │ Workload (stimulus)    │         │ (plant/ in Full)        │
   │ SHARED KERNEL │◄──SK──│ trace hash · log       │         │ CONFORMIST: to control's│
   │ (controller,  │       └───────────┬────────────┘         │ actuation PL + to POSIX │
   │  monitors,    │                   │ drives              └────────────▲────────────┘
   │  plant hash,  │                   ▼                                  │
   │  prod mkdir)  │       ┌────────────────────────┐  ACL: GroundTruth   │
   │               │◄──SK──│ D VERIFICATION         │─────────────────────┘
   └───────────────┘       │ sim/oracle.go,         │  (durable view only)
                           │ invariant.go           │
                           │ (verify/ in Full)      │◄── PL: Telemetry, Op, Reply, Status
                           │ black-box ⟂ control    │    (SEPARATE WAYS from controller
                           └────────────────────────┘     internals: oracle.go:53-55)
                                       ▲
                                       └── SK: value encoding (MakeValue/ValueWriter) with stimulus
```

The relationships, read upstream (U) to downstream (D):

| Upstream | Downstream | Pattern | Notes |
|---|---|---|---|
| A Control | F2 Prod runtime | OHS + PL / Conformist | Prod has no logic of its own and dispatches every Action (`prod/loop.go:78-99`). |
| A Control | B Plant model, and `prod.OSExecutor` | PL / **Conformist** | The plant adapters implement exactly the actuation vocabulary. The *port is owned by the core*, which is hexagonal style, so no separate `plant` vocabulary package is needed (see §5 for why). |
| A Control | E Testbench | **Customer/Supplier** | `Hashable` and `Factory` exist for the bench (`core/machine.go:9-25`). Mutants are **decorators** on the `Machine` port. |
| A Control | D Verification | PL (Telemetry, Op, Reply, Status) | The black-box invariants use only the client-visible history: **Separate Ways** from controller internals (`sim/oracle.go:53-55`). |
| C Format | A, B, D, F2 | **Shared Kernel** | Small, stable and versioned by `Magic`. It is the contract between incarnation N and N+1, which is why it deserves its own package. |
| B Plant model | D Verification | **ACL** (proposed) | Monitors need only a durable read view (`DurableFile`, an iterator over durable files, `DurableGen`), not `*FakeFS` (`sim/invariant.go:26`). |
| Stimulus (E) | D Verification | **Shared Kernel** | The self-describing value encoding (`sim/workload.go:157-192`, used at `sim/oracle.go:323`). |
| Clients (S3) | F1 Transport | **ACL** | `ParseRequest` (`prod/http.go:81-107`) turns S3 REST into `Op`. Buckets are flattened. |
| OS / POSIX | F2, B | Conformist | |

**Dependency direction** (imports) always points toward Control and Format. Nothing imports `sim` or `prod`
except `cmd/*`.

---

## 4. Tactical design

### 4.1 Aggregate: one Controller, and why not one per key

**Aggregate root: `control.Controller`.** The consistency boundary is **one `Step`**. Everything the controller
knows changes atomically inside a single `Step`, and nothing outside can see a half-applied step.

The per-key FIFO *looks* like a per-key aggregate ("ObjectLoop"). It is not one, for five reasons:

1. **COPY spans two keys.** It must hold an interlock on both src and dst, in arrival order
   (`core/server.go:68-76`, `298-309`). As a per-key aggregate that would need a cross-aggregate saga, which
   would change action ordering and therefore behaviour and the trace hash.
2. **The queues are derived, not stored.** There is no per-key state at all. A key's queue is the subsequence
   of `reqs` that touches it (`server.go:81-86`). There is no per-key identity to root an aggregate on.
3. **Global state.** `nextIO` and `nextSeq` are controller-global. Recovery gates *all* keys (`mode`). Trip
   stops *all* keys.
4. **`StateHash`** is defined over the whole controller (`server.go:483-508`).
5. **Ordering.** Inside one Step, `startPermitted` starts newly permitted procedures in global arrival order
   (`server.go:313-321`). That order is observable in the action list.

The per-key FIFO therefore becomes a **named invariant of the aggregate**, the *key interlock*:

> A procedure may start only when its start permissive holds: no earlier unfinished procedure is interlocked
> with it (that is, shares a key).

It is enforced in exactly one place, `permitted()`.

### 4.2 Entities and value objects

| Kind | Members |
|---|---|
| Aggregate root | `Controller` (identity: one incarnation; rebuilt from nothing on every cold start) |
| Entity (inside the aggregate) | `procedure` (today `request`). Identity is its arrival `seq`. Its lifecycle is waiting (permissive false) → protocol steps → finished. |
| Value objects | `Event`, `Action` and all their bodies; `Op` (`Put`, `Get`, `Delete`, `Copy`); `Telemetry`; `IOID`; `ClientReq`; `ErrCode`; `Status`; `Time`; `Duration`; `Config`; keys (strings); object file bytes (format) |
| Plant-model entities (context B) | inodes (`InodeID`) and dirents |
| Verification entities (context D) | `HistOp` (identity `Req`), carried estimate groups |

**Repository.** None, deliberately. Memory is disposable, and **recovery reconstitutes nothing**: `obj/`
needs no recovery, and the controller starts empty. Recovery only cleans up `tmp/`, so in control terms it is
*homing*, not reconstitution. This is worth stating in the docs, because DDD readers will look for a
repository.

### 4.3 Commands, feedback and domain events

| In or out | Kind | Types |
|---|---|---|
| in | Operator command | `Request{Client, Op}` |
| in | Lifecycle | `Started` (optionally `ColdStart`) |
| in | Feedback | `FS…Done` (implementing `Feedback`) |
| out | Actuation commands | `FS…` (implementing `Actuation`) |
| out | Reply to operator | `Reply` |
| out | Safety | `Trip{Cause}`, always the last action of its Step (contract checked by the bench at `sim/simulator.go:199-203`) |
| out | **Domain events** (past tense) | `Telemetry`: `Committed`, `Recovered`, `CorruptionDetected` |

There is no event sourcing, and none is needed. The plant is the source of truth.

### 4.4 Ports and adapters (hexagonal)

| Port | Direction | Defined in | Adapters |
|---|---|---|---|
| `Machine.Step(Event) []Action` | driving (inbound) | `control/machine.go` | `prod.Loop`, `sim.Simulator`, `control` test harness, mutant decorators |
| `Actuation` / `Feedback` | driven (outbound) | `control/plant.go` (new file, the same types moved) | `prod.OSExecutor` (real plant), `sim.PlantModel` via the Simulator (plant model), map-backed harness in `controller_test.go` |
| `Env` (`Rand`, `Buggify`) | driven | `control/env.go` | `prod.Env`, `sim.env`, `testEnv` |
| `Telemetry` (via `Emit`) | output signal | `control/telemetry.go` | prod log line; sim `Sample` stream feeding monitors and stats |
| `Reply` | output | `control/action.go` | prod waiter (HTTP); sim oracle and workload |
| S3 REST | driving, at the edge | `prod/http.go` | the ACL to `Op` |

The new port types are behaviour-neutral:

```go
// control/plant.go
// Actuation is a command to the plant. Its result returns as the Feedback with the same ID.
type Actuation interface { Action; ID() IOID }
// Feedback is the plant's answer to one Actuation.
type Feedback interface { EventBody; ID() IOID }

func (a FSWrite) ID() IOID     { return a.IO }
func (d FSWriteDone) ID() IOID { return d.IO }   // … one line per FS type
```

The field is called `IO` today, so a method named `IO()` would clash with it. There are two options:

- **(a, recommended)** Name the method `ID() IOID`.
- (b) Rename the field to `ID`.

Option (a) touches no field names, so the logs and the book's struct literals stay valid.

### 4.5 Where invariants live

| Invariant | Lives in | Enforced or checked how |
|---|---|---|
| Ack only after fsync(tmp), rename and fsync(obj/) | Controller (protocol steps) | By construction. Unit tests in `controller_test.go`; mutants a, b, c. |
| Key interlock (per-key FIFO, COPY holds both keys) | Controller, `permitted()` | By construction. `TestSameKeySerialized`, `TestCopyHoldsBothKeys`. |
| Trip after any error that may have changed `obj/` invisibly-to-durability | Controller, `failTrip` | By construction; mutants d and d2. |
| Trip is the last action of its step; nothing happens after a trip | Controller (`modeTripped`) | Contract, checked at the port by the bench. |
| Recovery completes before serving | Controller (`mode`) | By construction; `TestRecovery`. |
| Linearizability (a), crash-erases-nothing (b) | Verification (black box) | Oracle at quiescent points |
| Acked ⇒ durable (b, eager) | Verification (white box: estimate vs. true state) | `AckedIsDurable`, through the ground-truth ACL |
| No corrupt 200 (c) | Verification (black box) | `NoCorruptReads` |
| Settles: every request answered, no 500 in Settling (d) | Verification (black box) | `Liveness` |
| `obj/` never torn (e) | Verification (white box) | `ObjNeverTorn` |
| Object file self-validates | Format | `DecodeObject`. **No unit tests exist today** (no test references `EscapeKey`). Add round-trip tests when extracting `format`. |

**Ground-truth ACL (Full variant; optional in Minimal).** Verification declares what it needs from the plant
using only builtin types, so the plant never imports verification and verification never imports the plant:

```go
// verify/groundtruth.go
type GroundTruth interface {
    DurableFile(path string) ([]byte, bool)
    EachDurable(fn func(path string, data []byte)) // sorted by path
    DurableGen() uint64
}
```

`ObjNeverTorn` iterates in the same sorted order as `Durable().Files` does today. Failures are not hashed, so
this cannot change the trace, and the ordering is identical anyway.

---

## 5. Proposed package layout

### 5.1 Minimal variant (recommended)

The minimal variant has four packages, up from three: it adds `format`.

```
toys3/
├── control/                (was core/; detcheck)            ← context A + the plant port
│   ├── doc.go              control law, durable protocol, trip policy, modes, interlocks
│   │                       (the on-disk layout section moves to format/doc.go)
│   ├── controller.go       Controller (was Server), Config, mode, procedure, permitted/interlocked,
│   │                       start/issue/finish/commit/failTrip, onRequest, onRequestIO   [server.go]
│   ├── recovery.go         onStarted, onRecoveryDone, fsyncTmpDir, recStep              [server.go:168-255]
│   ├── hash.go             StateHash + FNV hasher (unchanged byte-for-byte)             [server.go:479-532]
│   ├── event.go            Event, EventBody, Started, Request, Op, Put/Get/Delete/Copy  [event.go:1-53]
│   ├── action.go           Action, Reply, Trip (was Halt), Emit                         [action.go]
│   ├── plant.go            Actuation, Feedback, FS* commands, FS*Done, IOID, ErrCode    [action.go:19-71, event.go:55-101, types.go:22-47]
│   ├── telemetry.go        Telemetry (was Fact), Committed, Recovered, CorruptionDetected [fact.go]
│   ├── types.go            Time, Duration, ClientReq, Status
│   ├── env.go, machine.go  unchanged apart from renames
│   └── controller_test.go  (was server_test.go)
├── format/                 (new; detcheck)                  ← context C, shared kernel
│   ├── doc.go              byte layout table + obj/ and tmp/ layout   [core/doc.go:16-35]
│   ├── object.go           Magic, HeaderSize, Checksum, EncodeObject, DecodeObject
│   ├── layout.go           MaxNameLen, EscapeKey, UnescapeKey, ValidKey, ObjDir, TmpDir, ObjPath,
│   │                       TmpPath(root string, id uint64)   ← takes uint64; format must not import control
│   └── format_test.go      new: round-trip and injectivity tests (behaviour-neutral)
├── sim/                    (detcheck; one package; files grouped by context, listed in doc.go)
│   │  testbench (E):   doc.go simulator.go queue.go rand.go config.go hash.go
│   │                   trace.go  (hashEvent/hashAction + frozen telemetry hash key; from describe.go)
│   │                   log.go    (Describe*, LogEntry.String, FormatLog; from describe.go)
│   │  plant model (B): plantmodel.go   (was fakefs.go: PlantModel, Disturbances, ActuationPlan, …)
│   │  stimulus:        workload.go  value.go (MakeValue/ValueWriter/ValueHeader, from workload.go)
│   │  verification (D): oracle.go  history.go (History/HistOp/Outcome, from oracle.go)  invariant.go
│   └── tests: unchanged files, plus golden_test.go and testdata/golden-v1.txt (§6)
├── prod/                   renames only (ErrTripped; control./format. imports)
└── cmd/toys3, cmd/toys3sim  imports only
```

**Dependency graph (Minimal):**

```
cmd/toys3 ──► prod ──► control ──► format
               └──────────────────► format          (PrepareDir: ObjDir/TmpDir)
cmd/toys3sim ──► sim ──► control ──► format
                  └─────────────────► format
```

**detcheck gate (`scripts/ci-determinism.sh:11`):**

```bash
# before
pkgs=(./core/... ./sim/... ./toys3/core/... ./toys3/sim/...)
# after (Minimal)
pkgs=(./core/... ./sim/... ./toys3/control/... ./toys3/format/... ./toys3/sim/...)
```

The canary loop (`ci-determinism.sh:31`, `./toys3/sim/...`) is unchanged. **Recommended hardening:** derive the
toys3 part of the list so a new pure package cannot be forgotten. For example:

```bash
mapfile -t toys3pure < <(go list ./toys3/... | grep -v -e '/prod$' -e '/cmd/')
```

and pass `"${toys3pure[@]}"` instead of the hand-written toys3 entries. `tools/detcheck/allow.txt` needs no change,
because `manifester/...` is already allowed.

### 5.2 Full variant (compiler-enforced contexts)

The full variant has seven packages.

```
toys3/
├── control/          as Minimal
├── format/           as Minimal
├── internal/mix/     Fold, FNV-1a string/bytes  (needed by sim, plant and verify; byte-identical copies of sim/hash.go)
├── plant/            Model (was FakeFS), Disturbances, ActuationPlan, Counters, CrashReport, Snapshot
│                     takes `type Source interface{ Intn(int) int; Chance(float64) bool; Between(lo, hi int64) int64 }`
│                     (sim.Rand satisfies it; the draw order is unchanged)
├── verify/           Oracle, History, HistOp, Outcome, Invariant, View, Failure, Sample, GroundTruth (ACL),
│                     MakeValue/ValueWriter (value encoding owned by verification), Default/BlackBoxInvariants
├── sim/              Simulator, Queue, Rand, Config, Result, Stats, Workload, log, trace hash, mutants_test
└── prod/, cmd/       as Minimal
```

**Dependency graph (Full):**

```
sim ──► verify ──► control ──► format
 │        └──► internal/mix, format
 ├──► plant ──► control, format, internal/mix
 └──► control, format, internal/mix
verify does NOT import plant (it uses the GroundTruth ACL); plant does NOT import verify.
```

The detcheck list adds `./toys3/plant/... ./toys3/verify/... ./toys3/internal/...`, or comes from the derived
list.

### 5.3 Recommendation

**Do Minimal.** It removes the real mixing (M1, M2, M3, M4), makes the language explicit, and costs one
package. In Full, three of the seven packages exist only to break import cycles that one package does not have
(`internal/mix`, the `Source` interface, re-homing `Failure` and `Sample`). Full also changes about 40% of the
book's import paths for a modest gain.

Move to Full when a **second plant** (the simulated network from Chapter 17 III.2) or a **second
controller** (primary/backup, III.3) arrives. At that point `plant/` and `verify/` will each have two users,
and the boundaries will pay for themselves. Minimal step 9 (the ground-truth ACL inside `sim`) is a cheap step
toward Full.

---

## 6. Behaviour-preservation strategy

### 6.1 Is the trace hash rename-sensitive? **Partially. Go type names never enter it, but one rendered string does.**

| Hash input | Where | Sensitive to |
|---|---|---|
| step, time, `Kind`, `Inc` per popped entry | `sim/simulator.go:94` | **Enum order** of `EntryKind` (iota). Not names. |
| Event tags 10–19 and fields | `sim/describe.go:170-196` | Nothing, as long as each renamed type keeps its literal tag |
| Action tags 30–40 and fields | `sim/describe.go:199-223` | Same, **except Emit** |
| **`Emit` → `hashString(DescribeFact(f))`** | **`sim/describe.go:219-220`** | **The rendered text**: `Committed{c%d %q crc=%08x}`, `Committed{c%d delete %q}`, `Recovered{temps=%d}`, `CorruptObject{%q}` (`describe.go:104-114`), plus the `%T` fallback. Renaming `CorruptObject`, or "prettifying" `DescribeTelemetry`, **changes every hash after the first corruption or commit.** |
| `Halt.Reason` text | `sim/describe.go:218`; strings at `core/server.go:191,216,248,438,448,463,472` | The *text* of the causes. The field name and type name are irrelevant. Keep the strings verbatim; none of them contains the word "halt". |
| FS paths in actions, and dir names in the plant hash | `describe.go:204-216`, `fakefs.go:597-601` | The layout (`/obj`, `/tmp`, 16-hex temp names, `%XX` escaping). Moving code to `format` is fine; changing behaviour is not. |
| `StateHash` | `core/server.go:483-508` | **Field order and the numeric values** of `phase`, `recStep`, `opKind` and `reqStep`. Renaming a constant is fine; **reordering an iota is not.** |
| Plant hash | `sim/fakefs.go:593-627` | Uses `core.Checksum`. Moving it to `format.Checksum` changes nothing. |
| `RandCalls` (and everything downstream) | `sim/rand.go` | **The order of draws.** Any "tidy-up" that moves an `rng` call breaks it, for example draws in `Plan`, `Schedule`'s `Tie`, `Crash`'s iteration, or `writeback`'s candidate order. Pure renames and moves cannot. |
| Initial trace value is `cfg.Seed` | `sim/simulator.go:62` | Nothing |

The book quotes `trace=1ca97eb77a9044e1` (seed 1) at `book/00-preface.md:156,209`. It quotes mutant traces
`f9a6442332345f03` and `c4f18946dc209c4e` at `book/15-mutation-testing.md:594`. A hash-stable refactor keeps
all three true.

### 6.2 The hash-stable approach (recommended)

Before any rename, add one function to `sim/trace.go` and switch `hashAction`'s `Emit` case to it:

```go
// telemetryHashKey is the FROZEN hash encoding of telemetry (trace-hash epoch 1).
// It reproduces the pre-refactor DescribeFact output byte for byte, so renaming
// telemetry types or changing log rendering never changes TraceHash.
// Changing this function is a deliberate re-baseline: update testdata/golden-*.txt
// and the hashes quoted in the book in the same commit.
func telemetryHashKey(t control.Telemetry) string {
    switch x := t.(type) {
    case control.Committed:
        if x.Deleted { return fmt.Sprintf("Committed{c%d delete %q}", x.Client, x.Key) }
        return fmt.Sprintf("Committed{c%d %q crc=%08x}", x.Client, x.Key, x.Sum)
    case control.Recovered:
        return fmt.Sprintf("Recovered{temps=%d}", x.TempsRemoved)
    case control.CorruptionDetected:
        return fmt.Sprintf("CorruptObject{%q}", x.Key) // legacy spelling, on purpose
    }
    return "?" // unreachable for a closed set; today's %T fallback never fired
}
```

After that, renames are free, and every refactor commit must reproduce the golden values **exactly**.

A cleaner hash would use numeric telemetry tags (41, 42, 43, …) like the events and actions. That is a later,
optional, **separate** "epoch 2" commit that re-baselines the goldens and the three quoted hashes in the book.
It is not part of the refactor.

### 6.3 What is recorded before the refactor (step 0), and how it is compared

This is a new test, `sim/golden_test.go`, with data in `sim/testdata/golden-v1.txt`. The baseline is
generated once from `0f0aad9` and committed.

| Set | Per row | Comparison after each step |
|---|---|---|
| `DefaultConfig(seed)`, seeds 1..500 | seed, `Steps`, `RandCalls`, `TraceHash`, `Crashes`, `EndTime`, `len(Failures)`, and `Stats` as a **fixed-order int vector** (not `Stats.String()`) | Exact equality. The vector's field list is updated in the same commit as a field rename, keeping the order. |
| `BaseConfig(seed)`, seeds 1..50 | same | Exact |
| 7 mutants × {`DefaultInvariants`, `BlackBoxInvariants`} = 14 rows | first catching seed, `Failures[0].Invariant`, `Failures[0].Step`, that run's `TraceHash` | Exact. Today the catches are a:1@46, b:1@44, c:1@42, d:1@241, d2:132@380, e:18@657, f:20@1750; black-box: 1, 2, 1, 20, 132, 18, 20. |
| Mutant f, seed 1, and the correct server, seed 1 | `TraceHash` | Must equal the values quoted in the book |
| `FormatLog` for seeds 1, 18, 20 and 132 (correct server, plus mutants e@18, f@20, d2@132) | the full log text, saved to `testdata/logs-v1/` | **Modulo a rename map**: `sed -f testdata/renames.sed old.log` must equal the new log (for example `Halt{` → `Trip{`, `CorruptObject{` → `CorruptionDetected{`, `quiet tail over` → `settling over`, `tail-end` → `settling-end`, `quiet` → `settle`). Run it as a script during the refactor; delete it afterwards or keep it as an optional test. |
| `control` unit tests' `describe()` expectations | strings in `server_test.go` | Mechanical edit (`halt` → `trip`, `corrupt k` stays as a test-local word). Review the diff to confirm it is a pure rename. |
| `TestDeterminismCanary`, `TestSeeds`, `TestCoverage`, `TestCatchesBugs`, `prod` tests | as today | Must pass |

Expected observable changes, all outside the hash:

- Log text (via the rename map).
- Production log lines: `prod/loop.go:90` prints `%T`, so `core.Committed` becomes `control.Committed`, and
  `HALT:` becomes `TRIP:`.
- `Stats.String()` keys (Q6).
- `ErrTripped`'s message.

The golden test makes "no behaviour change" a property every commit is checked for, not just a claim.

---

## 7. Migration plan

Each step compiles, passes `go test ./toys3/...` and `scripts/ci-determinism.sh`, and **leaves the golden
file unchanged**. Make one commit per step.

| # | Step | Touches | Risk / guard |
|---|---|---|---|
| 0 | **Baseline.** Add `golden_test.go` and testdata (§6.3) generated on `0f0aad9`. Save the logs and `renames.sed` for the log diff. | sim tests only | None. The test documents current behaviour. Run 500 seeds in under 3 s (the mutants take about 2 s today). |
| 1 | **Freeze the telemetry hash key** (`telemetryHashKey`, §6.2), and split `describe.go` into `trace.go` (hashing) and `log.go` (rendering). | sim | The golden test proves the frozen key is byte-identical. |
| 2 | **Port types.** Add `Actuation` and `Feedback` with `ID() IOID` methods. Replace the switches: `ioOf` (`server.go:134-153`), `issue` (`343-361`), `ioOfAction` (in both tests), and `simulator.go:186` (`case Actuation`). | core, sim | Watch the type-switch case order in `simulator.deliver`: `Reply`, `Emit` and `Halt` do not implement `Actuation`, so the semantics are identical. |
| 3 | **Extract `format`.** Move `format.go`; `TmpPath` takes `uint64`; move the layout section of the doc. Add `format_test.go`. Update imports in core, sim, prod and mutants. Add `./toys3/format/...` to the detcheck list. | core, sim, prod, CI | Pure move. Risk: forgetting the CI list, mitigated by deriving it (§5.1). |
| 4 | **Rename the package** `core` → `control` (`git mv`; rewrite `manifester/toys3/core` and the `core.` qualifiers). Update the CI list. | everything | Mechanical. Review sed hits in comments ("core" as an English word). |
| 5 | **Controller language.** Split `server.go` into `controller.go`, `recovery.go` and `hash.go`; `server_test.go` becomes `controller_test.go`. Renames: `Server` → `Controller`, `phase` → `mode` (constants as in §2), `canRun` → `permitted`, `conflicts` → `interlocked`, `startRunnable` → `startPermitted`. Optionally `request` → `procedure` and `reqStep` → `procStep`. | control | **Do not reorder any iota.** Add a comment above each enum: "values are hashed by StateHash; append only". |
| 6 | **Trip.** `Halt{Reason}` → `Trip{Cause}`, `halt`/`failHalt` → `trip`/`failTrip`, `modeTripped`, `prod.ErrHalted` → `ErrTripped`, `Stats.Halts` → `Trips`. Update the core test expectations from "halt" to "trip". | control, sim, prod, cmd | The cause strings stay verbatim (they are hashed). |
| 7 | **Telemetry.** `Fact` → `Telemetry`, `CorruptObject` → `CorruptionDetected`, `TimedFact` → `Sample`, `View.Facts` → `View.Telemetry`, `DescribeFact` → `DescribeTelemetry`, `Stats.CorruptObjects` → `CorruptionsDetected`, `Indetermined` → `Indeterminate`. | control, sim, prod | The golden test passes only because of step 1. This is the step that proves step 1 was needed. |
| 8 | **Testbench and plant language.** `fakefs.go` → `plantmodel.go`, `FakeFS` → `PlantModel`, `FSFaults` → `Disturbances` (`Config.FS` → `Config.Disturbances`), `FSPlan` → `ActuationPlan{Fail, DeadTime, FeedbackDelay}`, `Latency` → `DeadTime`, `ReplyDelay` → `FeedbackDelay`, `QuietTail` → `Settling`, `KindQuiet` → `KindSettle`, `KindTailEnd` → `KindSettlingEnd`, `Quiet()` → `Settling()`. In the oracle, `modelState` → `refState` and `groups` → `estimate`. Split `value.go` and `history.go` out. | sim | **Keep the `EntryKind` numeric values** (they are folded at `simulator.go:94`). Keep every rng call in place. |
| 9 | **Ground-truth ACL inside sim** (optional in Minimal). `View.FS()` → `View.GroundTruth()`, returning the interface in §4.5. `PlantModel` gains `EachDurable`. | sim | Iterate in sorted path order, identical to `Durable().Files`. |
| 10 | **Doc and comment pass.** Package docs, a `DESIGN.md` glossary section plus the context map, and comment-only fixes from Chapter 17 (§3 "final boot recovered", §4 `Invoke` = issue time, §16 eager drop). Doc comments get the control-theory terms (sampling instant, dead time, start permissive, …). | docs, comments | Zero code. |
| 11 | *(Full variant only.)* Create `internal/mix`, `plant/` (with a `Source` interface) and `verify/`; re-home `Failure` and `Sample`; update the detcheck list. | sim | `mix.Fold` and `FNV` must be byte-identical copies (the golden test catches any drift). |
| 12 | **Optional epoch 2.** Numeric telemetry tags in the hash, re-baseline the goldens and the book hashes. | sim, book | A deliberate hash change, in a commit of its own. |

**Rollback.** Every step is a pure refactor checked by the same golden file, so any single commit can be
reverted on its own.

### 7.1 Book update

About 1,700 identifier mentions across 19 chapters are affected. These are rough grep counts:

- `toys3/core` or `core.`: about 860
- `Halt`: 289
- `FakeFS`: 246
- `Fact`: 111
- `CorruptObject`: 83
- `QuietTail`: 65
- `halts=`/`corruptobj=`: 43

| Chapter | Impact | What changes |
|---|---|---|
| 18 Glossary & appendix | **Rewrite** | The glossary becomes the §2 table. Add an "old → new names" appendix for readers with older checkouts. |
| 02 Architecture | High | Add the context map (§3) and the control-loop diagram (§2.1). Change the package names. |
| 03 Vocabulary | High | `Fact` → `Telemetry`, the `Actuation`/`Feedback` port types, `Trip`; 46 `toys3/core` paths |
| 07 Errors & fail-stop | High | `Halt` → `Trip` throughout (67 mentions). Introduce "trip" as the safety-system concept. |
| 06 Server protocols, 05 Server lifecycle | High | `Server` → `Controller`; `canRun` → `permitted` (11 mentions in Chapter 6); `phase` → `mode`; key interlock |
| 10 Simulator, 11 Fake filesystem | High | `PlantModel`, `Disturbances`, `DeadTime`/`FeedbackDelay`, `Settling` (Chapter 11 has 48 `FakeFS` mentions) |
| 14 Invariants, 13 Oracle | Medium | `Sample`, `View.Telemetry`, `CorruptionDetected`; observer and reference-model language in Chapter 13; the linearizability/linearization caution |
| 08 Hashing & determinism | Medium | Explain the frozen telemetry hash key and why it exists. This is a good DST lesson: *hash inputs must be stable codes, not rendered names.* |
| 09 Production runtime | Medium | 88 `core.` mentions; `ErrTripped` |
| 12 Workloads, 15 Mutation testing, 16 Campaigns | Medium | Stats keys in printed outputs (`trips=`, `corruptdet=`). Quoted traces stay valid. |
| 04 Durability | Low-medium | `format` package; `CorruptionDetected` |
| 17 Limitations | Low | Mark Part II §3, §4 and §16 as fixed (comments). Keep the rest. |
| 00 Preface, 01 Why DST | Low | Package paths. `trace=1ca97eb77a9044e1` **is unchanged**. |

Recommended method: a reviewed `sed` rename map, applied chapter by chapter, followed by a manual pass on
Chapters 02, 03, 07 and 18, where the *concepts* change and not just the names.

---

## 8. Open questions

1. **How far should the renames go?**
   *Recommendation:* do the strong-fit renames (RENAME rows):
   - `core` → `control`, `Server` → `Controller`
   - `Halt` → `Trip`, `Fact` → `Telemetry`, `CorruptObject` → `CorruptionDetected`, `TimedFact` → `Sample`
   - `phase` → `mode`; `canRun` → `permitted`, `conflicts` → `interlocked`
   - `FakeFS` → `PlantModel`, `FSFaults` → `Disturbances`
   - `Latency`/`ReplyDelay` → `DeadTime`/`FeedbackDelay`, `QuietTail` → `Settling`

   Skip the OPT rows: `ColdStart`, `procedure`, `KindLand`. Keep `Event`, `Action` and `Step` (shared with
   `loop.md`), `Linearizable`, `Oracle`, `Invariant`, `Committed`, the S3 ops and the POSIX `FS*` names.

2. **Minimal or Full packages?**
   *Recommendation:* Minimal (`control`, `format`, `sim`, `prod`), plus the in-package ground-truth ACL. Go to
   Full when a second plant or a second controller appears.

3. **Keep the trace hash bit-identical through the refactor?**
   *Recommendation:* yes. Freeze the telemetry hash key in step 1, so `trace=1ca97eb77a9044e1` and the
   book's mutant hashes stay true. Do any move to numeric telemetry tags ("epoch 2") later, as a separate,
   announced re-baseline, or never.

4. **Rename `Oracle` → `Observer`?**
   *Recommendation:* no. Keep `Oracle`, the DST term, and avoid the GoF clash. Use set-membership-observer
   language *inside* it (`estimate`, `refState`) and explain the analogy in Chapter 13.

5. **Should the refactor also fix the Chapter 17 discrepancies?**
   *Recommendation:* only the comment-only ones (§3, §4, §16), in step 10. Leave out the ones that change
   behaviour or logs: `KindFailure` (§5), scheduling `KindCrash` (§15), the first buggify site (§1, which
   reshuffles every seed). Each gets its own PR after the refactor, so the golden test stays a clean proof.

6. **Rename user-visible output too** (`Stats.String()` keys, log kind strings, the prod `HALT:` line)?
   *Recommendation:* yes, so the ubiquitous language is the same in code, logs and book. The goldens compare
   numeric fields, so this costs nothing in safety. Update the book's printed outputs in the book PR.

---

## Appendix: DDD scorecard

| Criterion | Today | After (Minimal) |
|---|---|---|
| Ubiquitous language in code | 6/10: good protocol words; "core", "Fact" and "FakeFS" are technical | 9/10 |
| Explicit bounded contexts | 4/10: four contexts in `sim`; format inside core | 7/10 (file-level in `sim`); 9/10 in Full |
| Aggregate design | 8/10: a single-Step consistency boundary, already clean | 9/10: the interlock invariant is named |
| Ports and adapters | 7/10: the Step port is excellent; the plant port is implicit | 9/10 |
| Domain events | 6/10: they exist, are unnamed as a concept, and one is not past tense | 9/10 |
| ACLs at boundaries | 7/10: the HTTP ACL is good; monitors use the concrete fake FS | 8/10 (9/10 with the step 9 ACL) |
| **Overall** | **≈ 6/10** | **≈ 8.5/10** (Full: about 9/10, at a cost this toy does not need yet) |
