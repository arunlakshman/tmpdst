# Decisions log

| # | Decision |
|---|----------|
| 1 | Core emits Actions; an Executor runs them (prod: goroutines over `sys` adapters; sim: engines on one queue). |
| 2 | Each event carries its own time (`At` node-local monotonic, `Wall` UTC). No `Now()` in the core; `Env` = randomness, buggify, assertions. |
| 3 | A separate **control plane** (HTTP) assigns offsets. Commit must be idempotent per segment key. Designed in a later session. |
| 4 | Control plane's metadata store: SQLite locally, DynamoDB in prod. The broker never sees it. |
| 5 | No local disk in v1. Caches are v2. |
| 6 | Network in scope: client↔broker and broker↔control plane. |
| A | Kafka framing + codec run in the deterministic path using franz-go `kmsg` (pure). The core receives `BytesReceived`. `kgo` is not used inside simulation. |
| C | Same-time events: seeded random tie-break across lanes, strict FIFO within a lane (one lane per connection direction). |
| D | Fake object store models two-speed latency (mostly fast, slow tail), 503s, timeouts, and PUTs that succeed but lose their response. |
| S | This session: core interfaces only. Implementations come in the next session. |
| F | Throttling is admission control inside `Step`: soft limit means Produce `ThrottleTimeMs` + mute the connection (KIP-219); hard limit means pause reads on all client connections; hysteresis on resume. Never drop admitted requests. Details: `flow-control.md`. |
| G | Two inboxes per node. **Control** (`Started`, `TimerFired`, all `Obj*Done`, `CPDone`, `SendDone`) is strict priority and bounded by construction. **Data** (`ConnOpened`, `BytesReceived`, `ConnClosed`, one ordered stream per connection) is bounded. Classification is a fixed table `core.ClassOf`. |
| H | The priority policy is a pure function (`core.InboxPolicy`) shared by the production loop and the simulator. The simulator models per-node inboxes and a seeded CPU `CostModel`, so priority, head-of-line blocking and metastability are deterministic and simulation-tested. |
| I | Deterministic packages (`core/`, `sim/`) use an **exact-path import allow-list** (fail closed), not a banned list, enforced by `tools/detcheck`. detcheck also bans goroutines, channels, `select`, map iteration, reflect map iteration and package-level vars (`//detcheck:ok <reason>` to suppress). A third-party module needs an audit record at `docs/dep-audits/<module>@<version>.md` (so a version bump forces a re-audit). CI: `scripts/ci-determinism.sh` (plus `.github/workflows/determinism.yml`). Audits: `scripts/audit-dep.sh` and the `audit-go-dependency` skill. |
| J | Go 1.24 (required by kmsg v1.14.0). kmsg rules: the framer gives each frame a fresh buffer that is never reused (decoded byte fields alias it); `ReadFrom` only, never `UnsafeReadFrom`; we compute CRC/lengths/timestamps ourselves. See `docs/dep-audits/github.com/twmb/franz-go/pkg/kmsg@v1.14.0.md`. |

Rows 7–22 come from building toys3 end to end. The signatures and evidence are in `interface-changes-from-toys3.md`. Status: accepted, not yet applied to the Go files.

| # | Decision |
|---|----------|
| 7 | **Fail-stop.** `core.Trip{Cause}` is the last action of its Step and kills the incarnation (sim: crash path, counted as a trip; prod: `ErrTripped`, process exits). Use it when in-memory state can no longer be trusted. |
| 8 | **Graceful stop.** `core.Stop{Deadline}` (Control class) asks for shutdown. The machine finishes or hands off its work and emits `core.Exit{}` as its last action. If the deadline passes first, the node is killed. This drives `Attrition.GracefulWeight`. |
| 9 | **`core.Machine = StateMachine + Hashable`.** `Factory` returns `Machine`, and `sim.Workload` embeds it. Hashing is no longer optional. |
| 10 | **Typed I/O.** `core.IOAction{Action; ReqID()}` and `core.Completion{EventBody; ReqID()}`. Add `DialDone`. A completion with an unknown, finished or wrong-kind ReqID changes nothing. ReqIDs are unique only per incarnation and never reach durable names. |
| 11 | **Contract rules.** Actions start in slice order. Each IOAction from a live incarnation gets exactly one completion; a lost response is `ErrTimeout`+`MaybeApplied`, never silence. `ErrInterrupted` is removed. `ErrNotFound`, `ErrConflict` and `ErrFenced` are never injected. `ErrCorrupt` comes only from the core's decoder. Hashed enums are append-only. `Started` is always the first Step. |
| 12 | **Facts are hash inputs.** Names are frozen, past-tense constants, never Go type names, and renaming one re-baselines. An ack fact (`produce_acked`) is emitted in the same Step immediately before the `Send` that acks. |
| 13 | **Invariants see a `View`:** step, now, phase, liveness, timeline, history and read-only ground truth (`ObjectTruth`, `CPTruth`, builtin types, sorted iteration, `Gen()`). `Observe(View)` and `AtEnd(View)` replace `Observe(tl, now)`. `Reset` is removed. |
| 14 | **Engines** embed `core.Hashable` and take `IOAction`. All fault coins and delays are drawn at issue time. `EngineCtx.Assert()` reports fault coverage. `DropNode` keeps engine-internal apply entries, because a request on the wire can land after the broker dies. Faults add an apply/respond latency split, `GetCorruptProb` and a correlated `Outage`. |
| 15 | **History and the oracle are separate from the workload.** `Workload` = `Machine` + `Quiescent()`, and `Check` is removed. `HistOp` has `OpPending/Definite/Indeterminate/Abandoned`, separate request and write IDs, and logical sequence numbers. **`MaybeApplied` ops are unbounded (Jepsen `:info`):** they may take effect at any time after invoke. The oracle widens and reports `Inconclusive` when it goes over budget, never a violation. |
| 16 | **Per-seed results.** `SeedResult{Steps, RandCalls, TraceHash, Crashes, Trips, Exits, Stats, Failures, Log}` and `Failure{Kind, Prop, Step, At, Msg}`, where Kind is one of Violation, Inconclusive, Protocol, Panic, MaxSteps or Nondeterminism. `Config.MaxSteps`. Determinism is checked on every Kth seed by comparing (TraceHash, RandCalls, Steps), and logging must not change the hash. Each seed's results and each mutant's first-catching seed are pinned in a golden file. |
| 17 | **Derived streams.** The swarm knobs and per-site buggify activation draw from `CountedRand.Derive(label)`, never the run stream. This supersedes lessons §9 ("same stream"). Add `Chance(p)` (draws nothing when p ≤ 0) and `Between`. |
| 18 | **Scheduler order is `(At, Tie, Seq)`** with one draw per Schedule. FIFO within a lane is preserved. `Entry` gets `Kind`. Crash, restart, stop, phase and quiesce are entries. Adds `CrashProb`, scripted faults, `DropNode` returning a count, and a rename of `QuietTail` to `Settling`. |
| 19 | **sys.** `Executor.Execute(core.IOAction)` (the loop handles everything else). `Sink.Closed()`: deliveries after the incarnation ends are dropped. `Classify(a IOAction, err)`, because the Outcome depends on the operation. The two inboxes stay. |
| 20 | **A runtime loop in sys:** `sys.Run(ctx, Bundle, Factory, NodeRef) error`, which returns `ErrTripped` on Trip and nil on Exit. |
| 21 | **`veritas/` package.** It holds executable specs (`ObjectStore`, `ControlPlane`, `Partition`) that double as test fakes, plus a lockstep runner with a shrinker. Four conformance checks: C1 broker vs spec, C2 sim engines vs spec, C3 `sys` adapters vs spec (the only check that runs `sys`), C4 oracle vs spec. The oracle never imports veritas. |
| 22 | **`format/` package** for the segment object layout and key naming, shared by the broker, the sim and veritas. Not adopted from toys3: porcupine (its verdicts depend on wall-clock time), the full package split, and the done-callback Executor. |
| 23 | **Control plane stays a separate HTTP service over DynamoDB** (confirms 3, 4). Brokers only issue `CPCall`. The sim fakes the CP at the op level (`veritas.ControlPlane` plus faults). The CP service gets its own tests against DynamoDB. |
| 24 | **Real-service test backends** (C3 conformance and integration, run with docker-compose). DynamoDB: ExtendDB (`extenddb/extenddb-dev`, SQLite, for CI; `extenddb/extenddb-postgres` for fidelity runs). S3: SeaweedFS by default, RustFS as an alternative. Not needed for milestone 1. |
| 25 | **Milestone 1 is the simulator.** Kernel (rand, scheduler, timeline, node inboxes and cost model, runner, phases, attrition, trace hash, report), engines (object store, control plane, network), an echo broker and workload, and a determinism canary in CI, all before any Kafka logic. |
| 26 | **Kafka APIs in v1:** ApiVersions, Metadata, Produce (acks=all, idempotent producer), Fetch, ListOffsets. No consumer groups and no transactions. The sim workload speaks these via `kmsg`. Integration tests use real franz-go clients. |
| 27 | **No CRC in the control plane.** S3 checksums cover whole-object PUT and GET. Open caveat: S3 does not check arbitrary byte-range GETs. If needed, the per-batch CRC already in the `format/` index covers them without any CP change. |

## Threading model

### Production (per broker process)

```
 accept goroutine ──► per-conn reader goroutine ── ConnOpened/BytesReceived/ConnClosed ──► DATA inbox (bounded)
                                                    (parks on PauseReads)                         │
 time.AfterFunc callback goroutine ── TimerFired ──────────────────────────────► CONTROL inbox    │
 executor goroutines (S3, CP HTTP) ── Obj*Done / CPDone / SendDone ─────────────► (never blocks)  │
                                                                                      │           │
                         loop goroutine (the ONLY owner of broker state):             ▼           ▼
                           ev := InboxPolicy.Next(control, data)   // control first, always
                           ev.At, ev.Wall = clock at dequeue
                           actions := sm.Step(ev)
                           for a := range actions { exec.Execute(a) }  // never blocks
                                                  │
                  ┌───────────────────────────────┼──────────────────────────────┐
                  ▼                               ▼                              ▼
     per-conn writer goroutine         goroutine per I/O action         time.AfterFunc(d, push TimerFired)
     (ordered; Send/Close;             (completion → control inbox)
      Pause/ResumeReads → reader)
```

- Exactly one goroutine runs `Step`. No locks around broker state.
- `Execute` must never block the loop. The state machine caps in-flight work, so
  the control inbox is bounded by construction and its senders never block.
- Backpressure is explicit: the broker emits `PauseReads`/`ResumeReads` from its
  admission control (`flow-control.md`). The bounded data inbox is only a safety net.
- A timer is a goroutine that sleeps and then sends `TimerFired` to the control
  inbox. It can race with a size-triggered flush (decided inside `Step`, never
  queued). The generation check makes that race harmless.

### Simulation (whole cluster)

```
 one goroutine, no channels, no real time:
   for !queue.Empty():
       e := queue.PopMin()            // order: (simTime, random tie across lanes, FIFO within a lane)
       clock = e.time
       if e is for node N:  actions := N.Step(e stamped with N's local At/Wall)
                            for a in actions: engine(a).Execute(...)  // schedules completions
       else:                engine.OnInternal(e)                      // e.g. "apply PUT now", "deliver bytes"
       check invariants; fold state hash; record RNG fingerprint
```

- `SetTimer{after: 200ms}` becomes a queue entry at `now + 200ms (+ skew, +
  buggified delay)`. If nothing else is pending, time jumps straight there.
- An object-store PUT becomes two internal entries: *apply* at t1 and *respond*
  at t2. Faults can drop the response, turn it into a timeout, or delay it into
  the slow tail.
- A crash discards the node's state machine, drops queue entries addressed to
  that incarnation, and later calls the factory again with a new incarnation.
- Deliveries to a node go into that node's **simulated control/data inboxes**,
  not straight into `Step`. The node picks with the same `InboxPolicy` as
  production and stays busy for a seeded `CostModel` duration per step, so
  head-of-line blocking and overload replay exactly (`flow-control.md` §4).
