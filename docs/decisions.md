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
