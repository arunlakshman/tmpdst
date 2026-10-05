# Lessons that change the interfaces

Distilled from the Moonpool Sim Book (https://pierrez.github.io/moonpool/), which
synthesizes FoundationDB, TigerBeetle and Antithesis practice, and applied to our
Go, WarpStream-like engine built on the `Step(Event) []Action` loop (see `loop.md`).

## 0. The framing: in Go, the Action/Event vocabulary *is* the provider interface

Moonpool's main technique is the *provider pattern*: application code calls
`time.sleep()` and `network.connect()` through traits, and a custom
single-threaded executor picks which ready task runs next using the seeded RNG.
Go cannot do this. We cannot replace the goroutine scheduler, and `select`
chooses randomly among ready cases. So in Go, library-level determinism needs
sans-I/O state machines.

Consequences:
- The **Action/Event vocabulary is the boundary** that production and
  simulation share. `ObjPut` / `ObjPutDone` plays the role of a `StorageProvider`.
- The `sys.*` interfaces are only the production adapter seam. The simulator
  does **not** implement them. It implements *engines* that own state, draw from
  the single seeded RNG, and schedule completions on one global queue. A
  synchronous `Put() error` cannot express "the PUT was applied at t1 and its
  response was lost at t2"; an engine can.

## 1. Errors say what they prove (NotApplied / MaybeApplied / Applied)

Every completion event carries `Err` **and** an `Outcome`:

| Outcome        | Meaning                                         | Examples |
|----------------|-------------------------------------------------|----------|
| `NotApplied`   | The operation provably had no effect            | connect refused, 503 before admission, rejected by validation |
| `MaybeApplied` | It may have taken effect                         | timeout after the request left, connection lost mid-call |
| `Applied`      | It took effect (the reply may still be unusable) | success, or reply too large or undecodable |

Rules:
- Producers are acked only on `Applied` from the control-plane commit.
- `MaybeApplied` is retried only if the operation is idempotent. So the
  control-plane `CommitSegment` **must be idempotent per segment key**, and
  object-store keys are unique per upload.
- Adapters classify SDK and HTTP errors into `(ErrCode, Outcome)` in exactly
  one place.

## 2. Two clocks seen by the node, one clock owned by the simulator

- **Scheduling time** is global simulation time. Only the simulator sees it; it
  orders the queue.
- **Node-local monotonic time** is `Event.At`. Each node may drift (up to
  ~100 ms in simulation). Leases, deadlines and the 200 ms batch window use it.
- **UTC (wall) time** is `Event.Wall`. It is used for Kafka `LogAppendTime`,
  token expiry and anything persisted. It is scripted in simulation.

There is no `Now()` call anywhere in the core. The runtime stamps both clocks on
each event when it leaves the inbox, so time never goes backwards in `Step` order.

## 3. One counted RNG stream

All randomness comes from one seeded stream with a call counter: fault coins,
latencies, tie-breaks, buggify and IDs.
- **Determinism canary**: run every seed twice, fingerprint each draw (RNG state
  plus logical time), and fail on the first divergence. This complements the
  state-hash trace.
- **Debugging**: "diverged at draw 847".
- **Exploration later** (multiverse): reseed after N draws and continue from
  interesting states. This only works if the RNG supports `Calls()` and
  reseeding from day one.

## 4. Incarnations

Every boot of a node gets fresh random incarnation bits. Leases, segment
ownership and any reference to a node carry `(NodeID, Incarnation, Epoch)`.
References to an old incarnation are rejected (`ErrFenced`), never silently
refreshed. Learning the new incarnation is an explicit step.

## 5. Explicit pending results; crashes resolve them

Every operation has an exact `ReqID` and an explicit pending or completed
result. A missing result is never treated as success. On a crash:
- completions addressed to the dead incarnation are dropped,
- timers of the dead incarnation never fire,
- effects that the outside world (object store, control plane) already applied
  **stand**. This is exactly the "PUT landed, response lost" case.

## 6. Network faults are connection-level, not packet-level

Simulate a TCP byte stream:
- partial reads and writes (short transfers),
- black holes (looks alive, delivers nothing; finds missing timeouts),
- graceful half-close vs abort (RST),
- partitions that **freeze** in-flight bytes and re-time them on heal (never drop
  them silently), and asymmetric partitions,
- clogging, and stably slow peer pairs.

Consequence: Kafka framing and decoding run **inside the deterministic path**.
The core receives `BytesReceived` and frames them with a pure framer plus codec
(franz-go `kmsg`). Otherwise short reads never test our framing code.

## 7. Buggify is a first-class interface

- Each site has a stable string ID. It is activated once per run (50%) and then
  fires per call (default 25%, or a given probability).
- `Maybe` / `MaybeP` take rare but valid paths and keep firing in the recovery
  tail.
- `Fault` makes an operation fail and goes silent when the simulator enters
  recovery mode.
- It is inert (always false) in production, and it reads the one RNG stream
  through `Env`.
- Pair every buggify site with a `Sometimes` assertion and a safety check.

## 8. A run has explicit phases

1. **Chaos window**: faults are injected.
2. **Recovery mode**: no new faults; existing damage stays (closed conns,
   applied-but-unacked writes, crashed nodes).
3. **Quiet tail**: nodes still alive; liveness and convergence are checked here.
4. **Settle**: drain remaining events.
5. **Check**: final validation against the reference model.

## 9. Swarm testing and correlated faults

- **Passive suppression**: "everything a little bit on" means fault families
  crowd each other out and the extremes are never reached.
- Per seed, switch a random subset of fault families fully **off**, and swarm
  the workload's operation alphabet the same way.
- Faults must be **correlated**: sustained partitions, stably slow pairs
  (sampled once per pair), two-speed latency (mostly fast, a slow tail), and
  episodic slowness. Independent per-message drops average out to a healthy network.
- Swarm masks are drawn from the same stream with a fixed number of draws, so
  they replay.

## 10. Three separate roles: Process, Workload, Invariant

- **Process** (broker): built by a factory on every boot. In-memory state dies
  on crash; only external stores survive.
- **Workload** (Kafka clients): never crashes, holds the **reference model**
  (what was acked, at which offsets), reconciles ambiguous results, and runs
  `Check` at the end. In Go this is *also* a sans-I/O state machine, because a
  real franz-go `kgo` client uses goroutines and real sockets and cannot run in
  the simulator. It speaks Kafka bytes produced with `kmsg`.
- **Invariant**: reads an append-only **fact timeline** after every step. The core
  emits facts as an `Emit` action (pure). Injected faults land in the same
  timeline as `sim_fault` facts, so violations can be correlated with causes.

## 11. Assertions record and continue

- `Always`, `AlwaysOrUnreachable`, `Sometimes`, `Reachable`, `Unreachable`, plus
  numeric watermarks later.
- A violation is recorded with the seed; the run keeps going to expose
  cascades. The report is the authority.
- A `Sometimes` that never fires means the simulation isn't reaching that code.
  Add faults, not excuses.
- Messages are **stable property IDs**: no interpolation. Dynamic context goes
  in detail fields.

## 12. Go-specific determinism hazards inside the core

Forbidden in `core` and the state machines: `time.Now`/`time.Since`/timers,
`math/rand`, `crypto/rand`, `uuid.New`, goroutines, channels, `select`,
`sync`, `os`, `net`, SDK imports.

**Map iteration order is randomized by Go on purpose.** Never let `range` over
a map decide behavior: iterate sorted keys or use ordered structures.

Enforce in CI with `depguard` (import allow-list for core packages) plus
`forbidigo` (banned calls), plus a lint against ranging over maps in core.
Events and actions are plain data (no funcs, channels, or pointers into shared
state); byte slices are immutable once sent.

## 13. Control before data, and backpressure you can see

From Moonpool's RPC layer, where a stuck writer once filled a shared control
queue and tore down unrelated calls:

- Keep **control** (completions, timers, lifecycle) separate from **data**
  (client bytes). Control is strict priority. Data is taken in bounded batches,
  so no source starves the rest.
- The thing that frees resources (completions) must never wait behind the thing
  that consumes them (ingress). Otherwise overload becomes metastable: it
  persists after its trigger is gone.
- Every queue has a budget. A refusal or a pause happens **where work would
  enter**, never by dropping work already admitted.
- Backpressure must be an explicit decision of the state machine (`PauseReads`,
  Kafka `ThrottleTimeMs`), not a side effect of a full channel. Only then can the
  simulator exercise it.
- The priority policy is pure, shared by production and the simulator. The
  simulator models per-node inboxes and a seeded CPU cost, so priority and
  metastability are deterministic and testable (`flow-control.md`).
