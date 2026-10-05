# Flow control, throttling and inbox priority

Status: design agreed, interfaces applied (see "Interface changes" at the end).
Related: `decisions.md` (F, G, H), `lessons.md` (13).

## Summary: queues, priority and how they are tested

### Which events go in which queue

| Control queue (strict priority) | Data queue |
|---|---|
| `Started`, `TimerFired`, `ObjPutDone`, `ObjGetDone`, `ObjDeleteDone`, `ObjListDone`, `CPDone`, `SendDone` | `ConnOpened`, `BytesReceived`, `ConnClosed` |

The rule: **events that must stay in order relative to each other share a queue.**
Everything the node caused itself (completions of its own actions, its own
timers) goes to the control queue. Everything a connection sends goes to the
data queue as one ordered stream per connection.

`ConnClosed` has to go in the data queue. As a control event, a client's EOF
could overtake its last request still waiting in the data queue, and that
request would be lost.

### How the priority works

1. If the control queue has anything, take from it. Always.
2. Otherwise, take the next event from the data queue.
3. If both are empty, sleep until either has something.

Neither queue can starve the other:
- Control events only exist because of work the broker started itself, and that
  work is capped (in-flight uploads, commits, fetches, sends, timers). So the
  control queue is **bounded by construction**. Its senders never block, and
  data always gets a turn.
- A completion only ever waits for the `Step` currently running, never behind
  queued client bytes.

Throttling is a decision inside `Step`, made against a soft and a hard limit:

| Buffered bytes | What the broker does |
|---|---|
| Above the soft limit (default 64 MB), or a client over its quota | Answer the Produce request with `ThrottleTimeMs`, then pause reading that connection (how Kafka's own quotas work). |
| Above the hard limit (default 128 MB) | Pause reading every client connection. |
| Back below the soft limit | Resume. Resuming at the lower limit, not the one that triggered the pause, prevents flapping. |

The broker never drops a request it has already accepted.

### How the priority is deterministic and simulation-tested

1. **The policy is a pure function, `core.InboxPolicy.Next(ctrl, data)`, shared
   by production and the simulator.** Go's `select` never decides priority,
   because it picks randomly among ready cases. Production reads the control
   queue first without blocking and uses `select` only to sleep when both
   queues are empty.
2. **The simulator models each node's two inboxes and its CPU time.** Deliveries
   go into the node's queues, not straight into `Step`. The node picks its next
   event with the same policy code and stays busy for a seeded cost per step:
   a per-event cost, a per-byte cost, and occasional slow bursts that stand in
   for Go GC pauses. Without that cost, `Step` is instantaneous and head-of-line
   blocking can't happen in the simulator. With it, overload really does queue
   up and replays exactly from the seed.
3. **Same seed, same sequence of picks.** Queue lengths are part of the node's
   state hash, so any divergence shows up immediately.
4. **What the simulator checks:**
   - **Always:** the control queue stays within its bound; a completion never
     waits behind a queued data event; every accepted request is answered or its
     connection is closed; buffered bytes stay within the hard limit plus one
     maximum request; each connection's events stay in order.
   - **Sometimes:** a throttle was sent; the hard pause happened; a paused
     connection resumed; a completion arrived while client bytes were queued; a
     timer fired with a stale generation.
   - **Liveness after chaos stops:** paused connections resume, and buffered
     bytes fall back below the soft limit within T. A seed that stays above the
     limit after the load spike is gone has found a metastable state.
5. **Proving the checks catch something:** swap the policy for a single
   first-in-first-out queue and run the same seeds. Some must fail. If none do,
   the cost model or the workload is too weak.

Real CPU, GC and goroutine scheduling stay outside the simulator. Production
load tests calibrate the cost model so simulated overload resembles real
overload.

The sections below give the full detail.

## 1. Problem

The first threading sketch used one bounded inbox, with "inbox full" as the only
backpressure. That has three defects:

1. **It tells the client nothing.** Kafka clients get no throttle signal; they
   just see a slow socket.
2. **It cannot be simulation-tested.** The simulator calls `Step` directly and
   has no inbox, so the overload path never runs under test.
3. **It causes head-of-line blocking of the events that free memory.** A flood
   of `BytesReceived` delays `ObjPutDone` → `CPDone` → ack. Those completions are
   what release buffered bytes. Memory stays high, so we throttle more and queues
   get longer. That is a **metastable spiral**: the system can stay stuck after
   the load spike that caused it is gone.

Note: size-triggered flushes are **not** events. They are decided inside `Step`
while appending records, so they can never be stuck in a queue. Under heavy
load, size triggers dominate and fire immediately. The 200 ms `TimerFired`
matters mainly at low load, where there is no queue. The real head-of-line
victims are the **completions**.

## 2. Admission control lives in the state machine

Admission is decided in `Step`. It is deterministic and plain data, so the
simulator exercises it. The broker tracks its own pressure:

```
buffered = bytes in open batch
         + bytes in sealed batches waiting for ObjPutDone
         + bytes in uploaded segments waiting for CPDone (not yet acked)
```

| Level  | Trigger (defaults, tunable)                                  | Broker behavior |
|--------|--------------------------------------------------------------|-----------------|
| Normal | `buffered < SoftLimit` (64 MB) and client within quota        | Accept. |
| Soft   | `buffered ≥ SoftLimit`, or the client's byte-rate quota exceeded | Accept the request, answer with `ThrottleTimeMs > 0` in the Produce response (Kafka's native mechanism, honored by kgo and the Java client), **and** emit `PauseReads{Conn}` plus a timer to `ResumeReads` after that throttle time. KIP-219 semantics: respond, then mute. |
| Hard   | `buffered ≥ HardLimit` (128 MB)                               | Emit `PauseReads` for **every** client connection. Resume all when `buffered < SoftLimit` (hysteresis), driven by completions. |

Rules:
- **Never drop an admitted request.** Every admitted request eventually gets a
  response or its connection is closed. We do not invent "busy" errors: Kafka
  Produce has no clean overload error code. Delay plus throttle time is what
  clients expect.
- **Pausing reads is the real backpressure.** In production the connection's
  reader goroutine parks. The kernel TCP buffer fills and the client's sends
  stall. In the simulator the network engine keeps the bytes in the sender's
  flow-control window. Bytes already received are still framed and processed.
- A single request larger than the remaining headroom is still admitted when
  `buffered < HardLimit`, so the cap is
  `HardLimit + MaxRequestSize`. This is asserted.
- Hysteresis (pause at Hard, resume below Soft) prevents pause/resume flapping.

## 3. Two inboxes per node

### 3.1 Classification (a fixed table in `core`: `core.ClassOf(EventBody) Class`)

| Event            | Queue       | Why |
|------------------|-------------|-----|
| `Started`        | **control** | First event of a boot. |
| `TimerFired`     | **control** | Batch windows, throttle expiry, retries. |
| `ObjPutDone`     | **control** | Completion: frees memory, advances the commit pipeline. |
| `ObjGetDone`     | **control** | Completion: finishes a fetch and frees its reservation. |
| `ObjDeleteDone`  | **control** | Completion. |
| `ObjListDone`    | **control** | Completion. |
| `CPDone`         | **control** | Completion: assigns offsets and releases producer acks. |
| `SendDone`       | **control** | Completion: frees outbound buffer accounting. |
| `ConnOpened`     | **data**    | Must stay ordered before that connection's bytes. |
| `BytesReceived`  | **data**    | Client ingress; the only unbounded-by-us source. |
| `ConnClosed`     | **data**    | **Must stay ordered after that connection's bytes.** In the control queue a graceful EOF could overtake the connection's last request still waiting in the data queue, and that request would be lost. |

**The classification rule:** events that must stay ordered relative to each other
share a queue. All inbound events of one connection form one ordered stream in
the data queue. Everything the node itself caused (completions of its own
actions, its own timers) goes to the control queue.

### 3.2 Bounds

- **Control queue: unbounded type, bounded by construction.** Every control event
  is the result of an action the state machine already emitted, and the state
  machine caps those:
  `controlMax ≤ MaxInflightFlushes·2 (PUT + commit) + MaxInflightFetches
  + MaxOutstandingSends + live timers + 1`.
  Because it can't overflow, its senders (I/O goroutines, timer callbacks)
  **never block**, which removes the loop/sender deadlock risk.
- **Data queue: bounded** (for example 1024 entries). It is a safety net only;
  real backpressure is `PauseReads`. When it is full, reader goroutines block,
  which also leads to TCP backpressure.

### 3.3 Priority policy (deterministic)

```
Policy (pure function of the two queues' contents and a counter):
  1. If control is non-empty → take the head of control.
  2. Else if data is non-empty → take the head of data, increment burst.
  3. After every DataBurst (default 64) consecutive data events, re-check
     control before taking more data (already implied by rule 1, since control
     is checked before every pick).
  4. If both are empty → block until either becomes non-empty.
```

Properties:
- **Control is strict priority.** A completion waits behind at most **one**
  in-progress `Step`, never behind queued data.
- **Data cannot starve forever.** Control events only exist because of capped
  in-flight work, so the control queue drains in bounded time and data always
  gets a turn.
- **Per-connection order is preserved,** because each connection's events are in
  one first-in-first-out queue. Control and data events may interleave
  differently; correctness must not depend on that interleaving (the simulator
  checks this, §4).
- **Deterministic given the queue contents.** The policy is a pure function
  (`core.InboxPolicy`), with no Go `select` deciding priority. In Go, `select`
  picks randomly among ready cases, so the production loop drains control with
  non-blocking receives first and only uses a two-way `select` to *sleep* when
  both queues are empty.
- **Fairness between connections:** v1 uses one first-in-first-out data queue
  plus per-client quotas and `PauseReads`. Round-robin queues per connection
  are a later option if one hot connection proves to hurt the others.

### 3.4 Production loop

```go
for {
    ev, ok := policy.Next(ctrl, data)   // pure: prefers ctrl; reads ctrl non-blocking first
    if !ok { ev = waitEither(ctrl, data) } // blocking select used ONLY to sleep
    ev.At, ev.Wall = clock.Mono(), clock.Wall()
    for _, a := range sm.Step(ev) { exec.Execute(a) } // Execute never blocks
}
```

Arrival order into the channels is nondeterministic in production; the choice
made *given* what has arrived is deterministic and is the same code the
simulator runs.

## 4. Making it deterministic and simulation-testable

Treating `Step` as instantaneous hides everything in §1. So the simulator
**models each node's inboxes and CPU time**, using the same pure policy code.

### 4.1 Node model in the simulator

```
simNode = {
    sm        core.StateMachine
    ctrl,data []Entry              // the node's two inboxes, classified by core.ClassOf
    busyUntil sim.Time             // models the loop goroutine being busy
}

engines/network deliver → append to node.ctrl or node.data (NOT straight into Step)
when node is idle and has queued events:
    ev   := core.InboxPolicy.Next(ctrl, data)     // the SAME code as production
    cost := CostModel.Sample(ev, rng)             // seeded
    run Step(ev) at now; node busy until now+cost; actions issued at now
```

**CostModel** (seeded, swarmable, buggify-able), part of `sim.Config`:
- a base cost per event plus a cost per byte for `BytesReceived` (decoding,
  copying into the batch);
- rare **pause episodes**: a burst of slow steps that stands in for a Go GC
  stop-the-world or CPU contention, sampled as a two-speed distribution;
- `0` cost reproduces today's instantaneous model, for fast functional seeds.

With costs, a flood of client bytes really does queue up in the simulator.
Completions really do arrive while the node is busy. The policy's choices really
do change outcomes, and all of it replays from the seed.

### 4.2 Determinism

- Queue contents come from the global scheduler (which orders by time, random
  tie-break across lanes, and first-in-first-out within a lane). Each pick comes
  from `InboxPolicy` (pure). Each cost comes from the counted RNG. **The same
  seed gives the same sequence of picks.**
- Queue lengths and `busyUntil` are folded into the node's state hash, so a
  divergence shows up on the first differing pick.

### 4.3 What the simulator asserts

Safety (`Always`):
- `inbox control queue never exceeds controlMax`: the bound-by-construction claim.
- `completion waits behind no queued data event`: measured per completion as
  the number of data events stepped between its enqueue and its own step. Must be 0.
- `admitted produce request is answered or its connection closed`: no admitted
  request is lost.
- `buffered bytes stay within HardLimit + MaxRequestSize`.
- `connection events stay in order`: `ConnOpened` < its `BytesReceived` < its
  `ConnClosed`.
- `producer acked only after commit Applied`: unchanged from the core contract.

Coverage (`Sometimes` / `Reachable`):
- `soft throttle sent with ThrottleTimeMs > 0`
- `hard limit paused all reads`
- `connection resumed after pause`
- `completion arrived while data queue non-empty`: proves the priority path is
  actually exercised.
- `timer fired with stale generation`: proves the race is exercised.

Liveness (checked in the quiet tail, after chaos stops):
- Every paused connection resumes within `T`.
- `buffered` returns below `SoftLimit` within `T` after offered load drops.
  **This is the metastability detector**: a seed that stays above the limit
  after the trigger is gone has found a bad stable state.

Workload support: the simulated Kafka client has an operation alphabet that
includes *open-loop bursts* (send faster than acks return) and must honor
`ThrottleTimeMs` the way kgo does, so the throttle path is driven by realistic
clients.

**Mutation check (proves the oracle works):** swap `InboxPolicy` for a single
first-in-first-out queue under the same seeds. Some seeds must turn red: either
on "completion waits behind no queued data event" or on the quiet-tail
metastability check. If none do, the cost model or workload is too weak.

### 4.4 What stays outside the simulator

Actual CPU time, the GC and scheduler behavior. The cost model approximates
them. Production load tests calibrate `CostModel` (the approach Moonpool takes
with measured latency bounds) so simulated overload resembles the real thing.

## 5. Interface changes (applied)

| Where | What |
|---|---|
| `core/action.go` | Actions `PauseReads{Conn}` and `ResumeReads{Conn}`. |
| `core/inbox.go` | `Class` (`Control`, `Data`) and `ClassOf(EventBody) Class`, the fixed table from §3.1. An unclassified event type panics, so a new event can't silently land in the wrong queue. |
| `core/inbox.go` | `Queue` (`Len`, `Pop`) and `InboxPolicy` (`Next(control, data Queue)`), pure and shared by production and the simulator. |
| `sys/sys.go` | `Sink` split into `DeliverControl` (never blocks) and `DeliverData` (may block). `Conns` gains `PauseReads`/`ResumeReads` so the production executor can park a reader goroutine. |
| `sim/cost.go` | `CostModel`, `CostParams` (per-event cost, per-byte cost, pause episodes) and `NodeInbox` (a node's simulated two inboxes plus `BusyUntil`). |
| `sim/run.go` | `Config.Cost`. Zero means an instantaneous `Step`. |
