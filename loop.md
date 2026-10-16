# Designing for Deterministic Simulation Testing (DST)

Guidelines for building the Manifester broker so the whole cluster can run inside
a single-threaded, seeded simulator, while production pays close to zero cost.

## Core idea: one event loop per broker

Each broker is a **single-threaded state machine**. It never performs I/O. It
consumes **Events** and returns **Actions**. A runtime outside the broker executes
the actions and feeds their results back in as new events.

```
                ┌──────────────────── Broker (pure logic, no I/O) ───────────────────┐
  events  ───►  │  inbox ──► Step(event) ──► mutate state ──► []Action               │ ──► actions
                └────────────────────────────────────────────────────────────────────┘
  Event  = ProduceReq | FetchReq | S3PutDone{ok|err} | MetaCommitDone | TimerFired | ...
  Action = S3Put{key,bytes} | MetaCommit{offsets} | Reply{conn,resp} | SetTimer{d,id}
```

A produce request (the ack goes out only after the data is durable):

```
ProduceReq ──► Step ──► [S3Put(seg-42)]
S3PutDone  ──► Step ──► [MetaCommit(p0: 100..199)]
CommitDone ──► Step ──► [Reply(conn7, offset=100)]     ← ack happens here, never earlier
```

### Production: actions do real I/O, run concurrently

```
 TCP conns ──┐                       ┌──► goroutine: s3.PutObject ──┐
             ▼                       │                               │
        ┌─────────┐   Step()   ┌─────┴────┐                          │
        │  inbox  │ ─────────► │ executor │ ──► goroutine: conn.Write│
        │  (chan) │            └──────────┘                          │
        └─────────┘ ◄──────────── S3PutDone / CommitDone ────────────┘
```

Hundreds of S3 PUTs can run in parallel. Only `Step()` runs one event at a time,
and it only changes memory, so it takes microseconds.

### Simulation: one seeded scheduler owns everything

```
                 ┌──────────────── Simulator (single goroutine) ────────────────┐
                 │  Global event queue, ordered by (simTime, seededRandomTie)    │
                 │   t=5ms  B1 ← ProduceReq                                      │
                 │   t=5ms  B2 ← FetchReq       ◄── ties broken by seeded RNG    │
                 │   t=37ms B1 ← S3PutDone(err=503)  ◄── fake S3 chose latency + │
                 │   t=40ms ⚡ CRASH B2                   failure from the RNG   │
                 └──────────────────────────────────────────────────────────────┘
   loop: e := queue.PopNext(); clock = e.time
         actions := brokers[e.target].Step(e)
         for a in actions: fake.Execute(a) → schedules the completion event at clock + rng latency
```

- There are no real goroutines, sockets or wall clock, so **the same seed gives the exact same history**.
- Time jumps straight to the next event, so an hour of simulated time runs in milliseconds.
- **Crash** means throwing away the broker's memory, rebuilding it from the object
  store and the metadata store, and dropping every in-flight completion addressed to it.

## Instructions

1. **Separate logic from I/O.** Core packages (`broker`, `partition`,
   `groupcoord`) never import `net`, `os`, `time`, `math/rand`, or the AWS SDK.
   Enforce this in CI with a lint check such as `depguard`.
2. **Use the `Step(Event) []Action` shape** for every stateful component. State
   changes happen only inside `Step`.
3. **Make Events and Actions plain data:** structs without channels, funcs or
   pointers into shared state. That makes them easy to log, compare and replay.
4. **Inject every source of nondeterminism** through a small `Env` interface:
   `Now()`, `Rand()`, `NewID()`. Never call `time.Now()` or `uuid.New()` directly.
5. **Wrap external systems in narrow interfaces you own,** such as
   `ObjectStore{Put, Get, List, Delete}` and `MetaStore{CommitOffsets, Lease, ...}`.
   Never let SDK types leak outside the adapter.
6. **Make every I/O operation asynchronous:** an action goes out and a
   `...Done{id, result, err}` event comes back. Nothing in the logic ever blocks.
7. **Timers are actions too:** `SetTimer{id, d}` goes out and `TimerFired{id}`
   comes back. Timeouts, retries and flush delays all work this way.
8. **Give every request a correlation ID** so late or duplicate completions can
   be recognized and ignored. The simulation will deliver these.
9. **Put all durable state in the store.** A broker must fully rebuild from
   `ObjectStore` plus `MetaStore` after a crash, and anything in memory is
   treated as disposable. This is exactly what "stateless brokers" means.
10. **Keep the production runtime thin** (`cmd/broker`): accept on TCP → decode
    → inbox channel → `Step` → execute actions in goroutines → feed results back
    into the inbox. It shouldn't contain any business logic.
11. **Build the simulator** (`sim/`) as one goroutine with a priority queue
    ordered by `(time, rngTie, seq)` (the tie must come before seq or it never decides anything; decision 18), one `*rand.Rand` seeded from `-seed`, and a
    virtual clock.
12. **Make the fakes adversarial:** random latency, 503s, timeouts, a PUT that
    succeeds but whose response is lost, duplicated responses, reordering,
    network partitions between AZs, and crash plus restart at any event.
13. **Check invariants after every step, not just at the end.** For example: no
    offset acked unless it's durable, offsets strictly increasing per partition,
    a consumer never reads data that wasn't acked, and no two leaseholders at once.
14. **Hash the state after every step** into a running trace hash, and run each
    seed twice in CI. If the hashes differ, nondeterminism has leaked in.
15. **Make failures one command:** `go test ./sim -seed=81237` reproduces the
    failure and prints the event log. In CI, run many seeds (for example 10k per
    PR), and keep failing seeds as regression tests.

## Production cost

`Step` is a plain function call, and actions are small structs sent over one
channel hop. That's nanoseconds, next to S3 calls that take milliseconds.
