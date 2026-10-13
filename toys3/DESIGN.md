# toys3: a toy S3 for learning deterministic simulation testing

toys3 is a single-node object server: PutObject, GetObject, DeleteObject and
CopyObject, with each object stored as one local file and fsynced before the
200. Being a good S3 is not the point. The point is to have a small system with
real durability bugs to make, so that a deterministic simulator can catch them.
It follows [`../loop.md`](../loop.md) strictly.

```
toys3/control   the controller: pure state machine + Storage port (detcheck-clean)
toys3/format    on-disk object format and layout                  (detcheck-clean)
toys3/sim       SIL testbench: simulator, storage model, workload,
                oracle, invariants                                (detcheck-clean)
toys3/prod      real runtime: os calls, HTTP, event loop
toys3/cmd/toys3      main (-dir, -addr)
toys3/cmd/toys3sim   parallel DST campaign runner
```

Every toys3 package except `prod` and `cmd/*` is gated by `tools/detcheck`;
`scripts/ci-determinism.sh` derives that list with `go list`.

## Domain model and vocabulary

The code is organized with domain-driven design, and its vocabulary comes
from control theory where the fit is genuine (see `REFACTOR-PROPOSAL.md` for
the full analysis). The server is a **controller** in a feedback loop around
**storage**, the disk and its page cache. (Control theory calls the controlled
system the *plant*; toys3 says *storage*, because that is what it is.) The
controller drives storage with actuations and reads its feedback:

```
     requests (operator)                            disturbances: EIO, torn writes,
              │                                     bit flips, writeback, power loss
              ▼                                                   │
     ┌──────────────────┐   actuations (FS* commands)  ┌─────────▼──────────┐
     │   Controller     │ ────────────────────────────►│      storage       │
     │ Step(Event)      │ ◄────────────────────────────│ (disk + page cache)│
     │  []Action        │   feedback (FS*Done events)  └────────────────────┘
     └──┬────────┬──────┘   dead time: actuation → effect; feedback delay: effect → completion
        │        └──► telemetry (Committed, Recovered, CorruptionDetected)
        └──► replies, or Trip (fail-stop) when its own state can't be trusted
```

| Term | In code | Meaning |
|---|---|---|
| Controller | `control.Controller` | The state machine: the control law. The single aggregate; one `Step` is its consistency boundary. |
| Actuation / Feedback | `control.Actuation` (FS* actions), `control.Feedback` (FS*Done events) | The Storage port. The controller owns it; `prod.OSExecutor` (real disk) and `sim.StorageModel` (simulated) implement it. |
| Trip | `control.Trip{Cause}` | Safety trip: fail-stop when in-memory state can't be trusted (fsyncgate); restart re-arms. |
| Mode | `modeStartup → modeRecovering → modeAuto → modeTripped` | Operating mode of one incarnation. |
| Key interlock, start permissive | `interlocked`, `permitted` | Per-key FIFO: a request starts only when no earlier request shares a key. |
| Telemetry | `control.Telemetry` via `Emit` | Domain events the controller publishes: logged by prod, checked by the simulator's monitors. |
| Storage model | `sim.StorageModel` | The simulated filesystem with crash semantics. Its durable view is the true state. |
| Disturbances | `sim.Disturbances` | Faults the controller cannot command, plus storage's dead time and feedback delay. |
| Chaos, Settling | `Config.Chaos`, `Config.Settling` | Disturbed phase, then the phase in which the closed loop must settle (every request answered, no 500s). |
| Sample | `sim.Sample` | Telemetry stamped with step, time and incarnation. |
| Reference model, estimate | oracle's `refState`, `estimate` | The sequential spec the history must match; the oracle's set of possible states. |
| Ground truth | `sim.GroundTruth` | The only view of storage the monitors get (durable files). |

Plain names are kept where a control metaphor would be forced: requests and
the S3 ops, `Committed`, `Linearizable` (linearizability, not linearization),
`Oracle`, `Invariant`, the CRC and the format.

Bounded contexts: **storage control** (`control`, the core domain),
**object format** (`format`, a shared kernel between the controller, the
monitors, the storage model and prod), **storage** (the port in `control`; adapters
`prod.OSExecutor` and `sim.StorageModel`), **verification** (`sim`: oracle,
history, invariants), **testbench** (`sim`: simulator, queue, RNG, trace hash,
workload) and **production runtime/transport** (`prod`, with `Handler` as the
anti-corruption layer from S3 REST to `control.Op`).

## The state machine

The controller never does I/O. It turns Events into Actions:

```
Event  = Started | Request{Client, Put|Get|Delete|Copy}
       | FSWriteDone | FSFsyncDone | FSRenameDone | FSFsyncDirDone
       | FSReadDone | FSUnlinkDone | FSListDone            (each carries IOID, Err)
Action = Reply{Client, Status, Body}
       | FSWrite | FSFsync | FSRename | FSFsyncDir | FSRead | FSUnlink | FSList
       | Trip{Cause} | Emit{Telemetry}
```

Every FS action (an `Actuation`) carries an `IOID`. Its completion comes back as a separate
event, so the server never blocks and many operations on different keys can be
in flight at once. Time arrives as `Event.At`. Randomness and buggify come from
the injected `Env`. No timers are needed: every wait is on an FS completion.

A PUT, one step per completion (the ack is the last thing to happen):

```
Request{c7, Put k}    ──► Step ──► [FSWrite{io1, tmp/01, hdr+data}]
FSWriteDone{io1}      ──► Step ──► [FSFsync{io2, tmp/01}]
FSFsyncDone{io2}      ──► Step ──► [FSRename{io3, tmp/01 → obj/k}]
FSRenameDone{io3}     ──► Step ──► [FSFsyncDir{io4, obj}]
FSFsyncDirDone{io4}   ──► Step ──► [Emit{Committed}, Reply{c7, 200}]
```

## Durability protocol, and why each step exists

| Op | Steps | Why |
|---|---|---|
| PUT | write tmp → fsync tmp → rename → fsync obj/ → 200 | fsync makes the bytes durable. Rename swaps old for new atomically, so readers never see half a file. The dir fsync makes the rename durable. |
| DELETE | unlink → fsync obj/ → 200 (404 if absent) | Without the dir fsync, a crash can bring the object back. |
| COPY | read src, verify CRC → the PUT path for dst | Never copy garbage, and give the new object the same durability as a PUT. |
| GET | read, verify CRC → 200 / 404 / 500 | Never serve a torn file. |

On-disk object file: `"TOY3" | crc32(payload) | len(payload) | payload`. A
correct protocol never leaves a torn file under `obj/`. The checksum turns a
protocol bug into a visible 500 (and `CorruptionDetected` telemetry) instead of silent
garbage.

**Errors.** Any FS error replies 500 and is never acked. A 500 tells the client
nothing: the rename may have landed. After an fsync error the page cache can no
longer be trusted (Linux "fsyncgate"), so the server should `Trip`, which
means fail-stop and recover from disk. If it kept serving instead, it could
expose a value that a later crash erases. The oracle flags that.

**Per-key serialization (the key interlock).** Each key has a FIFO queue, and a
request runs when its start permissive holds: it is at the head of every queue
it touches (COPY touches src and dst). All
queues share one arrival order, so a cycle and therefore a deadlock is
impossible. Different keys run concurrently.

**Recovery.** Memory is disposable. On `Started`: `FSList(tmp)`, unlink every
leftover, `FSFsyncDir(tmp)`, emit `Recovered`, then serve the requests that
queued up meanwhile. `obj/` needs no recovery because it only ever gets
complete, renamed files.

## Production vs simulation

```
prod:  HTTP ─► inbox(chan) ─► Loop.Run: Step ─► OSExecutor goroutines ─► Done ─► inbox
                                         └──► Reply ─► waiting HTTP handler

sim:   one goroutine, Queue ordered by (At, seeded Tie, Seq), virtual clock
       t=3ms  deliver  Request{c4, Put k1}              (inc 2)
       t=3ms  deliver  FSFsyncDone{io9}                 (inc 2)   ◄─ tie broken by seed
       t=5ms  fsLand   FSRename{io10}  (plan: OK)       ◄─ effect lands in the StorageModel
       t=6ms  ⚡ crash → StorageModel resolves volatile state, entries for inc 2 dropped
       t=9ms  restart  inc 3, deliver Started
```

An FS action lands (its effect is applied) and its completion is delivered at
two separate times (after the dead time, then after the feedback delay). A crash in between gives "it happened, but nobody was
told". Entries carry the server incarnation, and anything addressed to a dead
incarnation is dropped.

## Fault model (StorageModel and its Disturbances)

Every inode has *durable* and *volatile* content, and every directory entry has
*durable* and *current* versions. `FSFsync` makes content durable. `FSFsyncDir`
makes the directory's entries durable. Reads see the volatile and current
versions. On crash, the RNG resolves each un-synced change on its own, the way
an adversary would:

- a directory entry change is kept or reverted. A cross-directory rename can
  leave both names, or neither new name.
- dirty file content is kept, reverted to durable (possibly empty), or, with
  `TornWriteProb`, cut to a random prefix.

Each op can also fail with EIO (`EIOProb`). A failed write may leave a prefix,
and a failed rename or unlink may or may not have applied. Dead time and feedback delay are drawn per op. `FsyncGate` makes fsync lie
after a failure. New faults stop during `Settling`.

Two more behaviours turned out to matter:

- **Background writeback** (`WritebackProb`). After any op the kernel may make
  one random un-synced change durable on its own, the way a journal commit
  does. Without it, a temp file's name is durable only if a crash happens to
  keep it. With it, the name can already be durable when the file is renamed
  into `obj/`, so a later crash brings it back as a second hard link to the
  committed object.
- **Read corruption** (`ReadCorruptProb`). A read may come back with one bit
  flipped, while the stored data stays intact. With the correct protocol,
  `obj/` never holds a torn file, so without this fault the CRC check would be
  dead code (`TestCorruptionUnreachableWithoutBitFlips` checks this).

**Crash model.** A crash, including a `Trip`, is a power loss. The StorageModel resolves
all volatile state at that instant. Queued entries for the dead incarnation are
dropped: FS actions that have not landed yet never happen, and completions
that have not been delivered are lost. An action that landed before the crash
stays applied but is never acknowledged.

**Swarm.** `DefaultConfig(seed)` draws the number of keys (1 to 32), clients,
crash rate and EIO rate per seed, from a stream separate from the run's. A few
keys produce same-key races. Many keys leave objects untouched long enough for
slow damage (like the aliasing above) to become visible.

## Invariants

The workload writes self-describing values: each value encodes the PUT that
wrote it, and the rest of its bytes are derived from that. Any bytes read
therefore name their writer exactly.

| | Invariant | Checked |
|---|---|---|
| a | Every 200 GET is linearizable against a per-key register model (COPY reads src and writes dst atomically) | at quiescent points (Oracle, WGL search) |
| b | Acked mutations survive crashes unless superseded. Crashes erase nothing in the model. | same search; also eagerly: `Committed` telemetry ⇒ the storage model's durable state (ground truth) matches, on the same step |
| c | No torn or corrupt bytes are ever returned with 200 (`NoCorruptReads`) | every step |
| d | Every request gets a reply while the server is alive, the final boot recovers, and no request issued in Settling gets a 500 | 500s: every step; the rest: end of run |
| e | Every file under `obj/` in the *durable* view is a complete object (`ObjNeverTorn`) | whenever durable state changes |

**In-flight at crash (and 500s):** the outcome is indeterminate. The mutation
may take effect at one instant in `[invoke, crash or reply time]`, or never.
Reads with no reply constrain nothing. By locality, each window is split into
connected components of keys (a COPY links its src and dst), and each component
is searched on its own. The possible end states are carried into the next
window, factored per key. If a component would start from more than 4096
possible states, the oracle *widens* the keys involved to "unknown": any read
of such a key is accepted and fixes its value. This weakens the check for a
moment but never causes a false failure, and the `widened` stat counts it.
Ops are ordered by logical
invoke and return sequence numbers, not by virtual time, so several events
that share one instant keep their exact order. The run's state is folded into a trace
hash after every step: the controller's `StateHash`, the storage model's hash, the step
number and the time. Events, actions and telemetry are folded as stable
numeric tags and field values, never as Go type names; telemetry goes through
a frozen encoding (`telemetryHashKey`), so renames cannot change a trace hash.
`TestGolden` pins steps, random draws, trace hashes and stats for 550 seeds and
where each mutant is caught. Two runs of one seed must produce identical hashes.

## How to run DST

```
go test ./toys3/sim                                   # unit tests, canary, 200 seeds, coverage, mutants
go test ./toys3/sim -run TestSeeds -seeds=5000 -v     # more seeds (prints aggregated stats)
go test ./toys3/sim -run TestSeeds -duration=2m -v    # seeds 1,2,... for two minutes
go test ./toys3/sim -run TestSeeds -seed=81237 -v     # replay one seed; event log printed on failure
go test ./toys3/sim -run TestCatchesBugs -v           # which invariant catches each broken server
go run ./toys3/cmd/toys3sim -duration 10m -check-determinism   # parallel campaign
scripts/ci-determinism.sh                             # detcheck + audits + both canaries
```

A failing seed prints its own repro command. The campaign runner runs each seed
on its own goroutine (no shared state), prints progress every 10s, replays
every 10th seed to compare traces with `-check-determinism`, and exits non-zero
with the repro commands. Plug in your own broken server through
`Config.Factory` (see `mutants_test.go`).

## Bugs DST catches

`TestCatchesBugs` wraps the real server in deliberately broken variants (one
protocol step each) and runs seeds 1..500 until one fails. The "black-box"
column uses only the client-visible invariants (no peeking at durable state).

| Variant | Caught by (all invariants) | First seed | Black-box only |
|---|---|---|---|
| a. skip `FSFsyncDir(obj/)` | acked-is-durable | 1 | linearizable, seed 1 |
| b. skip the temp file's `FSFsync` | obj-never-torn | 1 | liveness (500s in Settling), seed 2 |
| c. reply 200, then rename and fsync | acked-is-durable | 1 | linearizable, seed 1 |
| d. 500 without `Trip` after any post-write error | obj-never-torn | 1 | liveness, seed 20 |
| d2. same, only for a failed `FSFsyncDir(obj/)` | linearizable (a read saw the value, then a crash erased it) | 132 | linearizable, seed 132 |
| e. skip the CRC check on read | no-corrupt-reads | 18 | no-corrupt-reads, seed 18 |
| f. recovery skips temp cleanup | linearizable (resurrected temp aliases an object; a reused temp name overwrites it) | 20 | linearizable, seed 20 |

Lessons from building this table:

- (b) is invisible to the linearizability oracle. The CRC turns a lost object
  into a 500, and a 500 promises nothing. Only a ground-truth check, or
  "no 500s once faults stop", catches it.
- (e) is unreachable without read corruption, because a correct protocol never
  leaves a torn object to read.
- (f) took seed 429 with neither background writeback nor swarm, seed 36 with
  writeback alone, and seed 20 with both. Whether a bug gets caught depends on
  how realistic the fault model is and how varied the workload is.
