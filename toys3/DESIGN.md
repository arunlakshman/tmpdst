# toys3: a toy S3 for learning deterministic simulation testing

toys3 is a single-node object server: PutObject, GetObject, DeleteObject and
CopyObject, with each object stored as one local file and fsynced before the
200. Being a good S3 is not the point. The point is to have a small system with
real durability bugs to make, so that a deterministic simulator can catch them.
It follows [`../loop.md`](../loop.md) strictly.

```
toys3/core   pure state machine + on-disk format       (detcheck-clean)
toys3/sim    simulator, fake FS, workload, oracle      (detcheck-clean)
toys3/prod   real runtime: os calls, HTTP, event loop
toys3/cmd/toys3   main (-dir, -addr)
```

## The state machine

The server never does I/O. It turns Events into Actions:

```
Event  = Started | Request{Client, Put|Get|Delete|Copy}
       | FSWriteDone | FSFsyncDone | FSRenameDone | FSFsyncDirDone
       | FSReadDone | FSUnlinkDone | FSListDone            (each carries IOID, Err)
Action = Reply{Client, Status, Body}
       | FSWrite | FSFsync | FSRename | FSFsyncDir | FSRead | FSUnlink | FSList
       | Halt{Reason} | Emit{Fact}
```

Every FS action carries an `IOID`. Its completion comes back as a separate
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
protocol bug into a visible 500 (and a `CorruptObject` fact) instead of silent
garbage.

**Errors.** Any FS error replies 500 and is never acked. A 500 tells the client
nothing: the rename may have landed. After an fsync error the page cache can no
longer be trusted (Linux "fsyncgate"), so the server should `Halt`, which
means fail-stop and recover from disk. If it kept serving instead, it could
expose a value that a later crash erases. The oracle flags that.

**Per-key serialization.** Each key has a FIFO queue, and a request runs when it
is at the head of every queue it touches (COPY touches src and dst). All
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
       t=5ms  fsLand   FSRename{io10}  (plan: OK)       ◄─ effect lands in FakeFS
       t=6ms  ⚡ crash → FakeFS resolves volatile state, entries for inc 2 dropped
       t=9ms  restart  inc 3, deliver Started
```

An FS action lands (its effect is applied) and its completion is delivered at
two separate times. A crash in between gives "it happened, but nobody was
told". Entries carry the server incarnation, and anything addressed to a dead
incarnation is dropped.

## Fault model (FakeFS)

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
and a failed rename or unlink may or may not have applied. Latency is drawn per
op. `FsyncGate` makes fsync lie after a failure. Faults stop during
`QuietTail`.

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
  dead code (`TestCorruptObjectUnreachableWithoutBitFlips` checks this).

**Crash model.** A crash, including `Halt`, is a power loss. The FakeFS resolves
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
| b | Acked mutations survive crashes unless superseded. Crashes erase nothing in the model. | same search; also eagerly: `Committed` fact ⇒ FakeFS durable state matches, on the same step |
| c | No torn or corrupt bytes are ever returned with 200 (`NoCorruptReads`) | every step |
| d | Every request gets a reply while the server is alive, every boot recovers, and no request issued in QuietTail gets a 500 | 500s: every step; the rest: end of run |
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
hash after every step: the server's `StateHash`, the FakeFS hash, the step
number and the time. Two runs of one seed must produce identical hashes.

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
| b. skip the temp file's `FSFsync` | obj-never-torn | 1 | liveness (500s in QuietTail), seed 2 |
| c. reply 200, then rename and fsync | acked-is-durable | 1 | linearizable, seed 1 |
| d. 500 without `Halt` after any post-write error | obj-never-torn | 1 | liveness, seed 20 |
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
