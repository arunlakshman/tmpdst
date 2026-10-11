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

## Invariants

The workload writes self-describing values: each value encodes the PUT that
wrote it, and the rest of its bytes are derived from that. Any bytes read
therefore name their writer exactly.

| | Invariant | Checked |
|---|---|---|
| a | Every 200 GET is linearizable against a per-key register model (COPY reads src and writes dst atomically) | at quiescent points (Oracle, WGL search) |
| b | Acked mutations survive crashes unless superseded. Crashes erase nothing in the model. | same search; also eagerly: `Committed` fact ⇒ FakeFS durable state matches, on the same step |
| c | No torn or corrupt bytes are ever returned with 200 | every step |
| d | Every request gets a reply while the server is alive | end of QuietTail |

**In-flight at crash (and 500s):** the outcome is indeterminate. The mutation
may take effect at one instant in `[invoke, crash or reply time]`, or never.
Reads with no reply constrain nothing. The run's state is folded into a trace
hash after every step: the server's `StateHash`, the FakeFS hash, the step
number and the time. Two runs of one seed must produce identical hashes.

## Running DST (phase 2)

```
go test ./toys3/sim -run TestSim -seeds=10000      # many seeds
go test ./toys3/sim -run TestSim -seed=81237 -v    # replay one, print event log
scripts/ci-determinism.sh                          # detcheck + determinism canary
```

Useful experiments: drop the `FSFsyncDir` after a rename, skip the tmp fsync,
remove `Halt` after an fsync error, or skip CRC verification on COPY. Then
watch which invariant catches each change, and at which step.
