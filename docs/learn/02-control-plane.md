# 02 - The Control Plane, inside out

**Goal of this page:** after reading it you can open any file under `internal/controlplane/`,
say which layer it belongs to, say what event or call reaches it, and say what it hands
to the next layer. Page 01 followed one Produce request across the whole system and only
touched the control plane (CP) at the middle. This page stays *inside* the CP and goes
down to the bytes.

Code was read on branch `feat/cp-stacked-faults` (commit `c1725af`). Paths are relative to
the repo root. Names are more stable than line numbers, so this page points to names.
Every `go test` command in this page was run and passes.

---

## 0. Before any file: five ideas

### 0.1 What the control plane is for

The broker (page 01) is stateless. It keeps no durable memory. So *something* has to
remember four things for the whole system. That something is the control plane.

| # | Job | Example |
|---|-----|---------|
| 1 | **Give offsets.** Decide which offsets a batch gets. | "orders-0 batch of 3 records gets offsets 7, 8, 9" |
| 2 | **Remember where bytes are.** For each batch, which S3 object and which byte range. | "offsets 7..9 live in `seg/9f3c...` bytes 120..480" |
| 3 | **Know the topics.** Name and partition count. | "`orders` has 2 partitions" |
| 4 | **Make retries safe.** The same commit sent twice gives the same answer. | "key `seg/9f3c...` was already committed at offsets 7..9" |

It offers five gRPC calls (file `api/v1/controlplane.proto`):

```
CommitSegment   write   "I uploaded this object. Give offsets to its batches."
Lookup          read    "Which batches hold offsets >= N in this partition?"   (Fetch uses it)
DescribeTopics  read    "List topics and partition counts."                    (Metadata uses it)
CreateTopic     write   "Make this topic, at least this many partitions."      (dev and test helper)
GetStatus       read    "Are you active or standby? How far have you applied?" (operators)
```

Two of these are writes that change state (`CommitSegment`, `CreateTopic`). The others only
read. The difference matters a lot later: **writes go through the log, reads do not.**

### 0.2 State is a function of a log (the passbook idea)

Think of a bank passbook. The passbook is a list of lines: "deposit 500", "withdraw 200".
Your balance is not stored anywhere on its own. You get the balance by reading the lines
in order and adding them up. Two clerks reading the same passbook always get the same
balance.

The control plane works the same way:

- The **log** is the passbook. Each line is called a **slot**: slot 0, slot 1, slot 2, ...
  A slot holds one *entry* (bytes). In production the log lives in DynamoDB.
- The **fold** is the balance. It is a key-value store built by reading the log from slot
  0 upward and applying each entry. "Fold" is the programmer word for "go through a list
  and keep a running result".
- **Offsets are decided while folding, not while writing.** The writer only puts
  "CommitSegment for these batches" into the log. The offsets come out when each replica
  folds that line.

Why this is powerful: two CP nodes that fold the same log in the same order get the same
offsets, with no talking between them. The log is the *only* agreement they need.

### 0.3 One rule gives you consensus: "write only if the slot is empty"

Two CP nodes might both want to write slot 41 at the same moment. The log supports one
operation for this: **put if absent**. In DynamoDB this is `PutItem` with
`attribute_not_exists(pk)`. Exactly one writer wins slot 41. The loser is told "taken",
and receives the winner's bytes in the same answer. The loser then tries slot 42.

That one rule is the whole safety story:

- No two entries ever share a slot.
- Everyone reads slots in order, so everyone sees one single history.
- If a node thinks it is the boss but is not (a "zombie"), it can only waste effort. It
  cannot corrupt anything, because it still has to win each slot.

The code calls the "boss" an **appender** and calls roles **advisory**: a hint to avoid
fighting, not a lock. The slot put is the real fence.

### 0.4 The CP is itself a `Step(Event) []Action` machine

Same idea as page 01, section 0.1. The brain of the CP (`statemachine.Machine`) never
opens a socket and never reads a clock. It gets an **Event**, and returns **Actions**
("append this to slot 41", "read log from slot 30", "put this checkpoint object", "set a
timer"). Real I/O is done by a different layer, and its result comes back later as another
Event with the same `ReqID`.

Because of this, the same Machine runs in two places:

- **Production:** inside `mdscontrolplane`, driven by `runloop`, with real DynamoDB and S3.
- **Simulator:** inside `kafka-log` scenario, driven by the sim kernel, with fake
  everything and seeded random faults.

### 0.5 The package is built in layers, and the layers are not allowed to cheat

```
        brokers (gRPC)                                  ┐
             │                                          │ the "edge": real
   controlplaneclient ──► api/v1 + apiconvert ──► server (Node)   goroutines,
                                                     │   │          real network
                                  inject work        ▼   │
                                              runloop (one goroutine)
                                                     │  Step(Event)
                                                     ▼
   ┌─────────────────────────── statemachine (Machine) ──────────────────┐
   │ pure: no I/O, no clock, no goroutines. Emits Log* / Object* actions │
   └──────────────────────────┬──────────────────────────────────────────┘
                              │ folds entries into
                              ▼
                     stateupdater (the fold: T/E/R/K/S keys in a KV)
                              ▲
   actions run by:  executor (DynamoLog, MemoryLog, object store)
```

There is a **test that enforces the walls**: `statemachine/import_boundary_test.go` fails
if `statemachine` imports the broker, the simulator, `net/http`, Pebble, the DynamoDB SDK
and a few more (decision 56). This is why the CP is "already standalone": the Machine can
be lifted out of this repo because it depends only on `internal/core` and its sibling
packages.

---

## 1. The map of the package

All sizes are non-test lines. "Edge" means not deterministic (goroutines, network).

| Package | Lines | Layer | One-line job |
|---------|------:|-------|--------------|
| `statemachine` | ~1,700 | pure brain | The `Machine`: catch-up, commit queue, roles, checkpoints, fresh reads |
| `stateupdater` | ~1,260 | pure fold | Apply entries into an ordered KV; answer Lookup/Describe; snapshot/restore |
| `stateupdater/pebblestore` | 143 | KV driver | Pebble implementation of the KV seam. `mdscontrolplane` passes it to the Machine through `Config.OpenStore` (fixed in `56417bd`, see section 12) |
| `executor` | ~660 | edge | Run `Log*` and `Object*` actions: `DynamoLog`, `MemoryLog`, object store, error classify |
| `runloop` | ~370 | edge | The one goroutine that calls `Step`, runs timers, dispatches actions |
| `server` | ~470 | edge | gRPC `Node`: turns RPCs into `Offer` calls and waits for answers |
| `api/v1`, `apiconvert` | ~120 + 250 | wire | Protobuf contract and conversion to `core` types |
| `controlplaneclient` | ~375 | edge (broker side) | gRPC client with failover; error classifier |
| `controlplanemetrics` | 340 | edge | Prometheus metrics and per-call logs |
| `memorystore`, `fakedynamodb`, `grpctestserver`, `controlplanetest` | | test helpers | in-memory stores, fake DynamoDB, conformance tests |
| `store.go`, `doc.go` (root) | | | `Store` interface (the shape every CP backend must have) |

**Name map from page 01.** Page 01 was written before the "simple names" refactor. When
you see an old name in page 01 or in an old comment, translate:

| Old name (page 01, old comments) | Current name |
|----------------------------------|--------------|
| `machine` | `statemachine` |
| `applier` / `Applier` | `stateupdater` / `StateUpdater` |
| `applier.KV` | `stateupdater.KeyValueStore` |
| `commitTx` | `commitTransaction` |
| `exec` | `executor` |
| `run` (`controlplane/run`) | `runloop` |
| `cpclient` | `controlplaneclient` |
| `memkv` / `MemKV` | `MemoryKeyValueStore` |
| `pebblekv` package | `pebblestore` package (file is still `pebblekv.go`) |
| `fold.Apply`, `fold` | same: "fold" is the `StateUpdater` held in `Machine.fold` |

---

## 2. The contract: what comes in and what goes out (`internal/core/controlplane.go`)

Read this file first. It is short. These are the types that cross the CP boundary.

```go
CommitSegment{ SegmentKey string; Batches []PendingBatch }
PendingBatch { TopicPartition TopicPartition; Records int64; Loc SegmentRef }   // Loc = {Key, Off, Len} in S3
CommitSegmentResp{ Assigned []BatchRange }                          // one per request batch, same order
BatchRange   { TopicPartition; Base Offset; Count int64; Loc SegmentRef }       // offsets [Base, Base+Count)
FindSegments { TopicPartition; From Offset; MaxBytes int32 }
FindSegmentsResp{ Ranges []BatchRange; End Offset }                 // End = next offset to be assigned
```

Three promises in the comments of this file are the specification of the whole package:

1. **Atomic.** For one `CommitSegment`, *every* batch gets offsets or *none* does.
2. **Contiguous and never reused.** Per partition, offsets grow by exactly `Records` per
   batch. A later batch of the same partition in the same request continues after the
   earlier one.
3. **Idempotent per `SegmentKey`.** Same key + identical request = the original ranges
   again. Same key + different request = `ErrConflict`. This is the *only* use of
   `ErrConflict`.

### 2.1 Two kinds of failure, and the `Outcome` word

Every call returns three things that are easy to mix up:

| What | Type | Meaning |
|------|------|---------|
| Business answer | `core.ErrCode` in `ControlPlaneDone.Err` | "your request is wrong or conflicts" (`ErrInvalid`, `ErrConflict`) or "not ready" (`ErrUnavailable`) |
| Infrastructure failure | Go `error` | store broke, network broke |
| **Outcome** | `NotApplied` / `MaybeApplied` / `Applied` | *did the write happen?* |

`MaybeApplied` is the dangerous middle. A timeout on a write means "I do not know if it
landed". The caller must be allowed to retry **the same request** and be safe. That is
exactly why idempotency per `SegmentKey` exists. Keep this in your head: almost every
complicated branch in this package is about turning `MaybeApplied` into a safe retry.

`store.go` at the package root defines `cp.Store`, the four-method interface every
backend promises (`CommitSegment`, `Lookup`, `DescribeTopics`, `CreateTopic`). It is the
shape that the conformance tests (`controlplanetest`) compare against the executable spec
`speccheck.ControlPlane` ("veritas").

---

## 3. Chapter A - `stateupdater`: the fold (where offsets are born)

Start here. It is the heart, and it is plain, synchronous Go with no events.

### 3.1 The KV seam (`kv.go`)

The fold keeps its data in an *ordered key-value store*. The interface is deliberately tiny:

```go
type KeyValueStore interface {
    View(fn func(Transaction)) error     // read-only, consistent view
    Update(fn func(Transaction)) error   // one atomic write transaction, reads see own writes
    Close() error
}
type Transaction interface {
    Get(key) []byte                      // nil if absent. values are never empty
    Floor(lo, hi) (key, val)             // LAST entry with lo <= key < hi
    Ascend(lo, hi, fn)                   // walk entries in key order
    Set(key, val); Delete(key)
}
```

Two details worth learning:

- **Errors are latched.** A store error does not interrupt `fn`. The first error is
  remembered, later reads return nothing, and `View`/`Update` returns the error at the
  end. This keeps the apply code free of `if err != nil` noise.
- **Durability is not required.** State is rebuilt from the log plus a checkpoint, so a
  store may skip fsync. The Pebble driver commits with `pebble.NoSync` for this reason.

Two implementations: `MemoryKeyValueStore` (`memkv.go`, a sorted slice copied on every
write, fine for tests and small state) and `pebblestore.DB` (Pebble: indexed batch for
`Update`, snapshot for `View`).

### 3.2 The five kinds of key (`keys.go`)

Every key starts with one letter that says what kind it is. All keys of one kind are
*prefix-free*, which means no key is the start of another, so range scans never mix them up.

| Letter | Key | Value | Used for |
|:------:|-----|-------|----------|
| `T` | `T` + topic name | uvarint partition count | topic list (Describe) |
| `E` | `E` + tp | uvarint **end offset** (next offset to give) | where to start the next batch |
| `R` | `R` + tp + base (8 bytes) | packed **run** | where the bytes of each batch are (Lookup) |
| `K` | `K` + segment key | slot + 16-byte digest + one base per batch | idempotency record |
| `S` | `S` + slot (8 bytes) + segment key | `0x01` | index so Trim finds old `K` by slot |

`tp` (topic-partition) is encoded as `uvarint(len(topic))`, then the topic bytes, then the
partition as 4 bytes big-endian. Big-endian is chosen so that byte order equals number
order, which makes range scans return partitions and offsets in numeric order.

Example, topic `orders`, partition 0, end offset 3:

```
key   45 06 6f 72 64 65 72 73 00 00 00 00      'E', len=6, "orders", partition 0
value 03                                       uvarint 3
```

An `R` key adds 8 more bytes at the end: the run's **base offset**, big-endian. Since runs
for one partition sort by base, "the run that holds offset 100" is simply the last key at
or below `R tp 100`. That is what `Floor` is for.

### 3.3 The entry: what a log slot contains (`entry.go`)

A log slot holds an `Entry`, one of three kinds:

```go
KindNoop         // changes nothing. Used by heartbeat and hole-fill.
KindCommit       // a CommitSegment
KindCreateTopic  // topic name + partition count
```

`Encode`/`Decode` write it with varints. The commit encoding has one clever trick: a
**string table**. Topic names and S3 keys repeat a lot across the batches of one commit
(24 batches of the same segment key). So the encoder stores each distinct string once and
every batch refers to it by index. The decoder rejects bad indexes, short buffers, and
leftover bytes with `ErrCorrupt`. Corrupt input never mutates state.

The entry also carries an `ID` (`"writer/seq"`, for example `"cp-host-1/17"`). The ID is
how a node later recognises "this slot is *my* write" (section 4.7).

### 3.4 `commitTransaction`: the most important function in the package

(`stateupdater.go`.) It runs inside one `Update`, so it is all-or-nothing. In order:

```
1. dig := checksum(op.Batches)                 // sha256, first 16 bytes, of the batches (not the key)
2. rec := Get("K"+SegmentKey)
   if rec exists  ->  replay(rec, dig, op)     // see 3.5. Return. Nothing is written.
3. if !validCommit(op) -> ErrInvalid           // empty key, no batches, records<=0, empty topic, partition<0
4. assigned := assign(tx, op.Batches)          // offsets are decided HERE (3.6)
5. storeCommit: Set K record, Set S record     // remember the key (idempotency)
6. return CommitSegmentResp{Assigned}, Outcome: Applied
```

Notice the order: the replay check comes **before** validation. If a key is already known,
the request is never validated. `replay` compares digests instead, and a different request
under a known key gets `ErrConflict`, which is the correct answer.

### 3.5 Replay: same key, same answer

`K` record layout: `uvarint(slot) + 16-byte digest + uvarint(base) for each batch`.

`replay` does:

- Digest differs from stored digest -> `ErrConflict` (same key, different content).
- Otherwise rebuild the answer: for batch `i`, `Base = bases[i]`, `Count = Records`,
  `Loc` from the *request*. Note it does not store the whole answer. It stores only the
  bases, and rebuilds `Count` and `Loc` from the request, which the digest proved to be
  identical. This keeps the record small.

### 3.6 `assign`: handing out offsets

```go
func assign(tx, batches) []BatchRange {
    p := newOffsetPlan()
    for each batch b, in request order:
        base := p.ends[b.TopicPartition]                       // already seen in THIS request? chain from there
        if !p.seen[b.TopicPartition] { base = readVarint(Get(endKey(b.TopicPartition))) }   // else read the stored end
        out = append(out, p.add(b, base))          // range = [base, base+Records); end moves up
    p.write(tx)
    return out
}
```

`offsetPlan` collects, per partition, the new end and the list of ranges. Then
`writeAssign` writes, for each touched partition, in this one transaction:

1. `E tp` = new end offset.
2. `R tp base` = one **packed run** with all the batches this commit gave that partition.
   Key base is the base of the *first* batch.
3. `T topic` = partition count raised to at least `partition+1` (`addPartitions`). This is
   how a topic grows if a commit names a partition that is higher than known. It never shrinks.

Two batches of the same partition in one request chain correctly: the first gets base 0,
end 3; the second reads `p.ends` (3) not the store, gets base 3.

### 3.7 The packed run (`encodeBatchRun`, `decodeBatchRun`)

A run is "the contiguous batches one commit gave one partition". It is packed for space,
about 6 bytes per batch:

```
format byte (1)
uvarint  number of distinct segment keys, then each key (len + bytes)
uvarint  number of batches
per batch: uvarint key-index, uvarint count, varint off, varint len
```

The run does **not** store each batch's base offset. The base of batch 0 is in the key
(`R tp base`), and the base of batch `n+1` is the base of batch `n` plus its count. The
decoder just adds counts as it walks. This is why offsets are contiguous *by construction*
inside a run: there is nowhere to write a gap.

### 3.8 Reads: `Lookup` and `Describe`

`Lookup(tp, From, MaxBytes)`:

1. `End` = value of `E tp`.
2. Find the starting run: `Floor` on keys between `R tp 0` and `R tp From` gives the last
   run that starts at or before `From`. (The run holding offset `From` may start earlier.)
3. `Ascend` from there to the end of the `R tp` prefix. Decode each run, skip batches that
   end at or before `From`, collect the rest.
4. Stop when collected bytes (`Loc.Len` sum) reach `MaxBytes` (if `MaxBytes > 0`). It may
   exceed by one batch. The first batch is always returned.
5. A run that cannot be decoded is **a panic** (section 3.11), not an error.

`Describe` scans all `T` keys and filters by the requested names (empty list = all).

### 3.9 Trim: forgetting keys but never offsets

`Trim(slot)` deletes every `K` record whose commit slot is below `slot`, using the `S`
index (`S` keys sort by slot, so "all keys older than slot N" is a prefix scan). It
returns how many keys it dropped. **`E`, `R`, `T` are never touched.** Offsets are
forever; only the *memory of the key* expires.

Consequence (decision 48): the idempotency window of a key equals "how long the key
stays in the fold". After Trim, the same key commits **again as a new segment** and its
records are duplicated. This is accepted: at-least-once delivery already allows it.

### 3.10 Snapshot and Restore (`snapshot.go`)

A checkpoint is the whole KV at one log slot. Format:

```
"APLR1"            5-byte marker (old name "applier" still in the bytes)
varint slot        FIRST slot NOT covered. Replay starts here.
repeat: uvarint klen, key, uvarint vlen, val       (in key order)
uvarint 0          end mark
4 bytes            CRC-32 (IEEE, big-endian) of everything before
```

- `SnapshotAt(w, begin)` takes a consistent `View`, then calls `begin()` **inside** it to
  read the applied slot. The slot and the data are therefore from the same instant.
- `CheckSnapshot(data)` verifies length and CRC without touching any store. The Machine
  calls it *before* `Restore` because `Restore` writes into the store while reading.
- `Restore` reads in 8 MiB chunks (one `Update` per chunk), re-computes the CRC as it reads
  (`crcReader`), and compares at the end. `readBytes` copies with `io.CopyN` so a lying
  length cannot make it allocate gigabytes.
- The checkpoint **includes** `K` and `S`, so the idempotency window survives a restart.

### 3.11 Fail-stop

If the KV store fails, `Exec`/`Commit` **panic** on purpose (`"store failed"`). The reason
is written in `doc.go`: if a write half-fails, this replica's memory no longer equals "the
log folded". Continuing would fork the truth. Crashing is safe because the process is
disposable: it restarts, restores a checkpoint, replays the log. The same rule applies to
a corrupt stored record (`errCorrupt`). It also appears in the Machine as `trip(err)`,
which emits `StopForRestart`.

### 3.12 Smaller things in the same package

| Name | What it does |
|------|--------------|
| `CommitBatch(cmds)` | Apply many commands in ONE store transaction. Same result as one by one, fewer page writes. |
| `StateHash()` | Digest of `T`, `E`, `R` only (skips `K`, `S`). Two replicas that applied the same commands agree even if they trimmed differently. |
| `Assigned(slot, op)` | After the fact: "did this command own its key at this slot?" (`OffsetOwnedHere / Elsewhere / Unknown`). For an indexer. |
| `Store` (`store.go`) | A `cp.Store` over a StateUpdater with a local counter and no log. For conformance tests and tools. |
| `Apply(slot, Entry)` | Switch on `Kind`. Used by the Machine. |
| `KnownCommit(op)` | Read-only "is this key already in the window?" Used by the Machine's fast path (4.8). |

**Try it:**

```
go test ./internal/controlplane/stateupdater -run 'TestTrimForgetsKeysNotOffsets|TestAssignedStates' -v
go test ./internal/controlplane/stateupdater -run 'TestLockstepMem' -v     # fold vs the oracle
```

---

## 4. Chapter B - `statemachine`: the brain

This is the biggest and hardest part. Read slowly. Files: `machine.go` (events, catch-up),
`commit.go` (queue, groups, apply), `role.go` (roles, faults, trimmed-slot handling),
`checkpoint.go`, `reads.go`, `client.go` (sim front door), `envelope.go`, `config.go`.

### 4.1 One `Step`, many reasons to wake up

```go
func (m *Machine) Step(ev core.Event) []core.Action {
    switch body := ev.Body.(type) {
    case NodeStarted:          onStarted()
    case StopRequest:          stop()
    case TimerFired:           onTimer(body)
    case LogGetEndDone:        onCheckTailDone(body)     // answer to "what is the log end?"
    case LogReadDone:          onReadDone(body)          // answer to "read slots from N"
    case LogAppendDone:        onAppendDone(body)        // answer to "put this in slot N"
    case ObjectGetDone:        onObjGetDone(body)        // checkpoint read finished
    case ObjectPutDone:        onObjPutDone(body)        // checkpoint write finished
    case LogDeleteOldDone:     onTrimDone(body)
    case ControlPlaneDeliver:  onCPOffer(body)           // sim only: a broker's call
    }
    acts = append(acts, m.serveReads()...)               // after every event: answer ready reads
    return m.withReplies(acts)                           // turn answers into ControlPlaneReply (sim)
}
```

After `ShutdownDone`, `exited` is true and every event is ignored.

### 4.2 The state of the Machine, in groups

`Machine` has many fields. Group them and it is easy:

| Group | Fields | Meaning |
|-------|--------|---------|
| Lifecycle | `started, booting, ready, active, stopping, exited, crashed` | where in life the node is |
| Log position | `applied, goalTail, high, buf` | `applied` = next slot to fold. `goalTail` = log end seen by the last tail check. `high` = next slot to allocate for a new write. `buf` = slots read but not yet folded |
| One control I/O | `pending, pendingKind, fillSlot, fillData` | the single in-flight tail/read/hole-fill/checkpoint/trim |
| Commit I/O | `queue, flights, waiters, answers, seq` | client commands waiting, appends in flight, who to answer |
| Timers | `timerGen, holeGen, roleGen, checkpointGen` | generation numbers (4.3) |
| Roles | `lastPullOK, lastOtherAt, lastOwnAt, promoSlot, lowerSeen` | evidence for takeover and failback |
| Checkpoint | `checkpointSlot, checkpointAt, checkpointPending, checkpointData, needsCheckpoint, checkpointRetry, trimFloor` | snapshot progress |
| Reads | `readWaiters, tailsStarted, tailRound` | reads waiting for a fresh tail |
| Folded state | `fold *stateupdater.StateUpdater` | the passbook balance |

**Two lanes of I/O.** This is the key to the file layout:

- **`pending` lane** (at most one at a time): tail check, log read, hole-fill append,
  checkpoint get/put, trim. Request IDs come from `nextReq`. The Machine ignores any
  completion whose `Req` and kind do not match `pending`/`pendingKind`. A stale
  completion is simply dropped.
- **`flights` lane** (up to `K` at a time): client-commit appends. Each has its own
  `ReqID` in the `flights` map.

So the log can be read (catch-up) while commits are being appended. They run in parallel.

### 4.3 Timers use generation numbers instead of cancel

There are four timers: `1` catch-up, `2` gap-fill, `3` role, `4` checkpoint. A timer is
armed as `SetTimer{ID, Gen, After}` with a fresh `Gen`. When `TimerFired{ID, Gen}` comes
back, the handler checks `e.Gen == m.timerGen` (or `holeGen`, `roleGen`, `checkpointGen`). An old
timer has an old `Gen` and does nothing. This beats cancelling: there is no race, and the
code is easy to replay in the simulator. (The same trick was in page 01, station 3.)

### 4.4 Boot: from `NodeStarted` to `Ready`

```
NodeStarted
   │ checkpointing on (CheckpointEvery > 0 or CheckpointKey set)?
   ├─ yes: booting=true, ObjectGet "ckpt/latest"
   │       ├─ read failed (not NotFound)  -> checkpointRetry, timer, try again  (never guess: log may be trimmed)
   │       ├─ NotFound                    -> fresh start, go to catch-up
   │       ├─ bad CRC                     -> read again immediately
   │       └─ good                        -> fold.Restore; applied = checkpoint slot; go to catch-up
   └─ no : go to catch-up
catch-up:  LogGetEnd -> goalTail ; LogRead(from applied) ... drain ... until applied >= goalTail
finishBoot: booting=false, ready=true; rank 0 becomes active; arm role timer and checkpoint timer
```

`Ready` is what `server.Node.Start` waits for before it flips the health check to
`SERVING`. While `booting`, `canCommit()` is false, so commits get `ErrUnavailable` and
the broker's client moves to the other address.

### 4.5 Catch-up: reading the log and folding it

This loop runs forever, every `CatchUpEvery` (5 ms in `mdscontrolplane`):

```
timerCatchUp fires (if pending == 0)
  -> emitCheckTail:  LogGetEnd{Req}                          pendingKind = tail
LogGetEndDone{Tail}
  -> goalTail = Tail ; noteTail(round, Tail)                 (wakes fresh reads, 4.12)
  -> progress()
progress():
   if applied < goalTail:
        if a hole is open (buf has later slots but not buf[applied]) -> hole fill (4.6)
        else                                                          -> emitRead: LogRead{From: applied, Limit: 64}
   else if booting and nothing pending -> finishBoot  (Ready)
   else                                                          -> re-arm the catch-up timer
LogReadDone{Entries}
  -> for each entry with Slot >= applied: buf[Slot] = copy of bytes ; observeSlot (raise high)
  -> drain()
```

`drain()` is the only place that moves `applied`:

```go
for {
    data, ok := buf[applied]
    if !ok { return }          // gap: stop. Never skip a slot.
    delete(buf, applied)
    applySlot(applied, data)   // fold (4.9)
    applied++
}
```

**Strictly in slot order, stopping at the first gap.** This one loop is why every replica
reaches the same state.

### 4.6 Holes and hole-fill

A **hole** is a slot that stays empty while later slots are already written. How? A writer
picked slot 41, then crashed before its put. Another writer already wrote slot 42. Now
`applied=41` can never move.

Fix: any node may write a **no-op** entry into slot 41 (`emitHoleFill`, a `LogAppend` with
`KindNoop`). It is also put-if-absent, so:

- If the original writer is just slow and writes first, the no-op loses. The node gets the
  real bytes back (`d.Taken`, use `d.Existing`) and folds those.
- If the no-op wins, the slow writer later gets "taken" and re-appends elsewhere.

`HoleAfter` is a patience delay (250 ms in `mdscontrolplane`). The first time a hole is seen, the
Machine only arms timer 2 (`armHoleFill`). Hole fill happens only if the hole is still
there when the timer fires. Zero `HoleAfter` fills at once. A mutant (`InjectedBugHoleSkip`)
skips the hole without writing a no-op; the simulator must catch it (section 11).

### 4.7 The commit write path

**Front door:** `Offer(ClientOperation)` (`commit.go`). Prod calls it from `server.Node` via
`runloop` inject. The simulator reaches it through `onCPOffer`. In order:

1. `exited` -> nothing.
2. `K <= 0` or `!canCommit()` -> answer `ErrUnavailable` at once. `canCommit` is false while
   booting, crashed, stalled, or (when roles are armed) when not both `ready` and `active`.
   **A standby refuses all writes.**
3. If the entry is a commit: **`fold.KnownCommit`** is asked. Key already folded? Answer
   from memory, no log write (`"replayed key answered from the window"`).
4. Else **`retargetCommit`**: is the same `SegmentKey` already queued or in flight (the
   broker timed out and retried)? Then re-point that waiter's `Req`/`From` to the new
   caller. The old caller is gone; the answer must reach the live one. To make the walk
   deterministic, flight `ReqID`s are collected from the map and **sorted** before use
   (Go map order is random; the simulator would otherwise get different runs per seed).
5. A command that cannot fit in one log entry even alone (more than `MaxEntryBytes`) -> `ErrTooLarge`,
   never queued. The server maps it to gRPC `ResourceExhausted` with `outcome = NOT_APPLIED`.
6. If `queue` already has `QueueMax` (4096) items -> `ErrUnavailable`.
7. Else append to `queue` and call `flushCommits()`.

**Making a group.** `flushCommits` keeps up to `K` appends in flight (4 in `mdscontrolplane`).
`emitCommitGroup` takes up to `MaxCmdsPerSlot` (256) queued commands, **and stops earlier when
the entry would pass `MaxEntryBytes`** (default 350 KiB, under DynamoDB's 400 KiB item limit).
It builds **one** log entry:

```
id    = "<writer>/<seq>"                    seq++ each time
inner = 0xC5 | uvarint len(id) | id | uvarint n | n × ( uvarint len(enc) | enc )     <- encodeGroup
data  = 0xC7 | uvarint rank | uvarint len(writer) | writer | inner                    <- wrap (envelope)
slot  = alloc()  =  max(high, applied) , then high = slot + 1
emits LogAppend{Req, Slot, Data}
```

- `0xC5` marks a **group**: many client commands in one slot. This is what makes 100
  brokers' commits cost one DynamoDB `PutItem`, not 100.
- `0xC7` marks the **envelope**: advisory rank and writer name. It is how a node
  recognises "who wrote this slot" (4.10) without changing the fold's own codec.
- `waiters[id] = cmds` remembers whom to answer.

**What can the append come back as?** (`onCommitAppendDone`.) This is the table to learn:

| `LogAppendDone` | Meaning | Machine does |
|-----------------|---------|--------------|
| `Err=OK, Outcome=Applied, Taken=false` | we won the slot | `buf[slot] = our bytes`, `drain()` |
| `Taken=true`, `Existing` has **our** group id | we already wrote it (our own earlier retry landed) | `buf[slot] = Existing`, `drain()` |
| `Taken=true`, `Existing` has **another** id | we lost the slot | `buf[slot] = Existing` (their bytes!), `drain()`, then `retryFlight`: same group, **new slot** |
| `Err != OK` or `Outcome != Applied` (timeout, 5xx; `MaybeApplied` or `NotApplied`) | unknown | `retrySameSlot`: the same bytes to the **same slot** |
| trimmed slot (see 4.11 and section 12) | slot was deleted under us | `hitTrimmed` |

Why retry the **same** slot on an unknown result? Because the put is put-if-absent. If the
first try had landed, the retry sees "taken" with our own id and knows it won. If it had
not landed, the retry wins. Moving to a *new* slot would risk the first one landing too,
and the same group would be applied twice. (A comment in the code records that an older
version moved on and left a permanent hole that wedged all commits.)

"Is it mine?" is answered by `groupIDMatches(data, id)`: decode the group header and compare
the id string. That is why the id is inside the entry.

### 4.8 Answering the client: `deliver`

The answer to a commit is produced **only when its slot is folded**, never when the append
succeeds. In `applySlot`:

```go
rank, writer, payload := unwrap(data)
observeWriter(slot, rank, writer)                      // role evidence
if id, cmds, ok := decodeGroup(payload); ok {
    for each cmd: dones[i] = fold.Apply(slot, cmd)     // THE OFFSETS ARE DECIDED HERE
    deliver(id, dones)                                 // match dones[i] to waiters[id][i]
} else {
    d := fold.ApplyBytes(slot, payload)                // single entry (hole-fill, heartbeat, old format)
}
afterApply(slot)                                       // maybe Trim the idempotency window
```

`deliver` moves each `done` into `m.answers` as `ReplyToClient{Req, From, Done}`. A node
that did not write this slot (a standby folding someone else's group) finds no `waiters[id]`
and just folds silently. Same code path, same state, no answer needed.

This answers a question that page 01 left half open: **why offsets are assigned at apply,
not at append.** Appending can retry, lose, or land twice at different slots. Applying is
the one step that is identical on every replica.

### 4.9 Roles: who writes (all advisory)

Roles are on when `Heartbeat > 0` or `TakeoverAfter > 0` (`rolesArmed`). Otherwise every
node can commit (`isAppender` returns true) and no role timer exists.

| Piece | Rule |
|-------|------|
| Start | At `finishBoot`, **rank 0** becomes `active`. Higher ranks are **standby**. |
| Standby | Tails the log, folds, serves reads. Refuses writes with `ErrUnavailable`. |
| Heartbeat | An active node idle for `Heartbeat` (no queue, flights or pending) appends a no-op. This proves liveness to standbys. |
| Takeover | A standby promotes itself when (a) it saw no slot from *another writer* for `TakeoverAfter` **and** (b) its own log reads succeeded within that time. Rule (b) stops a standby from taking over just because *it* is cut off. |
| Failback | An active node steps down when a *lower-rank* writer appends at or after `promoSlot` (the slot where this node became active). `lowerSeen` is set by `observeWriter`. |
| Fence | None of the above is a lock. If two nodes are active at once, they race for slots, one of them loses each time, and the loser retries. This costs retries, not correctness. |

`observeWriter` sets `lastOwnAt` when the slot's writer name is ours, otherwise
`lastOtherAt`. That is where the evidence comes from. A node must therefore have a unique
`Writer` name; `mdscontrolplane` uses the hostname (`-node-name` overrides).

### 4.10 Checkpoints and log trim (`checkpoint.go`)

Without these, a restart would replay the whole log and the log would grow forever.

**Publish** (`maybeCheckpoint`, on the checkpoint timer, **appender only**). It is skipped when:
a control I/O is pending; this node is not the appender or not ready; `applied == 0`;
**`applied < goalTail`** (a node behind the tail must not write: its snapshot could replace
a newer one, and the log below the newer one may already be trimmed, so a restart could
never recover); fewer than `CheckpointMinSlots` slots since the last one.
Otherwise: `fold.Snapshot(slot = applied)` -> `ObjectPut{Key: "ckpt/latest"}`.

**Then trim** (`onObjPutDone` -> `emitTrim`). Only after the object is safely stored:
`through = checkpointSlot - KeepSlots - 1`, emit `LogDeleteOld{Through}`. On success,
`trimFloor = through + 1`. In DynamoDB trim does not delete; it overwrites each slot with a
one-byte **tombstone** `0xFE` (so a late put-if-absent cannot sneak into the trimmed area).

**Restore** is section 4.4. The checkpoint slot is "first slot not covered", so after
restore `applied = slot`, and catch-up continues from there.

### 4.11 Trouble handlers

| Situation | Handler | Result |
|-----------|---------|--------|
| Trimmed slot hit by an append or fill | `hitTrimmed` | refuse all clients (`ErrUnavailable + MaybeApplied`), step down, `booting`, `needsCheckpoint`, wipe local queues, restore from a checkpoint, rejoin |
| Fold or snapshot fails | `trip(err)` | `exited`, emit `StopForRestart{Cause}`. The loop ends; the process is supposed to be restarted |
| Simulated stall | `maybeReplicaFault` | `stallUntil`: node cannot commit for a while (active node only, so a standby can take over) |
| Simulated crash | `softCrash` | `crashed`, `ready=false`, refuse reads, wipe local state; later `afterCrashRestart` rebuilds a fresh fold, reseeds topics, restores checkpoint |
| Graceful stop | `stop` | `exited`, `ShutdownDone` |

`clearLocal` and `afterCrashRestart` set `fold = stateupdater.NewMem()` again. A crash in
the sim is a real "memory wiped" crash.

### 4.12 Fresh reads (`reads.go`)

The rule: **a Lookup or Describe is answered only after this node has folded the log tail
that was read AFTER the read arrived.**

Why? Suppose a consumer already saw end offset 100 from the active node. It asks a standby
for end offset and the standby (a bit behind) says 90. The consumer sees "offset 100 is out
of range". The fix is to make the standby check the log end first.

How: `queueRead` stores a `readWaiter` with `round = tailsStarted + 1` (the first tail
check that *starts after* now). `noteTail(round, tail)` gives it a goal slot. `serveReads`
(called at the end of every `Step`) answers waiters with `applied >= goal`. Not ready or
crashed? `refuseReads` answers `ErrUnavailable` so the caller tries the other node.

**Important reality check:** this machinery is reached only by `onCPOffer`, the *simulator*
front door. In production, `server.Node.Lookup` reads the fold directly (section 12.3).

### 4.13 The envelope, the group, and what the log really holds

For a commit slot the bytes in DynamoDB are:

```
C7 [rank] [writer-len] [writer...]          envelope
   C5 [id-len] [id...] [n]                  group header
      [len1] [entry1 bytes...]              each entry = stateupdater.Encode (kind, id, key table, batches)
      [len2] [entry2 bytes...]
```

A *no-op* (hole-fill, heartbeat) is `C7 ... ` + a single `stateupdater.Encode` with kind 0
and no group marker. `applySlot` handles both: group path or single-entry path.

---

## 5. Chapter C - `executor`: doing the I/O

The Machine emits an action; `executor.Executor.Execute` (the `runloop.Executor`) runs it.

```go
func (e *Executor) Execute(a core.IOCall) {
    go func() { e.deliver(e.run(a)) }()     // one goroutine per action. never blocks the loop.
}
```

`run` switches on the action type and returns **exactly one** `IOResult` (`LogAppendDone`,
`LogReadDone`, `LogGetEndDone`, `LogDeleteOldDone`, `ObjectPutDone`, ...). `deliver` sends it
to the inbox's **control** lane, unless the inbox is closed.

Each call gets a timeout: `LogTimeout` 10 s, `ObjTimeout` 30 s.

### 5.1 The `Log` interface

```go
Append(ctx, slot, data) (existing []byte, taken bool, err error)
Read(ctx, from, limit)  ([]LogEntry, error)
CheckTail(ctx)          (LogPosition, error)       // first slot that is NOT written yet
TrimThrough(ctx, through) error
```

Two implementations:

- **`MemoryLog`**: a slice of `{empty | entry | deleted}` records under a mutex. For
  `-store mem` and tests. `Append` onto a deleted slot returns `(nil, taken=true)`.
- **`DynamoLog`** (`dynlog.go`): the production log.

### 5.2 `DynamoLog` row layout and operations

One DynamoDB partition holds the whole log: `pk = "LOG"`, `sk = %020d` (slot as 20 digits,
zero padded, so **text order = numeric order**), `d` = the entry bytes (binary),
optional `ttl`.

| Operation | DynamoDB call | Notes |
|-----------|--------------|-------|
| `Append` | `PutItem` with `attribute_not_exists(pk)` and `ReturnValuesOnConditionCheckFailure=ALL_OLD` | On "condition failed" the old item arrives **in the error**: no extra read. If the item is missing from the error, it falls back to one `Read` |
| `Read` | `Query` `sk BETWEEN from AND max`, **`ConsistentRead`**, forward | Skips tombstones. Pages until `limit` |
| `CheckTail` | `Query` same range, **backwards**, `Limit 1`, consistent | `last slot + 1` |
| `TrimThrough` | `Query` (pk, sk only) in pages of 25, then `BatchWriteItem` | Overwrites rows with `d = 0xFE` and a `ttl` stub (30 days) |

The consistent read matters: an eventually consistent read could miss a slot that was
already won, which would look like a hole.

### 5.3 Turning errors into `(ErrCode, Outcome)` (`classify.go`)

```
no error                        -> OK, Applied
timeout / context cancelled     -> ErrTimeout, MaybeApplied  (if the call changes data) else NotApplied
not found / invalid slot        -> ErrNotFound / ErrInvalid, NotApplied
anything else                   -> ErrIO, MaybeApplied  (if it changes data)
```

"Changes data" = `LogAppend`, `LogDeleteOld`, `ObjectPut`, `ObjectDelete`. Reads are always
`NotApplied` on failure (a failed read cannot have changed anything).

---

## 6. Chapter D - `runloop`: the one goroutine

`runloop.Run` is the production twin of the simulator kernel. Nothing but this loop ever
calls `Step`.

```
m := factory(env, self)
step(NodeStarted)                                  // first event
loop:
    if shutdown asked: deliver StopRequest into the control lane, start a kill timer (StopGrace)
    drainInject()                                  // run work handed in by gRPC handlers
    body := ControlFirstPicker.Next(control, data) // control lane always wins
    if none: waitIdle (wake on inbox, inject, shutdown, kill, ctx)
    step(body)
    drainInject()
```

- **Inbox** (`inbox.go`): two queues. *Control* (timers, all I/O completions) is unbounded
  and never drops, so completions cannot be lost. *Data* is bounded. The CP uses mostly
  control events.
- **`step`** stamps the event with `Mono()` and `Wall()` clock readings, calls `Step`, and
  `dispatch`es the actions in order: `IOCall` -> `Execute`; `RecordFact` -> facts sink;
  `SetTimer` -> `time.AfterFunc` that delivers `TimerFired`; `CancelTimer`; and the two
  endings, `StopForRestart` (returns `ErrStoppedForRestart`) and `ShutdownDone` (returns nil).
- **Inject** (`InjectFunc`): a closure from outside that must run **on the loop goroutine**.
  This is how gRPC handlers touch the Machine without a lock: they send a function into a
  channel and the loop runs it between events.
- **`AfterStep`**: runs after every dispatch. `server.Node` uses it to forward answers.

Single goroutine means the Machine needs no mutex. Concurrency is solved by one funnel.

---

## 7. Chapter E - `server`: from gRPC to `Offer` and back

`server.Node` owns the loop and implements `ControlPlaneServer`.

### 7.1 Start

`Node.Start(ctx)` builds an `Executor`, starts `runloop.Run` in a goroutine, then **polls
by injecting a "ready?" closure** until the Machine reports `Ready()`. Then it flips gRPC
health to `SERVING`. A 2-minute deadline and the caller's `ctx` bound the wait. `mdscontrolplane`
passes `-start-timeout` (10 min) as `ctx`.

### 7.2 The waiter dance for a write (`offer`)

This is the part to understand well, because gRPC handlers are many and concurrent and the
Machine is single-threaded.

```
handler goroutine                                       loop goroutine
-----------------                                       --------------
req := allocReq()                      (fresh ReqID)
ch  := make(chan Done, 1)
waiters[req] = ch                      (under mutex)
onLoop(fn): send closure to inject --------------------> runs fn: m.Offer(ClientOperation{req, cmd})
                                                          then n.flushAnswers(m)
   <-- done ---------------------------------------------
select ch? (Offer may have answered at once)
loop:  wait on ch  |  ctx.Done  |  inject a "flushAnswers" nudge
                                                         ... later: slot is folded, deliver() puts
                                                         ReplyToClient into m.answers ...
                                                         AfterStep -> flushAnswers:
                                                           for each answer: ch := waiters[a.Req]; ch <- a.Done
return d
```

If `ctx` ends first, `offer` returns `ErrTimeout + MaybeApplied` **and the write may still
land later**. The handler turns this into a gRPC error whose `ErrorDetail.outcome` says
`MAYBE_APPLIED`. The broker will retry with the same key (section 8).

Reads (`read`) do the same trick but answer right inside the loop closure: if the Machine
is not `Ready` -> `ErrUnavailable`, else call `m.Fold().Lookup/Describe` and return.

### 7.3 Error mapping (`status.go`)

`toRPCError(code, outcome)` picks a gRPC status code and attaches an `ErrorDetail`
`{code, outcome}`:

| `core.ErrCode` | gRPC code |
|----------------|-----------|
| `ErrUnavailable` | `Unavailable` |
| `ErrInvalid` | `InvalidArgument` |
| `ErrNotFound` | `NotFound` |
| `ErrConflict` | `AlreadyExists` |
| `ErrTimeout` | `DeadlineExceeded` |
| anything else | `Internal` |

The detail is what keeps the *outcome* alive across the wire. Without it, "timed out"
would not tell the broker whether the write might have happened.

---

## 8. Chapter F - the wire and the broker-side client

- **`api/v1/controlplane.proto`** is the contract. Enums mirror `core.ErrCode` and
  `core.Outcome`; number 5 (the old `ErrFenced`) is retired and left as a gap.
- **`apiconvert`** has the two-way converters (`CommitSegmentToCore`,
  `CommitSegmentFromCore`, ...). Unknown wire error values become `ErrIO`. Keeping this a
  separate package means the independent oracle (veritas) can share the schema types without
  sharing apply code (decision 21).
- **`controlplaneclient.Client`** (`client.go`) is what the broker uses. It takes a
  comma-separated address list.

**Failover rule** (`failover`, `shouldTryOtherNode`). Start at the last address that worked.
Try the next address when: the call never left (`!Sent`, for example dial failure) or
gRPC `Unavailable` or the business code `ErrUnavailable`. **Do not** try another node
after a timeout: the request may have been applied. The caller's retry loop (with the same
`SegmentKey`) decides that.

**Classifier** (`classify.go`): turns a failed call into `(ErrCode, Outcome)` for the
broker. Rules in one line: *a write whose request might have reached the server and whose
reply is lost is `MaybeApplied`; only failures that prove nothing was sent (dial error,
`Unavailable`) are `NotApplied`; reads are always `NotApplied`.*

The two halves meet: the server attaches `MAYBE_APPLIED` when it does not know; the
client believes it; the broker (page 01, station 9) retries the same key with backoff until
`CommitDeadline`.

---

## 9. Chapter G - wiring it up: `cmd/mdscontrolplane`

`mdscontrolplane` is small because everything above is a library.

```
flags -> statemachine.Config {
            CatchUpEvery: 5ms,   K: 4 (-log-k),   Rank, Heartbeat (1s), TakeoverAfter (5s),
            HoleAfter (250ms),   Writer: hostname (-node-name),
            CheckpointEvery (5m),      CheckpointMinSlots (1000),   KeepSlots: window-slots/8,
            WindowSlots: window-slots, TrimStride: 1024 }          (see machineConfig in main.go)
-store mem        -> MemoryLog + MemoryObjects            (fold in memory)
-store dynamolog  -> DynamoLog (+ optional S3 checkpoints via -checkpoint-bucket)
                     + Pebble state in -pebble-dir        (Config.OpenStore, rebuilt at every start)
server.New(...).Start(ctx)  -> gRPC server + health + Prometheus (/metrics) + run manifest
```

Without `-checkpoint-bucket`, `mdscontrolplane` turns checkpoints and trim **off** (a `log_no_checkpoint`
warning is logged): a restart then replays the entire log.

The metrics package `controlplanemetrics` is outside the determinism closure by design
(wall clock and atomics). It is nil-safe: stores and handlers run with it missing.

---

## 10. Four traces to check against the code

### Trace A - the happy path on the active node

Setup: one active node (`rank 0`), `K=4`, log empty, topic `orders` has 2 partitions
(already created by `CreateTopic` at slot 0).

1. gRPC `CommitSegment{key=seg/9f3c, [A: orders-0 × 3 records, B: orders-1 × 2 records]}`
   arrives. `Node.CommitSegment` converts it and calls `offer`.
2. `offer` registers `waiters[req=7]`, injects `m.Offer`. `Offer`: ready, active -> not in
   fold (`KnownCommit` false) -> not queued -> queue has room -> `queue=[cmd]` ->
   `flushCommits` -> `emitCommitGroup`:
   `id="host/3"`, `slot = max(high=1, applied=1) = 1`, `high=2`,
   returns `LogAppend{Req: 12, Slot: 1, Data: C7…C5…}`.
3. `runloop.dispatch` -> `Executor.Execute` -> goroutine -> `DynamoLog.Append` -> `PutItem`
   with `attribute_not_exists` succeeds -> `LogAppendDone{Req 12, Err OK, Applied, Taken false}`
   lands in the control lane.
4. `Step(LogAppendDone)` -> `onCommitAppendDone`: flight 12 found, we won:
   `buf[1] = our bytes`, `drain()` -> `applySlot(1, data)` -> `decodeGroup` -> `fold.Apply(1, cmd)`
   -> `commitTransaction`: `E orders/0` absent -> A base 0, end 3; B base 0, end 2;
   writes `E, R, K, S` -> `deliver("host/3", [done])` -> `answers=[{Req 7, Done}]`.
5. `AfterStep` -> `flushAnswers` -> `ch <- done` -> `offer` returns -> response
   `Assigned = [{orders-0, 0, 3}, {orders-1, 0, 2}]`.

Same key tables as page 01, section 3, so you can compare.

### Trace B - two writers fight for one slot

Nodes N0 (active) and N1 (just promoted after N0 went quiet). Both pick slot 50.

1. N0 `LogAppend{Slot 50, id "n0/9"}`. N1 `LogAppend{Slot 50, id "n1/4"}`.
2. DynamoDB lets one win. Say N0. N1's `PutItem` fails the condition; the error carries N0's item.
   `executor` returns `LogAppendDone{Taken: true, Existing: N0's bytes}`.
3. N1: `onCommitAppendDone` -> `lost = !groupIDMatches(existing, "n1/4")` = true.
   `buf[50] = Existing` (N0's bytes), `drain()` folds N0's group (N1 has no waiters for `n0/9`, so
   no reply, just state). Then `retryFlight`: same group `n1/4`, `alloc()` -> slot 51. A new `LogAppend`.
4. Both nodes folded slot 50 with the same bytes -> same offsets. N1's commits get offsets
   *after* N0's. The simulator tracks this branch with `Reachable("logcp-sim: lost slot retried at a later slot")`:
it wants every campaign to hit it at least once.

Run: `go test ./internal/controlplane/statemachine -run TestLostAppendRetriesSameEntryLaterSlot -v`.

### Trace C - the reply was lost

1. The broker's call times out after the CP already folded slot 51. The broker retries with
   the same `segment_key`, a new gRPC request.
2. Case (i) *the key is already folded*: `Offer` -> `KnownCommit` true -> `replay` -> same
   ranges, **no log write**. Test: `TestOfferKnownCommitSkipsAppend`.
3. Case (ii) *the key is still queued or in flight*: `retargetCommit` points the old waiter
   at the new `Req`. One log entry, one answer to the live caller. Test:
   `TestOfferRetargetsInFlightCommit`.
4. Case (iii) *the key was trimmed out of the window*: nothing remembers it; it is committed
   again as a new segment (records duplicated, allowed by at-least-once).

Four lines of defence together: broker key reuse, `KnownCommit`, `retargetCommit`, and the
`K` record in `commitTransaction` (which also catches the case where the retry reaches a
*different* node).

### Trace D - the active node dies and restarts

1. N0 dies. N1 stops seeing slots from N0 (`lastOtherAt` ages). After `TakeoverAfter` with
   healthy reads of its own, `takeoverDue` is true -> `setActive(true)`, `promoSlot = max(high, applied)`.
2. Brokers' client sees `Unavailable` from N0's address, moves to N1. N1 accepts commits.
   Commits in flight on N0 either landed (the keys replay on N1) or never will (the retry
   commits them).
3. N0 comes back. `NodeStarted` -> `ObjectGet ckpt/latest` -> good CRC -> `fold.Restore` ->
   `applied = checkpointSlot`. Catch-up reads from there to the tail. It is `Ready` and, because it is
   rank 0, `setActive(true)`. Then it appends. N1 sees a writer with a *lower rank* at
   `slot >= promoSlot` -> `lowerSeen` -> `failbackDue` -> `setActive(false)`. N1 is a
   standby again.
4. For a short time both are active. They race for slots; the loser re-appends. Nothing breaks.

---

## 11. How the simulator attacks all of this

The Machine was made pure so the simulator can run **two CP replicas plus a faulty fake
DynamoDB** on a seed and check the outcome. Facts to know (`docs/sim-log-engine.md`):

- Scenario `kafka-log` (`go run ./cmd/mdssimulator -scenario kafka-log -seeds 300`). Brokers emit
  `CPCall`; a bridge turns that into `ControlPlaneDeliver` on `cp-0` / `cp-1`.
- **Faults:** the kernel can kill or stall a CP process; the Machine has its own `StallProb`
  and `CrashProb` (coin flips on each role tick, only for the active node); the bridge can
  return 503, time out, or lose a response; the fake log can fail a put at random.
- **Mutants.** `InjectedBug*` constants in `config.go` are deliberate mistakes that the
  simulator **must** catch (if they do not fail, the checks are too weak):

| Mutant | What is broken | Caught by |
|--------|----------------|-----------|
| `InjectedBugWriterOffsets` | the writer picks the base, skipping one offset per partition | a replica's state no longer equals "replay of the log" |
| `InjectedBugTrimDelete` | trimmed slots are really deleted, not tombstoned | "a Put never wins a slot below the trim floor" |
| `InjectedBugPerReplicaWindow` | each replica forgets keys on its own clock | state divergence |
| `InjectedBugHoleSkip` | skip a hole without a no-op | state divergence |

- **Properties.** `env.Always(...)` is something that must be true every time.
  `env.Reachable(...)` is something that must be true at least once across the campaign (so
  the faults really reach this code). Text like `"logcp-sim: lost slot retried at a later
  slot"` is a Reachable name. These calls are noise for reading; the simulator uses them.
- **Oracle.** Veritas (`speccheck.ControlPlane`) is the independent executable spec. Lockstep
  tests (`statemachine/lockstep_test.go`, `stateupdater/stateupdater_test.go`) feed the same
  random commands to both and compare every answer.
- **`CPTruth`** replays the durable log from slot 0 with `ReplayPosition` (`replay.go`) into
  a fresh fold, ignoring queues and writer-chosen bases. At the end, every live replica's
  digest must equal that of the log replayed cold.
- **Trace hash pins.** The last commits (`c1725af`, `39ebcdd`) pin the trace hash of five
  seeds each for the broker and the control plane. Any change to behaviour changes the hash
  and must be explained.

---

## 12. Reality check - things I found while reading

These are *observations from reading code*, not a bug list from running anything. I
re-checked each with `grep`. Please verify them yourself before acting.

### Status at a glance

| # | Finding | Status |
|--:|---------|--------|
| 1 | Idempotency window never trims in production | **Fixed**, `56417bd` |
| 2 | Production reads are not "fresh" | Open |
| 3 | Trimmed-slot signal differs between simulator and production | Open |
| 4 | Pebble was not wired in | **Fixed**, `56417bd` |
| 5 | No backoff on append retry | Open |
| 6 | Corrupt checkpoint can be re-read forever | Open |
| 7 | `GetStatus.log_tail` is not the tail | Open |
| 8 | Stale names in comments and strings | **Fixed**, `da09578` |
| 9 | Misplaced coverage marker in `afterApply` | Open |
| 10 | Dead HTTP-era metrics code; gRPC calls are not measured | Open (decision needed) |

Also done in `56417bd`: removed the flags nothing read (`-slot-rate`, `-final-checkpoint`,
`-trim-delete`) and the unused `ParseErrCode`.

### Summary of all findings

**Behaviour findings (1 to 4)**

1. **Idempotency window never trims in production. FIXED (commit `56417bd`).**
   `mdscontrolplane` never set `WindowSlots`, so `afterApply` never trimmed the `K` and `S` records.
   Memory grew with every committed segment, and the "window" of decision 48 was effectively forever.
   `mdscontrolplane` now sets `WindowSlots` from `-window-slots` and `TrimStride` to 1024; a test pins it.
2. **Production reads are not "fresh".**
   The fresh-tail rule (commit `226ea69`) lives in `queueRead` / `serveReads`, reached only from the simulator path.
   `server.Node.Lookup` and `DescribeTopics` read the fold directly after checking `Ready()`.
   A lagging standby can serve a stale end offset; the active node is safe.
3. **Trimmed-slot signal differs between simulator and production.**
   `isDeleted` needs `ErrConflict` + `Taken` + empty `Existing`; only the simulator's `fakelogstore` returns that.
   The real executor returns `OK` + `Taken` + `Existing=[0xFE]` (Dynamo) or `nil` (MemoryLog), so `hitTrimmed` never fires from an append.
   A hole-fill onto a tombstone ends in `ErrCorrupt`, then `trip` and a restart: safe, but not the path the simulator tests.
4. **Pebble was not wired in. FIXED (commit `56417bd`).**
   The gRPC cutover (`1c3c11b`) had dropped Pebble: the Machine built `stateupdater.NewMem()` and `-pebble-dir` was unused.
   New `Config.OpenStore` gives the fold its store; `mdscontrolplane -store dynamolog` opens an empty Pebble store at start and after every restore.
   The simulator keeps the in-memory store (`OpenStore` is nil), so its trace hashes did not change.

**Smaller points (5 to 10)**

5. **No backoff on append retry.**
   `retrySameSlot` re-emits `LogAppend` at once on any non-`Applied` result.
   If DynamoDB fails fast (for example connection refused), this is a tight loop.
   Only the executor's return speed limits it.
6. **Corrupt checkpoint can be re-read forever.**
   On a bad CRC, `onObjGetDone` calls `emitCheckpointGet()` again immediately.
   That is fine for a bit flipped in transit.
   For a permanently damaged stored object it repeats with no delay.
7. **`GetStatus.log_tail` is not the tail.**
   It is set to `m.Applied()`, the same value as `applied_slot`.
   Operators cannot see replication lag from it.
   It should use `goalTail` or a real tail check.
8. **Stale names in comments and strings. FINISHED (commit `da09578`).**
   Package `doc.go` said `controlplane/run`, `controlplane/machine`, `cpclient`, `applier`, `exec`.
   Error strings said `"applier:"` and `"machine:"`; `-slot-rate` was "unused by machine".
   All now use the new names; no logic, file name or identifier changed.
9. **Misplaced coverage marker in `afterApply`.**
   `afterApply` starts with `Reachable("replayed key answered from the window")`.
   The same text is already in `Offer`'s `KnownCommit` branch, where it belongs.
   Harmless, but it makes that coverage point easier to satisfy than intended.

10. **Dead HTTP-era metrics code; gRPC calls are not measured.**
    `controlplanemetrics` still holds `Middleware`, `statusWriter`, `countBody` and the per-request `Call` logging, which wrap an HTTP handler.
    Since the gRPC cutover (`1c3c11b`) nothing calls them (found with `deadcode`, which lists code no binary can reach).
    So `mdscontrolplane_handler_seconds`, `mdscontrolplane_request_body_bytes`, the in-flight gauge and the "cp call" log lines are never recorded.
    The better fix is a gRPC interceptor, not deleting the code. Decision needed.

Details for each follow.

### 12.1 Pebble was not plugged in (FIXED in `56417bd`)

Page 01, section 4, saw this too. Before the fix, `statemachine.New` always built
`stateupdater.NewMem()`, and the `-pebble-dir` help text said "unused (machine fold is
in-memory); kept for flag compatibility". History: `git log -S` shows the cutover commit
`1c3c11b` (2026-10-27) removed the old Pebble code; before it, `mdscontrolplane -store dynamolog`
opened Pebble.

How it works now:
- `statemachine.Config.OpenStore func() (stateupdater.KeyValueStore, error)`. Nil means
  memory (simulator and tests). The Machine calls it in `New` and again in
  `afterCrashRestart`, after it has closed the old store. If the first open fails,
  `NodeStarted` answers `StopForRestart`; if a reopen fails, the node trips the same way.
- `cmd/mdscontrolplane/state.go` (`pebbleState`) wipes the directory (only if it holds a Pebble
  database or nothing: any other content is refused) and opens an empty Pebble store each
  time. Pebble state is never trusted after a restart; it is rebuilt from the checkpoint
  and the log. With no `-pebble-dir`, a temporary directory is made and removed at shutdown.
- The `statemachine` import-boundary test still passes: the Machine never imports Pebble.

### 12.2 `WindowSlots` was never set in `mdscontrolplane` (FIXED in `56417bd`)

`afterApply` trims `K`/`S` records only if `cfg.WindowSlots > 0`. The simulator sets it
(`kafka/control_plane.go`), but `cmd/mdscontrolplane/main.go` built the `Config` without it; the flag
`-window-slots` only fed `KeepSlots = windowSlots/8` (log trim). So in production the fold
kept every `K` and `S` record forever (a restart did not help, because the checkpoint
contains them). The simulator could not see this, because it runs with the window on.

Now `machineConfig(f)` in `cmd/mdscontrolplane/main.go` sets `WindowSlots: -window-slots` (131072),
`TrimStride: 1024` and `KeepSlots: -window-slots/8`. The window and the stride are cluster
constants: every node must use the same values, because the cut is a function of the slot
number. At the 200 slots/s ceiling, 131072 slots is 655 s, which is at least twice the
commit deadline plus the checkpoint period.

### How to find dead code yourself

`golangci-lint` only sees one package at a time, so it cannot tell that an exported function
is never called. `deadcode` can: it starts from the real `main` functions and lists what no
binary reaches. It lives in the `golang.org/x/tools` module, which the repo already uses.
Install it outside the repo so `go.mod` is not changed:
`GOBIN=/some/dir GOFLAGS=-mod=mod go install golang.org/x/tools/cmd/deadcode@v0.52.0`, then
`deadcode ./cmd/...`. Run it with `-test` to count tests as roots too (then almost nothing
is dead, so it is less useful). Code that only tests use is not always garbage (for example
`stateupdater.Store` is a test helper), so read each hit before you delete.

### 12.3 Production reads are not "fresh"

Section 4.12 describes the fresh-tail rule (commit `226ea69`). It lives in `queueRead` /
`serveReads` and is reached only from `onCPOffer` (simulator). `server.Node.read` calls
`m.Fold().Lookup(...)` directly after checking `Ready()`. On the **active** node this is
fine: it only acks a commit after folding it, so it never serves a stale answer for its own
commits. On a **standby** (after failover, or a client that tries the second address) the
fold may lag the log. The broker always starts at the last good address, so the exposure is
small, but the sim and prod behave differently here.

### 12.4 The trimmed-slot signal differs between sim and prod

`isDeleted(d)` is `Err == ErrConflict && Taken && len(Existing) == 0`. Only the simulator's
`fakelogstore` returns that shape. The real executor returns `Err=OK, Taken=true` with
`Existing = [0xFE]` (Dynamo) or `nil` (MemoryLog). So in production `hitTrimmed` does not
fire from an append. A hole-fill onto a tombstone would put `0xFE` into `buf`, and
`ApplyBytes` would fail with `ErrCorrupt`, so `drain` returns the error and `trip` stops the
process (restart then restores from the checkpoint). A commit append onto a tombstone is
seen as "lost" and retried at a later slot. Both end safely, but through a different path
than the one the simulator tests.

### 12.5 Smaller points

- **Append retry has no backoff.** `retrySameSlot` re-emits the `LogAppend` immediately. If
  DynamoDB fails fast (connection refused), this is a tight loop, bounded only by how fast
  the executor returns errors.
- **Corrupt checkpoint, permanent.** On a bad CRC, `onObjGetDone` calls `emitCheckpointGet()`
  again at once. Fine for a flipped bit in transit (the comment says so); for a damaged
  stored object it would repeat without delay.
- **`GetStatus.log_tail`** is set to `m.Applied()`, the same value as `applied_slot`. It is
  not the real tail.
- **Stale names (FINISHED, commit `da09578`)** in comments and strings: package `doc.go` mentioned `controlplane/run`,
  `controlplane/machine`, `cpclient`, `applier`, `exec`; error texts say `"applier:"` and
  `"machine:"`; `-slot-rate` is "unused by machine".
- **`afterApply`** starts with a `Reachable("replayed key answered from the window")`
  that looks like it belongs in `Offer`'s `KnownCommit` branch (where the same text also
  appears). Harmless, but it makes that coverage point easier to satisfy than intended.

---

## 13. How to read the code (suggested order, ~2-3 hours)

1. `internal/core/controlplane.go` (the contract) and `core/machine.go`.
2. `stateupdater/keys.go`, then `stateupdater.go`: `commitTransaction`, `assign`, `replay`,
   `lookupTransaction`. Then `entry.go`.
3. `stateupdater/query.go` (Trim), `snapshot.go` (format comment first).
4. `statemachine/config.go` (the knobs), then `machine.go`: `Step`, `onStarted`,
   `onReadDone`, `drain`, `progress`.
5. `statemachine/commit.go`: `Offer`, `emitCommitGroup`, `onCommitAppendDone`, `applySlot`,
   `deliver`. Then `envelope.go`.
6. `statemachine/role.go` and `checkpoint.go`. Then `reads.go`.
7. `executor/dynlog.go`, `classify.go`; `runloop/run.go`; `server/node.go` (`offer`).
8. `controlplaneclient/client.go` last.

**Noise you can skip on a first pass:** `InjectedBug*` branches, `env.Always/Reachable/
Unreachable` calls, `StateHash` bodies, `//detcheck:ok` comments (they mark map walks that
sort first), `controlplanemetrics`, `fakedynamodb`.

**Run these (all pass today):**

```
go test ./internal/controlplane/statemachine -run 'TestKConcurrentLogAppend|TestLostAppendRetriesSameEntryLaterSlot|TestOfferKnownCommitSkipsAppend|TestOfferRetargetsInFlightCommit' -v
go test ./internal/controlplane/statemachine -run 'TestHoleFillEmitsNoopAppend|TestTakeoverAfterSilence|TestFailbackOnLowerRank' -v
go test ./internal/controlplane/statemachine -run 'TestCheckpointPublishAndTrim|TestRestoreReadsAgainOnBadCRC' -v
go test ./internal/controlplane/statemachine -run 'TestReadWaitsForFreshTail' -v
go test ./internal/controlplane/server -run 'TestCommitLookupRoundTrip|TestStandbyCommitUnavailable' -v
go test ./internal/controlplane/statemachine -run 'TestImportBoundary' -v
go test ./internal/controlplane/statemachine -run 'TestOpenStore' -v      # the Pebble hook (Config.OpenStore)
go test ./cmd/mdscontrolplane -run 'TestPebbleState|TestMachineConfig' -v          # Pebble directory and window settings
```

---

## 14. Check yourself

Answer without looking, then verify in the code.

1. Two CP nodes fold the same log. Why do they give the same offsets without talking?
2. The Machine asks DynamoDB to write slot 41 and the answer is a timeout. Why does it retry
   **slot 41 again** and not slot 42?
3. What tells a node that a slot it lost belongs to another writer, and what does it do
   with the winner's bytes?
4. `drain()` stops at the first missing slot. What stops a missing slot from blocking the
   node for ever?
5. Where is the code that turns "slot folded" into "answer the waiting RPC"? Why not answer
   when the append succeeds?
6. What does `Trim` delete and what does it never delete? What happens to a key after Trim?
7. Why must a checkpoint be written only by a node whose `applied >= goalTail`?
8. What is the difference between the `pending` lane and the `flights` lane?
9. What makes a standby take over, and why must its own log reads be healthy too?
10. List the four defences that make a retried commit safe.
11. Why does the Machine collect map keys and sort them before walking `flights`?
12. A gRPC call times out inside `Node.offer`. What does the broker learn, and what is the
    state of the write?
13. (Reality check) What would happen in production to the `K` records over a month of
    traffic if I read `cmd/mdscontrolplane/main.go` right?

Answers: (1) Same entries, same order, same deterministic code. (2) The put is
put-if-absent: a retry on the same slot either lands or shows our own id; a new slot could
double-apply. (3) The group id inside the entry (`groupIDMatches`); the winner's bytes go
into `buf[slot]` and are folded, then the group is re-appended at a later slot
(`retryFlight`). (4) Hole fill: after `HoleAfter` any node puts a no-op into the empty slot.
(5) `applySlot` -> `deliver` -> `answers` -> `AfterStep` -> `flushAnswers`. Append can retry,
lose or land twice; only apply is the same on every replica. (6) It drops `K` and `S`
records below the slot; never `E`, `R`, `T`; the key then commits again as a new segment.
(7) A behind node could overwrite a newer snapshot, and the log below the newer snapshot may
be trimmed, so recovery would be impossible. (8) `pending` is one at a time for catch-up,
hole fill, checkpoint and trim; `flights` is up to `K` commit appends in parallel.
(9) Silence from other writers for `TakeoverAfter`; healthy own reads prove the silence is
real and not the standby's own network problem. (10) Broker key reuse, `KnownCommit`,
`retargetCommit`, the `K` record in `commitTransaction`. (11) Go map order is random; the
simulator needs the same run for the same seed. (12) `ErrTimeout` with `MAYBE_APPLIED`; the
entry may still be in the log and will be folded; the retry with the same key is safe.
(13) Before the fix they grew without bound, because `WindowSlots` was zero and `afterApply`
never trimmed. Now `mdscontrolplane` sets it, so they are cut every 1024 slots.

---

## 15. Glossary

| Term | Meaning here |
|------|--------------|
| Control plane (CP) | The service that owns offsets, segment locations, topics and idempotency |
| Log / slot | The shared ordered list of entries / one position in it |
| Entry | The bytes in a slot: a no-op, a commit, or a create-topic |
| Group | Many client commands packed in one entry (marker `0xC5`) |
| Envelope | Rank and writer name wrapped around an entry (marker `0xC7`) |
| Fold | The state built by applying entries in slot order (`StateUpdater`) |
| Put-if-absent | Write that succeeds only if the slot is empty: the only consensus rule |
| Appender / active / standby | The advisory writer role / the node that has it / a node that only reads |
| Rank | Preference number. 0 is preferred, used for failback |
| Hole | An empty slot below a written slot; filled with a no-op |
| Tombstone | A trimmed slot, overwritten with the byte `0xFE` |
| Checkpoint | Snapshot of the fold at one slot, stored as `ckpt/latest` |
| Window | How long a `SegmentKey` is remembered, equal to how long its `K` record is kept |
| Run | Packed record (`R` key) of the contiguous batches one commit gave one partition |
| Outcome | `NotApplied`, `MaybeApplied`, `Applied`: did the write happen? |
| Flight | One commit append in progress (up to `K`) |
| Trip | Fail-stop: emit `StopForRestart` because memory cannot be trusted |
| Mutant | A deliberate bug the simulator must catch (`InjectedBug*`) |

## 16. Next pages I'd suggest

- 03 Fetch and `Lookup`: how a consumer read turns `FindSegments` into ranged S3 `GET`s
- 04 The simulator for the CP: `kafka-log`, `fakelogstore`, the bridge, and `CPTruth`
- 05 Pebble in the Machine at real sizes: what restore, snapshot and one `Update` per command
  cost (the Machine does not use `CommitBatch` yet)
- 06 Consumer groups on the CP (`docs/consumer-groups-design.md`): a new entry kind in the log
