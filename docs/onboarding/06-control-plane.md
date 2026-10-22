# Chapter 6: the replica assigns the offset when the replica applies the log

You meet two DynamoDB stores in this chapter. The old store lives in `cp/dynamo`. The log store lives in `cp/logcp` and uses the applier in `cp/applier`. The two stores share the contract in `cp/store.go`, the HTTP layer in `cp/handler.go`, and the run-row format. The two stores share little else.

The old store and the log store solve the same problem in opposite orders. The old store reads the partition end, computes offsets, and defends the computed offsets with a transaction. The log store writes raw commands to a log slot first. The replica then applies the log in slot order, and the applier assigns offsets during that apply step. The log order is the order that matters. A broker uploads a segment object to S3 and asks one store to commit the segment. The store assigns every batch in the segment a contiguous range of offsets in one atomic step, or the store assigns no offsets.

> Old store and log store. Read this first.
>
> | Generation | Packages | Status |
> |---|---|---|
> | The old store | `cp/dynamo` with `Store` and `Sequencer` | The old store stays in the tree for rollback. One `TransactWriteItems` handles each commit. A compare-and-set on the run item provides safety. Decisions 39, 42, and 46 record the old design. |
> | The prototype | `cp/logsm` | The prototype has no wiring into any binary. The seeded model proved design A. The prototype applies the log through `veritas.ControlPlane`. |
> | The log store, design A | `cp/logcp` and `cp/applier`, with `cp/applier/pebblekv` | The log store is the current production store. A DynamoDB log holds raw commands. Every node applies the log in slot order through the Pebble applier. Decisions 48-53 and `docs/log-cp-service.md` record the log design. |

## 1. The `cp/` packages split the old store from the log store

```
cp/
  doc.go          package doc: the old DynamoDB design + Sequencer, in full
  store.go        the cp.Store interface and cp.ErrUnavailable
  wire.go         JSON wire types and core <-> wire conversions
  handler.go      cp.NewHandler: POST /v1/{commit-segment,lookup,describe-topics,create-topic}
  memstore/       cp.Store over veritas.ControlPlane behind a mutex (dev/tests)
  dynamo/         OLD: Store (one transaction per commit) + Sequencer (group commit)
    runs.go         packed run encoding, shared with the log CP's indexer
    fakedb/         in-memory DynamoDB for tests
  logsm/          PROTOTYPE (decision 48): log of commands + Replica + Committer over veritas
  logcp/          NEW: the production log-backed service (this chapter's second half)
    dynlog.go       DynLog: the log as DynamoDB items LOG/<slot>
    committer.go    the commit queue, K flushers, slot allocation and retry
    replica.go      tailing, buffering, hole filling, apply position
    apply.go        applying contiguous slots through the applier
    role.go         advisory active/standby rules
    fresh.go        shared tail checks for fresh reads
    checkpoint.go   snapshots to S3, the CKPT pointer, log trimming
    index.go        background writer of the old R# rows (decision 50)
    service.go      Service: cp.Store + loops + Start/Close lifecycle
    handler.go      role-aware HTTP edge (standbys refuse writes with 503)
    format.go       slot entry codec and versioning (decision 53)
  applier/        NEW: the production state machine on an embedded KV (decision 49)
    pebblekv/       the only Pebble-importing package; NoSync commits
  cpclient/       broker-side HTTP client (sys.ControlPlane) with sticky failover
  cpobs/          Prometheus metrics + per-call logging at the edge (decisions 37-38)
  cptest/         conformance: every store vs veritas, op for op (see §8)
cmd/manicp/       the binary: main.go (mem/dynamo wiring) + logstore.go (dynamolog wiring)
```

The old store and the log store are not deterministic code. Each store uses goroutines, the wall clock, and `net/http`. `scripts/ci-determinism.sh` excludes both stores. `cp/doc.go` records the exclusion. `veritas.ControlPlane` is the reference implementation under decision 21. The reference implementation is a small in-memory spec. `cp/cptest` checks each store against the reference implementation.

## 2. `cmd/manicp` serves the log store in production and the old store for rollback

The flag `-store` in `cmd/manicp/main.go:parseFlags` accepts three values. The table below lists the three values.

| `-store` | Backend | Served by |
|---|---|---|
| `mem` as the binary default | `memstore.New()` | `cp.NewHandler(st)` |
| `dynamo` | `dynamo.Store`, wrapped by `dynamo.Sequencer` unless you pass `-sequencer=false` | `cp.NewHandler(st)` |
| `dynamolog` | `logcp.Service` with a DynamoDB log, Pebble, and S3 checkpoints | `svc.Handler()`, the role-aware wrapper in `cp/logcp/handler.go` |

`openHandler` sends `dynamolog` to `openLog` in `cmd/manicp/logstore.go`. `openHandler` sends the other values to `openStore`, and `openStore` calls `cp.NewHandler`. Two cases matter when you run the binary. The list below states the two cases.

* The binary default is `mem`, and the `mem` store starts empty each time. You do not test either production store when you run `manicp` with no flags.
* Production serves `dynamolog`, and `docs/log-cp-service.md` records the `dynamolog` wiring from stage 4. The Terraform default for the benchmark is `manicp -store dynamolog`. `deploy/aws/terraform/variables.tf` makes `log` the default `cp_store`, and `log` maps to `-store dynamolog`. Terraform documents `dynamo` as the rollback store. The old store uses one host for rollback. Brokers paired with the old store use `-max-segment-partitions 96`.

`openLog` shows the startup order of the log store. `openLog` builds the `DynLog` and opens Pebble in a wiped directory. Pebble state does not carry across a restart. `svc.Start(ctx)` restores the checkpoint and replays the log. `main.go:serve` opens the HTTP port after replay ends. A restoring node refuses connections on the HTTP port. Brokers use another URL from the broker URL list during restore. Section 8 covers the broker behaviour.

## 3. Both stores keep the same `cp.Store` contract

`cp/store.go` defines four methods. Each method behaves exactly like `veritas.ControlPlane`, op for op.

```go
type Store interface {
    CommitSegment(ctx, core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error)
    Lookup(ctx, core.Lookup) (core.LookupResp, core.ErrCode, error)
    DescribeTopics(ctx, core.DescribeTopics) (core.DescribeTopicsResp, error)
    CreateTopic(ctx, name string, partitions int32) (core.ErrCode, error)
}
```

The contract keeps two error channels apart. A business error is a non-OK `core.ErrCode` with a nil Go error. The handler returns the business error as HTTP 200 with `{"err":"ErrConflict"}`. A Go error covers infrastructure failure. `cp.ErrUnavailable` gives 503, a context error gives 503, and another Go error gives 500. `cp/wire.go` holds the JSON structs and the conversions between `core` types and wire types. The structs include `CommitReq{segment_key, batches[]}` and `Range{topic, partition, base, count, seg_key, seg_off, seg_len}`. `api/openapi/controlplane.yaml` describes the same API. A test keeps the OpenAPI file and `cp/wire.go` in sync.

The broker never proposes an offset to either store. `core.CommitSegment` in `core/controlplane.go` carries only `{SegmentKey, Batches{TP, Records, Loc}}`. The request states where the bytes are. The request does not state where the bytes belong. Decision 42 sets this rule, and the rule makes the retry strategies in this chapter safe.

## 4. Three rules explain both stores

### 4.1 The replica assigns offsets at apply time, and no code assigns offsets before apply time

A batch receives `Base` from the end of the partition at the moment the commit applies. The applier reads the end of the newest run for the partition. The applier then adds the records of earlier batches for the same partition in the same request, in request order. No offset counter exists in either store. No high-watermark item exists in either store. No reservation exists in either store. Decision 42 records the rule.

Two results follow from the rule. The list below states the two results.

* A commit that loses a race has assigned nothing, so the broker can retry the commit without creating a hole. The retry burns no offsets.
* Two commits applied in any order produce contiguous offsets for each partition, and the two stores never reuse an offset. Concurrency control only picks an order and makes the chosen order atomic.

Each store picks a different mechanism to pick the order. The old store defends computed offsets with a transaction. The log store writes commands to a slot and lets the slot order become the apply order. You pay for the log order with replay and checkpoint work. You gain a commit path where a lost race costs a new slot and nothing else. That trade favours the log store at production scale, because the old store pays for the same race with fresh reads and a fresh transaction. The old store stays simpler to reason about. The log store stays safer to run with more than one node ready to write.

### 4.2 One run stores the contiguous batches that one commit assigns to one partition

The batches that one commit assigns to one partition are contiguous. The stores save the batches as one run. A run holds a base offset, a total count, and a packed per-batch list. The packed list holds a format byte, a table of distinct segment keys, and one entry for each batch. Each entry holds a key index, a record count, a byte offset, and a byte length. The code uses varints in `cp/dynamo/runs.go`, mirrored in `cp/applier/keys.go:encodeRun`. The packing costs about 6 B for each batch.

`Lookup{TP, From, MaxBytes}` finds the run that holds `From`. The lookup selects the last run with `base <= From`. The lookup expands that run and the runs after that run into `core.BatchRange` values. The lookup drops batches that sit wholly below `From`. The lookup cuts the answer on `MaxBytes` for each batch. `LookupResp.End` comes from the newest run at `base+cnt`. The end is 0 when no run exists. ListOffsets at latest and Fetch both use the end.

### 4.3 One segment key identifies one commit in both stores

Decision 3 states the idempotency rule, and `core/controlplane.go` restates the rule. A replay of a committed key with an identical request returns the ranges that the first commit assigned. The same key with a different request returns `ErrConflict`. No other code path uses `ErrConflict`. The broker retry loop depends on the rule. Decision 47 sets the broker retry until `-commit-deadline`, and the default deadline is 120 s.

A commit with a lost answer raises `MaybeApplied` in the broker. The broker resends the same request under the same `seg/<128-bit random id>` key. Decision 43 sets the key form. The resend must never assign offsets twice.

The old store and the log store remember a key for different lengths of time. You can see the difference as a user of either store. The old store keeps the `G#` item forever. The log store keeps the key for a deterministic window of `WindowSlots`, and `WindowSlots` is 131,072 slots. Section 7.4 covers the window. The same key commits again as a new segment after the window, and the records duplicate. The system accepts the duplicate because delivery is at-least-once. Decisions 41, 48, and 53 record the acceptance.

## 5. The old store computes offsets first and defends the computed offsets with one transaction

Read `cp/doc.go` in full before you touch the old store. The file is the design doc for the old store. The old store uses one DynamoDB table named `<prefix>cp`. The table uses string keys `pk` and `sk` and runs on demand.

### 5.1 The old store keeps three item kinds in one table

The table below lists the item kinds in the old store.

| `pk` | `sk` | Attributes | Meaning |
|---|---|---|---|
| `TOPICS` | `<topic>` | `parts` | The partition count. `CreateTopic` and growth raise the count. Neither operation lowers the count, because the merge keeps the maximum. |
| `G#<segkey>` | `-` | `blob` with JSON that holds the request batches and the full answer | The idempotency key. Presence means the segment has committed. A replay returns the stored `blob`. `maxBlob` bounds the blob at 300 KB, and 300 KB holds about 1000 batches. |
| `R#<topic>#<p>` | `<base,%020d>` | `base`, `cnt`, and `runs` in packed form | One run for each pair of partition and segment commit. The newest run at `base+cnt` gives the partition end. `maxRun` bounds the run item at 200 KB. |

The old store has no node item and no producer state. The old store has no offset counter and no high-watermark item. Decisions 41-44 removed the first three categories. Decision 42 derives the partition end and removes the fourth category.

### 5.2 The old store reads the ends, computes offsets, and writes one transaction

`cp/dynamo/commit.go` runs the old commit path, and `cp/doc.go` summarises the path. The sequence below gives the steps in order.

1. The old store reads with at most 16 reads in flight. One consistent `BatchGetItem` reads `G#<segkey>` and the `TOPICS` items. One consistent descending `Query Limit 1` reads the newest run for each distinct partition and gives the partition end.
2. The old store checks `G#` first. A present `G#` item replays the stored answer, and a different request returns `ErrConflict`. A malformed request reads `G#` only, and the malformed request can still replay or conflict. The old store returns `ErrInvalid` when the malformed request does neither.
3. The old store assigns bases from the ends that step 1 read, and the old store assigns the bases in request order.
4. The old store writes one `TransactWriteItems` call. The transaction puts `G#` with `attribute_not_exists(pk)`. The transaction puts one `R#` item at the computed base for each partition with `attribute_not_exists(sk)`. The transaction updates each grown `TOPICS` item when the item is absent or `parts < new`.

The put on the run item is the compare-and-set for the old store. Another commit may write to a partition after step 1. An item then sits at the computed base in step 4, and DynamoDB cancels the transaction. The old store re-reads only the partitions that lost. The old store retries with jittered backoff for 20 attempts and then returns `ErrUnavailable` with 503. The broker resends the same key after 503, so the broker loses nothing.

A commit with `P` partitions and no retry costs `P + 2` calls. The 100-item transaction limit in `dynamo.MaxTxItems` gives the refusal rule `1 + P + grown topics ≤ 100`. Brokers paired with the old store must run `-max-segment-partitions 96`. Decisions 32 and 52 record the limit and the broker flag.

The history matters here. The old store computed offsets before the write, so the old store had to defend the computed offsets. A lost race made the computed offsets stale. The old store paid a fresh read and a fresh computation for each lost race. That cost forced the next change.

### 5.3 The Sequencer groups old-store commits in one process and reads offsets from memory

Decision 46 exists because the compare-and-set in section 5.2 cancelled 387 of 705 transactions in the measured run. Six brokers each ran a separate manicp against shared partitions in that run. `dynamo.Sequencer` wraps `cp.Store` and owns sequencing in one process. `manicp -store dynamo` serves the Sequencer by default, and `-sequencer` defaults to true.

The Sequencer works as follows. The list below states the four parts.

* One loop goroutine takes queued `CommitSegment` requests in order and forms groups. One `TransactWriteItems` call handles many segments in one group. The transaction holds 1 `G#` item for each segment, 1 run item for each distinct partition, and 1 `Update` for each grown topic. The transaction holds at most 100 items and at most 1 MiB of `G#` blobs. The Sequencer adds no artificial wait. Groups form from the requests that arrive while a transaction runs.
* The Sequencer reads offsets from in-memory partition ends. The Sequencer loads each end with the same descending Query on first touch. The Sequencer also holds topic counts in memory. A bounded LRU holds recent segment keys for replay without a read, and the default LRU size is 4096.
* The Sequencer moves retries inside the process. Per-item cancellation reasons tell the Sequencer what to refresh. One cancellation refreshes one partition, one topic, or one `G#` item. An outcome that may have applied drops the cached state for the group and reads every `G#` item first. A 5xx response, a timeout, or a lost answer counts as an outcome that may have applied. The group answers the caller from storage after a lost answer. The group does not assign offsets twice. The Sequencer allows 40 attempts in 8 s and then returns 503.
* Pipelining in `SeqOptions.Pipeline` allows groups on disjoint partitions and topics to run at the same time. The default pipeline depth is 4. A group reserves the partitions and topics for the group. The Sequencer never issues a transaction with a base that chains after an in-flight base on the same partition. A failed transaction cannot leave a hole.

Correctness does not depend on one Sequencer. The compare-and-set in section 5.2 stays in place when you run two Sequencers. A second Sequencer costs retries, and a stale Sequencer costs retries. One manicp process is a performance rule for the old store. The log store changes the performance rule, and section 7.5 covers the new form. The constraint is now plain to you. The old store needs one process to own the in-memory ends before the old store can group commits without constant cancellations.

## 6. The prototype proved the log design before the log store used the design in production

Design A first ran in `cp/logsm`. The `doc.go` file in `cp/logsm` states that no binary and no default path uses the prototype. The prototype has the same basic shape as the log store. The list below states the three parts.

* The log holds raw commands, and `logsm.Log` provides `Append` at a slot if the slot is empty, `Read`, and `CheckTail`. `MemLog` and a DynamoDB `DynLog` implement the log. Each command is a `CommitSegment`, a `CreateTopic`, or a no-op. The log never holds computed bases.
* Each `Replica` applies the log in slot order through a `Machine`, and the default `Machine` is `veritas.ControlPlane`. The replica derives identical offsets, replays, and conflicts on every node. Replica state is a function of the applied prefix of the log.
* A `Committer` appends with up to K appends in flight. The `Committer` answers only after the replica on the same node applies the winning slot through `Await`.

Two ideas carry from the prototype into the log store. Offsets exist only after a replica applies the log. A writer that loses a slot has computed no offsets, so the writer can re-append the same commands later. A hole is a slot that a dead writer allocated and did not fill. The replica fills a hole with a no-op after `HoleAfter`. The log does not block behind one missing slot.

## 7. The log store records commands first and assigns offsets at apply time

Brokers send every commit to one appender node in the log store. The appender groups commands into slots of a DynamoDB log. The appender writes one conditional `PutItem` for each slot. Every node applies the slots in order through `cp/applier` on Pebble. The appender applies slots, and each hot standby applies slots. A node answers a commit only after the node applies the slot. Decisions 48 and 51 record the flow.

### 7.1 The `DynLog` slot holds commands and holds no bases

`cp/logcp/dynlog.go` implements the `Log` interface over the same `<prefix>cp` table. The default comes from `-table <table-prefix>cp`. A slot is one item in the table. The table below lists the items that the log store writes.

| `pk` | `sk` | Attributes | Meaning |
|---|---|---|---|
| `LOG` | `<slot,%020d>` | `d` as B for the entry, `v` for the format version, `w` for the writer, `id` for the append ID, `ts` for writer milliseconds as an advisory value, `n` for the command count, and `ttl` | One slot holds up to 256 grouped commands or one no-op. A no-op serves as a heartbeat or a hole fill. The entry plus 256 B of overhead must fit in `MaxEntryBytes` at 350 KB. A single larger command returns `ErrTooLarge` and never enters the queue. |
| `CKPT` | `-` | `ck_slot`, `ck_key`, `ck_size`, `ck_sum` as sha-256, and `ck_ts` | The checkpoint pointer. A conditional put moves the pointer forward only. The writer moves the pointer after the S3 object is complete. The object is `ckpt/<slot,%020d>` in the checkpoint bucket. |
| `IDX` | `-` | `ix_slot` and `ix_ts` | The index watermark. Slots below the watermark have `R#` rows in the table. Section 7.6 covers the indexer. A conditional put moves the watermark forward only, as with `CKPT`. |
| `TRIM` | `-` | `tr_below` | The point that trimming has reached. A restarted node reads `tr_below` and does not rescan from slot 0. |
| `TOPICS` and `R#<topic>#<p>` | As in section 5.1 | As in section 5.1 | The log service does not read these rows. The background indexer writes these rows so the old store can take over on rollback. Section 7.6 covers the indexer. |

`cp/logcp/format.go:MarshalEntry` writes the entry itself. The entry holds a version byte, a rank, a writer, an append ID, a timestamp, and a command count. Each command follows in the `cp/logsm` codec with a kind, a key table, and batches. The log holds commands. The log never holds bases.

Versioning follows decision 53. A reader meets a version above `logcp.Version` at some slot. The reader stops with `Blocked`, returns 503, and sets `/healthz` to 503. A reader that cannot decode a payload also stops. The reader does not skip a slot and does not produce different state. An upgrade takes two deploys. All nodes read v+1 first. Writers then switch to v+1.

Trimming needs care from you. Seeded tests found a bug in trimming. Trimmed slots receive one-byte tombstones. The log store does not delete the trimmed slots. `TrimBelow` controls the trim, and `TrimDelete` defaults to true. A lagging node holds a stale slot number and tries to append into a deleted slot. The append would win an `attribute_not_exists` put against a deleted slot and would rewrite history. The same append fails against a tombstone. The node learns `ErrTrimmed` and must restart and restore from the checkpoint. The `ttl` attribute is only a backstop. The default `ttl` is 7 days through `-log-ttl`.

### 7.2 One if-absent slot write replaces a lease and a fencing epoch

The old store uses `attribute_not_exists(sk)` on the run item. The log store uses `attribute_not_exists(pk)` on the slot item. `DynLog.Append` makes one `PutItem` call with `ReturnValuesOnConditionCheckFailure: ALL_OLD`. The loser learns the winner entry from the failure response. The loser needs no extra read.

You can now see why the log store needs no lease and no fencing epoch. A lease gives one writer the right to write for a period of time. A fencing epoch gives each new lease a higher number, and storage rejects writes from an older number. The slot write needs neither mechanism. DynamoDB checks the absence of the slot at the instant of the write. One slot accepts exactly one entry, and a later writer cannot overwrite the entry in that slot. A stale appender cannot claim a slot that another appender already filled. A stale appender cannot create a second entry for the same slot. Safety rests on the conditional write for one slot. Safety does not rest on a belief about which node holds a lease.

Two nodes may both believe that each node is the appender. The two nodes race on the same slot, and one node wins. The loser re-appends the same entry at a later slot. No offset came from a local view in either node. A lost slot costs no offset work. Decision 51 records that no lease is needed for safety.

Rank, heartbeats, and takeover timers make one node the usual appender. Sticky single-appender routing keeps the log at one `PutItem` for each commit. The routing avoids the contention in decision 46. Decision 48 and experiment X4 record the routing. The role machinery saves writes. The role machinery does not carry safety.

### 7.3 The appender answers the caller only after the replica applies the slot

`cp/logcp/committer.go` and `service.go` hold the appender path. The list below states the four parts.

* `submit` admits the command. `submit` refuses with 503-class errors when the service has stopped, the service is `Blocked`, the node is not active, or the queue is full. `QueueMax` limits the queue to 4096 commands and 64 MiB. `submit` encodes the command, checks the single-command size guard, and queues the command in FIFO order.
* K flusher goroutines take everything in the queue as one slot entry. The flag `-log-k` sets K, and the default is 4 appends in flight. Each flusher takes at most 256 commands for one slot. Each flusher reserves a slot-rate token before the flusher writes. The flag `-slot-rate` sets the rate, and the default is 200 slots per second. The rate paces writes to the one hot `LOG` partition key. A lone commit goes at once. Groups form from load, as groups form in the Sequencer.
* `appendGroup` allocates the next slot from `max(high, applied)`. `appendGroup` appends the entry, and a lost slot shows the winner entry to the loser. The loser retries the same entry at a later slot. An unknown outcome retries the same slot until the condition answer reveals the owner of the slot. `appendGroup` matches the owner by append ID.
* The appender does not answer the caller. The replica applies the slot, and the answer arrives at that moment. `replica.deliverLocked` matches the entry ID to the waiters for the entry. `replica.deliverLocked` hands the waiters the `core.CPDone` values from the applier in `cp/logcp/apply.go`. A commit ack means the slot is in the log and the replica has applied the slot. The node that applies the slot sends the answer.

### 7.4 The replica assigns offsets when the replica applies slots in order

Every node runs the same replica loop in `replica.go`, `apply.go`, and `service.go:runLoop`. The replica reads the log with consistent `Read` calls. The replica buffers out-of-order slots above a hole. The replica fills a hole with a no-op after the hole stays empty for `HoleAfter`. `HoleAfter` is 250 ms, and later slots must wait behind the hole before the replica fills the hole. The replica applies contiguous slots in batches through `applier.CommitBatch`. A batch holds at most 256 slots. A batch does not cross a `TrimStride` boundary. Each batch uses one Pebble transaction. Each command sees writes from commands before the command in the same batch.

`cp/applier` is the production state machine under decision 49. The applier has the same semantics as `veritas`. The applier uses an ordered KV in place of in-memory maps. Veritas keeps every run without limit. Veritas grows by about 6 GB each hour at the target rate.

The layout in `applier/doc.go` and `keys.go` uses `tp` for one topic-partition key. The key `tp` is a uvarint topic length, the topic, and a 4-byte big-endian partition. The table below lists the applier keys.

| Key | Value | Meaning |
|---|---|---|
| `T <name>` | The partition count | The topic registry. Growth keeps the maximum count. |
| `E <tp>` | The end offset as a uvarint | The next offset for the partition. The value stores the end of the newest run. |
| `R <tp> <base,8BE>` | A packed run | One run for each pair of partition and segment commit. The packing matches section 4.2. |
| `K <segment-key>` | The slot, a request digest with 16 B of SHA-256 over the batches, and the assigned bases | The idempotency record. A digest match replays the stored bases. A digest mismatch returns `ErrConflict`. |
| `S <slot,8BE> <segment-key>` | A marker | The window index. `Trim` uses the index to find and drop the `K` records of old slots. |

`assign` in `applier.go` implements section 4.1. For each batch, `Base` is the current `E` for the partition. `assign` reads `E` once for each partition in each transaction. `assign` then adds `Records` to `E`. `assign` writes one `R` record for each partition and grows topics. `commitTx` checks `K` before `assign` runs. The first slot wins. A replay rebuilds the ranges from the replayed request and the stored bases. A replay changes no offset.

Replicas cut the idempotency window by slot number. The apply batch starts at a multiple of `TrimStride`, and `TrimStride` is 1024. The batch start is at or past `WindowSlots`, and `WindowSlots` is 131,072. `Trim(start - WindowSlots)` then runs before the batch. The cut is a function of the slot number. Every replica forgets the same keys at the same point of the log.

A clock-based cut would give you a different system. Replicas that trimmed keys 2× `CommitDeadline` ago by separate clocks would answer the same replay in different ways. The replicas would produce different state. Finding 1 in `docs/log-cp-service.md` and decision 53 record the clock problem. The window lasts at least 655 s at the 200 slots per second ceiling. The window lasts at least 2× the broker commit deadline plus a checkpoint interval. A broker retry arrives inside the window in the normal case.

### 7.5 Roles choose the usual appender, and the slot write keeps a wrong role safe

Roles are advisory in `cp/logcp/role.go`. The slot check in section 7.2 provides safety. A wrong role belief costs retries. A wrong role belief does not break correctness. The rules work as follows. The list below states the four rules.

* Rank 0 starts active, and higher ranks start as standbys. A standby reads and applies slots as the slots arrive. Pebble on the standby holds current state. The standby serves fresh reads. The standby refuses `commit-segment` and `create-topic` at the HTTP edge with `503`, `Retry-After: 1`, and `X-CP-Role: standby` in `cp/logcp/handler.go`. The handler refuses the request before the handler reads the body.
* The active node appends a no-op heartbeat slot after an idle period of `Heartbeat`. `Heartbeat` is 1 s. The heartbeat makes the liveness of the appender visible in the log itself.
* A standby promotes the standby node after the standby sees no slot from another writer for `TakeoverAfter`. You set `TakeoverAfter` with `manicp -takeover-after`, and the default is 5 s. You keep `TakeoverAfter` at several times `-heartbeat`. You keep `TakeoverAfter` far below the 120 s broker commit deadline. Reads on the standby must keep succeeding during the period. A node with failing reads does not promote. The node cannot tell node death from a network partition.
* An active node steps down when a writer of lower rank appends at or after the slot where the active node promoted. The lower-rank writer is the preferred node back in service. Tests cover split brain. Both nodes append in the split-brain test, and the if-absent slot picks one entry for each slot. The states stay identical.

Standbys serve fresh reads before any failover. `Lookup`, `DescribeTopics`, and `End` call `fresh()` in `fresh.go`. One consistent descending `CheckTail` with `Limit 1` covers every read that arrives inside a `TailBatch` window. `TailBatch` is 10 ms. The node waits until `applied >= tail`. The node then answers from Pebble. Any node can serve a read that reflects prior writes. Standbys can scale reads in the log store.

### 7.6 Checkpoints shorten replay, and the indexer keeps rollback to the old store open

The log is the record that decides state in the log store. Pebble holds a copy that the node rebuilds from the log. `pebblekv` commits with `pebble.NoSync`. `cmd/manicp` wipes the Pebble directory at every start. Checkpoints in `checkpoint.go` make restart faster. The sequence below gives the steps in order.

1. The active node snapshots the applier at a consistent slot every `CkptEvery`. `CkptEvery` is 5 min, and the node needs at least `CkptMinSlots` at 1000 new slots. `SnapshotAt` takes the store view and reads `applied` inside the view. `SnapshotAt` streams the `APLR1` format with a magic value, a varint slot, and length-prefixed key and value entries in key order. The stream includes the `K` and `S` window records. Replays of recent keys still answer after a restart.
2. The node writes the object to S3 as `ckpt/<slot>` through the injected `sys.ObjectStore`. `logstore.go` injects `sys/s3store`, and `-ckpt-bucket` names the bucket. An empty bucket name means no checkpoints and no trim. The node then moves the `CKPT` pointer item with a forward-only conditional put. Readers see the old checkpoint or the new checkpoint. Readers never see a partial checkpoint. The node keeps the pointed object and one older object.
3. The node trims the log below `min(ckpt - KeepSlots, index watermark)`. `KeepSlots` defaults to `WindowSlots`. The node writes tombstones as section 7.1 describes.
4. A starting node reads the `CKPT` pointer in `Start`. The node gets the S3 object and verifies the SHA-256 sum. The node runs `applier.Restore`, calls `startAt(slot)`, and replays the log to the tail. The node serves after replay ends. A damaged object falls back to replay from slot 0 when the log still starts at slot 0. A damaged object returns `ErrNoCheckpoint` when the log no longer starts at slot 0. A graceful shutdown writes a final checkpoint first. `-final-checkpoint` controls the final checkpoint, and `logstore.go:stopLog` allows a 20 s budget.

The indexer in `index.go` writes the `R#` and `TOPICS` rows of the old store in the background. Decision 50 records the indexer. The active node runs the indexer every `IndexEvery`, and `IndexEvery` is 10 s. The indexer moves `IDX.ix_slot` after the indexer writes the rows. The indexer plan is a pure function of the log plus `applier.Assigned`. Rows from the indexer match the bytes that the old store would have written through `dynamo.RunRow`. The indexer writes rows for each partition in offset order. The table holds a gap-free prefix of rows.

No part of the log service reads the indexer rows. The log store answers `Lookup` from Pebble. The rows support rollback to `-store dynamo`. You quiesce commits and wait for `manicp_log_index_lag_slots` = 0. You then switch stores. The log store does not write `G#` keys. A key from before rollback replays as a new commit after rollback. At-least-once delivery permits the duplicate records. Decision 41 records the delivery rule.

## 8. The broker retries with one segment key across the old store and the log store

`cpclient.New(baseURL)` accepts a comma-separated URL list under decision 51. Calls go to the URL that last worked. The client stores the working URL in `cur` as an atomic index. A connection failure triggers failover to the next URL inside the same call. A 503 response also triggers failover. A standby node, a starting node, or a blocked node returns 503. The client tries each URL at most once in `shouldFailover`.

Failover is safe for commits because section 4.3 gives one segment key to one commit. The retry carries the same segment key to the next node. Business errors come back as `*cpclient.Error`. An HTTP 200 response with `err` carries a business error. Transport failures come back as `*CallError{Op, Sent, Status}`. `Sent` records whether the client obtained a connection. The broker uses `Sent` to classify `MaybeApplied` and to decide whether a resend could duplicate records. The broker cannot duplicate offsets in either case. `Sent` drives error classification, and `Sent` does not drive safety. Deadlines come from the context of each call. The broker commit-deadline loop under decision 47 sits above the client.

`cp/memstore` supports development and tests. The `mem` store puts `veritas.ControlPlane` behind a mutex. `cp.NewHandler` serves the `mem` store. A behaviour differs between `mem` and another store in a test. The other store has the bug in that case. `cp/cptest` enforces the rule. Conformance tests in `conform_test.go` run every store through the same op sequences against veritas. The test set covers memstore, dynamo on `fakedb`, the `logsm` store, and the log store over HTTP. The test set includes the seeded X2 tests for `logcp` with crashes, takeovers, split brain, and trim. `docs/log-cp-service.md` describes the X2 tests.

## 9. The diagrams show commit and failover in slot order

### 9.1 `CommitSegment` on the log store follows the current path

```mermaid
sequenceDiagram
    participant B as Broker (cpclient)
    participant H as manicp active (handler)
    participant Q as Commit queue + K flushers
    participant D as DynamoDB LOG/<slot>
    participant R as Replica apply loop
    participant A as Applier (Pebble)
    B->>H: POST /v1/commit-segment {segment_key, batches}
    H->>Q: submit: admit (active? queue full?) + size guard
    Q->>Q: group queued commands (<=256), reserve slot-rate token
    Q->>D: PutItem LOG/<n> if attribute_not_exists(pk)
    alt slot won
        D-->>Q: ok
    else slot lost (another writer / hole filler)
        D-->>Q: ConditionalCheckFailed + winner entry (ALL_OLD)
        Q->>D: PutItem same entry at LOG/<n+1> ... (no offsets computed yet)
    end
    D-->>R: tail Read / observed append: slot n now contiguous
    R->>A: CommitBatch: check K<segkey> -> assign bases from E<tp>, write R, E, K, S
    A-->>R: assigned ranges per command
    R-->>H: deliver by entry ID: applied >= n
    H-->>B: 200 {assigned: [{base, count, ...}]}
    Note over B: Retry with same key at any point -> K record replays original ranges; different request -> ErrConflict
```

The old store replaces the middle steps in the diagram. The old store reads the partition ends. The old store runs one `TransactWriteItems` call with the `G#` put, the run compare-and-set, and the topic updates. DynamoDB cancels the transaction in a lost race. The old store re-reads the losing partitions and retries. Section 5.2 gives the old sequence. The broker contract stays the same across the old store and the log store.

### 9.2 A standby takes over after the active node dies

```mermaid
sequenceDiagram
    participant B as Broker (cpclient URL list a,b)
    participant P as CP rank 0 (active)
    participant S as CP rank 1 (standby)
    participant D as DynamoDB LOG
    Note over S: tailing + applying continuously (warm Pebble); reads served fresh
    P->>D: heartbeat / commit slots ... then P dies
    B->>P: POST /v1/commit-segment
    P--xB: connection refused / 503
    B->>S: same call, same segment key (failover within the call)
    S-->>B: 503 X-CP-Role: standby (until promotion)
    Note over S: no slot of another writer for TakeoverAfter (5s), own reads OK -> promote
    B->>S: retry (broker commit-deadline loop, same key)
    S->>D: PutItem LOG/<tail> if-absent (wins; or loses to a late P slot and re-appends later)
    S->>S: apply slot through applier
    S-->>B: 200 {assigned}
    Note over P,S: If P revives and appends: both race per slot (if-absent arbitrates);<br/>S steps down only when a lower-rank writer appends after its promotion (failback).
```

Entries from the dead node landed in the log before the node died. The node that applies an entry sends the answer for the entry. Entries from the dead node did not land in the log. Entries that did not land received no offsets. The broker resends an entry that did not land under the same key. The resend commits as a new entry.

A standby discovers that trimming passed the standby position. The standby receives `ErrTrimmed` and stops with 503. A standby meets an unknown slot version. The standby enters `Blocked` and stops with 503. The standby does not guess in either case. A restart restores the standby from the newest checkpoint.

## Key takeaways

The list below gives the points to keep from this chapter.

* The old store and the log store assign offsets at commit-apply time from the newest run for the partition. The broker never proposes a base. Decision 42 records the rule. A lost race costs a retry in either store, and a lost race costs no offset.
* Idempotency is per segment key in the old store and the log store. An identical replay returns the original ranges. A different request returns `ErrConflict`. The old store remembers keys without limit in `G#`. The log store remembers keys for a deterministic `WindowSlots` window.
* A run is the storage unit in the old store, the log store, and the applier. One run holds the contiguous batches that one commit assigns to one partition in packed form.
* The old store uses `attribute_not_exists` on the run item inside one transaction for safety. The Sequencer uses the same compare-and-set under one sequencing loop. The log store uses `attribute_not_exists` on the log slot for safety. Exactly one writer is a performance rule in the three designs, and exactly one writer is never a correctness rule.
* `manicp` defaults to `mem`, and Terraform serves `-store dynamolog` in production. The log store uses one appender, and hot standbys read the same log. Nodes rebuild Pebble state from S3 checkpoints and log replay.
* `cp/logsm` is the prototype, `veritas` is the reference implementation, and `cp/applier` is the production state machine. `cp/cptest` checks each store against `veritas`.

## Checkpoint

<details>
<summary>1. A broker resends a <code>CommitSegment</code> after a timeout and gets different offsets than the first attempt would have given. Which invariant broke, and where is it enforced in the log store?</summary>

Idempotency per segment key in section 4.3 broke. The first attempt never applied in one case. The resend is then the first commit, and any offsets from the resend are correct. The first attempt applied in the other case. The `K <segment-key>` record in `cp/applier` must replay the stored bases in the second case. `commitTx` checks `K` before the applier assigns offsets. Different offsets mean the log store forgot the key before the resend. Section 7.4 covers the window cut. You check `WindowSlots` against the broker `-commit-deadline`. The resend may differ from the first request in a third case. A digest mismatch gives `ErrConflict` in the third case, and a digest mismatch gives no new offsets.

</details>

<details>
<summary>2. Why can the log store's appender lose a slot race and re-append the same entry later, while the old store had to re-read partition ends before retrying?</summary>

The old store computed bases from the partition ends that the old store read. The old store defended the computed bases with the run-item compare-and-set. A lost race meant the computed bases were stale. The old store re-read the ends and recomputed the bases. Section 5.2 gives the old sequence. The log entry contains no bases. A replica derives offsets only when the replica applies the log in slot order. Sections 7.3 and 7.4 give the log sequence. A lost slot changes only the position of the commands in the order. Any order yields contiguous offsets under section 4.1.

</details>

<details>
<summary>3. Two <code>manicp -store dynamolog</code> nodes both think they are active for a minute. What breaks, and what does not?</summary>

Correctness holds for the log store. Both nodes race for each slot on `attribute_not_exists(pk)`. The loser observes the winner entry and re-appends the entry later. All replicas apply the same slot sequence into identical Pebble state. Sections 7.2 and 7.5 cover the race. Performance degrades, and latency degrades. The costs are extra lost slots, extra retries, and extra role changes when the lower-rank node appends again. Roles, ranks, and heartbeats make the race rare. Roles, ranks, and heartbeats do not make the race safe.

</details>

<details>
<summary>4. A standby is restarted after being down for a day. Walk through what it needs before it may serve, and two ways that can fail permanently.</summary>

`Start` in `cmd/manicp/logstore.go:openLog` wipes Pebble and reads the `CKPT` pointer. The node gets the S3 object and verifies the SHA-256 sum. The node restores the object with the window records included. The node replays the log from the checkpoint slot to the tail. The HTTP port opens only after replay ends. Sections 2 and 7.6 give the startup sequence. Two failures can be permanent. The checkpoint object is unusable and the log no longer starts at slot 0, so the node returns `ErrNoCheckpoint`. The node meets a slot in a newer format version, or the node meets a tombstone where the node expected history and receives `ErrTrimmed`. Both failures stop the node with 503. The node does not skip a slot and does not produce different state. Sections 7.1 and 7.6 cover the two failures.

</details>

<details>
<summary>5. You must roll production back from <code>dynamolog</code> to <code>dynamo</code>. What do you wait for, what flag must the brokers get, and what quietly changes about replays?</summary>

You quiesce commits and wait until `manicp_log_index_lag_slots` = 0. The background indexer has then written every `R#` row that the old store will read. Section 7.6 covers the indexer. You stop the log nodes and start the old manicp. You give brokers `-max-segment-partitions 96` for the 100-item transaction rule in the old store. Section 5.2 gives the transaction rule. One change is easy for you to miss. The log store never wrote `G#` items. A replay of a segment key from before rollback commits again as a new segment after rollback. The replay duplicates the records from the segment. At-least-once delivery permits the duplicate records under decision 41. Operators must accept the duplicate records before rollback, and `docs/log-cp-service.md` records the acceptance.

</details>

## Try it

```sh
# Terminal 1: the current production store, fully local
# (needs DynamoDB + S3 stand-ins: scripts/it-up.sh starts ExtendDB + SeaweedFS)
go run ./cmd/manicp -store dynamolog -create-tables \
  -dynamo-endpoint http://127.0.0.1:18080 \
  -ckpt-bucket test -s3-endpoint http://127.0.0.1:8333 -s3-path-style

# Terminal 2: the old store, for contrast (note: exactly one, sequencer on)
go run ./cmd/manicp -store dynamo -create-tables \
  -dynamo-endpoint http://127.0.0.1:18080 -addr :8081

# Either one: create a topic, commit the same segment twice, compare.
curl -s localhost:8080/v1/create-topic -d '{"topic":"t","partitions":3}'
curl -s localhost:8080/v1/commit-segment -d '{"segment_key":"seg/0123456789abcdef0123456789abcdef",
  "batches":[{"topic":"t","partition":0,"records":10,"seg_key":"seg/0123456789abcdef0123456789abcdef","seg_off":0,"seg_len":100}]}'
# …repeat the commit verbatim (same ranges back), then change "records" (ErrConflict).
go test ./cp/cptest/ -run TestConformance -count=1   # every store vs veritas
```

You see the difference with `dynamolog` when you add a second node. You give the second node `-rank 1`, a different `-addr`, and a different `-node-name`. You commit against the second node and expect `503` with `X-CP-Role: standby`. You kill the rank-0 node. You time the delay until the standby promotes and the retry with the same key succeeds.

## Further reading

The list below gives the files to read next.

* `cp/doc.go` covers the old DynamoDB design and the Sequencer. You read `cp/doc.go` to understand section 5.
* `docs/log-cp-service.md` is the reference for the log store. The file lists flags, gives a failure-case table, and reports verification results. The file covers the two findings that changed the design. The findings are the deterministic window and tombstone trim.
* `docs/decisions.md` rows 42-53 cover offsets from runs, no fencing, the Sequencer, design A, the applier, hot standby, and versioning.
* `core/controlplane.go` defines the contract that the old store and the log store implement.
* `docs/design-a-prototype.md` explains the choice of a log from experiments X1-X4. `docs/applier-bakeoff.md` explains the choice of Pebble over bbolt. The two files give the evidence behind decisions 48-49.
