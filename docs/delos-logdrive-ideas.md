# Ideas from Delos and the LogDrive for the commit path

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This document is a record.*

Status: IDEAS (2026-10-20), not a decision. Nothing here is built, nothing else is changed. Labels: **paper** (quoted
from the text, section named), **measured** (our w2/w3), **computed** (arithmetic from stated inputs), **inferred**
(my reasoning, not in any paper), **unknown**.

## 1. What was read

- **Delos:** "Virtual Consensus in Delos", OSDI 2020 (Facebook). Full text from usenix.org, sections 1 to 6 read closely,
  related work skimmed. Not read: the 2025 erratum on seal-check ordering that the LogDrive paper cites ([35]).
- **LogDrive:** the paper exists. "The LogDrive: Composable Durability for Cloud-Based Shared Logs", OSDI 2026
  (Confluent, Vickers et al.), usenix.org/system/files/osdi26-vickers.pdf. It builds the system **Conflux**, the
  metadata store of Confluent's S3-based Kafka, K2. Sections 1 to 8 read; the proofs in the appendix only skimmed.
  No other paper was substituted.
- **Repo:** `commit-path-redesign.md` (design L), `w2-bottleneck-verification.md` only through the numbers L quotes,
  `w3-affinity-and-window-experiments.md`, decisions 21/29/35/39/41-47, `TODO.md`, `internal/verify/veritas/controlplane.go` size only.

## 2. What the papers say that matters here

- **Delos database is state machine replication over a log (paper, section 4).** A server "serializes and appends [a
  read-write transaction] to the VirtualLog without executing it", then applies it when it reads it back. A read-only
  transaction does `checkTail` and syncs to that position. Many readers queue behind one outstanding sync (the
  "bus stand", LogDrive section 5.1).
- **Reconfiguration is sealing (Delos 3.2).** Seal the active Loglet, read its tail, then conditionally write a new
  chain into a versioned register (the MetaStore). Any client can do it; the winner of the conditional write is the only
  consensus in the system. A Loglet needs only a highly available `seal` and `checkTail`, not highly available appends
  (3.4). A sealed Loglet may still accept "zombie" appends that become durable beyond the seal point; they are invisible
  because the chain maps fixed address ranges to each Loglet (3.4).
- **Trimming and checkpoints.** Delos has `prefixTrim` in the API (3.1, Figure 2) and "takes local snapshots every 10
  minutes and ships them to a backup service every 20 minutes" (5). The paper gives no trim policy beyond that.
  Conflux does the same: "we frequently checkpoint state to cloud storage and trim the shared log" (LogDrive 5.1),
  snapshots every 10 minutes (LogDrive 6).
- **LogDrive (LogDrive 3 and 4).** A numbered address space of single-value registers plus `weakTail(K)`. It layers on
  S3, DynamoDB and S3Express with about 520 LOC each. The **AtomicLog** adds one soft-state sequencer
  (`acquireSlot`/`completeSlot`, no payloads) and a window of at most K in-flight writes (K=16 in their runs). Appends
  are plain writes; they do not need conditional puts. Exclusion comes from the seal bit, checked after the write
  ("Each append checks this bit after completeSlot").
- **What they measured (LogDrive 6).** p99 write about 130 ms with 100 ms / 60 KB batching, 165 byte records, 1:3
  write:read. Conflux over DynamoDB cut metadata cost 10x against one DynamoDB write per record (strawman, not
  something they ran). "S3LogDrive had suboptimal performance ... as well as cost ... compared to DynamoDBLogDrive."
  Early DynamoDB tests "hit rate limits on DDB partition key accesses" and were fixed with the StripedLogDrive.
  Production runs 20 Conflux shards of 3 servers each, with brokers affinitized to topic ranges to limit fan-out (5.3).
- **Delos striping (4.2.2).** Round-robin across Loglets; an append is acknowledged only after all earlier logical
  positions on all stripes are filled. A StripedLoglet is sealed as a whole. 1M+ appends/s on 90 log servers; the
  database then "bottlenecks on log playback" (5.1).

**What the papers do not say.** Nothing on per-partition offset assignment, on DynamoDB transactions, on the cost of a
conditional-put slot, on how Conflux de-duplicates a retried command, on checkpoint size or replay time, or on any
ShardedLog (Delos has no batching section; Conflux has a time/size batcher). Delos's workload is 150 puts/s, ours is 30-64
entries/s of 2-4 KB: the papers show the shape works, not that our numbers hold.

## 3. The bottleneck, restated in one line

Measured: tx time about 9 ms + 0.7 ms per item, one transaction in flight, items = partitions touched per segment
(w2, w3). Design L removes the items from the commit. The ideas below are about what else the papers suggest once that is
done, and one weakness of L that I found (idea B).

## 4. Ideas

### A. Log commands, not results; CP replicas as learners (the top idea)

**What changes.** Today L's `G#`/`LOG` entry holds *outputs*: bases computed by the sequencer from in-memory `ends`.
Instead the log entry holds the **input**: the `CommitSegment` request (partition table, batch locations, key), no bases.
Every CP replica is a Delos-style learner: it reads the log in order, applies each entry to the `veritas` state machine,
and gets identical bases, identical `ErrConflict`/replay answers and identical state. The appender answers the broker
when its own slot has been applied locally (Delos 4; LogDrive 5.1 "multi-master mode").

```
 broker --CommitSegment(key, batches, no bases)--> any CP replica (sticky to a preferred one)
                                                    |
                          PutItem LOG/<slot>  attribute_not_exists, up to K in flight, 1 item/segment
                                                    v
        +------------------ DynamoDB: LOG/000..N (single pk, sk = slot) -------------------+
        +--------+---------------------+----------------------+----------------------------+
                 | tail (Query sk > n) | tail                 | tail
          +------v------+       +------v------+        +------v------+
          | CP replica 1|       | CP replica 2|        | CP replica 3|    each: apply(entry) ->
          | spec state  |       | spec state  |        | spec state  |    ends, tail runs, key set
          +------+------+       +-------------+        +-------------+
                 | answer when own slot applied (bases known)
   Lookup(any replica): checkTail (1 consistent Query, shared by queued lookups) -> catch up -> answer from memory
   every ~10 min: replica writes CKPT (ends, key set, index cursor) to S3 -> prefixTrim LOG below it
   async: indexer (L's R# runs, or idea B) is a deterministic function of the log, any replica may run it
```

**Why the papers support it.** It is the Delos/Conflux database model. The one thing L needs a single in-flight
transaction for (L section 5.2: offsets "computed from a state that never existed") exists only because L logs
outputs. With input logging a lost or late slot cannot poison a later one: the command is just re-appended at a later
slot and gets its bases at apply time (**inferred**, but a direct consequence of Delos section 4).

**What it fixes.**
- Measured bottleneck: same as L (no per-partition items on the commit). Extra: no serial loop; K=16 at ~20 ms is
  800 entries/s (computed), far above the 30-64/s needed.
- L's weak spot 5.6 and decision 13.1: any replica can serve a fresh `Lookup` (checkTail, then apply, then answer), so no
  "active-only Lookup", no `OFFSET_OUT_OF_RANGE` window, no 503 from non-active CPs.
- Failover: a standby already tails the log, so takeover replay is about zero. There is no lease for safety; a routing
  preference (preferred CP, others as fallback) only reduces contention. The slot CAS is the safety net, as today.
- The permanent `G#` items go away. Idempotency per key becomes "first occurrence in the log wins", with the key set
  carried in the checkpoint. Keys older than the checkpoint window lose the guarantee, which is a product decision
  (**inferred** risk; decision 41 already allows duplicate records on client retry, and `CommitDeadline` is 120 s).
- It fits the project: the state machine is `veritas.ControlPlane` (208 lines, only maps, no clock or randomness, so
  deterministic), the sim can replay one log through N replicas and compare `StateHash`. Decision 21 gets stronger.

**Numbers (computed).** Entry about 3.4 KB at 16 MiB segments (L section 5.1), 4 WRU as a plain Put, 30 segments/s =
120 WRU/s; 64 seg/s at 1.7 KB = 128 WRU/s. That is about $0.2k/month against L's commit ~290-340 WRU/s (about
$0.5k). A saving of about $0.3k/month: cost is not the reason. Tailing: one consistent `Query` of an empty result per 50 ms
per replica = 20 RRU/s = about $7/month per replica. Hot key: 1,000 WCU/s per partition key / 4 WCU = about 250 entries/s
before striping (idea D).

**Risks and unknowns.**
- **Contention with several appenders** (decision 46: 387 of 705 attempts cancelled with 6 contenders). With input
  logging a lost slot costs one cheap retried Put, not a lost 97-item transaction (**inferred**), but unmeasured.
- **Holes.** K in-flight Puts by one writer can leave slot n empty if the writer dies after n+1. Learners apply only the
  contiguous prefix. A learner that sees a hole older than T fills it with a no-op via `attribute_not_exists`; the
  original command then fails its condition and the broker retries under the same key (Corfu-style fill; LogDrive uses
  weakTail with window K for the same problem). Needs sim coverage.
- **Apply-time errors.** Invalid commands (unknown topic) consume a slot and are rejected at apply. Fine, but changes
  the cost of garbage input.
- **Playback bound** (Delos 5.1): one apply thread per replica. Our apply is about 13k batch ops/s at 64 seg/s
  (computed); should be far from a limit, but is untested.
- **Ordering.** One in-flight segment per broker (L 5.8) keeps seal order equal to slot order for a broker. Unchanged.
- Latency of a single PutItem of 4 KB versus a 2-3 item transaction is **unknown** (L's E1 measures the second).

### B. Take the per-partition index out of DynamoDB (and fix a hole in L at 40k partitions)

**The hole (computed).** L section 7 says index cost is "bounded by bytes, not items": 290 WRU/s. That holds at 288
partitions because 288 run items carry ~22 batches each. At 40k partitions it does not: a run item costs at least 1 WRU
however small. 6,450 batches/s over 40k partitions with F=1 s touches 40000 x (1 - e^-0.161) = about 5.9k distinct
partitions per flush, so about 5.9k WRU/s = **about $9.8k/month**, nearly half of today's $21k. F=10 s gives about 3.2k
WRU/s ($5.3k); F=60 s gives about 0.7k WRU/s ($1.1k) but a 60 s tail to replay and keep in memory.

**Idea.** Follow Conflux: reads come from the materialized view, and the durable form of the view is a checkpoint in S3,
not rows in the database (LogDrive 2.1 "The Cloud is the Log!", 5.1). The CP keeps the recent index in memory (it
already must, for the tail). Every F' (say 1-10 min) it writes an immutable **index file** to S3 holding the runs
since the last file plus `ends` (a checkpoint), then trims the log below it. A `Lookup` below the in-memory tail uses a
small in-memory map from (partition, offset range) to file. A background compaction merges files (K2 does the same
for topics, LogDrive 5.3 "Logs on Logs").

**What it fixes.** DynamoDB write cost stops depending on partition count; commit-log WRU stay at ~120-340/s from 288 to
40k partitions. Consumer `Lookup` reads (TODO "Consumer read path": ~48k consistent reads) are served from memory.
Log trimming is safe by construction instead of a 7-day TTL with an alarm.

**Risks.** This is design L's D4, so it is a real build (compaction, a map-of-files that itself grows, S3 GET latency for
cold `Lookup`). Index memory for long retention is **unknown**: at about 10 B per batch and 6,450 batches/s it is about
5.6 GB/day (computed), so memory holds hours, not days. The checkpoint/trim half (without compaction) is cheap and is
needed by L as well. Conflux's checkpoint size and replay time are not reported.

### C. A Loglet/VirtualLog seam: sealing, not a lease, for reconfiguration

**What.** Put the `Loglet` interface (append, checkTail, readNext, seal, prefixTrim; LogDrive Figure 5 = Delos Figure 2)
at the CP storage seam. Implementation 1: DynamoDB slots under `pk = LOG#<epoch>`. A `CHAIN` item (conditional,
versioned) maps virtual slot ranges to loglets. Reconfigure = seal, read tail, write the new chain (Delos 3.2).
**What it buys.** A way to change the log's backing without downtime: move to more stripes if the hot key throttles, a
new table, S3Express or a second region (LogDrive 6.1, Figure 10, did S3 to DynamoDB live, one engineer, about a week),
and tiering old log ranges to S3 (BackupLoglet, Delos 4.2). Also, a standby can take over by sealing at once instead of
waiting for lease expiry (3 s in L). Under idea A, safety needs no seal, so this is insurance, not a fix.
**Risks.** Sealing on DynamoDB needs a consistent read of the seal item after each write batch (cheap) and the seal
check must not be linearized before the write (the erratum LogDrive cites exists; I did not read it). Extra moving parts in
the sim. Do the **interface only** now; build the chain register when a second loglet is actually wanted.

### D. Stripe the one total order versus shard by partition

- **Stripe the single order (cheap, no spec change).** Slot s goes to stripe `s mod S` (LogDrive 3.3, Delos 4.2.2);
  checkTail fans out and takes the max; hot-key limit becomes S x 250 entries/s. LogDrive needed exactly this on
  DynamoDB (6.3). Not needed below ~250 segments/s; keep as the answer to E2 failing.
- **Shard by partition hash (not recommended now).** Per-partition offsets need only per-partition order, so S
  independent logs would remove the total order. But a segment touches ~150 partitions in every shard, so `CommitSegment`
  stops being atomic per segment (needs a cross-shard marker or accepts partial visibility: a spec and oracle change).
  Conflux shards by topic range *and* affinitizes brokers to ranges (5.3); we cannot affinitize random clients
  (w3 affinity was a benchmark trick, not comparable). Revisit only past ~500 segments/s or for multi-topic isolation (D5).

### E. Rejected or low value

- **Segment object as the log entry (S3 data-plane append).** Variant 1: the broker gets a slot from the CP, then PUTs
  `log/<slot>` with `If-None-Match`. This is the AtomicLog `acquireSlot` protocol (LogDrive 4). A broker that dies
  after acquiring a slot stalls the window; the paper handles that by sealing the whole log. That breaks decisions 29/44
  (brokers are cattle) unless the CP fills holes, which adds a hop and a failure mode to the ack path. Variant 2: embed
  the batch table as a footer so S3 alone can be rescanned (LogDrive 5.2 "reconstruct the system entirely from S3 and
  Conflux"): useful for disaster recovery, but a scan loses the order. The most DynamoDB it could save is the
  ~$0.2-0.5k/month commit cost. Not worth it.
- **S3 as the commit log's loglet.** The LogDrive authors found S3 worse than DynamoDB on latency and cost (6.3), and
  our w2 measured PUT p50 of 93 ms. Only worth it as a cold tier (C).
- **Cross-region quorum LogDrives.** Out of scope; not asked for.

## 5. Ranking

| # | idea | gain at our bottleneck | build | main unknown |
|---|---|---|---|---|
| 1 | A: command log + learner CPs, pipelined | same as L, plus fresh Lookup anywhere, hot standby, no `G#`, no lease | about L's size (L ~20 days; A replaces the tx with Put and adds apply/hole-fill) | single Put latency, contention, hole fill |
| 2 | B: checkpoint to S3 + trim, index off DynamoDB | removes the 40k-partition index cost ($9.8k to ~$0.3k/month, computed) | checkpoint/trim 3 days; compacted S3 index weeks | index memory, cold Lookup latency |
| 3 | C: Loglet seam, chain later | insurance, faster takeover | interface 1-2 days | seal check ordering |
| 4 | D: stripe the order | hot-key insurance only | 2 days when needed | none until E2 |
| - | D shard by partition, E | not now | | |

## 6. Recommendation: prototype two, in this order

1. **A, with its checkpoint/trim half from B**, as a variant of L, not a rival: same indexer, same broker contract. First
   in the sim and fakedb (no AWS), then X1 on AWS. The decision between L and A is cheap to make after X1 and X2.
2. **B's cost model at 40k partitions**, measured before building (X4). If it confirms about 6k small items/s, then the
   async R# index in L must change (adaptive F or S3 index) before anyone plans a 40k-partition run.

## 7. Cheap experiments

- **X1 (AWS, under $2).** Sequential slots on one pk, `PutItem` 4 KB with `attribute_not_exists`, K = 1, 4, 16 in
  flight, at 30, 64, 128, 250 entries/s, 10 minutes each. Record p50/p99, throttling, and compare with L's E1 and E2. Kills or
  confirms A's latency and hot-key assumptions.
- **X2 (local, no AWS).** Wrap `veritas.ControlPlane` as a state machine over a command log. Run 3 replicas with
  random interleavings, duplicate keys, lost slots, holes and a hole-filler. Check `StateHash` equality and the oracle.
  Also benchmark apply rate in Go (the Delos 5.1 playback limit).
- **X3 (AWS, under $1).** Consistent `Query` Limit 1 descending and `Query sk > n` at 20/s: latency and RRU; then
  50 ms bus-stand batching of lookups.
- **X4 (AWS, under $2).** `BatchWriteItem` of 6k x 60-byte items/s (the 40k-partition index, F=1 s), then 3k/s at
  F=10 s. Confirm 1 WRU per item minimum, and measure latency and `UnprocessedItems`. A local script can also check the
  distinct-partition count with a random simulation.
- **X5 (local).** Checkpoint size and load time for 40k partitions' `ends` plus 1 h of key set (about 100k keys x 16 B).

## 8. Interaction with project constraints

- **Determinism and sim:** A makes the CP a pure state machine over an ordered input, the best case for deterministic
  simulation; the new sim surface is the append/hole-fill/tail protocol (Conflux authors report their simulator found
  a linearizability bug in their S3 LogDrive and a seal-ordering bug, LogDrive 4.1). Every trace hash changes, as in L.
- **Decision 29/44:** brokers are untouched; the CP also becomes cattle (any replica works).
- **Decisions 41-47:** at-least-once stays; key idempotency becomes windowed (a product decision). Decision 46's "one
  sequencing thread" becomes "one preferred appender", correctness no longer depends on it.
- **DynamoDB and S3 only:** every idea uses only them; no new stateful service (the replicas are CPs we already run).
- **Not shown by any paper:** that any of this reaches 1 GiB/s on our workload. L's model (219-432 segments/s) still
  stands; ideas A and B change risk and cost shape, not the first-order throughput.
