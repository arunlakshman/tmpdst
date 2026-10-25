# Commit path redesign: a sequenced log with an asynchronous index

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This document is a record.*

Status: PROPOSAL (2026-10-20). Nothing here is built. No code or other doc changed. Evidence labels as in
`surprises-audit.md`: **measured** (AWS run w2), **modelled** (local fakedb experiment, §6), **computed**, **suspected**.

## 0. Recommendation in one paragraph

Adopt **design L**. A commit becomes ONE `TransactWriteItems` per group of segments, holding one `G#<segkey>` item per
segment and one `LOG/<seq>` item. The `G#` item is binary and stores the request and its bases. The `LOG/<seq>` item is the
next slot of a single totally ordered log, written with `attribute_not_exists`. That slot is the new compare-and-set
safety net: it replaces the per-partition run-item CAS. Offsets come from the sequencer's memory, as today. The per-partition
run items that `Lookup` reads are written **after the ack**, by an indexer in the same process: about once a second, with
`BatchWriteItem` and no transaction, one run item per partition covering many segments. The active CP answers `Lookup`
for the not-yet-indexed tail from memory. A restarting CP rebuilds that tail from a checkpoint item plus the log after it.

Commit cost no longer depends on partitions per segment. Measured transaction size goes from 31-97 items to 2-8. The
model puts CP capacity at 200-430 segments/s, against 30-64 needed. DynamoDB writes fall from a predicted ~12.7k WRU/s
to ~0.6-0.8k WRU/s at 1 GiB/s, which is about $1.0-1.3k/month at us-east-1 on-demand prices. The broker contract
(`CommitSegment`, `Lookup`) and `veritas/controlplane.go` keep their behaviour. A second sequencer still costs only
retries. The one new restriction is that **only the active CP may serve `Lookup`**; §5.6 explains why.

## 1. The problem in numbers (measured, w2, `results/w2`)

Per 10 s window, from `results/w2/metrics/cp` (deltas of `manicp_seq_group_items`,
`manicp_dynamodb_call_seconds{TransactWriteItems}` and consumed WRU):

| stage | tx/s | items/tx | tx ms | WRU/tx | segments/tx |
|---|---|---|---|---|---|
| smoke 100k | 22.2 | 31.3 | 31 | 96 | 1.00 |
| ramp 250k | 18.0 | 55 | 44 | 168 | 1.00 |
| ramp 500k (aborted) | 15.5-16.1 | 82 | 65-67 | 209-221 | 1.12-1.22 |

- **Transaction latency grows with items.** The fit is **tx_ms ≈ 8 + 0.7 x items** (31→30, 55→47, 82→65). On top of
  that, one group runs at a time, because every segment shares partitions with every other one, so the sequencer cannot
  pipeline (decision 46).
- **The cluster is bounded at about 1/tx segments/s.** The broker commits one segment at a time, in seal order. At 500k
  the broker queue wait reached 8.5-10 s.
- **WRU per transaction is about 2 x items, plus about 40 for the JSON `G#` blob.** Request bodies average 11.3 KB per commit.
- **About 94 batches per segment** (11.3 KB body / ~120 B of JSON per batch), over 31-55 distinct partitions.

**What 1 GiB/s means (computed).** Use the measured Java batch shape (79.5 KB lz4, 155 records; audit §2.3). 1M msg/s is
**6,450 batches/s and ~500 MB/s stored**. At 250k, w2 stored 124 MB/s, so 1M would store ~495 MB/s.
- 16 MiB segments: **~30 segments/s** of ~215 batches, touching ~150 partitions each.
- 200 ms windows with 12 brokers, or 8 MB segments: **~64 segments/s** of ~100 batches.
- Today's partition cap of 96 seals first: ~117 batches and 9.4 MB, so **~53 segments/s of 97-item transactions**.

At 76 ms per transaction that is 13 tx/s. Today's layout is therefore capped at ~13 x 117 x 155 ≈ **240k msg/s**, which
is the plateau w2 hit.

## 2. Pricing assumption

DynamoDB on-demand pricing in **us-east-1** is **$0.625 per million WRU and $0.125 per million RRU**. This is the
current AWS on-demand page; the price was halved in Nov 2024. A transactional write costs 2 WRU per KB, rounded up per
item. A plain `PutItem` or `BatchWriteItem` costs 1 WRU per KB. Storage costs $0.25 per GB-month.

`summarize.py` and the comparison doc use **$1.25/M**, which is the pre-cut price, so their dollar figures are 2x too
high. The repo's "$43-59k/month" corresponds to **$21-30k/month** at today's price.

The **ap-south-2 price was not verified**. Asia Pacific regions usually list 10-20% above us-east-1; check it with the
AWS pricing calculator (§10, E5). All dollar figures below are us-east-1, 730 h/month. Multiply by your region's ratio.

## 3. Constraints kept

- Brokers stay interchangeable and stateless: no partition ownership (declined) and no broker fencing (decision 44).
- At-least-once delivery, with no idempotent producers (decision 41).
- One active manicp sequences, but **correctness never depends on there being only one**: a storage-level CAS remains
  the safety net, and split brain only costs retries.
- `CommitSegment` stays atomic per segment and idempotent per key. Its `ErrConflict` meaning is unchanged. `Lookup`
  semantics are unchanged.

## 4. Designs compared

### C: keep the layout; more commits in flight per broker; bigger groups (rejected)

**Arithmetic limit.** A transaction holds `1 per segment + |union of partitions| + grown topics <= 100` items. For k
random segments of P partitions out of 288, the union is `288 x (1-(1-P/288)^k)`:
- P=31: k <= 3.
- P=55: k = 1-2 (two segments need ~99.5 items).
- **P >= 60: k = 1.**
- P > 99 (16 MiB segments at 1 GiB/s) cannot commit at all, so the 96-partition cap is forced.

The work per second is therefore at least the number of distinct (partition, segment) pairs, which is ~3.8k-6.4k items/s
at 1 GiB/s. That needs **38-64 full transactions/s, against the 13-16 measured**. Getting there needs 3-5 transactions in
flight, and they must be on disjoint partitions, which random segments never are (audit S1). Chaining transactions on
one partition without waiting would leave an offset hole whenever an earlier one fails. Allowing more commits in flight
per broker only moves the queue from the broker to the CP.

Cost: 6.4k items x 2 WRU plus the `G#` blobs ≈ **12.7k WRU/s ≈ $21k/month**. **Plateau: ~240k msg/s (measured and
modelled).** This is a dead end. The run items have to leave the transaction.

### A: G# items plus a per-topic "ends" item (CAS by version); asynchronous index

- **Transaction:** k `G#` items, plus one `ENDS#<topic>` item holding every partition's next offset and `ver`, under the
  condition `ver = :old`. **Recovery:** read the ends items; one read, no replay.
- **The catch:** the indexer and recovery still need the **ordered list of committed segments** after the index cursor.
  The ends item does not provide it. A GSI on a sequence number is eventually consistent and so unsafe for recovery.
  Design A therefore also needs a log item, and the ends item becomes a per-transaction checkpoint of what the log already
  implies.
- **The ends item is the hot spot:**
  - at 288 partitions it is ~1.5 KB, costing 4 WRU per transaction;
  - at ~2,400 partitions and 40 tx/s it reaches DynamoDB's 1,000 WCU/s per-key limit;
  - beyond that it needs paging, which brings back items that grow with partitions.

**Verdict:** design L below with a checkpoint written on every transaction. It is strictly costlier, and its only benefit
is a recovery that is ~1 s faster.

### B: pure log, a single `PutItem` per group, `G#` written later

- **Commit:** one `PutItem LOG/<seq>` holding the whole group, under `attribute_not_exists`. **Idempotency:** an
  in-memory key map, with `G#` written asynchronously.
- **Cost and latency:** the cheapest option, roughly half of L's commit WRU (~150 WRU/s, ~$250/month), and probably a few
  ms faster than a 3-item transaction (suspected; E1 measures it).
- **Weaker guarantee:**
  - With two active CPs, a segment resent to the other CP after a lost answer can be committed twice. Its records
    then appear at two offset ranges.
  - After a restart, every unknown key needs a consistent `GetItem G#` before it can be logged, and that check is
    itself racy under split brain.
  - That breaks "idempotent per key" exactly in the case the safety net exists for.

**Kept as a fallback** if E1 shows that small transactions are much slower than `PutItem` (§11).

### D: other alternatives considered

- **D1, run pages** (run items sharded into ~16 page items per transaction): items are bounded by pages, but a page item
  keyed by sequence cannot be searched by offset, so `Lookup` needs a scan or a second index, and index bytes stay in
  the transaction (2 WRU/KB). Inferior to an asynchronous index.
- **D2, GSI on (topic, partition, base):** a GSI gives one key per item, but a segment covers ~150 partitions, so it
  needs one item per (segment, partition) again. GSI writes are billed again and eventually consistent.
- **D3, DynamoDB Streams to a separate indexer:** works, but adds ~0.5-1 s of lag, a shard-reading component and a
  second writer of run items, and the CP must replay the log anyway to serve the tail. Defer it; it matters only if
  indexing moves out of the CP process.
- **D4, index in S3** (WarpStream-style metadata files): compact the run index into S3 objects and keep DynamoDB for the
  log and checkpoints only. It is the long-term answer for consumer read cost, but out of scope for produce throughput.
- **D5, sharding the sequencer by topic:** helps multi-topic deployments. The OMB workload has one topic, so not now.

### L (recommended): log-slot CAS in a small transaction, plus an asynchronous per-partition index

## 5. Design L in detail

### 5.1 Items (one table, pk/sk strings, on demand, as today)

| pk | sk | attributes | written by | when |
|---|---|---|---|---|
| `G#<segkey>` | `-` | `b` packed binary, `seq`, `ep` | commit transaction | sync, condition `attribute_not_exists(pk)` |
| `LOG` | `<seq %020d>` | `keys` (the group's segment keys in queue order), `ep`, `ttl` (now + 7 d) | commit transaction | sync, condition `attribute_not_exists(pk)` |
| `TOPICS` | `<topic>` | `parts` | commit transaction (rare) / CreateTopic | as today |
| `R#<topic>#<p>` | `<base %020d>` | `base cnt runs` (today's run format, `runs.go`) | indexer | async, `BatchWriteItem`, unconditional |
| `CKPT` | `-` | `seq` (indexed through), `plan` (pending range c..h, or none), `ends` (packed per topic/partition, at `seq`), `ver` | indexer | async, `UpdateItem` with condition `ver = :v` |
| `LEASE` | `-` | `owner`, `ep`, `exp` (ms) | active/standby CP | heartbeat, conditional |

**`G#` format.** It holds:
- a header;
- a per-partition table of (topic index, partition, first base): ~8 bytes per partition;
- per batch, varints for partition index, records, offset and length: ~10 bytes per batch.

That is ~3.4 KB for a 16 MiB segment (150 partitions, 215 batches) and ~1.7 KB for 100 batches. The full answer follows
from it: bases chain in request order. The request needed for the `ErrConflict` comparison is the same data without the
bases.

**`LOG` item.** It lists keys only, ~0.1-1 KB. Every `LOG` write goes to one partition key. At ≤ 85 tx/s x 2 WRU that is
≤ 170 WCU/s, below the 1,000 WCU/s per-key limit, and suspected safe (E2).

**`LOG` TTL.** 7 days, with an alarm if index lag exceeds a minute. The TTL must never delete unindexed entries.

### 5.2 Commit (active CP)

1. **Admission** (as in today's Sequencer):
   - A malformed request is `ErrInvalid`, after a `G#` read for replay.
   - A key in the recent-key LRU is answered as a replay, or `ErrConflict`, with no I/O.
   - A key that is already in flight attaches to that commit.
2. **Group formation.** One loop, **one transaction in flight**. The group takes queued requests in order, within these
   limits:
   - k ≤ 98 segments;
   - k + 1 + grown topics ≤ 100 items;
   - total `G#` bytes ≤ 1 MiB (the transaction limit is 4 MB).

   No artificial wait. Bases come from `ends` in memory, in queue order, and `ends` is advanced tentatively.
3. **The transaction:**
   - k x `Put G#`;
   - `Put LOG/<seq>`;
   - `Update TOPICS` for each grown topic;
   - `ClientRequestToken = hash(ep, seq)`, which makes a resend idempotent for 10 minutes.
4. **Success:** append the batches to the in-memory tail, insert the keys into the LRU, set `seq++`, and answer every request.
5. **TransactionCanceled.** The per-item reasons say what happened:
   - **`LOG` slot taken:** another sequencer exists.
     - Reload: `Query LOG sk > last` (consistent), then `BatchGetItem` its `G#`s, then apply them.
     - Rebuild the group at the new seq with recomputed bases.
     - Count the event in `manicp_seq_log_conflicts_total`.
   - **A `G#` exists:** read it, answer that request as a replay or `ErrConflict`, and retry the rest at the same seq.
   - **`TOPICS`:** reload that topic.
   - **Throttling or TransactionConflict before apply:** retry with the state kept.

   Retries are bounded (40 attempts / 8 s, as today). After that the CP returns 503 and the broker resends under the same key.
6. **Unknown outcome** (timeout, 5xx, reset):
   - Resend the identical transaction with the same token until the outcome is definite.
   - If the token has expired, or another writer is suspected, read `LOG/<seq>` (consistent) and compare its `keys`.
   - An applied group is answered from its stored bases, so an applied commit is never assigned twice.

**Why a single transaction in flight is required.** Suppose the CP sent slot n+1 before slot n returned. If slot n is
then lost to another writer, slot n+1 can still succeed, and its offsets were computed from a state that never existed.
The CAS on slot n+1 cannot see slot n. A single-item condition cannot express "after my n", and a `ConditionCheck` on n
inside the n+1 transaction races with the transaction that writes n. With one transaction in flight, contiguity proves
the writer's view: a successful `LOG/n` means the writer saw slots `0..n-1` exactly. Throughput instead comes from
group size, and §6 shows that is ample.

### 5.3 Indexer (same process, active CP only)

Every F = 1 s, or when the tail exceeds 50k batches:
1. **Plan.** Let c = `CKPT.seq` and h = the last acknowledged seq. Set `CKPT.plan = (c, h]` with a CAS on `ver`.
2. **Compute.** One run item per partition touched in (c, h], at `base` = the partition's end at c, containing that
   partition's batches in log order. This is **deterministic** from the log and the `G#` items.
3. **Write.** `BatchWriteItem`: 25 items per call, ~8 calls in parallel, retry `UnprocessedItems`.
4. **Advance.** `CKPT.seq = h`, `ends` = ends at h, `plan = none`, with a CAS on `ver`.
5. **Trim** the in-memory tail of entries with seq ≤ h, after a grace period of one `Lookup` timeout.

A pending `plan` found at recovery, or by a second indexer, is **completed with the same range**. Every writer therefore
produces byte-identical items, so a duplicate index writer is harmless. Run items already written above `CKPT` but not
yet covered by it are ignored by `Lookup` (5.4).

### 5.4 Lookup and Fetch

**Active CP.** It keeps `idxEnd[p]` (ends at `CKPT.seq`), the tail (runs after it), and `end[p]`.

| case | how `Lookup(TP, From, MaxBytes)` is answered |
|---|---|
| From ≥ idxEnd[p] (consumers at the head, ListOffsets latest) | **from memory, with no DynamoDB read**. This also removes the `End` Query that audit S3 counted. |
| From < idxEnd[p] | Query `R#` runs in [From, idxEnd[p]) as today, drop anything ≥ idxEnd[p], then append tail runs until MaxBytes |

**Consistency.** The active CP's answer equals the spec. Every acknowledged commit is in its memory before the ack, and
every older one is either in memory or indexed. So a consumer can read a record right after its ack, through any broker,
before indexing and after it, exactly as today.

**Non-active CP.** It **refuses `Lookup` with 503**. The broker turns that into a retriable Fetch error such as
`NOT_LEADER_OR_FOLLOWER`, which needs a small broker change. The reason is that a CP serving only indexed data would
report a smaller `End`. `broker/fetch.go:58` answers `From > End` with `OFFSET_OUT_OF_RANGE`, and consumers with
`auto.offset.reset=earliest` would then rewind to 0.

### 5.5 Recovery (start, or takeover by a standby)

1. Acquire the lease (5.7).
2. Get `CKPT`. If a `plan` is pending, finish it.
3. `Query LOG sk > CKPT.seq` (consistent, paged).
4. `BatchGetItem` those entries' `G#` items, then apply them in order. That rebuilds `end`, the tail and the recent-key LRU.
5. Set `seq = last + 1` and serve.

Size: with F = 1 s and a healthy indexer the replay is ≤ ~100 entries and ≤ 300 KB of reads, well under a second
(suspected; E6 measures it). Older segment keys stay idempotent through their permanent `G#` items: a conditional put
fails, `G#` is read, and the request is answered as a replay.

### 5.6 Split brain and the precise guarantees

Two CPs X and Y both commit:
- Each transaction needs the next `LOG` slot, so only one can take slot n.
- The loser reloads the winner's entries before it assigns anything.
- **Offsets are never duplicated and never holed.** That follows from contiguity plus the CAS.
- **A segment key is never committed twice**, because its `G#` condition is in the same transaction.
- The cost is retries, exactly as today.
- Two indexers converge on one plan through the `CKPT` CAS and write identical items.

**One guarantee is weaker than today.** Today any CP reads DynamoDB directly, so `Lookup` is fresh everywhere. Under L:

> `Lookup` on CP X reflects every commit X acknowledged, every indexed commit, and every log entry X has replayed.

So if two CPs both believe they hold the lease (clock skew beyond the margin), X can briefly report a smaller `End` than
Y. A consumer whose offset came from Y could then get `OFFSET_OUT_OF_RANGE` from X. The window lasts until X's next log
conflict or its next lease check.

Mitigations:
- Lookups are served only while the lease is held, with a safety margin (lease 3 s, serve until 2 s).
- An idle active CP re-reads the `LOG` head (`Query` Limit 1, descending, consistent) once per lease period.

The commit path does not depend on clocks.

### 5.7 CP host failure, lease and standby

`LEASE` item:
- The active renews every 1 s with `UpdateItem ... condition owner = me`, setting `exp = now + 3 s`.
- The standby polls once a second. Once `exp < now - margin` it takes the lease with a conditional update that sets
  `ep + 1`, then recovers (5.5).
- The old active stops serving on its first failed renewal (it returns 503 "not active"). Any transaction it still has
  in flight either lands, and the new active finds it by the slot CAS, or it does not.
- `ep` is diagnostic only: it is written into `LOG`/`G#` but never compared for safety.

Brokers get a **list** of CP URLs (`-cp-url a,b`) or a health-checked NLB / DNS name. cpclient tries the next URL on a
refused connection or on 503 "not active".

Expected failover: ~3 s lease expiry + under 1 s of replay ≈ 4-5 s of stalled acks. That is far below
`CommitDeadline` (120 s), so **no produce fails**. Today a lost host means minutes and failed produces (runbook A10).
Without a standby, L behaves like today plus a sub-second replay at restart.

### 5.8 Broker: does "one segment commit in flight" have to change? No.

**What the rule guarantees.** Seal order equals commit order equals offset order for each partition and connection.
Kafka clients rely on that ordering when retries are off. Buffer accounting releases in order.

**What it limits.** A broker's commit rate is at most 1 / commit latency, and under this rule group size is at most the
number of brokers.

**What L needs.** At 64 segments/s over 6 brokers, each broker needs 10.7 commits/s, which means a commit latency of
≤ 93 ms. The modelled latency is 15-28 ms, and 54 ms in the pessimistic case (§6). At 12 brokers the requirement is
looser. **So keep the rule.**

**What the broker must guarantee** (it already does):
- one key per segment, reused on every resend;
- resend after an indeterminate answer until `CommitDeadline`;
- ack only from the CP's answer;
- one commit in flight, so the CP sees a broker's segments in seal order. The CP keeps FIFO order inside a group.

**If it ever has to change**, there is an ordered variant. The broker sends segment i+1 with `after = key(i)`. The CP
queues it behind i, places both in the same or a later group, and fails i+1 if i fails. That is a contract field and a
spec rule, so it should be built only if E1 shows commit latency above ~90 ms.

**Other broker changes:**
- `MaxSegmentPartitions` (96) exists only for the transaction item budget. Under L it can go to 4096. Segments then seal
  by size or timer, giving ~30 segments/s of 16 MiB at 1 GiB/s.
- `MaxSegmentBatches` (256) now bounds the `G#` size: at ~10 bytes per batch it could be ~10k. Keep 1024.
- Map CP 503 on `Lookup` to a retriable Fetch error.

## 6. Local model (fakedb with a latency model fitted to w2)

**Program.** `commitmodel` lives in the session scratchpad and is not committed. It wraps `cp/dynamo/fakedb` to sleep
**8 + 0.7 x items ms** per `TransactWriteItems` and 4 ms per read. N brokers seal segments open-loop at the offered rate
with random phases, and each broker commits one segment at a time in seal order. Ten seconds per row.
- `seq` runs today's real `dynamo.Sequencer`.
- `log` is a minimal prototype of L's commit loop: a serial loop over groups of k binary `G#` puts (~11 bytes per batch)
  plus one `LOG` put, with bases in memory and no indexer (which is off the ack path).

| mode | offered seg/s | partitions / batches per segment | committed seg/s | tx/s | items/tx | commit avg / p99 ms | broker queue avg / p99 ms |
|---|---|---|---|---|---|---|---|
| seq (today) | 19.6 | 31 / 60 | 18.8 | 12.6 | 45.5 | 62 / 122 | 1 / 1 |
| seq | 20 | 55 / 94 | 19.3 | 17.2 | 61 | 111 / 246 | 1 / 2 |
| seq | 30 | 55 / 94 | **21.0** | 16.2 | 68.6 | 272 / 334 | 1,221 / 2,479 |
| seq (1 GiB/s shape, partition cap) | 53 | 96 / 117 | **12.2** | 12.2 | 97 | 471 / 528 | 3,393 / 6,996 |
| log (L) | 30 | 150 / 200 | 29.4 | 24.5 | 2.2 | 15 / 23 | 1 / 2 |
| log | 64 | 150 / 200 | 63.2 | 42.2 | 2.5 | 15 / 24 | 1 / 2 |
| log | 128 | 150 / 200 | 127 | 84.9 | 2.5 | 16 / 25 | 1 / 2 |
| log, 12 brokers | 256 | 150 / 200 | 254 | 84.1 | 4.0 | 18 / 26 | 1 / 2 |
| log, 12 brokers | 512 | 150 / 200 | **432** | 72.1 | 7.0 | 28 / 29 | 754 / 1,489 |
| log, pessimistic tx = 15 + 1.5 x items | 64 | 150 / 200 | 63.2 | 42.2 | 2.5 | 28 / 44 | 1 / 2 |
| log, pessimistic, 12 brokers | 256 | 150 / 200 | **219** | 36.7 | 7.0 | 54 / 59 | 652 / 1,317 |

**Reading it:**
- **The model reproduces the w2 plateau.** For today's layout it gives 21 segments/s at 55 partitions; w2 measured 16-18.
  Treat the model as about 20% optimistic, since it has no network jitter.
- **At the 1 GiB/s shape today's layout falls to 12 segments/s (~220k msg/s)**, because of 97-item transactions.
- **L holds 30-128 segments/s with p99 commit ≤ 25 ms.** It saturates only when every broker is waiting: 432/s =
  12 brokers / 28 ms, which is the one-in-flight bound of 5.8, not a CP bound.
- **With pessimistic latency, L still delivers 219 segments/s, which is 3.4x the 64/s needed.**

**Caveat.** The 0.7 ms per item was fitted on ~1 KB run items spread over many partitions. A 2-3 item transaction of
2-4 KB items on AWS is unmeasured (E1).

## 7. Throughput, WRU and cost at 1 GiB/s (computed)

| | today (C) | L, 30 seg/s (16 MiB) | L, 64 seg/s |
|---|---|---|---|
| transactions/s needed | 53 (achievable: ~13) | ~25 (modelled) | ~42 (modelled) |
| items per transaction | 97 | 2.2 | 2.5 |
| commit WRU/s | ~12.7k (S7: 240 per commit) | 30 x 8 (`G#` 3.4 KB) + 25 x 2 (`LOG`) ≈ 290 | 64 x 4 + 42 x 2 ≈ 340 |
| index WRU/s (F = 1 s) | (in the transaction) | 288 run items/s x 1-1.5 KB ≈ 300-450 | same |
| CKPT, LEASE | 0 | ~3 | ~3 |
| **total WRU/s** | **~12.7k** | **~0.6-0.75k** | **~0.65-0.8k** |
| **$/month, us-east-1 at $0.625/M** | **~$21k** ($42k at the repo's $1.25) | **~$1.0-1.2k** | **~$1.1-1.3k** |
| added ack latency (commit) | 35-60 ms, then seconds of queue | 15-25 ms modelled | same |

Notes on the index row:
- The index cost is bounded by bytes, not items. That is 6,450 batches/s x ~45 bytes (including the segment-key table)
  ≈ 290 KB/s ≈ 290 WRU/s.
- F only removes per-item rounding: F = 5 s brings it near ~300 WRU/s, at the price of a 5x larger tail and replay.

Reads in steady state are ~0: no reads on commit, and the head of the log is served from memory.

Storage grows by ~8-16 GB/month (`G#` plus runs; no GC, audit S13), about $2-4/month added each month.

**Comparison with WarpStream** at 1 GiB/s:
- Their control-plane fee is **$7,541/month**.
- L's DynamoDB is about **$1.2k/month**, plus one or two m6in.xlarge CP hosts (~$0.2-0.4k/month).
- S3 PUTs at 30-64 PUT/s cost ~$0.4-0.8k/month, against their $1,788.
- Consumer `Lookup` reads are not counted here (TODO "Consumer read path"). L removes the `End` query and the tail
  queries for caught-up consumers.

**Ack latency target (suspected).** Publish p50 at 250k was 446 ms: linger 25 ms + window 200 ms + PUT 93 ms + commit
55 ms + client queueing. L cuts the commit share to ~20 ms. p99 ≤ 1 s depends on S3 PUT p99 for 16 MiB objects, which is
not measured at that size.

## 8. Impact on the spec, contract, simulator and tests

- **`veritas/controlplane.go`:** behaviour is unchanged. Only the comment changes: "real stores guard step 3 with a
  compare-and-set on the run base" becomes "on the next slot of a sequenced log". `CommitSegment` and `Lookup` are
  unchanged on the wire and in `core`. Decision 21 holds.
- **`ErrTooLarge`:** the rule becomes "`G#` blob over ~300 KB or > 98 segments' worth", instead of "1 + partitions > 100".
  The spec never modelled it (decision 32).
- **sim:**
  - `cpengine`'s `MaxCommitItems` rule becomes a batch/byte bound.
  - The broker's `MaxSegmentPartitions` default changes.
  - So every sim/kafka trace hash changes. Re-baseline, review the golden file, and keep the
    `SegmentCaps`/`BugNoPartitionCap` mutants (or retire them if the cap goes).
  - The oracle does not change.
- **cp/cptest:**
  - The lockstep conformance (C3) runs the new store against the spec on fakedb and ExtendDB.
  - Add **(a)** two L sequencers on one table with random interleavings and injected faults (lost answers via
    `AfterApply`, cancellations via `BeforeTransact`). Checker: per-partition offsets are contiguous and unique, each
    key is committed once, and `Lookup` equals the spec.
  - Add **(b)** a crash at every step: after the transaction, after `plan`, mid `BatchWriteItem`, before the `CKPT`
    advance. Restart and compare `StateHash` with the spec.
  - Add **(c)** `Lookup` during index lag, and a 503 from a non-active CP.
  - Add **(d)** idempotent replay after restart, using the `G#` path.
  - fakedb needs `PutItem`/`BatchWriteItem` and `ClientRequestToken`. Whether ExtendDB supports the token is unknown;
    test it.
- **Decisions:** a new decision 48 supersedes the synchronous run-item parts of 39 and 46, the run-base CAS of 42 (the
  CAS moves to the log slot), and the item arithmetic of 35/46.

## 9. Effort (one engineer, days)

| component | work | days |
|---|---|---|
| cp/dynamo `LogSequencer` | queue and group loop (reused from seq*.go), binary `G#`, `LOG` slot transaction, token, cancel/unknown handling | 4 |
| tail and `Lookup` merge | in-memory tail, `idxEnd`, `End` from memory, active-only serving | 2 |
| indexer | plan/write/advance with `CKPT`, `BatchWriteItem`, fakedb support | 3 |
| recovery | `CKPT` + `LOG` replay + `G#` batch get, resume a pending plan | 2 |
| lease and standby | `LEASE` item, cpclient URL list, 503 "not active", broker retriable Fetch code, terraform second CP | 3 |
| tests | cptest (a)-(d), C3 on ExtendDB, sim cap change and re-baseline | 5 |
| ops | metrics (`manicp_log_*`, index lag, conflicts), runbook, decision 48 | 1 |
| **total** | stage 1 alone ≈ 11 days | **~20** |

## 10. Experiments only AWS can answer (each under $5, run from the CP host on a throwaway table)

- **E1: small-transaction latency.** Measure p50/p99 of `TransactWriteItems` with 1 `LOG` item and 1, 2, 4 and 8 `G#`
  items of 2-4 KB, at 25, 50 and 100 tx/s, for 10 minutes. Compare with `PutItem` of 4-8 KB. This decides L versus B and
  whether the ordered-pipelining variant is ever needed.
- **E2: hot `LOG` key.** Run the same loop with sequential sk on one pk at 100 tx/s, and also at a 3 KB `LOG` item, for
  5 minutes. Check throttling and latency drift.
- **E3: indexer writes.** `BatchWriteItem` of 288 items of 1-2 KB per second. Measure latency, `UnprocessedItems` and
  WRU (`ReturnConsumedCapacity`).
- **E4: billing check.** Confirm 2 WRU/KB for the transaction items and 1 WRU/KB for `BatchWriteItem`, and that a
  cancelled transaction costs what w1 suggests.
- **E5: price.** ap-south-2 WRU/RRU prices from the pricing calculator or price list.
- **E6: failover drill.** With two CPs, kill the active one mid-ramp. Measure the ack stall, the replay time, and that
  there are 0 failed produces and 0 offset anomalies (oracle-style check of the consumed log).

## 11. Staged plan

| stage | build | measure on AWS | expected plateau |
|---|---|---|---|
| 0 | nothing | E1-E5 (30 min on the next stack) | confirms or kills the latency model |
| 1 | `LogSequencer` + indexer + tail `Lookup` + recovery, single CP, behind `-commit=log`; broker partition cap raised | ramp 250k, 500k, 750k, 1M produce-only: `manicp_seq` group size, tx ms, WRU/s, index lag, broker queue wait ≈ 0 | **CP not the bound up to ≥ 3x target (modelled).** The next limit becomes visible: broker single-loop CPU (audit S10, ~240 MiB/s laptop ceiling vs ~167 MiB/s logical per broker at 1M), or S3 PUT time. Expect ≥ 750k; 1M likely but unproven |
| 2 | lease + standby + URL list | E6 under 500k load | same throughput; CP loss costs a ~5 s stall instead of a lost run |
| 3 | consumer path (TODO): long-poll Fetch, `End`/tail from memory (already in L), read cache | vanity with consumers | consume rate; RCU/s per consumer |
| 4 (only if E1 or stage 1 says so) | ordered per-broker pipelining (`after` key) or variant B | — | lifts the bound of brokers / commit latency |

## 12. What would change this recommendation

- **E1 shows 2-3 item transactions at p50 > ~60 ms or p99 > 300 ms**, with `PutItem` much faster. Then build variant B
  (accepting a possible double commit under split brain), or add ordered pipelining (5.8).
- **The hot `LOG` key throttles** (E2). Shard `pk` by `seq >> 16`. It is still one ordered stream; the shard is chosen
  by seq.
- **The product owner requires `Lookup` from any CP to be as fresh as the active one.** Then the index must be written in
  the commit, and we are back to item cost growing with partitions (design C or D1), or to every CP reading the log
  head on each `Lookup`.
- **Topics with many thousands of partitions, or many topics per group.** `CKPT.ends` needs paging, and grown-topic
  items eat into the 100-item budget. Revisit with D5 (sequencer per topic).
- **Measured broker CPU, not the CP, caps below 1M** after stage 1. Then broker work comes first; this design does not
  help there.

## 13. Decisions needed from the product owner

1. **Accept an asynchronous index with active-only `Lookup`.** A non-active CP answers 503 and the client retries. The
   only weaker guarantee is Lookup freshness during a dual-active window (5.6); offsets and idempotency are unchanged.
2. **Active/standby CP with an advisory DynamoDB lease, and a CP URL list (or NLB) on brokers**, in stage 2. The
   alternative is to stay single-CP and accept minutes of outage on host loss.
3. **Lift the broker's partition cap** (96 to 4096; segments seal by size or timer), and accept a sim trace-hash
   re-baseline.
4. **Index flush interval F = 1 s by default.** Larger saves little WRU (~150/s at most) but lengthens replay and the
   in-memory tail.
