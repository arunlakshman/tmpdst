# WarpStream agent vs this repo: architecture comparison

Date: 2026-10-21. Branch toys3. Status: desk study from PUBLIC sources only.

## 0. Scope and method (read first)

- **Sources used:** docs.warpstream.com, warpstream.com blog and marketing pages, third-party summaries found by search.
  Short quotes only. Our side comes from `docs/decisions.md`, `commit-path-redesign.md`, `log-cp-service.md`,
  `design-a-prototype.md`, `x1-x3-x4-results.md`, `w2-bottleneck-verification.md`, `aws-benchmark-warpstream-comparison.md`.
- **The container image was NOT studied.** The local Docker daemon was not running (`docker system df` failed: no socket
  at `~/.docker/run/docker.sock`), so nothing was pulled, inspected or run. Nothing below comes from the image. If it
  is done later it stays inside the public surface (manifest, labels, entrypoint, `--help` offline with `--network none`, no
  credentials, no disassembly).
- **License text:** WarpStream's terms URL now 301-redirects to Confluent's contracts index
  (`https://www.confluent.io/contracts/`), which I did not open. A search snippet only shows that agents are licensed
  "in binary form" for use "solely for purposes of accessing or using the Platform". I found NO verified
  reverse-engineering clause either way. Treat reverse engineering as presumptively prohibited. Public docs are enough
  for this comparison, which is why the image was not needed.
- Tags: **[V]** verified in a cited public source, **[I]** inferred by us, **[U]** unknown.
- Numbers about our system are measured unless marked "computed". Most were one run, one sample.

## 1. What is known, inferred, unknown

### 1.1 Verified [V]

| Fact | Source |
|---|---|
| Agent is "a single stateless binary"; any agent can lead any partition, commit group offsets, or coordinate | docs `overview/architecture` |
| No local disks, no cross-AZ replication; a file holds many topics and partitions; "a few files per second" | same |
| Metadata for every cluster lives in WarpStream's cloud "Virtual Cluster": a replicated state machine mapping files to offset ranges; each op "journaled to our strongly-consistent log storage system" before ack; replicas backed up to object storage | same |
| Write path: buffer Produce from many clients, write one file, commit file metadata to the Metadata Store, THEN ack. Order is fixed at commit, not at upload | docs `architecture/write-path` |
| Default buffer: 250 ms or 8 MiB, whichever first | same |
| `WARPSTREAM_BATCH_TIMEOUT` / `-batchTimeout` default 250 ms (min 50 ms); `WARPSTREAM_BATCH_MAX_SIZE_BYTES` default 4 MiB (1 to 64 MiB); `WARPSTREAM_FILE_CACHE_SIZE_BYTES` 0.5 GiB per vCPU; `WARPSTREAM_AGENT_GROUP`; `WARPSTREAM_FILE_CACHE_EXTRA_REPLICAS`; `WARPSTREAM_COMPACTION_BUCKET_URL`; `WARPSTREAM_KAFKA_HANDLE_CONSUMER_GROUPS_IN_BACKEND` | docs `byoc/.../agent-configuration` |
| Offsets: the Virtual Cluster "assigns offsets to every batch" and returns them to the agent | docs `life-of-a-request` |
| Control-plane load is driven by **batch count**, linear in batches; warning threshold "more than 400,000 batches per second"; minimum batches/s = partitions x flushes/s x AZs (1024 x 4/s x 3 = 12,288); metric `warpstream_control_plane_utilization` (0 to 1); a control plane "cannot be scaled further" at max | docs `kafka/reference/control-plane-utilization` |
| Same page: disabling idempotency lets agents merge batches and is "strongly" recommended for high volume; larger batch timeout and `consistent_spread` partition assignment need idempotency off | same |
| Idempotency: the store keeps the sequence IDs of the last **5** committed batches per (producer id, partition); duplicates stay as bytes in the file but are dropped from metadata ("retroactive tombstoning") and removed at compaction | blog `unlocking-idempotency-with-retroactive-tombstones` |
| Compaction: pool of agents merges small files per partition in the background; the control plane decides who | docs architecture |
| Read: Fetch asks the cloud which files hold offsets from X, fans IO to agents of the same zone, which share a zone-aware distributed file cache (consistent-hash ring); data loaded "once per availability zone" in large chunks | docs `architecture/read-path`, `life-of-a-request` |
| Producers are steered to an agent in their own AZ via Metadata/bootstrap (leader = arbitrary same-zone agent) | `life-of-a-request` |
| Benchmark: 6 x m6in.4xlarge, 1 GiB/s write + 1 GiB/s read (also 3 GiB/s read), 288 partitions (also 1,152 and 4,608), 64 producers, `linger.ms=25`, lz4 (about 490 MiB/s on the wire), agents use about 5 of 16 cores, producer P99 under 1 s, end to end P99 under 2 s | blog `warpstream-benchmarks-and-tco` |
| Their monthly cost at 1 GiB/s: compute $3,031, S3 PUT $1,788, S3 GET $508, control-plane fee $7,541, total $12,868 | same |
| Partitions: marketing says customers run "40,000+" on one cluster and there is no per-broker limit; third-party-indexed tier table (`llms.txt`) lists 4,096 / 16,384 / 50,000 / 100,000+ partitions for Dev / Fundamentals / Pro / Enterprise. No doc states a hard 40k limit | warpstream.com/leviathan, /llms.txt |
| Lightning topics journal produces and commit to the control plane asynchronously (different durability path) | docs write-path, benchmark FAQ |

### 1.2 Inferred [I]

- Their PUT bill of $1,788/month at $0.005 per 1,000 is about 357M PUTs, about 138 PUT/s. 490 MiB/s compressed over 138 files/s is about 3.6 MiB per file, which matches the 4 MiB default max file size. So the 1 GiB/s benchmark runs near 140 files/s at the cluster, not "a few". This is arithmetic on their numbers, not a statement from them.
- The commit RPC probably carries file key, size and per-(topic, partition) batch lists (record counts, sizes, producer id/seq when idempotent). Docs say the load scales with batches, which implies one row per batch or per partition per file. Exact schema unknown.
- Metadata store internals (consensus protocol, storage engine) are not described; "built from scratch" is the only claim. Do not assume FoundationDB, Raft, or a particular design.
- 5 batches per (producer, partition) mirrors Kafka's max in-flight of 5, so their duplicate window is by count, ours (decision 53) by log slots.

### 1.3 Unknown [U]

Commit RPC wire format and size; commits per second the metadata store sustains; how the 250 ms timer interacts with the 8 MiB cap per agent; whether files are per agent per interval or sharded by partition group; consumer group coordinator implementation (docs say any agent can coordinate; `HANDLE_CONSUMER_GROUPS_IN_BACKEND` hints at a backend mode); S3 GET coalescing details beyond "large chunks"; the real partition ceiling; their p50 latencies (the post gives charts, not numbers); license terms on inspecting the agent.

## 2. WarpStream write path (as far as public info goes)

```
 Kafka producers (same AZ as agent, via Metadata / bootstrap)
        |  Produce (no partition leaders: any agent accepts any partition)  [V]
        v
 +-----------------------------------+
 | Agent (stateless, no disk)        |   buffer 250 ms OR 8 MiB  [V]
 |  batcher: many clients x many     |   (-batchTimeout, min 50 ms)
 |  topic-partitions -> ONE file     |   file max 4 MiB default [V]
 +-----------------+-----------------+
                   | 1. PUT file  (object key, no order yet)
                   v
              +---------+
              |   S3    |
              +---------+
                   | 2. commit(file, batch list)   [V: "commits its metadata"]
                   v
 +-----------------------------------------------+
 | WarpStream cloud: Virtual Cluster (per cluster)|
 |  replicated state machine, journaled log  [V]  |
 |  assigns ORDER + OFFSETS at commit  [V]        |
 |  dedups idempotent batches (last 5)   [V]      |
 |  picks compaction / cache agents      [V]      |
 +-----------------------+-----------------------+
                         | 3. offsets returned
                         v
 agent fans out acks to waiting clients      (ack only after step 2) [V]
 background: agents merge small files per partition (compaction) [V]
```

Consequences, mostly their own words:

- **Ordering/consistency:** same ordering and idempotency guarantees as Kafka [V]; many agents can write one partition in parallel because order is decided at commit [V]. Kafka API operations in a Virtual Cluster are atomic, including multi-partition produces [V].
- **Cost model:** a file per partition every 100 ms would be about $130 per partition per month in PUTs [V], hence cross-partition batching. Files per second is what the control-plane fee and PUT bill track [V/I].
- **Control plane is the scaling limit.** One control plane per cluster, cannot be scaled further at max [V]. The 400k batches/s warning and the partitions x flush rate x AZ formula are the only capacity numbers they publish.
- **Partitions:** pointers in metadata, no on-disk state [V]. The cost of more partitions is more batches per file and more batches per second at the control plane, not more brokers.
- **Read path:** metadata lookup of files, then zone-local cache ring, GETs once per AZ [V]. Zone-awareness is the cross-AZ-cost story.
- **Object-store-only:** the standard path is S3 only, with S3 durability before ack [V]. Lightning topics (S3 Express, async commit) are a separate, weaker/faster mode [V].

## 3. Side by side

| Area | WarpStream (public) | This repo (toys3) |
|---|---|---|
| Broker state | stateless agent, any agent any partition [V] | stateless broker, leaderless Metadata (decisions 29, 44) |
| Segment/file sealing | 250 ms or 8 MiB; file max 4 MiB default (64 max) [V] | size 16 MiB or `BatchWindow` 200 ms fixed timer; caps 24 partitions, 32 batches in the DynamoDB-era build, partition cap 4096 under design A (decisions 35, 52). Measured mean segment 5.4 MiB at 500k, so the timer, not size, seals |
| S3 object | one file, many partitions | one segment, many partitions, `seg/<random id>`, batches stored verbatim with an index (decisions 30, 43) |
| Commit path | agent commits to hosted metadata store, journaled strongly consistent log | design A: raw `CommitSegment` command appended to a DynamoDB log slot (`LOG/<slot>`, if-absent), replicas apply in order; ONE appender, K=4 in flight (decision 48). Ack when slot applied |
| Commit cost/latency | not published | X1: PutItem p50 2.9 ms (K=1), 3.6 ms (K=4 or 16); ceiling about 200 entries/s per hot key (5 WRU/entry vs 1,000 WCU). Measured end to end, old path: commit 38 to 65 ms, ceiling 17 commits/s (w2) |
| Offset assignment | control plane at commit [V] | CP at apply time, contiguous per partition (decision 42), same idea |
| Ordering | order set at commit [V] | seal order = commit order = offset order per connection (one commit in flight per broker) |
| Idempotency | supported; last 5 per (producer, partition); tombstone in metadata [V] | dropped: at-least-once, brokers reject idempotent producers (decision 41). Duplicates and reordering possible on retries with in-flight above 1 |
| Consumer groups | yes; coordinator on any agent / backend [V] | none: no FindCoordinator, JoinGroup, OffsetCommit; benchmark uses static `assign()` |
| Read path/cache | zone-aware distributed file cache, consistent-hash ring, 0.5 GiB per vCPU, GETs once per AZ [V] | consumer read path not built (TODO); Lookup served from applier memory; no cache |
| Partition limits | metadata pointers; 40k+ claimed; tiers to 100k+ [V]; no doc'd hard cap | measured at 288. Cap is segment index size and the 4096 per-segment cap, not a cluster cap; nothing measured above 288 |
| CP failure/standby | hosted, replicated, multi-AZ; per-cluster isolated [V] | hot standby tails the log; the slot is the fence, no lease needed (decision 51); failover costs latency; single appender is a deliberate bottleneck. Brokers retry commits up to 120 s |
| Compaction | background merge by agents [V] | none; segments are never merged, log trimmed by tombstone (decision 53) |
| Cost per GiB at 1 GiB/s (about 2.6M GiB/month) | CP fee $7,541 + PUT $1,788 + GET $508 = $9,837, about $0.0038/GiB; with compute $12,868, about $0.0050/GiB [V arithmetic] | computed, NOT measured: DynamoDB $1.0 to 1.3k + CP hosts $0.2 to 0.4k + PUT $0.4 to 0.8k = about $1.8 to 2.5k, about $0.0007 to 0.001/GiB before broker compute and GETs. The old path was about $21k to 42k/month (commit-path-redesign section 7) |
| Measured performance | 1 GiB/s on 6 x m6in.4xl, P99 produce under 1 s, e2e under 2 s, 5 of 16 cores [V claim] | same instance type: 17 commits/s ceiling, about 120 MiB/s committed (about 244 MiB/s OMB payload); 500k msg/s stage built a backlog (buffered up to 364 MiB per broker); 250k msg/s: seal to ack about 120 ms, publish p50 446 ms. Design A and 1 GiB/s have not yet been run (stage 5 plan is produce-only at 288 partitions) |

Honest read of the performance row: we have not matched their headline. The old commit path topped out near one eighth of their rate. Design A removes the DynamoDB transaction ceiling on paper (about 30 to 64 commits/s modelled), but that is modelled, not measured at load.

## 4. Takeaways, ranked

1. **Cut commits per second by making files bigger and fewer (adopt, cheap to test).**
   Their control-plane cost and capacity both track batch/file count, and the file bound is 4 MiB at a 250 ms timer.
   Ours seals 5 MiB average at 200 ms, about 5 seals/s per broker, 27 to 30 cluster-wide at 500k. Under design A, at the
   1 GiB/s target that is 30 to 64 commits/s against an X1 hot-key ceiling near 200/s. Test: sweep `BatchWindow` 200 vs 400 ms
   and `MaxSegmentBytes` at 250k and 500k, watch `manibroker_segments_sealed_total` by reason (the w2 doc says the seal-reason
   metric was missing; add it first).
2. **Publish a control-plane capacity formula like theirs (adopt).** partitions x flushes/s x AZs gave them a concrete
   alarm (400k batches/s). Ours should be: commits/s x entries per commit versus the 200/s hot-key ceiling (5 WRU per
   4 KiB+ entry; 4 KiB or smaller entries would reach about 250/s). Expose a utilisation gauge 0 to 1 on manicp. Free to build.
3. **Keep idempotency out, but decide the ordering story (do not copy their feature yet).** Their own docs call turning
   idempotency off the way to scale (agents then merge batches). We already did that (decision 41). The gap is
   reordering with in-flight above 1. Cheapest fix if needed: their approach, a last-5 window by sequence, was a CP
   state cost we explicitly removed. Test cheaply: measure how often reordering actually appears with
   `max.in.flight=5` under injected PUT retries in the simulator before paying for it.
4. **Single appender is a conscious tradeoff, same as theirs in spirit (keep, but measure failover).** They run one
   consensus-backed store per cluster that "cannot be scaled further"; we run one appender with a warm standby. Test cheaply:
   kill the active manicp during an OMB stage and record the ack gap against the 120 s `-commit-deadline`. No AWS beyond the
   next run needed.
5. **Compaction is the thing we lack and they have (build later, not now).** Without it, a partition read touches many
   segments (each holds about 5 MiB across up to 288 partitions). Their design makes read cost per partition viable.
   Our consumer read path is unbuilt, so size the problem first: bytes of a given partition per segment at 288 and 4096
   partitions (computed from the index, no AWS).
6. **Zone-aware cache ring for reads (adopt in the read-path design, not before).** One GET per AZ per file is how they
   keep read cost at $508/month for 1 GiB/s (about 1 GET per 7 MiB on 3x read fan-out, inferred). Our single-AZ benchmark
   hides cross-AZ cost entirely; their cost model assumes multi-AZ. Say so in any cost claim.
7. **Do not chase the 40k partition number.** It is a marketing claim of customers' usage, not a documented limit, and
   their formula shows the real cost is batches/s, which grows with active partitions per flush. Our cheap test is the
   per-segment cap: run 288 vs 1,152 partitions at the same rate and see whether commit entry size or latency moves.
8. **Do not copy "hosted control plane" economics.** Their fee ($7,541/month at 1 GiB/s) is a business model. Our computed
   DynamoDB plus two hosts is about a fifth of it, but only after design A is verified at rate. Until then treat the
   cost comparison as a hypothesis (stage 5 run is the check).
9. **Lightning-style async commit is a trap for us.** It trades durability-before-ack for latency. Our contract (ack after
   slot applied) matches their standard path; keep it.

## 5. Cheap tests that need no new infrastructure

| Test | Cost | Answers |
|---|---|---|
| Add seal-reason and entries/commit metrics, rerun w2 stages 100k/250k/500k | existing harness | takeaway 1, 2 |
| `BatchWindow` 200 vs 400 ms at 250k | one stage each | commits/s vs latency tradeoff vs their 250 ms |
| Kill active manicp mid-stage | one stage | takeaway 4, failover gap |
| Simulator: retry storm with in-flight 5, count reorders | none (sim) | takeaway 3 |
| Index arithmetic: partition bytes per segment at 288/4096 | none | takeaway 5 |

## 6. What not to read into this

- The 138 files/s, 3.6 MiB per file and per-GiB figures are our arithmetic on their published bills. Their TCO is a vendor
  blog post, a single configuration, and does not include everything (their price list may differ today).
- Our cost per GiB is computed from measured unit costs, not billed. Our latency figures come from one run per stage.
- Sources not opened: Confluent contracts page, the multi-region control plane post, Lightning topics docs, the consumer
  group docs. Anything about those is [U].
