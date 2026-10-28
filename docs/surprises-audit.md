# Surprises audit (before the next AWS benchmark)

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This document is a record.*

Date: 2026-10-19. Scope: the working tree of branch `toys3` at commit `4854ad7` plus the uncommitted single-writer
sequencer (`cp/dynamo/seq*.go`, decision 46). That sequencer was still being written during this audit. Findings that
depend on it say so.

Trigger: in the w1 6-broker smoke run every broker host ran its own manicp. That gave six racing compare-and-set writers
on 288 partitions. The question here is which other assumptions are false once the system has N brokers, real AWS,
1 GiB/s, failures, or real clients.

Evidence labels:
- **verified by experiment**: I ran it, and the output is quoted below.
- **verified by reading code**: the file and line are cited.
- **computed**: arithmetic from measured inputs.
- **suspected**: AWS behaviour or a limit I could not check from here.

Experiments ran locally only, with no AWS calls:
- `seqbench`: a Go program over `cp/dynamo` and the in-memory `fakedb`, with injected DynamoDB latency and a capacity-unit
  meter. It ran against a snapshot of the tree.
- `manicp -store mem` plus `manibroker -objects mem`, driven by franz-go v1.20.7 clients and by the Java `kafka-clients`
  3.6.1 library, which is the version OMB uses.

The scripts are in the session scratchpad (`audit/seqbench`, `audit/java`). They are not part of the repo.

## 1. Ranked table

Severity:
- **BLOCKER**: blocks the AWS 1 GiB/s benchmark (or the stage named).
- **HIGH**: will distort or break results, or cost real money.
- **MEDIUM**, **LOW**: smaller effects.
- **KNOWN**: already in the docs. It is listed only where it matters for the run.

| ID | Sev | Area | Surprise (one line) | Evidence | Bites when | Cheapest verification | Fix, effort |
|---|---|---|---|---|---|---|---|
| S1 | BLOCKER (1 GiB/s produce) | CP sequencer | At 288 partitions the single sequencer runs one transaction at a time for the whole cluster. Every segment holds about 96 random partitions, so any two segments share a partition: the pipeline never overlaps and groups never merge (97 items each). Cluster commit rate = 1 / transaction latency, which gives about 430-600k msg/s at the AWS latency w1 measured, not 1M. The comparison doc's "WRU per segment should fall as group size grows" is false here. | verified by experiment (seqbench, §2.1); `seqplan.go:137-145` (`busy` per partition); w1 `TransactWriteItems` mean 33 ms, mostly 16-32 ms buckets (`results/w1/metrics/1/*-manicp.txt`) | N brokers x many partitions, at scale | Ramp stages: `manicp_seq_*` group size stays 1, in-flight stays 1, commit rate plateaus at 1 / tx latency | Partition affinity: each broker leads a disjoint partition share (static `-peers` list plus hashed leaders in Metadata, the TODO "Metadata lists every live broker" without the CP lease). Modelled result: 140-190 commits/s of 16 MiB segments (§2.1). Effort M (1-2 days plus sim re-baseline). |
| S2 | BLOCKER (precondition, depends on in-progress work) | CP wiring | `cmd/manicp` still serves the plain `dynamo.Store`; the `Sequencer` is not wired in. One manicp without the sequencer, serving 6 brokers concurrently, reproduces w1: 3.5 transactions per commit, commits that exhaust their retries, 4.8 s maximum commit. | verified by reading code (`cmd/manicp/main.go:139`); verified by experiment (seqbench `-mode store`, §2.1) | N brokers | `grep Sequencer cmd/manicp/main.go`; seqbench `-mode seq` shows `Canceled:0` | Finish the wiring (in progress). Effort S. |
| S3 | BLOCKER (any consumer stage) | Fetch / CP | Busy-poll Lookup storm. Fetch answers at once even with no data (decision 29), and every partition of every Fetch is one CP Lookup over HTTP, i.e. at least one consistent DynamoDB Query. One idle franz-go consumer on 8 partitions made 20.6k Lookups/s. One Java 3.6.1 consumer on 24 partitions made about 13k/s. Predicted on AWS with 64 consumers: about 48k Lookups/s, so about 48k RCU/s (above the 30k warm throughput, at or above the default 40k per-table on-demand quota), about $43/h, 48k HTTP requests/s into one 4-vCPU CP host, and 48k INFO log lines/s. | verified by experiment (§2.3); `internal/broker/fetch.go:12`, `internal/broker/ops.go:91`, `cp/dynamo/ops.go:96-101` | real clients, consumers caught up | Local: idle consumer plus `manicp_handler_seconds_count{op="lookup"}` rate | Long-poll Fetch (honour MaxWait/MinBytes; wake on a local commit or a short end-offset cache) and serve `End` from the sequencer's in-memory ends instead of DynamoDB. Effort M. |
| S4 | HIGH | HTTP clients | Connection churn and ephemeral-port exhaustion. cpclient keeps only 16 idle connections per host, and the AWS SDK default keeps 10 (S3 in the broker, DynamoDB in manicp). With 64 concurrent Lookups I measured **16,206 TIME_WAIT sockets after 6 s** (about 2.7k new TCP connections/s). Linux has about 28k ephemeral ports with 60 s TIME_WAIT, so the limit is about 470 new connections/s per destination. Commits share the same client, so a read storm also breaks produce. | verified by experiment (§2.3); `cp/cpclient/client.go:63`; `aws-sdk-go-v2@v1.47.3/aws/transport/http/client.go:21` | scale, consumers | `ss -s` on a broker during a consume stage | Set MaxIdleConnsPerHost to at least the peak concurrency (for example 512) on cpclient and on both SDK clients. Effort S (an hour). |
| S5 | HIGH (consume stage) | bench harness | Consumers start from offset 0 of a topic that earlier stages already filled. `run-omb.sh` uses one topic for all stages, patch 0001 calls `assign()` with no committed offsets, and `auto.offset.reset=earliest`. So `vanity` after `vanity-produce` starts behind by about 15 min x 1 GiB/s. Fetches of 100 MB per partition then mean about 1,000+ sequential ranged GETs per partition per response. At 20-50 ms each that is 20-60 s, past the 30 s request.timeout.ms, so the consumer never makes progress. | verified by reading code (`run-omb.sh:62`, patch 0001 lines 130-150, `driver-kafka-manifester.yaml.tmpl:48`, `internal/broker/fetch.go:104-107`) | real clients, multi-stage runs | Read the OMB consumer log for request timeouts; backlog grows from t=0 | `seekToEnd(mine)` after `assign` in the patch, or a fresh topic per consume stage. Effort S. |
| S6 | HIGH (consume stage, cost) | Data plane read | One ranged S3 GET per batch, sequential within a partition, with no cache or coalescing. With Java's measured 79.5 KB lz4 batches (155 records), a symmetric 1 GiB/s read is about 6.5k GET/s. That is above the 5,500 GET/s an unsplit `seg/` prefix starts with, so expect 503 SlowDown (the store has a NopRetryer and the broker gives up after 6 attempts). It costs about $6.8k/month against WarpStream's $508. Per-partition read throughput tops out at roughly batch size / GET latency. | computed from verified Java batch size (§2.3); `internal/broker/fetch.go:66-107`; `internal/sys/s3store/store.go:88` | scale, real AWS | Count `manibroker_s3_attempt_seconds_count{op="GetObject"}` against consumed bytes in a ramp-with-consumers stage | A per-broker segment cache (one GET per segment per broker) or at least coalesced adjacent ranges and parallel GETs per partition. Effort M-L (caches were deferred to v2 by decision 5). |
| S7 | HIGH (cost; prediction partly KNOWN) | DynamoDB cost | Each 97-item commit costs about 240 WRU, not the 194 in the comparison doc: each 96-byte run item rounds up to 1 KB times 2 transactional WRU, plus a G# blob of about 21 KB. At 1M msg/s that is about 55 commits/s, about 13k WRU/s, about **$59/hour** and $43k/month in us-east-1. That is 5.7x WarpStream's whole control-plane fee. The vanity plan's DynamoDB spend alone exceeds the $200 budget alarm in about 3 hours of full rate. | verified by experiment (meter, §2.1); w1 measured 74,354 WRU over 1,537 transactions of about 26 items, consistent within about 10% | scale | Ramp stage: `manicp_dynamo_consumed_capacity_units_total{kind="write"}` / commits | S1's affinity fix halves the items per commit and lets 16 MiB segments form (modelled: 183 WRU per 16 MiB commit, about $25/h). Longer term: no per-partition item per commit. |
| S8 | HIGH | Failure | A CP outage of more than about 0.6 s fails every in-flight segment on every broker. The broker retries a commit 6 times with 20/40/80/160/320 ms backoff, and connection-refused fails fast, so `manicp` restarting (`RestartSec=2`) fails every in-flight produce on every broker with REQUEST_TIMED_OUT. Producers retry, giving duplicates whenever the lost commit had landed. The runbook (A10) says a restart "only costs the cold cache". | verified by reading code (`internal/broker/config.go:100-102`, `internal/broker/segment.go` `retry`, `internal/broker/broker.go:71-77`, `cp-user-data.sh.tpl:37-38`) | failure (CP restart, host replacement) | Local: produce with franz-go, `kill manicp`, restart after 2 s, count failed batches | Retry the commit until a deadline (for example 60 s, under delivery.timeout.ms) instead of a count. Effort S. |
| S9 | HIGH (observability, CP CPU) | Logging | manicp writes one INFO `cp call` line per `/v1` request and runs at `-log-level info`. In a consume stage that is about 48k lines/s into journald (default rate limit: about 10k lines/30 s per unit, then drops), then Alloy and Loki (which already returned 429 at 75 req/s). Lines are dropped, and the CP host burns CPU on logging. | verified by reading code (`cp/cpobs/cpobs.go:212-216`, `cp-user-data.sh.tpl:36`) | consumers, scale | `journalctl -u manicp` "Suppressed" lines | Log commits at INFO and sample or aggregate Lookups (DEBUG plus a per-second summary). Effort S. |
| S10 | MEDIUM | Broker CPU | One goroutine runs `Step` for all I/O. Its measured ceiling is about 240 MiB/s (laptop, produce only, `docs/benchmarks.md`). The vanity run puts about 82 MiB/s in and 82 MiB/s out of lz4 bytes per broker through it, with CRC and copies. Probably fine, but not measured on m6in. | suspected (`docs/benchmarks.md` items 7-8) | scale | Process CPU of manibroker in the ramp stages (one core at 100% means the ceiling) | pprof if it saturates. |
| S11 | MEDIUM | Semantics | Retries reorder records. With `enable.idempotence=false` and the Java default `max.in.flight.requests.per.connection=5`, a retried batch can land after later batches of the same partition. Decision 41 documents duplicates but not reordering. | verified by reading docs and decision 41; Kafka client behaviour (KIP-91) | real clients, failures | n/a | Document it; recommend `max.in.flight=1` where order matters. Effort S. |
| S12 | MEDIUM | Ops | The self-destruct guard is too short for the full plan. `shutdown -h +150` (`my.tfvars`) counts from boot, which includes waiting for artifacts. The full plan with consumers is about 135-140 min including boot and collection (comparison doc §6), so the CP host can die inside the last stage, and that stops all writes (S8). | verified by reading code (`common.sh.tpl:9`, `my.tfvars:9`) | real AWS | n/a | `max_runtime_minutes` at least 240 for full plans. Effort S. |
| S13 | MEDIUM | Growth | Nothing is ever deleted. There is no TTL on G# items (21-31 KB each) or run items, no retention, and segments of failed or duplicate commits are orphaned. The run bucket's 3-day lifecycle deletes `seg/` objects that DynamoDB still points to, so after 3 days, Fetch from offset 0 hits `committed segment missing` and KAFKA_STORAGE_ERROR. Harmless for a stack that lives under 3 days. | verified by reading code (`storage.tf:25-40`, `internal/broker/fetch.go:70-74`) | long-lived stacks, production | n/a | TTL on G# items after the retry window, plus retention and GC (production). |
| S14 | MEDIUM | CP retries | manicp's DynamoDB client uses the SDK standard retryer: 3 attempts plus a client-side retry token bucket. Under a throttling burst it fails fast with "retry quota exceeded" underneath the sequencer's own retries. | suspected (`cmd/manicp/main.go:186-191`, no retryer option) | real AWS, throttling | Look for `retry quota exceeded` in WARN lines | Set `ratelimit.None` or `aws.NopRetryer` for the sequencer path. Effort S. |
| S15 | MEDIUM | Fetch memory/cost | Fetch reads every partition up to its own limit (100 MB in the OMB config), then trims to `fetch.max.bytes` only when it assembles the response, so GETs and memory are wasted when there is a backlog. | verified by reading code (`internal/broker/conn.go:147-163`) | real clients, backlog | n/a | Reserve the response budget before issuing GETs. Effort S-M. |
| S16 | LOW | Identity | `Env.NewID` is not a collision risk: it uses ChaCha8 seeded from `crypto/rand` per process, giving 128-bit segment keys. Duplicate `-broker-id` is mostly cosmetic because Metadata names only the answering broker; Terraform assigns `count.index+1` and the preflight warns. | verified by reading code (`internal/sys/prod/env.go:22-48`, `ec2.tf` `broker_id`) | n/a | n/a | None. |
| S17 | LOW | Kafka compatibility | `acks=0` closes the connection (`internal/broker/produce.go:12-13`). ListOffsets by timestamp is unsupported. The leader epoch is always 0. | verified by reading code | real clients | n/a | Document. |
| S18 | LOW | Ops | SSO expiry mid-run stops `bench-ctl`/collection and `destroy`. Self-destruct terminates only EC2; the NLB, table, bucket, endpoints and budget remain. `summarize.py` prices default to us-east-1, not ap-south-2. The `docs/decisions.md` row 46 that the commits cite is missing. | verified by reading code and docs | real AWS | n/a | Refresh SSO before a run; pass ap-south-2 prices; add decision 46. |
| S19 | KNOWN | Kafka compatibility | No consumer groups: a franz-go group client fails with "broker is too old". Also no CreateTopics or DescribeConfigs, no SASL/TLS, leaderless-metadata skew, no idempotent producers, no retention. | verified by experiment (group client, §2.3); decisions 26, 29, 41 | real clients | n/a | As planned. |

## 2. Details

### 2.1 S1, S2, S7: the control plane at 288 partitions (verified by experiment)

**Model.** Six broker-like committers each send one commit at a time, like `internal/broker/segment.go` `pump()`. Each segment is
built the way the broker seals it at 288 partitions: batches go to uniformly random partitions until 96 distinct
partitions (the partition cap seals first, at about 117 batches, well before 16 MiB). DynamoDB is `fakedb` with a fixed
latency per call. The meter bills transactional writes at 2 WRU per KB per item and consistent reads at 1 RCU per 4 KB.
The batch shape is the one measured from the Java client (§2.3): 79.5 KB and 155 records.

| run (10 s each) | tx latency | commits/s | msg/s equivalent | tx per commit | items/tx | WRU/commit | worst commit |
|---|---|---|---|---|---|---|---|
| Sequencer, pipeline 4 | 15 ms | 58.5 | 1.06M | 1.00 | 97 | 240 | 220 ms |
| Sequencer, pipeline 4 | 28 ms | 33.4 | 0.61M | 1.00 | 97 | 240 | 293 ms |
| Sequencer, pipeline 4 | 40 ms | 23.6 | 0.43M | 1.00 | 97 | 240 | 358 ms |
| Sequencer, pipeline 16 (25 ms, 97-record batches) | 25 ms | 36.2 | n/a | 1.00 | 97 | 240 | 265 ms |
| 6 independent Stores (= w1) | 28 ms | 12.6, 9 commits failed | 0.23M | 3.73 | 97 | 895 | 4,525 ms |
| one shared Store (= manicp today, S2) | 25 ms | 13.0, 7 commits failed | n/a | 3.49 | 97 | 835 | 4,824 ms |
| **Sequencer + partition affinity** (each broker owns 48 partitions, 16 MiB segments), pipeline 8 | 28 ms | 191 | 6.2M | 0.98 | 49 | 183 | 112 ms |
| same | 40 ms | 140 | 4.5M | 0.98 | 49 | 182 | 143 ms |

How the msg/s column is computed: commits/s x 117 batches x 155 records (affinity rows: x 211 batches x 155 records).
1M msg/s needs about 55 commits/s without affinity, which means a transaction of at most about 16 ms. With affinity it
needs about 31 commits/s, spread over 6 independent lanes.

Real AWS input: w1 `manicp_dynamodb_call_seconds{call="TransactWriteItems"}` was 1,537 calls with a mean of 33 ms. Of
those, 1,043 finished in 32 ms or less and 3 in 16 ms or less, and those transactions held only about 26 items. A
97-item transaction will not be faster. So the produce-only `vanity-produce` stage is predicted to plateau at roughly
430-600k msg/s. OMB is open loop, so publish latency will then grow until the p99 > 10 s abort rule fires. The ramp
stages up to about 500k should pass.

The comparison doc says (§5, "Control plane") that "DynamoDB WRU should drop as group size grows, because one
transaction now carries several segments". That is false at 288 partitions: Groups == Segments in every
non-affinity run, because a group's item budget (100) holds exactly one 97-item segment.

The `many` and `store` rows show that the w1 failure mode does not need six processes. One manicp that serves six brokers
concurrently through the plain Store races the same way, and that is what `cmd/manicp` serves today (S2).

### 2.2 S8: CP restart (verified by reading code)

- Broker defaults: `MaxAttempts 6`, `RetryBase 20 ms`, `RetryMax 640 ms`, so the commit backoffs are 20, 40, 80, 160
  and 320 ms.
- A refused connection is a dial error. It is classified NotApplied, so the broker retries at once.
- After the 6th failure the segment's batches get REQUEST_TIMED_OUT (`retry`, `failSegment`).
- So every segment that tries to commit during a CP gap of about 0.6 s or more fails on every broker. The commit may still
  have landed (MaybeApplied).
- systemd's `RestartSec=2` alone exceeds the gap. A host replacement (runbook A10) takes minutes.

Producers with delivery.timeout.ms=120 s retry under new segment keys. A batch whose earlier commit had in fact landed is
then duplicated (decision 41 allows this). The surprise is not the duplicates. It is that the runbook's "a restart only
costs the cold cache" is wrong for every in-flight produce.

### 2.3 S3, S4, S19: real clients on one local broker (verified by experiment)

Setup: `manicp -store mem`, `manibroker -objects mem`, both on loopback.

- franz-go, 1 consumer, 8 partitions, assign at the end of the log, idle for 10 s: `manicp_handler_seconds_count{op="lookup"}`
  went from 0 to **206,552** (20.6k/s).
- franz-go, 8 consumers x 8 partitions, idle: 17.7k Lookups/s (the laptop was CPU-bound). 6 s in: **16,206 TIME_WAIT** and
  32 ESTABLISHED sockets to the CP port.
- Java kafka-clients 3.6.1 with the OMB producer settings (acks=all, idempotence off, linger 25, batch.size 100000, lz4,
  50%-random 1 KiB payloads), 200k records over 24 partitions:
  - producer `batch-size-avg` 79,463 B, `compression-rate-avg` 0.51, no errors or retries;
  - consumer (assign, earliest, OMB fetch sizes): all 200k consumed in 0.8 s;
  - 3,236 fetches came to 77,642 Lookups (about 24 per fetch), about 13k Lookups/s over the run.
  - So the Java producer and consumer paths work against the broker, and the Lookup storm is not specific to franz-go.
- franz-go consumer-group client: `unable to join group session: broker is too old` (expected, KNOWN).

AWS prediction (computed): a caught-up consumer issues roughly 1 / (CP RTT + one DynamoDB Query) ≈ 1 / 6 ms ≈ 170
fetches/s. That times 64 consumers times 4.5 partitions is about 48k Lookups/s, each at least one consistent Query
(1 RCU). With data present, a Lookup is 3 sequential queries (`endOf`, `runStart`, `scanRuns`).

### 2.4 S5 and S6: the read stage, in numbers (computed)

- 1M msg/s at 155 records per batch is 6,450 batches/s. That is about 6,450 ranged GETs/s for a 1x fan-out read, about
  1,075 GET/s per broker.
- At about 20 ms per GET that is about 21 GETs in flight per broker, above the SDK's 10 idle connections, so churn again
  (S4).
- Per partition: 22 batches/s needed, about 50/s possible sequentially at 20 ms. That works at steady state, but any
  backlog is served at about 4 MB/s per partition. With 100 MB fetches, that backlog becomes the response timeout of S5.

## 3. Safe to proceed?

**Not for the 1 GiB/s stages.** A `vanity-produce` run on the current design is predicted to plateau at about
45-60% of the target, so it would spend money to confirm S1. A `vanity` (consume) run would fail on S3, S4 and S5 before
it measured anything.

A smoke and ramp run (at most 500k msg/s, produce only) is worth its roughly $40-70, but only as a measurement run, and
only after the preconditions below. Do them in this order:

1. **S2**: manicp serves the `Sequencer`. Check locally (seqbench-style test with 6 concurrent committers): 0
   cancellations, about 1 transaction per commit.
2. **S4**: raise the idle-connection pools (cpclient, S3 client, DynamoDB client). This is an hour's change.
3. **S8**: make the broker retry commits until a deadline. Repeat a local CP-restart test with franz-go: no failed
   batches for a 5 s outage.
4. **S12 and budget**: `max_runtime_minutes` at least 240. Set a run budget for about $60-75/h at full rate (DynamoDB plus
   $12/h fixed). The current $200 alarm is fine for smoke and ramp.
5. **Measure in the ramp** (and abort at the knee):
   - the real 97-item `TransactWriteItems` latency (`manicp_dynamodb_call_seconds`);
   - the sequencer group size and in-flight count (expected 1 and 1);
   - commits/s at the plateau;
   - WRU per commit (expected about 240);
   - manibroker CPU (S10).

   This turns S1 and S7 from model into fact.
6. **Before any 1 GiB/s produce stage: S1.** Add partition affinity (static `-peers` plus hashed leaders) and re-run
   seqbench with `-owned` as the acceptance check (at least 2x headroom over 55 commits/s at the measured latency).
7. **Before any consume stage:**
   - S3: long-poll Fetch plus end offsets from memory. Acceptance: an idle consumer makes at most a few Lookups per
     second.
   - S5: `seekToEnd` or a fresh topic.
   - S9: Lookup logging at DEBUG or sampled.
   - S6: at least coalesce or parallelize GETs, and estimate the GET bill.
   - Check the per-table on-demand quota (40k RRU/WRU default) in Service Quotas for ap-south-2.

## 4. Assumptions I could not check, and why

- **Real DynamoDB latency of a 97-item `TransactWriteItems`** (S1). w1 only had transactions of about 26 items. A local
  ExtendDB is not representative. It needs an AWS call, which this audit was not allowed to make.
- **Default on-demand quotas in ap-south-2** (40k RRU and 40k WRU per table, 80k per account in most regions), whether warm
  throughput above 30k is granted, and its one-time price. Checking needs the AWS console or CLI.
- **Billing of canceled transactions.** The meter counts them in full. w1's numbers (74,354 WRU over 962 successes and 575
  cancellations) fit better with canceled transactions costing little. This only affects the `many` and `store` rows.
- **S3 per-prefix behaviour for a fresh bucket under about 6.5k GET/s:** whether, and how fast, `seg/` splits. These are
  AWS-internal heuristics; the 3,500/5,500 per-prefix figures are the documented baseline.
- **The Java producer's batch size at 15.6k msg/s per producer with the sticky partitioner.** I measured 79.5 KB and 155
  records at an unthrottled local rate. At the OMB per-producer rate, batches could be smaller (the 25 ms linger rarely
  matters at 100 KB). Smaller batches mean more commits per byte and a worse S1.
- **CP host CPU under about 48k Lookups/s** (SigV4 signing, TLS, JSON, logging) on m6in.xlarge. I did not run it locally at
  that rate.
- **Alloy and Grafana Cloud ingestion limits** for the account (S9), and journald's rate limit on AL2023 (default assumed).
- **The single-thread broker ceiling on m6in.4xlarge** with lz4 traffic in both directions (S10). Only laptop numbers
  exist.
- **Java consumer behaviour under real fetch timeouts and with fetch sessions** at scale. Locally the sessionless full
  fetches worked; the timeout and retry behaviour of S5 is inferred from client defaults.
- The sequencer code was changing during the audit (new `seqrun.go`, `seq_test.go`, `tmp_test.go` appeared). The
  experiments ran on a snapshot taken at 17:40. A later change to `plan()`/`blocked()` could alter S1, but not the
  arithmetic: 97-item segments that all overlap cannot share a 100-item transaction or run in parallel.
