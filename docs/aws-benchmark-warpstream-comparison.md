# Benchmark parity with WarpStream's published numbers

Goal: run the benchmark WarpStream published ("WarpStream benchmarks and TCO",
https://www.warpstream.com/blog/warpstream-benchmarks-and-tco, and the client guidance in
https://docs.warpstream.com/warpstream/kafka/benchmarking and `.../configure-kafka-client/tuning-for-performance`)
as closely as this system allows, and say exactly where it differs. The run is driven by
`deploy/aws/bench/run-omb.sh` (flow in `docs/aws-benchmark-runbook.md`), summarised by `deploy/aws/bench/summarize.py`.

Sources of their figures: the post and the two docs pages above. WarpStream does not publish OMB driver or workload
files (the workload YAML is inline in the post, the driver settings are listed as prose); the files in
`deploy/aws/bench/omb/` are our reconstruction of those settings.

## 1. Setup mapping

| | WarpStream (published) | This repo (6-broker run) | Same? |
|---|---|---|---|
| Brokers / agents | 6 x m6in.4xlarge (16 vCPU, 64 GiB, up to 50 Gbps), stock AMI, no OS or kernel tuning | 6 x m6in.4xlarge by default (terraform variable `broker_instance_type`; availability in ap-south-2a unverified), Amazon Linux 2023, no tuning | yes (if the type is available) |
| Load generator | OpenMessaging Benchmark on m6in.8xlarge, "intentionally overprovisioned" | OMB on `loadgen_count` x m6in.8xlarge, 2 recommended (OMB gives the first half of its workers to producers, the second half to consumers) | yes in type; worker layout is ours |
| AZs | agents multi-AZ in their cost model; the benchmark itself does not say | all in ONE AZ | no, see section 3 |
| Object store | S3 (their bucket, same region) | S3 standard bucket, same region | yes |
| Metadata / coordination | WarpStream hosted control plane (proprietary) | DynamoDB on-demand table via `manicp` on each broker host (decisions 29, 39, 42) | no |
| Client tool | OMB (Kafka driver) | OMB at a pinned commit, Kafka driver with a small patch (`omb/patches`) | yes, with patch |
| Topic | 1 topic | 1 topic | yes |
| Partitions | 288 (also 1152, 4608) | 288 (1152 and 4608 in `workloads/later`) | yes |
| Producers / consumers | 64 / 64 (text of the post also says 4 elsewhere; the YAML says 64) | 64 / 64 (`producersPerTopic`, `consumerPerSubscription` = total over all workers) | yes |
| Fan-out | 1x (also 3x) | `subscriptionsPerTopic: 1` (3 in `workloads/later`) | yes |
| Record size / payload | 1 KiB, `randomBytesRatio: 0.5` | same (`useRandomizedPayloads: true`, pool 1000) | yes |
| Target rate | 1,000,000 msg/s = 976.6 MiB/s ("1 GiB/s") | same | yes |
| Duration | 1600 min | 15 min after a 2 min warm-up (cost) | no |
| linger.ms / batch.size | 25 / 100000 | same | yes |
| buffer.memory / max.request.size | 128000000 / 64000000 | same | yes |
| compression.type | lz4 | `none` TODAY (broker rejects compressed batches with UNSUPPORTED_COMPRESSION_TYPE); switch is one argument once compression lands | NO |
| enable.idempotence | not stated in the post; their docs recommend false | false (required: brokers reject idempotent producers, decision 41) | yes |
| acks | OMB default (all) | all | yes |
| Consumer | auto.offset.reset=earliest, enable.auto.commit=true, fetch.max.bytes = max.partition.fetch.bytes = 100485760 | same fetch sizes, but NO consumer group: `assign()` of a static partition share, no commits (broker has no FindCoordinator/JoinGroup/OffsetCommit) | partly |
| Metrics reported | producer P99 < 1 s, end-to-end P99 < 2 s (nearly all workloads), CPU ~5 of 16 cores per agent, 1 GiB/s symmetric | the same OMB statistics (publish latency aggregated p50/p99/p99.9, end-to-end percentiles, rates, backlog) plus server-side per-broker numbers | yes |
| Cost model | 1-year reserved compute $3,031/month, S3 PUT $1,788, S3 GET $508, control-plane fee $7,541, total $12,868 for 1 GiB/s | measured S3 PUT/GET counts and DynamoDB units, section 5 | different components |

Client tool notes that matter for fairness:

- OMB's producers are open-loop at the target rate. When the system cannot keep up, publish latency grows and
  the achieved rate drops below target: always read `pub msg/s` against `target msg/s`.
- WarpStream's tuning guide also lists `max.in.flight.requests.per.connection=1000`, `send/receive.buffer.bytes=-1`,
  `metadata.recovery.strategy=rebootstrap`, and `linger.ms=100` for the Java client. The post's OMB settings
  (linger 25) are the ones copied here; the in-flight setting is left at the Java default (5) because the post does
  not list it. The OMB build uses kafka-clients 3.6.1, which does not know `metadata.recovery.strategy`.
- The Kafka driver the repo builds is OMB's, plus `deploy/aws/bench/omb/patches/0001`: (a) every logical topic maps
  to one topic created beforehand through manicp (the broker has no CreateTopics); (b) consumers `assign()` partitions
  statically and never commit; (c) the consumer list is not shuffled so the static split is deterministic;
  (d) optional round-robin pinning of each client to one broker (`--bootstrap-mode balanced`, section 3.5).
  Patch 0002 only removes the unrelated drivers from the build.

## 2. What is and is NOT apples-to-apples

Not apples-to-apples, in order of how much they move the result:

1. **Compression (largest).** WarpStream used lz4 with `randomBytesRatio: 0.5`, so roughly half of each 1 KiB record is
   compressible. Their post says their 1 GiB/s of producer traffic was about 490 MiB/s compressed (they assume a
   conservative 200 MiB/s for TCO). With `compression.type=none` we move and store about twice as many bytes per
   msg/s: more NIC time, more S3 bytes, larger segments to PUT, more bytes to GET. Until compression lands
   (`docs/TODO.md` "Compression") the honest statement is "same msg/s, about 2x the bytes". `run-omb.sh` probes
   with a one-record lz4 produce, picks `lz4` automatically if the brokers accept it, prints a loud warning
   otherwise, and `summarize.py` lists it as an unmet item. A fairer per-byte comparison is available today:
   compare MiB/s and $/GiB, not msg/s.
2. **Metadata store.** Our commit path is a DynamoDB transaction per sealed segment plus one query per distinct
   partition in it (`cp/doc.go`: P + 2 calls, 1 + P items). Their control plane is proprietary and priced as a flat
   fee. Latency, throttling, and cost behave differently, and the number of partitions per segment drives our
   DynamoDB cost (section 5).
3. **Single AZ vs their multi-AZ cost model.** Their headline cost comparison is against Kafka with cross-AZ
   replication and producer-to-leader traffic ($27,288/month inter-AZ in the best case). Our run is single AZ,
   so it exercises no inter-AZ transfer and says nothing about the inter-AZ cost they are avoiding. Their latency
   numbers were measured on agents that serve multi-AZ clients.
4. **Durability and availability scope.** Data is durable once S3 PUT and DynamoDB commit succeed, but we test no
   AZ failure, no broker loss mid-run (WarpStream scaled 6 -> 3 agents during a run), and there is no retention,
   compaction, or consumer-group machinery. Delivery is at-least-once with no idempotent producers (decisions 41-44).
5. **Fetch implementation.** `Fetch` ignores MinBytes and MaxWait and answers immediately (decision 29); consumers
   here have no groups. Their guide says WarpStream also returns available data immediately, so the long-poll
   difference is smaller than it sounds, but consumer behaviour (rebalances, offset commits, lag) is not
   exercised at all. Read-side numbers are the least mature in this repo: produce-only is the default first stage.
6. **Metadata skew.** Every broker answers Metadata with itself as leader of every partition, so a client talks to
   the broker it bootstrapped from. WarpStream's service discovery also gives each client one agent at a time,
   but it balances connections across agents; their docs say a single benchmark instance often sends all traffic
   to one agent. See section 3.
7. **Run length and warm-up.** 15 minutes against their 1600; no steady-state-over-hours claim.
8. **Producer-side measurement.** Same OMB statistics, but our end-to-end latency depends on the static
   `assign()` consumers.

Apples-to-apples (same by construction): instance types (if available), OMB as the tool, partitions, producer and
consumer counts, record size and payload entropy, target rate, linger, batch, buffer and max-request settings,
fetch sizes, acks, the load generator being oversized, no OS tuning, S3 as the data store in the same region,
and the reported statistics.

## 3. Per-broker skew from Metadata

### 3.1 Why

Metadata lists only the answering broker (decision 29, `docs/TODO.md` "Metadata lists every live broker"). A
Kafka client learns the cluster from its first Metadata response, so it connects to one broker for every
partition and stays there; clients choose among `bootstrap.servers` at random. OMB opens each producer and each
consumer separately, so each independently lands on a broker.

### 3.2 How bad (simulation, uniform random choice, 6 brokers)

Equal-rate clients, so a broker's load is proportional to its client count:

| clients | fair share per broker | busiest broker (mean) | quietest (mean) | median max/min | p90 max/min |
|---|---|---|---|---|---|
| 64 producers | 10.7 | 15.0 (+40%) | 6.7 (-37%) | 2.2 | 3.4 |
| 256 producers | 42.7 | 51.1 (+20%) | 34.5 (-19%) | 1.46 | 1.77 |

At 976.6 MiB/s the fair share is 163 MiB/s per broker, and the busiest of the 64-producer layout carries about
228 MiB/s (+65 MiB/s). Produce latency is set by the busiest broker, and a skewed broker can hit its segment
pipeline limits (one S3 PUT stream and one ordered commit stream per broker) before the cluster average does. The same applies
independently to the 64 consumers.

### 3.3 Mitigations in this repo

- `--bootstrap-mode balanced` (default in `run-omb.sh`): the patched OMB driver pins client k to bootstrap entry
  `(k + loadgen index) mod N`, so 64 producers split 11/11/11/11/10/10. This imitates WarpStream's balanced
  service discovery. State it when you publish ("balanced client placement").
- `--bootstrap-mode random`: stock behaviour. Run it once and report the skew table, because it is what an
  unmodified client sees. The NLB (`BOOTSTRAP_NLB`) does not fix skew: it balances TCP connections by flow hash,
  which has the same binomial statistics.
- Principled fix, not done: "Metadata lists every live broker" from `docs/TODO.md` (a heartbeat lease in the CP
  and Metadata that spreads partition leaders over live brokers). With it, a client opens connections to all brokers
  and per-partition leadership spreads the load by partition rather than by client.

`summarize.py` prints the per-broker share of committed bytes and max/min and max/mean for each stage, and lists
a skew above 1.5 as an unmet item.

## 4. What to report for a fair statement

Report all of these together, never the headline alone:

1. msg/s AND MiB/s achieved vs target (and the target for each ramp step), write and read.
2. Producer P50/P99/P99.9 and end-to-end P50/P99/P99.9 (read stage only), with the OMB version/commit.
3. Compression setting, with the sentence "no compression, so about 2x WarpStream's bytes per msg/s" while it applies.
4. Bootstrap mode (balanced or random) and the per-broker skew table.
5. Broker CPU (the Hosts dashboard, "Process CPU" and node CPU) next to WarpStream's "about 5 of 16 cores".
6. Server-side: segment put/queue/commit/ack ms, S3 and DynamoDB errors, throttles, retries.
7. Cost per GiB from measured request counts (section 5), with the prices used and the region.
8. The structural deviations (single AZ, DynamoDB, at-least-once, no consumer groups, 15 min run).

What you can claim: "same hardware, same client tool and settings, same msg/s, this p99". What you cannot claim: a cost
or latency win/loss over WarpStream on the basis of one single-AZ, uncompressed, 15-minute run.

## 5. Cost model parity

WarpStream's monthly figures at 1 GiB/s, 1-year no-upfront reserved: compute $3,031, S3 GET $508, S3 PUT $1,788,
cloud infrastructure $5,327 (these three), plus a control-plane fee $7,541 = $12,868 (about $0.0049 per GiB
written at a sustained 1 GiB/s: 12,868 / 2,629,800 GiB per month; derived by us, they publish no per-GiB figure).
Their S3 PUT line implies about 358 million PUTs a month (about 136 per second at $0.005 per 1000), their GET
line about 1.27 billion GETs (about 480 per second at $0.0004 per 1000).

Ours, per stage window, from the broker and manicp snapshots (counters are exact; no estimate):

```
GiB written      = delta(manibroker_segment_committed_bytes_total) / 2^30
S3 PUTs          = delta(manibroker_s3_attempt_seconds_count{op=~"Put.*"})        (retries are separate attempts)
S3 GETs          = delta(manibroker_s3_attempt_seconds_count{op=~"Get.*"})
WRU, RCU         = delta(manicp_dynamo_consumed_capacity_units_total{kind="write"|"read"})
S3 $             = PUTs * put_price/1000 + GETs * get_price/1000
DynamoDB $       = WRU * wru_price/1e6 + RCU * rcu_price/1e6
compute $        = brokers * hourly_price * window_hours
$/GiB            = (S3 + DynamoDB + compute) / GiB written
$/month at 1 GiB/s = ($/GiB) * 86400 * 30.4167          (2,629,800 GiB per month)
```

`summarize.py` prints these (`--broker-hourly`, `--s3-put-per-1000`, `--s3-get-per-1000`, `--ddb-wru-per-million`,
`--ddb-rcu-per-million`; defaults are us-east-1 list prices, check ap-south-2). Compare line by line: compute against
their $3,031, S3 PUT/GET against $1,788/$508, DynamoDB against their $7,541 fee (the control-plane component).
Do not compare totals unless compression and AZ scope are stated.

Pre-run estimate for 1,000,000 msg/s x 1 KiB over 6 brokers, no compression (a prediction, to be checked against
the counters): the segment rule seals at 16 MiB, 200 ms, or 96 distinct partitions (`broker/config.go`). Each
producer's batches (~100 KB) walk across all 288 partitions, so a segment reaches the 96-partition cap after about
117 batches, about 11.7 MB: roughly 83 segments/s cluster-wide, about 83 PUTs/s, or 218 million PUTs a month
($1,091 at $0.005/1000), below WarpStream's $1,788. The commit of such a segment is 1 BatchGet + 96 Queries + one
97-item transaction (`cp/doc.go`): about 194 WRU and 96 RCU per commit, so about 16,000 WRU/s and 8,000 RCU/s,
which at on-demand us-east-1 prices ($1.25/M WRU, $0.25/M RCU) is about $53,000 + $5,000 a month at a sustained
1 GiB/s, far above their whole $12,868. If that holds, the per-partition run item design (decision 39) and the
96-partition cap make DynamoDB the dominant cost at 288 partitions, and raising segment size or lowering the
partitions per segment is the lever, not S3. The first ramp steps (100k, 250k msg/s) confirm or refute it
cheaply: check `WRU/s` per broker in the summary against 194 x segments/s.

## 6. Expected spend (estimates)

Fixed: about $11.8/hour for the whole stack at the default instance types (terraform agent's estimate). Variable at
full rate: DynamoDB about $80/hour at 1 GiB/s if the prediction above holds, S3 PUTs about $1.5/hour.

| plan | load minutes | session (boot 30 + load + collect 20) | fixed | variable | total |
|---|---|---|---|---|---|
| `smoke,vanity-produce` (default) | ~25 | ~1.3 h | ~$15 | ~$25 | **~$40** |
| `smoke,ramp,vanity-produce` | ~65 | ~2 h | ~$24 | ~$45 | ~$70 |
| `smoke,ramp,vanity-produce,vanity` | ~85 | ~2.3 h | ~$27 | ~$70 | ~$100 |

Hard guards: `run-omb.sh --max-minutes` refuses a plan above the limit, instances self-terminate after
`max_runtime_minutes` (default 360), and the budget alarm in tfvars. Abort rules stop a failing stage within one 10 s interval.
