# W4: the log store at 500k, 750k and 1M msg/s (random clients, 288 partitions)

Run w4, 2026-10-20/21, ap-south-2a, account 478483354994. Stack: 6 brokers m6in.4xlarge, 2 loadgens m6in.8xlarge (loadgen 0 produced, loadgen 1 idle),
2 CP nodes m6in.xlarge (rank 0 active, rank 1 hot standby), `cp_store = log`, `-takeover-after 5s`, NLB on, DynamoDB table with TTL and 30k warm
throughput, `broker_extra_args = ""` (default `-max-segment-partitions 4096`). Workload: the unmodified OMB workload (64 producers, 1 KiB,
`randomBytesRatio 0.5`, lz4), NO partition affinity, `bootstrap-mode balanced`, 288 partitions, produce-only, no consumers. Build toolchain go1.26.10, git sha 962a863.
Plan: `docs/stage5-run-plan.md`. Raw data in `results/w4` (git-ignored); server metrics from Grafana Cloud Prometheus (`run_id="w4"`),
read after teardown. Stack up 23:37Z to 00:14Z (destroyed and verified).

**These stages are short (1 to 4 minutes, no warm-up, one sample each) and the 15 minutes of load were a product-owner budget, not the 5-minute
stages of the plan. The numbers are indicative, not a steady-state characterisation.** Stage windows (UTC): smoke 23:51:07-23:52:17,
500k 23:52:50-23:56:00, 750k 23:56:33-23:59:43, 1M 00:00:16-00:04:27, kill test 00:05:34-00:09:44. Server metrics below are averages over the
interior of each window (first and last 15-30 s dropped).

## Results against the stage-5 pass criteria

OMB client side (`omb/*.json`, aggregated publish latency; errors were 0.0 err/s in every interval of every stage):

| stage | target | achieved msg/s (MiB/s) | per-10 s min / max msg/s | pub p50 / p99 / p99.9 / max ms | criterion | verdict |
|---|---|---|---|---|---|---|
| smoke | 100k | 99.5k (97.2) | 95.8k / 100.8k | 275 / 408 / 424 / 458 | >= 95k, p99 < 1 s | pass |
| 500k, 3 min | 500k | 498.9k (487.2) | 469.8k / 508.3k | 428 / 687 / 714 / 828 | >= 475k, p99 < 1 s | pass |
| 750k, 3 min | 750k | 748.3k (730.8) | 708.3k / 757.0k | 345 / 624 / 734 / 820 | >= 712k, p99 < 1.5 s | pass |
| 1M, 4 min | 1M | 997.9k (974.5) | 930.6k / 1,010.9k | 452 / 756 / 787 / 1,044 | >= 950k, p99 < 2 s, no errors | pass |
| 500k with appender kill, 4 min | 500k | 499.0k (487.3) | 243.9k / 749.4k (the 10 s intervals around the kill) | 436 / 4,953 / 7,016 / 8,031 | ack gap <= 15 s, 0 errors | pass (see below) |

Control plane (log store), steady-state window averages:

| metric | smoke | 500k | 750k | 1M | criterion |
|---|---|---|---|---|---|
| committed segments/s (= log slots/s) | 24.2 | 19.8 | 28.2 | 31.8 | <= seg/s: slots == segments, 1.00 to 1.01 entries per slot |
| slot attempts/s vs slots/s | equal | equal | equal | equal | `lost_slots` 0 in all stages |
| append p50 / p99 | 3.1 / 7.9 ms | 3.3 / 7.9 ms | 3.7 / 7.9 ms | 4.2 / 8.0 ms | (DynamoDB PutItem, one per slot) |
| slot entry bytes (avg) | 846 | 2,358 | 2,368 | 2,664 | 6-12 KB expected: it is 2.4-2.7 KB (about 3 WRU per slot) |
| CP queue wait p99 / queue depth max | 6.7 ms / 0 | 6.3 ms / 1 | 6.9 ms / 0 | 7.0 ms / 1 | not growing |
| `apply_lag` / `standby_lag` max (slots) | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | bounded |
| `hole_fills`, `blocked`, `reads_paused` | 0 | 0 | 0 | 0 | about 0 |
| `index_lag_slots` max (last) | 229 (200) | 230 (61) | 350 (307) | 412 (412) | < 10 s of slots: see note |
| index rows/s | 625 | 1,584 | 2,599 | 3,134 | `index_throttles`, `index_unprocessed` 0 |
| DynamoDB table WRU/s (CloudWatch) | - | 1,197 | 2,383 | 3,339 | slot WRU/s alone about 3 x slots/s = 60 to 95, far below 700 |
| DynamoDB throttles (read, write, throttled requests) | 0 | 0 | 0 | 0 | none |
| DynamoDB op latency avg: Batch / Put / Get / Query | - | 5.5 / 2.5 / 1.3 / 1.3 ms | 5.6 / 2.5 / 1.2 / 1.3 | 5.5 / 2.5 / 1.1 / 1.3 | |
| manicp CPU (active / standby), cores of 4 | 0.09 / 0.06 | 0.12 / 0.07 | 0.16 / 0.08 | 0.18 / 0.09 | |
| manicp RSS (max over both nodes) | 64 MiB | 189 MiB | 482 MiB | 497 MiB | 16 GiB host |
| CP network, active node | 0.25 rx / 0.6 tx MB/s | 0.5 / 1.2 | 0.8 / 1.8 | 1.0 / 2.7 | |

Note on index lag: the R# rows are written about 10 s after the ack by design, so a lag of 200 slots already shows at smoke. At 1M it settled
at 330 to 410 slots (10 to 13 s at 32 slots/s) and drained to 7 within 30 s of the load ending, so it is bounded, not growing without limit, but the "< 10 s" bar is missed by about 3 s at
1M. `manicp_dynamo_api_errors_total` counted 5 to 17 errors per window (about 0.08/s, also at idle); their kind is not exported, they never produced a lost slot, a throttle or a producer error.

Brokers (sum or average over the 6 brokers):

| metric | smoke | 500k | 750k | 1M |
|---|---|---|---|---|
| seals/s by reason | timer 24.2 | timer 17.3, size 2.6 | timer 18.4, size 10.0 | timer 9.0, size 23.0 |
| partitions per segment avg (p99) | 26 (32) | 81 (126) | 91 (128) | 99 (128) |
| segment size committed avg (S3 PUT bytes) | 2.0 MiB | 12.4 MiB | 13.0 MiB | 15.3 MiB (size cap 16 MiB) |
| committed MiB/s (after lz4) | 49.5 | 245.7 | 368.2 | 490.8 (0.50 x offered) |
| S3 PUT latency p50 / p99 (client side, one attempt) | 48 / 64 ms | 155 / 254 ms | 157 / 254 ms | 188 / 255 ms |
| `segment_queue_wait` p50 / p99 | 2.5 / 5 ms | 2.5 / 5 ms | 2.5 / 5 ms | 2.5 / 5 ms |
| `cp_call` p50 / p99 | 3.5 / 14.6 ms | 6.2 / 15.0 ms | 6.2 / 15.2 ms | 6.2 / 15.3 ms |
| `commit` p50 / p99 | 2.8 / 10 ms | 6.7 / 13 ms | 7.1 / 16 ms | 7.5 / 18 ms |
| `seal_to_ack` p50 / p99 | 59 / 80 ms | 162 / 317 ms | 163 / 317 ms | 228 / 318 ms |
| segments in flight per broker (max) / buffered (max) | 1 / 2 MiB | 1 / 16 MiB | 1 / 16 MiB | 2 / 32 MiB |
| `segments_failed`, `reads_paused` | 0 | 0 | 0 | 0 |
| broker host CPU, cores of 16 (max host) | 0.12 (0.13) | 0.27 (0.29) | 0.38 (0.40) | 0.48 (0.50) |
| manibroker RSS (max) / host memory used (max) | 48 MiB / 1.1 GiB | 155 MiB / 1.2 GiB | 189 MiB / 1.4 GiB | 237 MiB / 1.4 GiB |
| broker NIC rx / tx per host | 8.8 / 8.9 MB/s | 43 / 43 | 65 / 65 | 87 / 87 |
| broker share of committed bytes, 1M (6 brokers) | | | | 84.5 x4, 76.7 x2 MiB/s: max/min 1.10 |
| loadgen 0 CPU (cores of 32) | 1.3 | 15.6 | 20.8 | 23.8 |

CloudWatch S3 request metrics (`aws_s3_*_latency`) are still zero (the problem noted in w2 is unchanged); the S3 numbers above are the brokers' own histograms.

## S3 PUT latency: why 150 to 250 ms, and not 4 to 50 ms

The question came up during the run (S3 latency in Grafana looked far above 4 to 50 ms, with a worry that traffic goes through the internet). Facts:
- The VPC has a Gateway endpoint for S3 (`aws_vpc_endpoint.s3`, route table of the public subnet), so PUTs stay on the AWS network; they do not go through the NAT or the internet gateway. I could not
  capture a route or flow log (the stack is destroyed), so this is from the Terraform, not a packet-level check.
- The metric (`manibroker_s3_attempt_seconds`) is the time of one whole PutObject of a whole segment, not time to first byte. The segment is 2 MiB at smoke and 12 to 15 MiB at 500k to 1M.
  Latency tracks object size: 48 ms at 2.0 MiB, 155 ms at 12.4 MiB, 157 ms at 13.0 MiB, 188 ms at 15.3 MiB, i.e. about 27 ms fixed plus about 10 ms per MiB (about 100 MiB/s on one PUT stream),
  which is the normal single-stream rate for S3 from EC2. The 254 ms p99 is the upper bound of one histogram bucket, so p99 is "at most 254 ms" and is clipped.
- So 4 to 50 ms is the right expectation for objects up to about 2 to 4 MiB; at 12 to 16 MiB 150 to 200 ms is expected. It is paid once per segment, in parallel across the 6 brokers, and is the main part of
  `seal_to_ack` (162 to 228 ms of which the CP commit is 7 ms) and therefore of the producer p50 (about 430 ms = 200 ms window + PUT + commit + client batching).
- It does not limit throughput (at most 2 segments in flight per broker, 3.2 to 5.3 PUTs/s per broker). To cut it: a smaller size cap (more, smaller PUTs, more WRU/slots), or splitting the PUT in
  parallel parts (multipart or ranged parts), or a shorter window (smaller segments). Not tried here.

## Kill-the-appender test (500k, kill at 1:45 into the stage)

`bench-ctl.sh kill-appender 60`: SIGKILL of manicp on rank 0 at 00:07:19-21Z (marker epoch 1792541241 = 00:07:21), held down 60 s, then started again. (The first two attempts failed on a local terraform wrapper
problem and sent nothing; the kill ran on the third attempt at 00:07:19, 1:45 into the stage.)

| time (UTC) | event |
|---|---|
| 00:07:14 | last metrics snapshot of rank 0 before the kill |
| 00:07:19-21 | rank 0 SIGKILL; commits to rank 0 start failing |
| 00:07:22 | first `cp call` WARNs on the standby (it answers 503 until it is active); brokers retry |
| 00:07:27.15 | rank 1 `log: role changed` and `log store ready`: active, 8 s after the kill (5 s `takeover-after` plus tail pass) |
| 00:07:27-37 | OMB 10 s interval at 243.9k msg/s (about half), then 749.4k (the backlog of held acks released); mean publish latency of that interval 2.0 s |
| 00:07:37 | the stall is over; later intervals 492k-506k msg/s, p50 about 430 ms |
| 00:08:27.14 | rank 0 restarted (60 s down), restored, `log store ready`, active again; rank 1 demoted |
| 00:08:27-00:09:44 | no visible effect of the failback (rates 481k-506k, latency normal) |

Measured: the ack gap is about 8 s (broker `commit` p99 7.9 s and `seal_to_ack` p99 8.5 s over the 21 s window after the kill; OMB max publish latency 8.03 s; whole-stage p99 4.95 s, p99.9 7.0 s), well under the
15 s budget and the 120 s `-commit-deadline`. Producer errors 0, no ABORT, no `segments_failed`, `lost_slots` 0.01/s averaged over the 85 s around the kill (about one slot), `hole_fills` 0, `reads_paused` 0, brokers buffered
up to 64 MiB (4 segments in flight) during the stall. Role changes: one to active on rank 1, one back. The standby needed no catch-up: `standby_lag` 0.
Rank 0 RSS right after the restart read 950 MiB (checkpoint plus replay), 470 MiB a minute later.

## What limits throughput now, compared with w2 and w3

| | w2 / w3 baseline (transaction store, cap 96) | w4 (log store, no cap) |
|---|---|---|
| random clients, 500k target | about 275k falling to 240k, collapse, ABORT p99 60 s | 499k, p99 687 ms |
| commit path | one `TransactWriteItems` of about 86 items, 86 ms, 1 group in flight, 15 to 18 commits/s | one `PutItem` per slot, p99 8 ms, 20 to 32 slots/s, 1.0 segment per slot |
| broker `segment_queue_wait` p99 | 41 s | 5 ms |
| segment partitions | capped at 96 (seal reason `partitions` dominant) | 81 to 99 avg, p99 128, never capped |
| segment size | 6.7 MiB | 12 to 15 MiB (size cap 16 MiB reached by 23 of 32 seals/s at 1M) |
| 750k | only reachable with affine producers | 748k with random clients |

The old limit (the sequencer serialising big transactions) is gone: in w4 the CP is idle (0.2 of 4 cores, append p99 8 ms, queue wait 7 ms) and the brokers are idle (0.5 of 16 cores, 87 MB/s of NIC). At 1M no component
was near saturation, so this run did not find the new limit; it only shows 1M with RANDOM clients holds for 4 minutes with p99 756 ms. The things closest to a wall, in order:
1. The load generator: loadgen 0 (one OMB producer worker for 64 producers) used 23.8 of 32 cores at 1M, and the per-10 s rate varied 931k to 1,011k. More than about 1.3M msg/s from one m6in.8xlarge is unlikely;
   a second producer worker (loadgen 1 was idle) is the first thing to change for a higher target.
2. Commit rate and S3: 32 slots/s at 1M, scaling roughly with bytes because 23 of 32 seals hit the 16 MiB size cap. Doubling the load makes segments hit the cap and the slot rate rise towards 60 to 70/s;
   the slot (one PutItem of 2.7 KB, p99 8 ms) and the apply path (p99 3.4 ms) show no sign of limit up to there, but that is an extrapolation.
3. Index writes: 3.1k rows/s (BatchWriteItem 5.5 ms) with a lag of 330 to 410 slots, about 3 s over the 10 s bar; the table was never throttled at 3.3k WRU/s (30k warm).
4. S3 PUT latency per 16 MiB segment (155 to 255 ms) bounds the minimum ack latency, not the rate.

## Gaps and caveats

- Short stages, one sample each, no warm-up (the 15-minute load budget); 1 to 4 minutes is not a steady state. No repeat, so run-to-run spread is unknown. Stages were back to back (33 s gaps), not the 3 min of the plan.
- Produce-only, no consumers, so no read path, no end-to-end latency, and the OMB backlog numbers are meaningless; not comparable with WarpStream's symmetric benchmark.
- One kill test, at 500k only; the standby was already caught up. Failover under 1M, a dropped-packet failure (dial timeout path) and a whole-AZ loss were not tested.
- The checkpoint (`checkpoint_age`, `checkpoint_errors`), the standby serving reads, and the index S3 decision (stage 5 exit criterion) were not examined. Hole fills were never exercised (0), so the 250 ms hole-fill path is untested on AWS.
- `summary.md` from `collect-results.sh` only covers the kill-test stage (the OMB directory of the second `run-omb.sh` call replaced `stages.tsv` and `run.json`; the per-stage OMB JSONs are intact). The one-off DynamoDB warm-throughput charge is unverified.
- CloudWatch S3 metrics are zero; CloudWatch DynamoDB has 60 s granularity. S3 latency percentiles come from broker histograms with coarse buckets (p99 clipped at 254 ms).
- Two of the three `bench-ctl.sh` kill attempts failed locally ("no instance ids": the dockerised `terraform output` call returned nothing while Docker was restarting); instance ids were then exported by hand. No effect on the data.

## Cost

Stack up about 37 minutes at about $12.3/h fixed (README prices) is about $7.6; DynamoDB (about 1.5 to 2 M WRU of load plus the warm-throughput charge, unverified) and S3 requests (a few thousand PUTs a minute) a few dollars.
Estimate **$10 to $13**, under the $30 cap. Actual billing not checked.

## Teardown

`terraform destroy`: 65 resources destroyed (00:14Z). Verified empty afterwards for `run_id=w4`: tagged resources, EC2 instances and volumes, VPC, unattached EIPs, security groups, NLB and target groups, DynamoDB table,
S3 bucket, IAM roles and instance profiles, budget. `results/w4` kept locally (git-ignored).
