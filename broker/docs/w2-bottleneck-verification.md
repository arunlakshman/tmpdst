# Run w2: bottleneck verification from Prometheus

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This document is a record.*

Source: Grafana Cloud Prometheus, label `run_id="w2"`, scrape interval 5 s (no scrape gaps seen, 120 samples per 10 min).
Read-only analysis; no AWS resources used. Everything below was recomputed from the metrics, not from the earlier docs.
The OMB stage log (`results/w2/omb/stages.tsv`) gives the stage clocks.

## Verdict

The earlier analysis is right about the component and wrong about most of the numbers and the mechanism.

- At 500k the limit is the single sequencer's DynamoDB TransactWriteItems loop. It is back-to-back busy: `sum(rate(manicp_seq_tx_seconds_sum))` = 0.996
  over 1792441435-1792441480, with `inflight_groups` = 1 the whole time. It is not a 75% busy stage; 75% is the 250k stage.
- At 250k there is no plateau. The stage hit its target (250.4k msg/s, 0 errors) with the tx loop 76% busy, queue wait 6 ms avg (p99 46 ms) and
  no backlog. 250k is not shown to be the ceiling; it is the highest stage that completed.
- Tx time is not a constant 30-60 ms. It tracks the number of items per transaction (one run item per distinct partition touched by the segment, plus the G# item):
  31 items give 31 ms, 55 items give 44 ms, 82 items give 66 ms (about 0.7 ms per item plus about 9 ms). The cost is per partition touched, not per byte.
- The existing sequencer pipeline (`seq-pipeline=4`) never engaged: max `manicp_seq_inflight_groups` = 1 in every stage. Every broker produces to all 288 partitions
  (clients stay on the broker they bootstrapped from) and one segment touches 55-70 of them, so any two segments share partitions and the planner
  must serialise them. The redesign text that says "partitions are independent" is wrong for this layout.
- Not the limit: broker CPU (0.1 of 16 cores), broker network (about 20 MiB/s each), S3 (PUT 50-100 ms, 17-20 PUT/s, no errors), producer-side buffer
  (peak 364 MiB per broker against a 1 GiB cap, `reads_paused` never 1), load generator (9 of 32 cores at worst), CP host (0.08 of 4 cores).
- The 500k stage never reached steady state. It ran 50 s (start 1792441428) and was killed by the harness rule "publish p99 > 10 s" after 25 s of traffic.
  The queue was still growing linearly; the numbers are a ramp, not a plateau.

## Stage windows used

| stage | window (epoch) | seconds | note |
|---|---|---|---|
| smoke 100k | 1792440965-1792441105 | 140 | inside OMB 1792440922-1792441112 |
| ramp 250k | 1792441190-1792441385 | 195 | inside 1792441145-1792441395 |
| ramp 500k | 1792441435-1792441480 | 45 | loaded part only; brokers saw traffic from about 1792441435, drained by 1792441490 |

All histogram numbers: `histogram_quantile(q, sum by (le) (increase(<m>_bucket{run_id="w2"}[<window>])))` evaluated at the window end,
averages from `increase(_sum)/increase(_count)`. All instances of a service are pooled (6 brokers, 1 CP).

## 1. Broker side per segment (ms unless noted)

| metric | stage | p50 | p90 | p99 | avg |
|---|---|---|---|---|---|
| segment_open_age | smoke / 250k / 500k | 240 / 240 / 203 | 304 / 304 / 297 | 318 / 318 / 318 | 201 / 201 / 145 |
| segment_put (seal to stored) | smoke | 58 | 76 | 80 | 44 |
|  | 250k | 120 | 152 | 159 | 90 |
|  | 500k | 80 | 144 | 159 | 76 |
| s3_attempt | smoke | 48 | 61 | 88 | 43 |
|  | 250k | 96 | 122 | 127 | 86 |
|  | 500k | 73 | 119 | 189 | 72 |
| s3_operation (equals attempt, so no S3 retries) | 250k | 96 | 122 | 127 | 86 |
| segment_queue_wait (before commit starts) | smoke | 2.5 | 4.5 | 5 | about 0 |
|  | 250k | 2.5 | 4.5 | 5 | about 0 |
|  | 500k | 11,170 | 18,620 | 20,290 | 10,110 |
| cp_call (HTTP round trip) | smoke | 39 | 59 | 64 | 33 |
|  | 250k | 49 | 64 | 122 | 49 |
|  | 500k | 341 | 478 | 509 | 302 |
| segment_commit | smoke | 32 | 51 | 77 | 35 |
|  | 250k | 60 | 76 | 101 | 52 |
|  | 500k | 355 | 583 | 634 | 302 |
| segment_seal_to_ack | smoke | 69 | 135 | 158 | 79 |
|  | 250k | 122 | 155 | 281 | 142 |
|  | 500k | 11,540 | 18,690 | 20,300 | 10,930 |
| segments_in_flight_at_seal (count) | smoke / 250k | 0.5 | 0.9 | 0.99 | 1.0 |
|  | 500k | 32 | 32 | 32 | 37 (top bucket is 32, so p50 to p99 are clipped) |

Segment size and counts (cluster totals):

| | smoke | 250k | 500k |
|---|---|---|---|
| sealed seg/s | 21.3 | 17.2 | 19.5 avg, 27-30 at peak |
| committed seg/s | 21.3 | 17.2 | 19.8 avg, 17 steady |
| mean S3 request MiB | 2.3 | 7.1 | 5.4 (p50 6.3) |
| sealed MiB/s (compressed) | 49.4 | 122.7 | 104.9 avg, 211 peak |
| committed MiB/s | 49.4 | 122.8 | 117.7 avg, 120-123 steady |
| max segments in flight per broker | 1 | 1 | 56 |
| max buffered MiB per broker (cap 1024) | 2.6 | 7.8 | 364 |
| reads_paused | 0 | 0 | 0 |
| failed segments | 0 | 0 | 0 |

Committed bytes are lz4 output: 123 MiB/s committed corresponds to the 244 MiB/s OMB reports (payload is half random).

At 500k the brokers sealed at the 200 ms window rate (about 5 per broker per second, 27-30/s total) while commits stayed at 17/s,
so the in-flight segment count climbed linearly to 308 (peak at about +80 s of the table below). Segment PUTs ran concurrently without trouble
(56 in flight per broker); the backlog is entirely waiting for commit.

## 2. Control plane

| metric | smoke | 250k | 500k |
|---|---|---|---|
| tx busy = `sum(rate(manicp_seq_tx_seconds_sum))` | 0.66 | 0.76 | 0.996 |
| groups/s | 21.3 | 17.2 | 15.2 |
| mean segments per group | 1.00 | 1.00 | 1.17 (p99 2.5) |
| mean items per group (limit 100) | 31.4 | 55.3 | 81.6 (p50 95, p99 99.9) |
| seq_tx p50 / p90 / p99 / avg ms | 27 / 52 / 63 / 31 | 48 / 61 / 64 / 44 | 80 / 118 / 127 / 66 |
| dynamo_attempt avg ms (equals operation, no retries) | 29.6 | 41.6 | 65.4 |
| dynamodb_call avg ms | 29.8 | 41.9 | 65.8 |
| seq_queue_wait p50 / p90 / p99 / avg ms | 0.4 / 11 / 28 / 2.9 | 0.5 / 22 / 46 / 6.2 | 314 / 472 / 508 / 272 |
| seq_queue_depth avg / max | 0.07 / 1 | 0.10 / 1 | 4.8 / 5 |
| seq_inflight_groups avg / max | 0.61 / 1 | 0.69 / 1 | 1.0 / 1 |
| inflight_requests avg (CP http) | 0.68 | 0.82 | 6.0 |
| commit_prep p50 / avg ms | 0.25 / 0.11 | 0.25 / 0.17 | 0.25 / 0.19 |
| handler p50 / p99 / avg ms | 36 / 64 / 33 | 49 / 121 / 48 | 366 / 509 / 339 |
| dynamo request bytes avg | 31 KB | 53 KB | 57 KB (p99 257 KB) |
| WCU/s (consumed) | 2,044 | 2,886 | 3,210 |
| retries, cancel reasons, throttles | none exported (series absent) | same | same |
| CP process CPU (cores) / CP host busy (of 4) | 0.04 / 0.08 | 0.04 / 0.08 | 0.04 / 0.08 |

Steady 500k 15 s samples (t = seconds after 1792441440): tx busy 0.99-1.00 from t=5 to t=40, items per group 80-84, tx 64-68 ms, queue wait 242-295 ms.
Little check at 500k: CP `inflight_requests` = 6 (every broker has exactly one commit outstanding), handler 339 ms, throughput 6 / 0.339 = 17.7 requests/s,
which matches `calls/s` = 17.7. Queue 272 ms + tx 66 ms = 338 ms.

Why tx got slower with load: group items rose 31 to 55 to 82 and tx rose 31 to 44 to 66 ms. A line through the endpoints gives about 0.7 ms per item plus about 9 ms.
The item count is distinct partitions per segment plus one. Mean segment bytes did not rise from 250k to 500k (7.1 to 5.4 MiB), so at 500k each
segment carried fewer bytes and more partitions than at 250k.

CP-observed DynamoDB versus CloudWatch (`aws_dynamodb_successful_request_latency_average`, op TransactWriteItems, 60 s points):

| minute starting (offset s from 1792440840) | CloudWatch avg ms | CloudWatch max ms | stage | CP seq_tx avg ms |
|---|---|---|---|---|
| 180-360 | 30.1 / 29.5 / 29.7 / 29.7 | 81 / 50 / 50 / 49 | smoke | 31.0 |
| 420-600 | 47.0 / 43.2 / 42.6 / 43.3 | 89 / 80 / 77 / 80 | 250k | 44.2 |
| 660-720 | 54.4 / 56.7 | 95 / 91 | 500k | 65.8 |

Smoke and 250k agree within 1-2 ms, so the CP is seeing the DynamoDB service time, with no client-side queuing of its own. At 500k CloudWatch shows 54-57 ms
versus 66 ms on the CP; the CloudWatch minutes include ramp-up and drain at lower item counts, so the gap is mostly window mismatch. Not a discrepancy that changes the conclusion.
Server-side DynamoDB latency itself rises with items per transaction. CloudWatch `throttled_requests`, `transaction_conflict`, `system_errors`, `conditional_check_failed` are all 0.
DynamoDB consumed WCU, 2.9-3.2k/s, is far below any on-demand limit.

S3 cross-check: all `aws_s3_*` CloudWatch series (requests, bytes, first byte, total latency, 5xx) are exported but are 0 for every minute of the run.
S3 request metrics were not delivering data (filter `manifester-bench` or the delay of request metrics). So broker-side PUT latency could NOT be compared with S3-side latency.

## 3. Resource saturation

| | smoke | 250k | 500k |
|---|---|---|---|
| broker process CPU, avg / max cores (of 16) | 0.067 / 0.071 | 0.116 / 0.120 | 0.088 / 0.100 |
| broker host busy cores avg | 0.11 | 0.17 | 0.14 |
| broker net rx = tx MiB/s each | 8.4 | 20.7 | 18.2 |
| broker RSS MiB max (host RAM 62 GiB) | 65 | 101 | 1,515 (buffered data) |
| broker goroutines max | 541 | 86 | 88 |
| broker GC pause seconds per second | 0.0002 | 0.0002 | 0.00001 |
| open connections | 132 | 132 | 119-129 |
| CP host: CPU busy of 4 cores / net rx / tx MiB/s | 0.08 / 0.2 / 0.9 | 0.08 / 0.3 / 1.3 | 0.08 / 0.2 / 1.1 |
| loadgen hosts (2 x 32 cores), busy cores avg / max host | 0.36 / 0.70 | 2.6 / 5.3 | 4.6 / 9.2 |
| loadgen net tx MiB/s avg per host | 25 | 62 | 53 |

Nothing is saturated. `manibroker_reads_paused` stayed 0 and `reads_paused_seconds_total` rate is 0, so producers were never backpressured by the broker.
The buffer reached 364 MiB per broker against `max_buffered_bytes` 1,073,741,824, so with 45 s of traffic it would have needed roughly 2 more minutes at the same growth to pause reads.
OMB used one producer worker for the produce stages (log line `Workers list - producers: [http://10.42.1.188:8080]`), 64 producers on it, at most 9.2 of 32 cores.
No Kafka-side accept or produce latency metrics exist in Prometheus, so that path is only visible through OMB pub latency.

## 4. Where the time goes for a produced byte

Median stage budget, ms (from the p50s and averages above):

| step | smoke | 250k | 500k at its worst point |
|---|---|---|---|
| producer linger (client side, `linger.ms=25`) | up to 25 | up to 25 | up to 25 |
| segment open (BatchWindow 200 ms, open age p50) | 240 | 240 | 203 |
| S3 PUT (seal to stored) | 58 | 120 | 80 |
| wait for earlier commits on the same broker | 2.5 | 2.5 | 11,170 |
| CP call: sequencer queue | 3 | 6 | 272 |
| CP call: DynamoDB tx | 31 | 44 | 66 |
| seal to ack | 69 | 122 | 11,540 |
| OMB publish latency p50 (reported) | 320 | 446 | 6,800 to 17,300 |

At 250k the seal to ack path is about 120 ms, so a produced byte waits about 240 ms for the window, 120 ms for PUT and commit, plus client linger and batching;
OMB p50 446 ms is consistent.

Little's law at 500k:

- Offered load into the brokers: 211 MiB/s sealed at peak against 120 MiB/s drained. Net backlog growth about 90 MiB/s cluster-wide, which matches the buffered bytes
  climbing from 21 MiB to 1,950 MiB in 25 s (peak growth about 80 MiB/s per 5 s step in the 5 s table).
- Commit service: one TransactWriteItems at a time, 66 ms each, 15.2 groups/s, 1.17 segments each = 17.7 segments/s. Throughput cap = that rate times bytes per segment (about 7 MiB): 120 MiB/s committed, about 240 MiB/s of OMB payload.
- Queue length: 6 brokers each with one outstanding commit, so 5 waiting, 1 in the tx. 5 x 66 ms = 330 ms, observed queue 272 ms plus 66 ms tx.
- Per-broker one-commit-at-a-time loop: each broker needs a response before sending the next commit. With the CP answering in 340 ms the broker cap is 2.9 commits/s,
  which is 17.7 in total. If the CP answered in 66 ms, a broker could do 15/s alone. So the broker loop is a real second serial stage, but it only binds because the CP
  queue makes each call slow; today the CP loop is the one at 99.6% busy. The earlier "two serial stages" statement is true in structure, but not equal in weight.

What limits the 250k stage: nothing yet. The tx loop is 76% busy and the queue wait is 6 ms. A rough headroom bound is 1 / 0.76 = 1.3x on groups, so about 320k msg/s if bytes per segment and
items per segment stayed as at 250k. At 500k items per segment rose (82 per group), which is the point where capacity collapses to 15 groups/s.

What fails at 500k: the sequencer. It cannot overlap groups (inflight_groups = 1) because all segments share partitions, each tx costs about 9 ms + 0.7 ms x items, and segments are
sealed by the 200 ms timer or the 96-partition / 256-batch caps rather than by size (mean 5.4 MiB against a 16 MiB cap).
Unverified: why the segments are small. No seal-reason metric exists, so the 16 MiB, 96 partitions, 256 batches and window triggers cannot be separated.

## 5. Claims in the earlier analysis (runbook "Run w2" and commit-path-redesign)

| claim | verdict | evidence |
|---|---|---|
| Plateau at about 250k msg/s (about 245 MiB/s) | Wrong as stated. 250k completed with headroom. The sequencer ceiling measured at 500k is about 120 MiB/s committed (about 240 MiB/s payload), so the two numbers agree, but 250k was not the plateau | pub 250,393 msg/s, tx busy 0.76, queue wait avg 6.2 ms, committed 122.8 MiB/s; at 500k committed 117.7-123 MiB/s, tx busy 0.996 |
| Limit is the single sequencer commit rate, 15-20 groups/s | Confirmed at 500k only | groups/s 15.15, `sum(rate(manicp_seq_tx_seconds_sum[45s]))` 0.996, queue depth 4.8 |
| tx 30-60 ms | Confirmed numbers (31 / 44 / 66 ms avg); reason is the missing piece: tx scales with items per tx | `manicp_seq_group_items` avg 31 / 55 / 82; CloudWatch TransactWriteItems 30 / 43 / 55 ms |
| tx busy about 75% | Wrong for the failing stage (0.996). Correct only for 250k (0.76) | `sum(rate(manicp_seq_tx_seconds_sum{run_id="w2"}[W]))` |
| About 1 segment per group | Confirmed | `manicp_seq_group_segments` avg 1.00 / 1.00 / 1.17 |
| Commit-rate bound is 1 / (tx + about 20 ms) | Wrong. The loop issues the next tx immediately; at 500k busy is 0.996, so the bound is 1 / tx (15.2 groups/s x 66 ms). With +20 ms it would predict 11.6/s, but 15.2 observed | tx busy, groups/s above |
| Each broker seals about 2.6 segments/s regardless of rate | Partly: broker commits are about 2.6-3.5/s at every stage. Sealing at 500k was 5/s per broker, held back only by the queue | `rate(manibroker_segments_sealed_total)` 21.3 / 17.2 / 27-30; commits 21.3 / 17.2 / 17 |
| Segments about 8 MB at 100k | Wrong. About 2.3 MiB at 100k, 7.1 MiB at 250k, 5.4 MiB at 500k | `manibroker_s3_request_bytes` avg |
| S3 PUT p50 80-100 ms, S3 PUT/s about 16, S3 not the limit | Confirmed for 250k and 500k (96 and 73 ms p50, 17-20 PUT/s, 56 PUTs in flight per broker without trouble, no retries). At 100k p50 is 48 ms. S3 server-side metrics are all 0, so no independent check | `manibroker_s3_attempt_seconds` equals `_operation_seconds` |
| DynamoDB throttles, errors, cancellations and retries 0 | Confirmed for CloudWatch (throttled, conflicts, system errors, conditional failures all 0). The CP retry and cancel-reason counters are absent in Prometheus (not exported when zero), so only the CP's own stats say 0 | `aws_dynamodb_*` all 0 |
| WRU about 100-190 per commit, DynamoDB capacity is not the bound | Confirmed that capacity is not the bound (2.0 / 2.9 / 3.2k WCU/s, no throttles) | `manicp_dynamo_consumed_capacity_units_total` |
| Broker CPU was not captured | Now captured: 0.07-0.12 cores of 16 per broker, never a factor | `process_cpu_seconds_total`, `node_cpu_seconds_total` |
| A pipelined sequencer would overlap commits because partitions are independent | Wrong premise, and the pipeline already exists (`seq-pipeline=4`, run with it) but never overlapped: `inflight_groups` max 1. Segments from different brokers share 55-70 of 288 partitions, so the planner cannot overlap them | `max_over_time(manicp_seq_inflight_groups[W])` = 1 |
| Fewer partitions per segment would allow group size above 1 | Partly. Group size above 1 appeared only when the queue built (1.17), not as a fix: items per group were already 82 | `manicp_seq_group_items` |
| 500k "~250k then falling" | Unverifiable as a plateau. The stage lasted 50 s with 25 s of load; the OMB rate is ack-based. Brokers actually ingested up to 211 MiB/s compressed (about 420k msg/s) for about 20 s until the harness abort | sealed MiB/s 5 s series |
| "queue" on the broker (10 s) blames the broker loop or the CP | The 10 s is backlog of up to 56 segments per broker waiting for one slow commit slot; the slot is slow because of the CP queue (272 of 340 ms) | `manicp_seq_queue_wait` vs `manibroker_cp_call` |

## Gaps and caveats

- 500k lasted 45 s of load, so the numbers are a ramp, not a steady state. Treat the 0.996 busy and the 15 groups/s as the capacity at about 82 items per tx, not as a general ceiling.
- Histogram buckets are coarse (group items in steps of 5, in_flight_at_seal tops out at 32), so some p-values are clipped.
- CloudWatch S3 metrics are 0 for the whole run; the S3 comparison could not be done. CloudWatch DynamoDB is 60 s granularity.
- CP retry and cancel-reason counters do not exist as series; "0 retries" rests on `dynamo_attempt` equalling `dynamo_operation` and on CloudWatch.
- No seal-reason, no Kafka accept-latency and no per-partition-count-per-segment metric. The OMB side only reports acked rate, so offered rate at 500k is inferred from sealed bytes.
- The m6in.4xlarge brokers are 16 vCPU with 62 GiB RAM; CP m6in.xlarge is 4 vCPU. This was one run per stage with one sample each.

## Queries

All in Grafana Cloud Prometheus (`/api/prom/api/v1/`), `W` is the stage window, `R` = `{run_id="w2"}`.

```
histogram_quantile(Q, sum by (le) (increase(manibroker_segment_open_age_seconds_bucket{run_id="w2"}[W])))   # same for put, s3_attempt, s3_operation,
                                                                                                         # queue_wait, cp_call, commit, seal_to_ack,
                                                                                                         # segments_in_flight_at_seal, s3_request_bytes
histogram_quantile(Q, sum by (le) (increase(manicp_seq_tx_seconds_bucket{run_id="w2"}[W])))                 # same for seq_queue_wait, dynamo_attempt, dynamo_operation,
                                                                                                         # dynamodb_call, commit_prep, handler, seq_group_items, seq_group_segments
sum(increase(M_sum{run_id="w2"}[W])) / sum(increase(M_count{run_id="w2"}[W]))                               # averages
sum(rate(manicp_seq_tx_seconds_sum{run_id="w2"}[W]))                                                        # tx busy
avg_over_time / max_over_time(manicp_seq_inflight_groups{run_id="w2"}[W])
sum(rate(manibroker_segment_committed_bytes_total{run_id="w2"}[15s]))                                       # also segments_sealed_total, segments_committed_total,
                                                                                                         # segment_sealed_batch_bytes_total
sum(manibroker_segments_in_flight{run_id="w2"}), sum(manibroker_buffered_bytes{run_id="w2"}), sum(manibroker_reads_paused{run_id="w2"})
avg(rate(process_cpu_seconds_total{run_id="w2",job="manibroker"}[W]))
sum(rate(node_cpu_seconds_total{run_id="w2",role="broker",mode!="idle"}[W])) / 6
sum(rate(node_network_receive_bytes_total{run_id="w2",role="broker",device!~"lo"}[W])) / 6        # also transmit, role cp, role loadgen
aws_dynamodb_successful_request_latency_average{run_id="w2"}   # dimension_Operation="TransactWriteItems", step 60 s
aws_s3_first_byte_latency_average{run_id="w2"}                 # all zero
```

## What to measure next

1. A real 500k stage held for at least 3 minutes (the harness abort at p99 > 10 s ended it after 25 s). Use a larger abort threshold or no abort for the diagnostic run.
2. Export seal reason per segment (window, bytes, partitions, batches), partitions per segment and items per tx per group, so segment size can be tied to the 200 ms window.
3. Export the CP loop split: time building the group, time in the tx, time in post-processing. Today tx busy of 0.996 is measured only as the tx itself, and the remainder is invisible.
4. Add a measurement of the tx latency against items by a controlled DynamoDB-only benchmark (same table, 20 / 40 / 60 / 80 / 100 items, one writer), to confirm the 0.7 ms per item slope and decide whether to cut the items per commit.
5. Try the cheapest experiments before any redesign: raise `segment-window` (for example 500 ms to 1 s) so segments are fuller with the same partition count; keep brokers on disjoint partition sets
   (so the existing `seq-pipeline` can overlap groups, since inflight_groups was 1 only because the sets overlap); check whether tx cost per item changes with 288 versus fewer partitions per segment.
6. Fix the S3 request metrics (the CloudWatch filter returned zeros) so broker PUT latency can be cross-checked.
7. Export the broker accept/produce latency per request and the producer-side OMB in-flight count to see if the 250k stage was ack-gated on the client side (broker seal cycle is about window + PUT + commit at 100k and 250k, 2.9 per broker per second).
