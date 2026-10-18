# Benchmark summary: r1

```
{"run_id":"r1","tool":"kafka","topic":"bench-r1","bootstrap":"10.42.1.149:9092","cells":["c1_1k","c8_1k","c32_1k","c1_100b"],"reps":3,"duration_s":60,"gap_s":15,"warmup_s":20,"partitions":6}
```

## Per cell and repetition

Client side (from the load tool): throughput summed over clients; avg latency record-weighted; p50 record-weighted mean and p99 max over clients, from the tool's final summary line (n/a when the tool did not print one).

| cell | rep | clients | bytes | records/s | MB/s | avg ms | p50 ms | p99 ms | notes |
|---|---|---|---|---|---|---|---|---|---|
| c1_1k | 1 | 1 | 1024 | 16,091 | 15.71 | 5,656.0 | - | - | no final summary (tool killed): percentiles n/a |
| c8_1k | 1 | 8 | 1024 | 134,230 | 131.08 | 5,283.5 | - | - | no final summary (tool killed): percentiles n/a |
| c32_1k | 1 | 32 | 1024 | 114,503 | 111.82 | 16,120.4 | - | - | no final summary (tool killed): percentiles n/a |
| c1_100b | 1 | 1 | 100 | 150,714 | 14.37 | 5,629.8 | - | - | no final summary (tool killed): percentiles n/a |
| c1_1k | 2 | 1 | 1024 | 16,150 | 15.77 | 5,640.0 | - | - | no final summary (tool killed): percentiles n/a |
| c8_1k | 2 | 8 | 1024 | 131,535 | 128.45 | 5,382.9 | - | - | no final summary (tool killed): percentiles n/a |
| c32_1k | 2 | 32 | 1024 | 116,978 | 114.23 | 15,671.2 | - | - | no final summary (tool killed): percentiles n/a |
| c1_100b | 2 | 1 | 100 | 151,493 | 14.45 | 5,604.5 | - | - | no final summary (tool killed): percentiles n/a |
| c1_1k | 3 | 1 | 1024 | 16,187 | 15.81 | 5,626.9 | - | - | no final summary (tool killed): percentiles n/a |
| c8_1k | 3 | 8 | 1024 | 129,681 | 126.64 | 5,467.4 | - | - | no final summary (tool killed): percentiles n/a |
| c32_1k | 3 | 32 | 1024 | 120,428 | 117.62 | 15,201.3 | - | - | no final summary (tool killed): percentiles n/a |
| c1_100b | 3 | 1 | 100 | 150,870 | 14.39 | 5,630.3 | - | - | no final summary (tool killed): percentiles n/a |

## Broker and control plane, same cells (deltas between snapshots)

Averages are exact (delta sum / delta count). `seg/s` and `MB/s` use the snapshot window. put = seal to stored in S3; queue = waiting for earlier commits; commit = CommitSegment round trip; ack = seal to answer.

| cell | rep | seg/s | committed MB/s | put ms | queue ms | commit ms | ack ms | S3 throttles | S3 errors | S3 retries | DDB throttles | DDB errors | DDB txn cancels | DDB retries | DDB WRU | DDB call ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| c1_1k | 1 | 2.51 | 13.11 | 69.0 | 0.0 | 37.4 | 106.4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15,621 | 18.1 |
| c8_1k | 1 | 12.89 | 107.91 | 96.9 | 74.5 | 59.3 | 230.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 144,850 | 28.6 |
| c32_1k | 1 | 10.59 | 88.20 | 96.8 | 1,056.1 | 67.1 | 1,219.9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 129,894 | 31.6 |
| c1_100b | 1 | 2.55 | 13.30 | 68.7 | 0.0 | 37.9 | 106.5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15,603 | 18.3 |
| c1_1k | 2 | 2.56 | 13.35 | 67.0 | 0.0 | 38.2 | 105.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15,703 | 18.5 |
| c8_1k | 2 | 12.64 | 105.80 | 95.6 | 78.8 | 60.5 | 234.9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 143,992 | 29.3 |
| c32_1k | 2 | 10.82 | 90.05 | 96.0 | 1,032.9 | 65.5 | 1,194.5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 130,668 | 30.9 |
| c1_100b | 2 | 2.56 | 13.37 | 67.7 | 0.0 | 37.3 | 105.1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15,697 | 18.0 |
| c1_1k | 3 | 2.54 | 13.24 | 66.7 | 0.0 | 37.9 | 104.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15,785 | 18.3 |
| c8_1k | 3 | 12.61 | 105.53 | 94.8 | 132.2 | 61.6 | 288.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 142,902 | 29.8 |
| c32_1k | 3 | 11.13 | 92.62 | 95.6 | 997.4 | 63.6 | 1,156.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 134,234 | 30.0 |
| c1_100b | 3 | 2.55 | 13.30 | 68.5 | 0.0 | 38.0 | 106.4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15,623 | 18.4 |

## Mean over repetitions

| cell | reps | records/s | MB/s | avg ms | p99 ms (worst rep) | put ms | commit ms | ack ms | S3 throttles+errors | DDB throttles+errors |
|---|---|---|---|---|---|---|---|---|---|---|
| c1_1k | 3 | 16,142 | 15.76 | 5,641.0 | - | 67.6 | 37.8 | 105.4 | 0 | 0 |
| c8_1k | 3 | 131,815 | 128.72 | 5,377.9 | - | 95.7 | 60.5 | 251.3 | 0 | 0 |
| c32_1k | 3 | 117,303 | 114.56 | 15,664.3 | - | 96.1 | 65.4 | 1,190.4 | 0 | 0 |
| c1_100b | 3 | 151,025 | 14.40 | 5,621.5 | - | 68.3 | 37.7 | 106.0 | 0 | 0 |

DDB errors exclude `TransactionCanceledException` (a lost race, counted separately). CloudWatch-side AWS numbers are not in this report; use the Grafana AWS dashboard for the same window.
