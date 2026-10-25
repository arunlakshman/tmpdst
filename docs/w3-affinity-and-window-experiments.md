# W3: partition-affine producers and a longer segment window

*Note: the DynamoDB-transaction control plane described here was removed (decision 54); the log-based CP is the only one. This document is a record.*

Run w3, 2026-10-20, ap-south-2, same stack as w2 (6 brokers m6in.4xlarge, 1 CP m6in.xlarge, 2 loadgens, 288 partitions, lz4,
balanced pinning, produce-only, 1 KiB messages). Stack up about 40 minutes. Raw data in `results/w3` (git-ignored); metrics from Grafana
Cloud Prometheus (`run_id="w3"`). Each stage was short (see the windows below), so the numbers are indicative, one sample each.

Question: is the w2 diagnosis right (docs/w2-bottleneck-verification.md)? That is: at 500k msg/s the single sequencer's DynamoDB
transaction loop saturates because every segment touches many of the 288 partitions, so each group holds one segment and the
sequencer pipeline never overlaps groups.

Experiments (both approved by the product owner):
- A: partition-affine producers (`run-omb.sh --affine`, patch 0004). Producer k writes only partitions p with p % 6 == k % 6. **Partition-affine
  producers, not comparable to WarpStream.** A real client cannot be told to do this.
- B: longer broker segment window (`-segment-window 1s` through `bench-ctl.sh broker-args`) on the unchanged workload. The planned 500 ms
  step was dropped: 1 s was already enough to answer the question (see below) and the SSO session left no time.

## Results

OMB columns come from the coordinator log (MB/s there means MiB/s). Windows: baseline 01:55:25-01:56:45, affine 500k 02:00:25-02:04:10,
affine 750k 02:07:00-02:11:40, window 1 s 02:14:05-02:15:20 (UTC).

| run | target msg/s | achieved msg/s (MiB/s) | pub p50 / p99 | outcome |
|---|---|---|---|---|
| smoke, affine, 1 min | 100k | 99.5k (97) | 319 ms / 463 ms | pass; segments touched at most 48 partitions, so the partitioner works |
| baseline, default window 200 ms | 500k | about 275k falling to about 240k, then collapsing | p50 2.3 s rising to 46 s; p99 hit 60 s at 1:45 | ABORT (p99 > 60 s); backlog 22 M messages |
| affine producers | 500k | 500.8k (489) for the full 5 min | 274 ms / 462 ms | healthy, no errors |
| affine producers | 750k | 751k (733) for the full 6 min | 375 ms / 638 ms | healthy, no errors |
| window 1 s, unchanged workload | 500k | about 265k falling to about 230k, then collapsing | p50 11.6 s rising to 50 s; p99 hit 60 s at 1:45 | ABORT (p99 > 60 s) |

Not run: affine 1M msg/s (skipped for time; the SSO session expired at 02:44Z and teardown had to start by 02:30Z), window 500 ms.

Control plane and broker metrics at steady state (window averages; `-` means not captured):

| metric | baseline 500k | affine 500k | affine 750k | window 1 s 500k |
|---|---|---|---|---|
| in-flight groups, max (avg) | 1 (1.0) | 4 (1.3) | 4 (1.4) | 2 (1.07) |
| segments per group, avg | 1.14 | 1.00 | 1.00 | 1.00 |
| items per transaction, avg | 85.6 | 48.7 | 49.1 | 96.7 |
| segment partitions, avg (p50) | 75 (74) | 47.7 (40) | 48.0 (40) | 93 (79) |
| transactions per second | 15.6 | 28.3 | 34.0 | 13.4 |
| tx time p50 / p99 | 86 / 127 ms | 48 / 64 ms | 48 / 64 ms | 96 / 127 ms |
| tx busy (sum of tx seconds per second) | 1.04 | 1.21 | 1.44 | 1.00 |
| sequencer queue wait p50 / p99 | 310 / 508 ms | 0.25 / 0.5 ms | 0.25 / 0.5 ms | 384 / 509 ms |
| broker segment queue wait p50 / p99 | 22 s / 41 s | 2.5 / 5 ms | 2.5 / 5 ms | 21 s / 41 s |
| broker CP call p50 / p99 | 354 / 509 ms | 47 / 64 ms | 48 / 64 ms | 375 / 509 ms |
| seal to ack p50 / p99 | 22.6 / 40.6 s | 0.12 / 0.31 s | 0.24 / 0.32 s | 21.9 / 40.6 s |
| committed MiB/s (after lz4) | 118.6 | 246 | 369 | 118.4 |
| segments committed per second | 17.8 | 28.3 | 34.0 | 13.3 |
| segment size committed, avg | 6.7 MiB | 8.7 MiB | 10.8 MiB | 8.9 MiB |
| seals by reason (count in window) | partitions 966, timer 719, size 2 | timer 5816, batches 552, size 3 | batches 9365, timer 165 | partitions 1161, timer 71, size 3 |

"tx busy" is the summed transaction seconds per second. Above 1.0 it means transactions overlapped (several groups in flight); it is not
a utilisation fraction when the pipeline overlaps. In the baseline and window runs it is about 1.0 with one group in flight: the loop
is 100% busy.

Seal reasons: `timer` = the window expired after the first batch; `size` = the segment reached 16 MiB (`MaxSegmentBytes`);
`partitions` = the segment reached 96 distinct partitions (`MaxSegmentPartitions`); `batches` = the segment reached 256 batches
(`MaxSegmentBatches`, a cap that bounds the size of the CP's segment record). So "batches" is a cap, not a timer.

## Hypotheses against results

- H1 (affinity removes the bottleneck): **confirmed.** With the same 500k offered load, affine segments touched 48 partitions instead of
  75, each transaction carried 49 items instead of 86 and took 48 ms instead of 86 ms, the sequencer kept up (queue wait under 1 ms,
  broker queue wait 2.5 ms), overlapped up to 4 groups, and the run held 500k and then 750k msg/s with p99 under 0.65 s. The baseline
  collapsed to about half the offered rate with the queue growing without bound.
- H2 (a 1 s window gives fuller segments and fixes it): **refuted at 1 s.** The unchanged workload still collapsed the same way: segments
  touched 93 partitions on average, 1 group in flight, tx 96 ms with 97 items, throughput worse than the default (13.4 tx/s against 15.6).
  The reason is in the seal counts: at 500k with random partitioning the segment hits the 96-partition cap long before the timer
  (1161 partition seals against 71 timer seals), so a longer window only changes the rare timer seals; and it makes each transaction
  bigger and slower. What limits the baseline is partitions touched per segment, not the window.
- The commit rate is about 1 / (tx time + queue) with one group in flight; in w2 and in the baseline and window runs that is 13 to 16 per
  second, enough for about 240 to 275k msg/s with these segment sizes.

## Conclusion

The diagnosis holds: the bottleneck at 500k is the sequencer loop serialising large transactions that are driven by the number of
partitions each segment touches (and capped by the 96-partition seal rule), not DynamoDB capacity, S3 or broker CPU. Reducing the partitions
per segment (affinity) cut the transaction time and cost per segment, restored overlap of groups and moved the ceiling above 750k msg/s
(733 MiB/s) with no queueing. A longer window does not help with random partitioning.

Caveats. Affine results are for partition-affine producers, not comparable to WarpStream: real clients write all partitions. The
runs were short (a few minutes, one sample each); the 750k affine ceiling was not found (1M not run), and tx busy was already 1.44 at
750k, so the ceiling is probably not far above. Broker CPU and network were not captured. The baseline and the 1 s run were aborted by the
60 s p99 rule after about 1:45, so their "steady state" is the first 80 seconds of a collapsing queue; their throughput numbers are upper
bounds. The 10 MB objects seen in S3 are expected: committed segments average 7 to 11 MiB (16 MiB size cap).

Design implications: a server-side remedy (not a benchmark trick) would be to reduce the transaction cost per partition touched, for
example the async per-partition index of docs (commit path redesign proposal), or splitting sequencing so independent partition sets commit in
parallel groups; the affinity result shows the headroom is there once segments carry fewer partitions.
