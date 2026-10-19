# Produce benchmarks (local, single broker)

> **History:** every number in this file was measured before decisions 41-44 (no idempotent producers, no broker fencing, flat segment keys). `manibench` now disables idempotence, the default segment partition cap is 96 and the commit costs 1 + partitions + grown topics. Treat the figures as the old design's.

Measurement only. No broker, CP or core code was changed. Date: 2026-10-17, branch toys3.

## Method

`cmd/manibench benchmark-producer` (franz-go v1.20.7): N independent `kgo.Client`s, idempotent, acks=all, no
compression, `MaxBufferedRecords` 5000 per client, each client produces in a loop that blocks on the buffer.
Latency = time from `Produce()` to its callback (includes time blocked on a full buffer). Partitioner: kgo
UniformBytes sticky (64 KiB per partition, then another partition), so all 6 partitions are used with large
batches. One `kgo` default stays: `MaxProduceRequestsInflightPerBroker` = 1 per client.

Three setups, each with fresh processes (and a fresh DynamoDB table prefix / S3 bucket), topic `bench`, 6 partitions,
broker defaults (BatchWindow 50 ms, MaxSegmentBytes 4 MiB, MaxSegmentPartitions 24, MaxBufferedBytes 64 MiB):

| Setup | Control plane | Objects |
|---|---|---|
| A | `-store mem` | `-objects mem` |
| B | ExtendDB (`-store dynamo -dynamo-endpoint http://127.0.0.1:18080 -create-tables -table-prefix <u>_`) | `-objects mem` |
| C | same as B | SeaweedFS (`-objects s3 -s3-endpoint http://127.0.0.1:8333 -s3-bucket <u> -s3-path-style -s3-create-bucket`) |

Commands (repo root; binaries built with `go build -o <dir>/bin/<name> ./cmd/<name>`):

```
scripts/it-up.sh
manicp <cp flags above>                       # :8080
manibroker <broker flags above>               # :9092
manibench benchmark-producer -topic bench -create-partitions 6 -num-clients 1 -duration 2s   # warm-up, creates topic
manibench benchmark-producer -topic bench -num-clients {1,8,32} -record-size 1024 -duration 30s -interval 5s
manibench benchmark-producer -topic bench -num-clients 1 -record-size 100 -duration 30s -interval 5s
scripts/it-down.sh
```

CPU sampled with `ps -o %cpu,rss` at t=6,12,18,24 s of the 8-client run.

## Caveats

- Apple M1, 8 cores, Docker Desktop (Linux VM, 3.8 GiB). The load generator, CP, broker and the Docker VM share the
  same 8 cores. Another user container (`extenddb-postgres-dev-extenddb-1`) was crash-looping throughout and
  `kind-control-plane` was running (about 15% CPU); neither was touched.
- ExtendDB is SQLite behind an HTTP API; SeaweedFS runs in the VM (its volume server is a single process on a
  Docker volume). Absolute B and C numbers therefore do not predict real DynamoDB or S3 (real S3 PUTs of 4 MiB are
  typically slower than 60 ms, real DynamoDB TransactWriteItems is typically 10-30 ms). The A/B/C ratios and the
  structure of the latency (floor = window + PUT + commit) are the useful part.
- Single 30 s run per cell, no repeats; expect roughly 5-10% run-to-run noise (not measured).
- The 32-client runs end after 31-33 s because the drain of buffered records is included in the elapsed time.

## Results (30 s runs, 6 partitions, 0 produce errors in every run)

1 KB records:

| Setup | Clients | records/s | MiB/s | avg latency | p50 | p99 |
|---|---|---|---|---|---|---|
| A mem/mem | 1 | 86,246 | 84.2 | 54.3 ms | 69.2 ms | 104 ms |
| A | 8 | 239,931 | 234.3 | 162 ms | 158 ms | 243 ms |
| A | 32 | 114,460 | 111.8 | 1.38 s | 1.27 s | 2.55 s |
| B dynamo/mem | 1 | 68,856 | 67.2 | 69.4 ms | 83.0 ms | 101 ms |
| B | 8 | 163,593 | 159.8 | 240 ms | 232 ms | 395 ms |
| B | 32 | 99,168 | 96.8 | 1.57 s | 1.54 s | 2.67 s |
| C dynamo/s3 | 1 | 30,527 | 29.8 | 160 ms | 136 ms | 1.12 s |
| C | 8 | 52,252 | 51.0 | 755 ms | 734 ms | 1.25 s |
| C | 32 | 41,339 | 40.4 | 3.71 s | 3.51 s | 6.87 s |

1 client, 100 B records (per-record cost):

| Setup | records/s | MiB/s | avg | p50 | p99 |
|---|---|---|---|---|---|
| A | 74,807 | 7.1 | 64.2 ms | 63.0 ms | 86 ms |
| B | 56,941 | 5.4 | 85.2 ms | 85.1 ms | 102 ms |
| C | 51,444 | 4.9 | 94.7 ms | 91.8 ms | 155 ms |

Raw per-interval output was kept in the benchmark session scratch directory (not committed; kept only in the session scratchpad); the summary lines
above are copied from it.

## CPU (8-client run, samples at 6/12/18/24 s, %CPU of one core)

| Setup | manibroker | manibench | manicp |
|---|---|---|---|
| A | 106, 103, 95, 104 | 73, 74, 68, 69 | 1-2 |
| B | 54, 68, 77, 82 | 37, 41, 48, 50 | 5-8 |
| C | 31, 35, 32, 19 | 20, 27, 26, 14 | 3-6 |

Docker stats after C: extenddb 0.2%, seaweedfs 1.5% (RSS 1.1 GiB), i.e. the backends are idle between requests too.

- A: the broker sits at about 100% of one core while the machine has 8. It looks single-thread bound (the broker
  state machine runs on one executor; not proven here). The load generator uses 70-80%, so it is not the limit.
  RSS 430-750 MiB (mem object store holds all data).
- B: broker 54-82%, loadgen 37-50%; neither saturated, CP 5-8%.
- C: broker 20-35%, loadgen 14-27%, CP 3-6%. Nothing is CPU bound; the system is waiting on S3 and the CP.

## Analysis

1. Throughput follows Little's law with the client buffer: records/s is about (buffered records) / (avg latency).
   A c1: 5000/0.054 = 92k (measured 86k). A c8: 40,000/0.162 = 247k (240k). C c8: 40,000/0.755 = 53k (52k).
   C c32: 160,000/3.7 = 43k (41k). Since the load generator keeps the buffer full, latency is queueing time,
   and throughput is set by what the broker pipeline can drain.
2. Latency floor. A c1 min latency is 13 ms, avg 54 ms, p50 69 ms: that is the 50 ms BatchWindow plus a small
   in-process cost (p50 > window because a request arriving mid-window waits for the flush, then the produce
   response). B adds about 15 ms avg (+ 14 ms c1) for the DynamoDB commit (one TransactWriteItems to ExtendDB).
   C adds another 90 ms avg at 1 client (160 vs 69). The 100 B single client (A 64 ms, B 85, C 95) shows the
   same floor with tiny segments: about 50 ms window, +20 ms commit, +10 ms small PUT.
3. Why C collapses under load: the segment is flushed at 4 MiB (MaxSegmentBytes) before the window expires once
   the offered load is above about 80 MiB/s, and at 8 clients segments are PUT then committed one at a time.
   Direct curl probes against the same SeaweedFS (5 runs each, after a cold first request of 208 ms) gave:
   1 KB PUT 2.5-8 ms, 1 MiB PUT 19-54 ms, 4 MiB PUT 60-73 ms. A 4 MiB segment cycle of about 70 ms PUT + 15 ms
   commit gives 4 MiB / 85 ms = 47 MiB/s, which matches C c8 (51 MiB/s) and C c32 (40 MiB/s). That the cap is
   serialization and not S3 bandwidth is inferred from low CPU everywhere and from the sequential-commit design
   (decisions row 33, commits issued one at a time in seal order, per the task brief); I did not instrument the
   broker to log concurrent PUTs, so "the PUT is not pipelined with the previous commit" is the leading
   explanation, not a measured fact.
4. Backpressure: with 32 clients, 160,000 records (about 160 MB) are buffered client-side, above MaxBufferedBytes
   (64 MiB). In A the broker is CPU-bound and 32 clients are slower than 8 (114k vs 240k records/s), consistent
   with 32 kgo clients plus the broker contending for 8 cores and with read-pause/resume hysteresis, but I did not
   measure the pause time. Resolving measurement: a broker counter of time spent paused, or running 32 clients
   with smaller `-buffered-records`.
5. kgo's own limits: each client has 1 in-flight produce request per broker (default), so one client's throughput
   is bounded by request size / request latency regardless of the broker. 1 client in A (84 MiB/s) is almost
   entirely this: about 5000 records per 54 ms. Raising `-max-inflight` (flag in manibench, up to 5 with idempotence) would show
   how much of the 1-client number is kgo; not run (not in the approved plan).
6. Per-record cost: 100 B records give 75k records/s in A against 86k records/s at 1 KB, so the per-record cost
   is small relative to the per-request/per-batch fixed cost. In C the 100 B run (51k records/s) is faster in
   records than the 1 KB run (31k) only because segments are small and never hit the 4 MiB cut.
7. The 50 ms window dominates latency whenever the stores are fast (A, B: 54-69 ms at 1 client) and is irrelevant
   once the store is the bottleneck (C at 8+ clients: latency is queueing).

## Window and segment experiment (setup C): not run

`manibroker` has no flag for `BatchWindow`, `MaxSegmentBytes`, `MaxSegmentPartitions` or `MaxBufferedBytes`;
`cmd/manibroker/main.go` uses `broker.DefaultConfig()` and only overrides `BrokerID`, `Host`, `Port`. Per the
instructions I did not change broker code. Defaults: BatchWindow 50 ms, MaxSegmentBytes 4 MiB,
MaxSegmentPartitions 24, MaxAttempts 6, MaxBufferedBytes 64 MiB (resume at 32 MiB). I would expose these as flags
(a small cmd/manibroker change) and try on C with 8 clients: window 5/10/20 ms, MaxSegmentBytes 1/2/8/16 MiB.

## Speed-up ideas, ranked by evidence

1. Pipeline PUT and commit (several segments in flight; commit still applied in seal order). Evidence: C is
   idle on CPU (broker 20-35%) with latency 755 ms at 8 clients, and the PUT+commit cycle (about 85 ms per 4 MiB)
   matches the measured 51 MiB/s ceiling. Expected gain: up to the number of in-flight segments, until S3/CP or
   the single broker thread limits (A shows the broker thread tops out near 240 MiB/s).
2. Parallel PUTs for one segment or per-partition-group objects: same evidence; real S3 has higher per-PUT
   latency than SeaweedFS here (60-70 ms for 4 MiB), so the gain on real S3 is larger. Needs the commit order
   preserved.
3. Segment sizing: 1 MiB PUT took 19-54 ms vs 60-73 ms for 4 MiB, so smaller segments lower per-segment latency
   but multiply commits; larger segments amortize the commit. Needs the flags above to test; no measurement yet.
4. Smaller or adaptive BatchWindow: A c1 avg is 54 ms, almost all of it window. At low load, flushing when the
   executor is idle (or 5-10 ms) would cut latency 5x for A/B-like stores. It does nothing for throughput under
   load (C at 8 clients is store-bound).
5. Fewer commit items per byte / batching many producers per segment: already the case at 8+ clients (segments
   hit 4 MiB); at 1 client in C each commit carries about 1.5 MiB per 50 ms. Low priority.
6. Higher-capacity client settings (`-max-inflight 5`, larger `-buffered-records`): affects what a single client
   can offer, not the broker's ceiling. Worth measuring before concluding about the 1-client numbers.
7. S3 multipart or S3 Express One Zone (single-digit-ms PUT) for production: only matters on real S3; cannot be
   evaluated on SeaweedFS. Lowest evidence from this machine.
8. Broker thread (A at 100% of one core, 240 MiB/s): only relevant after items 1-2; profile with pprof first.

## Open questions and what would resolve them

- Whether PUT and commit are truly serialized with nothing else queued: log per-segment PUT start/end and commit
  start/end at debug level, or add pprof/trace.
- Time spent in read-pause due to MaxBufferedBytes: counter in the broker.
- Run-to-run variance and the kgo in-flight effect: repeat each cell 3x, add `-max-inflight 5` runs.
- p99 1.1 s at C c1 (versus p50 136 ms): a few slow S3 or commit calls (SeaweedFS volume growth or ExtendDB
  SQLite lock waits are guesses); not explained here.

## Settings after this change

The runs above used the old defaults: `BatchWindow` 50 ms, `MaxSegmentBytes` 4 MiB, `MaxBufferedBytes` 64 MiB.
Since decision 35 a segment seals at 16 MiB or 200 ms after its first batch (fixed timer), or at 24 partitions or
32 batches; the produce buffer is sized from machine memory (`clamp(memory/16, 128 MiB, 1 GiB)`). All of it is set
by flags (`-segment-bytes 4MiB -segment-window 50ms` reproduces the old segment rule). Re-measured on 2026-10-18 (same machine, single 30 s run per cell, 6 partitions, 0 errors; auto buffer 512 MiB):

| Setup | Load | records/s | MiB/s | avg | p50 | p99 | Before (50 ms, 4 MiB) |
|---|---|---|---|---|---|---|---|
| A mem/mem | 1 client, 1 KB | 19,971 | 19.5 | 242 ms | 237 ms | 448 ms | 86,246 rec/s, 54 ms avg |
| A | 8 clients, 1 KB | 160,360 | 156.6 | 241 ms | 229 ms | 480 ms | 239,931 rec/s, 162 ms avg |
| A | 1 client, 100 B | 22,796 | 2.2 | 214 ms | 214 ms | 228 ms | 74,807 rec/s, 64 ms avg |
| C dynamo/s3 | 1 client, 1 KB | 13,207 | 12.9 | 369 ms | 351 ms | 753 ms | 30,527 rec/s, 160 ms avg |
| C | 8 clients, 1 KB | 51,730 | 50.5 | 760 ms | 674 ms | 1.70 s | 52,252 rec/s, 755 ms avg |
| C | 1 client, 100 B | 17,984 | 1.7 | 272 ms | 266 ms | 363 ms | 51,444 rec/s, 95 ms avg |

Setup B and the 32-client runs were not repeated.

- Low load: latency is now about 200 ms (the window) plus the store time. A 1-client latency went from 54 to 242 ms
  and throughput fell by the same factor, because one kgo client has one produce request in flight and is therefore
  bounded by request size / latency (analysis point 5). This is the cost of the fixed timer with no early seal when idle.
- C at 8 clients is unchanged: 50.5 MiB/s against 51.0 before, with 16 MiB segments instead of 4 MiB. Analysis point 3
  explained that ceiling as a 4 MiB PUT + commit cycle. That explanation is wrong: quadrupling the segment size did not
  move the ceiling. The limit is something else that is independent of segment size, and it is still unmeasured
  (candidates: SeaweedFS write throughput on the Docker volume, the single executor or connection path in the broker,
  the commit). Per-segment timing in the broker would settle it; speed-up ideas 1 to 3 should not be trusted until it does.
- A at 8 clients dropped from 240k to 160k records/s. Not investigated (larger segments, 512 MiB buffer, one run).
Items 1 to 3 above still apply only as hypotheses.


## Where the time goes (per-segment metrics, setup C)

Since decision 36 `manibroker` serves Prometheus metrics on `-metrics-addr` (default `:9100`, `/metrics`).

How to read them (all names `manibroker_...`; histograms in seconds):

- `segment_put_seconds`: seal to successful PUT (retries included). `segment_queue_wait_seconds`: PUT done, waiting for
  the earlier segments' commits (commits go out one at a time, in seal order). `segment_commit_seconds`: CommitSegment
  issued to answered. `segment_seal_to_ack_seconds`: the sum of the three. `segment_open_age_seconds`: time the first
  batch waited in the open buffer before the seal. Client latency is about open age + seal to ack.
- The commit stage is a single serial pump, so `rate(segment_commit_seconds_sum[1m])` is its utilisation: near 1 means
  the commit is the ceiling, and throughput is about (bytes per segment) / (commit seconds per segment).
- `segments_in_flight_at_seal` above 1 means PUTs overlap; `segments_in_flight` and `buffered_bytes` are gauges as of
  the last segment event. `reads_paused` / `reads_paused_seconds_total` show backpressure.
- `segments_sealed_total{reason}` and `segment_sealed_batch_bytes_total` give the real segment size
  (bytes / segments) and which rule seals: with 8 clients most segments seal by the timer or the 32-batch cap, not at 16 MiB.

Measurement, 2026-10-18: setup C, fresh `scripts/it-up.sh` backends, 8 clients, 1 KB, 30 s (+ about 1 s drain), 6 partitions,
defaults of decision 35, metrics scraped before and after the run (the table is the difference). Two runs, same backends
(the second reuses the first's SeaweedFS and ExtendDB data, with new bucket and table prefix):

| | run 1 | run 2 |
|---|---|---|
| client result | 18.7 MiB/s, 19.1k rec/s, avg 2.04 s, p99 4.6 s | 13.6 MiB/s, 14.0k rec/s, avg 2.82 s, p99 7.2 s |
| segments committed (sealed by timer / 32-batch cap / size) | 117 (73 / 42 / 2) | 91 (64 / 25 / 2) |
| average segment | 5.3 MB | 4.9 MB |
| open age, avg | 171 ms | 173 ms |
| PUT, avg (per segment) | 716 ms | 676 ms |
| queue wait for commit, avg | 723 ms | 1363 ms |
| commit, avg | 242 ms | 323 ms |
| seal to ack, avg | 1.68 s | 2.36 s |
| sum of commit seconds / 31 s | 28.3 s (91%) | 29.4 s (95%) |
| segments in flight at seal, avg (p90 <=) | 6.6 (12) | 7.1 (12) |
| backpressure paused | 0 s | 0 s |

Reading:

- PUTs do overlap: 6 to 7 segments are in flight on average when one is sealed. The PUT is not the ceiling; a PUT
  (0.7 s for 5 MB, concurrent with its neighbours) waits behind the commit pump anyway.
- The commit stage is saturated: it was busy 91 to 95% of the wall time, one commit at a time at 240 to 320 ms each. That
  is the ceiling: 117 commits in 31 s at about 5.3 MB each is 20 MB/s (run 1), and the queue wait (0.7 to 1.4 s on
  average) is the symptom. Reads were never paused, so the 512 MiB buffer was not the limit either.
- The segments are small (about 5 MB, not 16 MiB), because at 8 clients the 200 ms timer or the 32-batch cap seals first.
  That is why quadrupling `MaxSegmentBytes` did not move the old 50 MiB/s ceiling: the size rule almost never fires, and
  the bytes per commit stayed constant. Throughput is commits per second x bytes per commit, with commits serial
  (about 4 per second here), so raising the batch cap or the window (more bytes per commit) or making the commit
  faster or concurrent per partition are the levers; speed-up ideas 1 and 2 above (pipelined or parallel PUTs) would not help: PUTs already overlap, and the commit is what is saturated.
- Caveats: these runs reached only 13.6 to 18.7 MiB/s against 50.5 MiB/s measured earlier the same day on the same
  machine, so the machine or Docker was slower during these runs (a kind cluster was also running); absolute numbers
  are not comparable to the table above, and the earlier ceiling was not re-measured with metrics. What does carry over is the
  shape: commit utilisation near 100% with PUTs overlapped. The commit time itself (240 to 320 ms for a
  TransactWriteItems of at most 32 batches on ExtendDB/SQLite) was not broken down further; the next step is to time
  the CP call inside the control plane. Two runs only.

## Where the commit time goes (control-plane metrics, setup C)

Decision 37 added `manicp -metrics-addr` (default `:9101`, `/metrics`, prefix `manicp_`) and `manibroker_cp_call_seconds{op}`
(the CP client's send-to-full-response time). Together they split a broker `commit_ns` into client RTT, server handler
time, DynamoDB calls and the commit's read/decide steps.

Measurement, 2026-10-18: setup C with fresh `scripts/it-up.sh` backends, 8 clients, 1 KB, 30 s, 6 partitions, defaults of
decision 35, both /metrics scraped before and after the run (deltas). Two runs, new bucket and table prefix each time.
Machine: the kind cluster container was up (about 20 to 30% of a core), SeaweedFS 150 to 170%, ExtendDB 32 to 39%, a
Chrome for Testing helper and fseventsd a few %, load average about 4.7 before the runs. Averages per CommitSegment:

| | run 1 | run 2 |
|---|---|---|
| client result | 66.4 MiB/s, 68.0k rec/s, avg 579 ms | 63.7 MiB/s, 65.2k rec/s, avg 602 ms |
| segments (timer / 32-batch cap / size), avg size | 181 (51 / 117 / 13), 11.9 MB | 178 (44 / 126 / 8), 11.5 MB |
| broker `segment_commit_seconds` | 96.3 ms | 116.3 ms |
| client CPCall RTT (`cp_call_seconds{commit-segment}`) | 96.2 ms | 116.0 ms |
| manicp handler (`handler_seconds{commit-segment}`) | 95.7 ms | 115.5 ms |
| DynamoDB `TransactWriteItems` | 63.0 ms | 77.2 ms |
| reads (1 + 3 GetItem, 1 BatchGetItem; `commit_prep{reads}`) | 32.2 ms (GetItem 5.8 each, BatchGet 16.3) | 37.8 ms (6.5 each, 18.8) |
| decide + build items (`commit_prep{decide_encode}`) | 0.23 ms | 0.28 ms |
| commit body | 5.8 KB | 6.0 KB |
| attempts / TransactionCanceled / unavailable retries | 181 / 0 / 0 | 178 / 0 / 0 |
| in-flight CP requests | 0 at both scrapes; commits are one at a time | same |
| broker queue wait / PUT / seal to ack | 59 / 290 / 445 ms | 52 / 297 / 465 ms |

Reading:

- The accounting closes. Client RTT minus handler time is 0.4 to 0.5 ms (HTTP, JSON and the loopback), and handler time
  minus (reads + decide + transact) is about 0.2 ms. So no time hides between the broker and the control plane or in the
  JSON code: everything is inside the DynamoDB calls, 60 to 65% in the one transaction and 33% in the five consistent reads.
- There were no conflicts: zero TransactionCanceled, zero retries, one attempt per commit. The commit is not slow because
  it races (there is a single serial committer per broker).
- The 240 to 320 ms of the earlier section could not be reproduced: this time commits took 96 to 116 ms and the run
  reached 64 to 66 MiB/s (against 13.6 to 18.7 MiB/s then, and 50.5 MiB/s earlier that day). The code path is unchanged
  between the two, so the earlier runs were slowed by the machine or ExtendDB state (that session had a kind cluster
  running and a reused ExtendDB database), not by the control-plane service code: the service adds under 1 ms around the
  DynamoDB calls. ExtendDB's own latency is the variable: 63 to 77 ms for a 32-batch transaction here, against 39 to 58 ms for the
  whole commit measured directly without SeaweedFS competing for the same cores.
- Cheap lever: the three serial GetItems (segment key, node epoch, and the producer counter) plus the BatchGet cost
  about 32 to 38 ms before the transaction starts; they could be one BatchGet (or overlapped), saving about 15 to 20 ms of ~100.
- Tried the lever: `commitOnce` now does one consistent BatchGetItem (segment, node, producer counter, HWM, topic and
  producer-state keys; split at 100 keys, UnprocessedKeys retried as before) instead of 3 GetItems + 1 BatchGet; malformed
  requests still read only the segment key. The write transaction's conditions are unchanged. Result on one 30 s run each
  (setup C, 8 clients, 1 KB, same machine): `commit_prep{reads}` 47.2 ms before, 44.2 ms after; the single BatchGetItem
  took 43.5 ms against 21.4 ms for the old smaller one, and 3 GetItems at about 8 ms were removed. So ExtendDB's
  BatchGetItem cost grows with the number of keys, and merging the reads saved only about 3 ms (within run-to-run noise;
  the commit avg was 137 vs 151 ms, throughput 51.5k vs 44.4k rec/s, both noise-dominated). On real DynamoDB the round-trip
  saving should be larger; against ExtendDB it is not measurable here.
- With larger segments (11.9 MB, not 5 MB), the commit is no longer saturated: summed commit seconds were about 0.55 to 0.67 of
  wall time. Two runs only; the earlier slow state was not reproduced, so a repeat with `reuse` of the old database would
  confirm that state, not the code, was the cause.


## CP range compaction (decision 39)

The r1 AWS run (`docs/aws-benchmark-results-r1.md`) averaged 5 to 8 MiB per segment because the 32-batch cap sealed before
16 MiB, and the serial commit stage was the ceiling. The DynamoDB commit now writes one range run item per (partition,
segment), and `MaxSegmentBatches` is 256, so the byte cap (16 MiB) or the 200 ms timer should seal instead; the counter
`segments_sealed_total{reason}` shows it (`batches` should disappear, `items` appears only with very many idempotent
producers per segment). Not re-run on AWS. Local commit cost, `go test -run X -bench Commit ./cp/dynamo` on ExtendDB
(`scripts/it-up.sh`; M1; one idempotent producer per partition; 200 commits each):

| segment | items before | commit before | items after | commit after |
|---|---|---|---|---|
| 32 batches, 24 partitions | 82 | 61.8 ms | 74 | 58.2 ms |
| 32 batches, 6 partitions | 46 | 33.0 ms | 20 | 22.3 ms |
| 256 batches, 24 partitions | refused (ErrTooLarge) | - | 74 | 62.1 ms |
| 256 batches, 6 partitions | refused (ErrTooLarge) | - | 20 | 25.9 ms |

The commit time follows the item count, not the batch count: a 256-batch segment commits in about the time of a
32-batch one with the same partitions. Old 32-batch results elsewhere in this file were measured with the old cap.

## Sequencer (decision 46)

Why: on AWS, 6 brokers each with their own manicp committed ~33 of 288 partitions to one DynamoDB table and lost the
run-item compare-and-set constantly (387 of 705 attempts cancelled on broker 1, 7 commits gave up after 20 attempts,
commit_ms up to 11 s). The control plane now owns sequencing: one manicp, one loop, group commit, offsets from memory
(`cp/doc.go`, "Sequencer").

Measurement: `MANIFESTER_SEQ_BENCH=fake|extenddb go test ./cp/cptest -run SequencerBench -v` (8 s per case, 2026-10-19,
Apple laptop, Go 1.24). Six committers loop at the maximum rate; each commit is a segment of 33 random distinct partitions
of 288 (one batch of 1-5 records each); a 503 is resent under the same key, as a broker does. "before" is six independent
`dynamo.Store` values on one table (the old path: every commit reads the newest run of each partition and does its own
CAS); "after" is one `Sequencer`. The latency a committer sees includes the resends. `items/tx` counts applied
transactions only.

fakedb with injected latency (30 ms per `TransactWriteItems`, 5 ms per read call):

| case | commits/s | avg ms | p50 ms | p99 ms | 503 | cancelled txs | txs | items/tx |
|---|---|---|---|---|---|---|---|---|
| before: 6 independent stores | 20.0 | 319 | 51 | 6501 | 9 | 280 | 440 | 34 |
| after: sequencer, pipeline 1 | 90.9 | 66 | 65 | 108 | 0 | 0 | 243 | 91 |
| after: sequencer, pipeline 4 (default) | 90.9 | 66 | 65 | 101 | 0 | 0 | 243 | 91 |
| disjoint partition sets (48 per committer), pipeline 1 | 61.5 | 98 | 96 | 150 | 0 | 0 | 246 | 68 |
| disjoint partition sets (48 per committer), pipeline 4 | 148.0 | 41 | 32 | 75 | 0 | 0 | 982 | 41 |

Local ExtendDB (Docker Desktop, `scripts/it-up.sh`; a transaction takes about 100 ms there, so this is a slow-database
case, not a prediction for DynamoDB):

| case | commits/s | avg ms | p50 ms | p99 ms | 503 | cancelled txs | txs | items/tx |
|---|---|---|---|---|---|---|---|---|
| before: 6 independent stores | 15.8 | 398 | 61 | 8301 | 7 | 192 | 318 | 34 |
| after: sequencer, pipeline 1 | 53.4 | 113 | 104 | 277 | 0 | 0 | 143 | 90 |
| after: sequencer, pipeline 4 (default) | 47.8 | 126 | 118 | 238 | 0 | 0 | 128 | 91 |
| disjoint partition sets, pipeline 1 | 46.6 | 129 | 126 | 265 | 0 | 0 | 187 | 68 |
| disjoint partition sets, pipeline 4 | 49.2 | 123 | 107 | 270 | 0 | 0 | 347 | 39 |

Reading it:
- The old path's median is fine (51 ms) but the tail is not: p99 6.5 s and 8.3 s, hundreds of cancelled transactions, the
  same shape as the AWS run. The sequencer has zero cancellations in steady state and a p99 about 1.5x its median.
- Group commit does the work: about 3 segments (91 items, near the 100 limit) per transaction. The bound is transactions
  per second times segments per transaction: 30 ms per transaction gives 91 commits/s (22 transactions/s x 3 plus a
  read at start), 1 GiB/s of 16 MiB segments needs 64/s. The ExtendDB row is the same arithmetic at 100 ms.
- Pipelining only helps when the partition sets of commits are disjoint (2.4x on fakedb). With six brokers that all write
  all partitions, two random 33-of-288 segments are disjoint with probability about 2%, so the groups conflict and
  serialise: pipeline 1 and 4 are equal. ExtendDB executes transactions about one at a time, so it shows no gain either.
  Default 4 costs nothing and is kept for deployments where brokers own partition ranges.
- Steady state costs one transaction per group and no reads (the "before" path does about 23 descending Queries per
  commit): `manicp_seq_cache_total{cache="end"}` hits dominate after the first touch of each partition.
- Not measured here: real DynamoDB (single-digit ms reads, 20-40 ms transactions): the AWS run is the evidence.
