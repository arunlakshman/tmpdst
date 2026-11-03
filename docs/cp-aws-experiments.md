# Control plane experiments on EC2 (real DynamoDB and S3)

Goal: test the control plane alone, on real AWS, one question at a time.
Region `ap-south-2` (Hyderabad). One small EC2 box. Two control plane nodes on it.
The table `mds_cpv1_cp` and the bucket `mds-cpverify-cpv1-478483354994` are shared by all runs.

Rules
- One experiment = one hypothesis. We run one, read the result, and only then start the next.
- Every experiment starts from an empty table and an empty bucket (the empty step is part of the script).
- Nothing in AWS is destroyed by any script. The user inspects first and says when to destroy.
- Numbers marked (provisional) are set again after E0 gives a real baseline.

How an experiment runs (all from the laptop)
1. `ops/scripts/cp-exp/build.sh` and `push.sh` put the three binaries and the scripts in S3.
2. `ops/scripts/cp-exp/box.sh` runs a script on the box over SSM.
3. The script on the box does the test, then `collect.sh` uploads logs, metrics and the `-check` output to `s3://BUCKET/results/NAME-TIME/`.
4. `fetch-results.sh` brings the results back. The PASS or FAIL verdict is printed by the script itself.

Tools
- `mdscontrolplane` is the thing under test.
- `mdscpverify -mode load` sends commits, retries the same key on error, writes every acked answer to a file, and prints one summary line (acked, failed, retries, latency, longest stall).
- `mdscpinspect -check -acked FILE` reads the log from DynamoDB and compares it with the acked file.
- `node.sh` starts, stops, kills (`-9`), freezes (`SIGSTOP`), thaws and wipes one node.

Checks that every experiment must pass (the "always" list)
- A1. The log has no gap, every slot decodes, every slot is under 350 KiB, every slot names its writer.
- A2. Every acked commit is in the log with exactly the offsets that were acked. No lost ack.
- A3. No segment key appears with two different answers. No two acked ranges overlap in a partition.
- A4. Replayed end offsets equal the records counted from the log, for every partition.
- A5. If a checkpoint exists: its CRC is good, and checkpoint plus tail gives the same state hash as a full replay.
- A6. Node logs have no `trip`, no `ErrCorrupt`, no ERROR line (unless the experiment says it is expected).
- A7. No `checkpoint_decision` with reason `control_io_pending` (that reason is gone since the checkpoint lane; seeing it means a regression).

---

## E0. Baseline: the box works and the numbers are known

Hypothesis: with the IAM role (no SSO), one node boots on an empty table and bucket, serves commits, and writes checkpoints. Latency is lower than from the laptop.

Steps
1. Install binaries on the box. Empty table and bucket.
2. Start `cp0` (rank 0) with `-checkpoint-every 30s -checkpoint-min-slots 200`.
3. `mdscpverify -mode load -duration 60s -workers 4`.
4. Collect.

Success criteria
- Node ready within 30 s of start (provisional).
- Load: `failed = 0`, p50 <= 20 ms, p99 <= 150 ms (provisional; the laptop mean was 25 ms).
- At least 1 `checkpoint_decision` with `written=true`; the S3 object exists.
- A1 to A7 all pass.

Verification: the load summary line, the `-check` output, the `checkpoint_decision` lines, the S3 listing.

---

## E1. Failover and recovery (kill -9, restart on a wiped disk, full outage)

Hypothesis: when the active node dies under load, the standby takes over; no acked commit is lost or doubled. The dead node comes back with an empty disk, restores from the checkpoint, and the cluster ends in a state equal to a full replay. After both nodes die and restart, state is still correct.

Steps
1. Start `cp0` (rank 0) and `cp1` (rank 1), `-checkpoint-every 30s -checkpoint-min-slots 200`.
2. Start load against both addresses (`-cp 127.0.0.1:P0,127.0.0.1:P1`), 4 workers, 150 s.
3. Phase A, at +30 s: `kill9 cp0`. Record the time.
4. Phase B, at +60 s: `wipe cp0`, `start cp0` (comes back with an empty Pebble dir). Record time-to-ready.
5. Load ends at +150 s. Wait 10 s.
6. Phase C (full outage): stop load, `kill9 cp0`, `kill9 cp1`, `wipe` both, start both. Record time-to-ready.
7. Short load again (30 s) to prove the new cluster serves. Collect.

Success criteria
- Load across A and B: `failed = 0` (every commit is eventually acked by retrying the same key).
- Longest stall between two acks (`max_stall_ms`) <= `takeover-after` + 5 s (provisional; `takeover-after` is 5 s by default).
- Takeover happened once in phase A: `cp1` log shows it became the appender once; no second takeover until `cp0` returns.
- Phase B: `cp0` is ready within 60 s (provisional) and its first applied slot is not 0 (it restored from the checkpoint). The log line shows restore, or the time-to-ready is far below a full replay.
- Phase C: both nodes ready within 120 s (provisional); the second load has `failed = 0`.
- Live end offsets from the cluster equal the records acked, per partition (`mdscpverify -mode ends -acked FILE`).
- A1 to A7 pass at the end.

Verification: `-check -acked`, the load summary, the node logs (`takeover`, `restore`), the time stamps written by the script.

---

## E2. Zombie writer (a paused node wakes up)

Hypothesis: an old appender that is frozen past the takeover time, and then resumes, cannot corrupt the log. Its writes are refused, and no segment gets two answers.

Steps
1. Start `cp0` (rank 0) and `cp1` (rank 1). Start load against both, 120 s.
2. At +20 s: `freeze cp0` (SIGSTOP) for `takeover-after + 10 s`. The standby takes over.
3. `thaw cp0` (SIGCONT). Keep load running for 40 s more. Collect.

Success criteria
- `failed = 0`; `max_stall_ms` <= `takeover-after` + 5 s (provisional).
- No slot was written twice (A1 contiguous) and A2, A3 pass: the thawed node did not make a second answer for any key.
- Writer names in the log: no slot between the takeover and the moment `cp0` caught up is written by `cp0`. The script prints the writer per slot range and the check fails on a violation.
- The `cp0` log shows its first append after the thaw lost (conflict) or the node caught up first. Count is printed.
- A1 to A7 pass.

Verification: `-check -acked`, `mdscpinspect -v` writer column, node logs.

---

## E3. Limits and errors (entry size and DynamoDB throttling)

Hypothesis (a): a commit too big for a DynamoDB item is refused with `ResourceExhausted` and the log is not harmed.
Hypothesis (b): when DynamoDB throttles, the node does not spin: the retry rate stays bounded, no acked data is lost, and it recovers when the load stops.

Steps (a), on the normal table
1. Start `cp0`. Send one commit of 256 batches x 256 partitions (> 350 KiB when encoded) and 20 normal commits.
2. Check the answers and the log.

Steps (b), on the 1 read / 1 write unit table (`enable_throttle_table = true`, node started with `-table mds_cpv1_throttle_cp`)
1. Start `cp1` on the throttle table. Load 50 commits per second for 60 s. Then stop the load and wait 30 s.
2. Read the metrics: DynamoDB attempts by result, and append retries per second.

Success criteria
- (a) The big commit gets `ResourceExhausted`; the 20 normal ones are acked; no slot over 350 KiB; A1 to A5 pass.
- (b) `ProvisionedThroughputExceeded` is seen (the test really throttled). Append attempts <= 200 per second at every 5 s sample (no tight loop). No acked commit is lost (A2). After the load stops, the node reaches `applied = tail` within 30 s.
- If (b) shows a tight loop, the experiment FAILS and that confirms open finding 5 (no backoff on append retry). A failure here is a useful result.

Verification: client answers, metrics scrape (`dynamo_attempt_seconds` by result), `-check` on both tables.

---

## E4. Sustained load (soak)

Hypothesis: for one hour at a moderate rate, memory and disk stay bounded, the checkpoint and the trim keep their cadence, and DynamoDB cost matches the estimate.

Steps
1. Start `cp0`, `cp1` with `-checkpoint-every 60s -window-slots 8192` (so trim happens in an hour).
2. Load at 100 commits per second for 60 minutes (shorter first run: 15 minutes).
3. Sample every minute: RSS of both nodes, Pebble dir size, applied slot, `checkpoint_decision` lines, DynamoDB consumed capacity.

Success criteria
- `failed = 0`; p99 commit latency in the last 10 minutes is within 1.5 x the first 10 minutes.
- RSS after warm-up (10 min) grows by no more than 20 % until the end; Pebble dir size is bounded (does not grow with every slot after trim starts).
- Checkpoints: at least one written every 3 x `checkpoint-every`.
- Trim: deleted-marker slots (0xFE) existed below checkpoint minus keep; no gap in the live part. *Superseded by decision 62: trim now deletes rows.*
- A1, A2, A3, A5, A6, A7 pass (A4 full replay is skipped when the log is trimmed; the checkpoint plus tail check is used).
- Consumed read and write units are within 20 % of the estimate from E0 x time.

Verification: the per-minute sample file, the `-check` output, the S3 checkpoint age, the AWS bill line for the table after the run.

---

## Order and stop points

E0, then E1, E2, E3, E4. After each experiment we stop, read the results together, fix what failed, and only then go on. A failure is a result, not an error in the plan.
