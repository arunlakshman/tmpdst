# Stage 5: AWS run w4 plan (NOT executed)

Status: plan only, 2026-10-21. Nothing here has been applied; no AWS resource exists for w4. Context: `docs/design-a-prototype.md`
section 8 (stage 5), `docs/log-cp-service.md`, runbook A11, `docs/w2-bottleneck-verification.md` and
`docs/w3-affinity-and-window-experiments.md` (what the old store did at 500k), `docs/x1-x3-x4-results.md` (DynamoDB numbers).

## Goal

Re-measure 500k, 750k and 1M msg/s (1 KiB, lz4, 288 partitions, 64 producers: the WarpStream benchmark workload, unmodified OMB)
with RANDOM clients and the production CP (log store, hot standby, `-max-segment-partitions 4096`). In w2/w3 this load collapsed
at about 250-275k because every segment hit the 96-partition cap and the sequencer serialised 86-item transactions. The log
store has no item limit and one `PutItem` per slot, so the question is whether 1M (about 977 MiB/s) holds, at what commit
latency, and what a failover costs.

## Stack (account 478483354994, profile hydsandbox, ap-south-2a, `run_id = w4`)

| part | choice | why |
|---|---|---|
| brokers | 6 x m6in.4xlarge | as WarpStream and w2/w3; broker CPU was 0.1 of 16 cores at 250k |
| loadgens | 2 x m6in.8xlarge | as before (9 of 32 cores at worst in w2) |
| CP | 2 x **m6in.xlarge** (4 vCPU, 16 GiB, 30 GB gp3), rank 0 active, rank 1 hot standby | see below |
| store | `cp_store = log` (default), `cp_standby_count = 1`, `-takeover-after 5s`, `broker_extra_args = ""` | no 96 cap, no affinity |
| table | new per run, on-demand, TTL on `ttl`, `ddb_warm_write_units/read_units = 30000` | X4: a cold table throttles at ~4k items/s; the index writes ~10k rows/s |
| other | NLB on (bootstrap), `bootstrap-mode balanced` (default), NOT `--affine`, `max_runtime_minutes = 150` | see decision 2 |

CP size. Model: committed segments are about 30-36/s at 500k to 1M (6 brokers x 5 seals/s on the 200 ms window; 16 MiB size
cap), each touching about 250-288 partitions. Pebble apply was measured at about 1.1k segments/s at 150 partitions (0.8 ms per segment on M1;
about 1.6 ms at 288), so one apply core needs about 6% of one core at 36/s: 4 vCPU is ample for apply, the HTTP handler, the
indexer (about 10k rows/s of BatchWriteItem) and Alloy. Memory: 20k segments / 4.5M batches was 234 MiB RSS and 286 MiB of checkpoint, built in memory;
the run produces perhaps 10M batches (about 0.7 GiB), so 16 GiB leaves more than 10x margin. Disk: Pebble 55 B/batch (about 0.6 GB) on a
30 GB root volume; checkpoints go to S3. m6in.2xlarge would add nothing measurable; it is cheap insurance only (+$0.23/h per host).

## Stages (limit 10 min per run INCLUDING warm-up and recovery; 3 min gap between stages for drain, snapshots and checks)

New workload files are needed (existing ones are 3-5 min test + 1 min warm-up; the 6 min stages below are 1 warm-up + 5 test).
Abort rules: `--abort-p99-ms 60000` (the w3 rule), errors > 0.1%, `run-omb.sh --max-minutes 10` per stage.

| # | stage | length | pass criteria (from the models) |
|---|---|---|---|
| 0 | checks | - | `check-cp` ok, `cp-roles` = active/standby, topic has 288 partitions, `metrics/cp/` and `metrics/cp-1/` filling |
| 1 | smoke 100k | 2 min | >= 95k msg/s, 0 errors; publish p99 < 1 s; about 20 seg/s; log slots/s <= seg/s; `lost_slots` 0 |
| 2 | 500k | 6 min | >= 475k, p99 < 1 s; broker `segment_queue_wait` p99 < 50 ms; CP call p99 < 250 ms; `queue_depth` not growing |
| 3 | 750k | 6 min | >= 712k, p99 < 1.5 s; same CP numbers; slot WRU < 700/s (hot key limit 1000 WRU/s, X1) |
| - | PAUSE | 3-5 min | re-login to SSO, read stages 1-3 in Grafana before spending the 1M stage |
| 4 | 1M | 8 min | >= 950k (95% rule), p99 < 2 s, no producer errors, brokers balanced (max/min < 1.5), segments mostly 8-16 MiB |
| 4b | 1M symmetric: 64 producers + 64 consumers (`vanity-1gibps`, needs Fetch) | 8 min | produce bars of stage 4 hold WITH consumers; consume >= 950 MiB/s (the reported higher-throughput number: ~1 GiB/s write + ~1 GiB/s read); backlog not growing; e2e p99 < 2 s (WarpStream's claim); broker fetch CPU/net and cache hit rate captured; CP Lookup/fresh-read rate and latency bounded |
| 5 | kill the appender at 500k | 10 min | 1 warm-up, kill at t = 3 min of steady load (`bench-ctl.sh kill-appender 60`, writes the marker `summarize.py` reads), 60 s down, failback; ack gap <= 15 s (5 s takeover + ~2 s backoff + tail pass), 0 errors, no ABORT, rank 0 active again |

Common pass bars (stages 2-4): `index_lag_slots` bounded (< 10 s of slots, not growing); `apply_lag_slots` and `standby_lag_slots` bounded;
`index_throttles` and `index_unprocessed` about 0; `lost_slots` rate < 1% of slots; `hole_fills` about 0; `blocked` 0;
`reads_paused` 0; S3 PUT p99 < 300 ms; no DynamoDB throttles; committed MiB/s about 0.5 x offered MiB/s (lz4 with 50% random bytes).
Expected at 1M: about 35 commits/s, slots 35/s, slot entry 6-12 KB (about 8 WRU), index rows about 10k/s.

## Capture

Grafana (read-only, token from the environment only, e.g. `read -rs GRAFANA_PROM_TOKEN`, never written to a file; same queries as the
w2 doc, `run_id="w4"`, `/api/prom/api/v1/`): `manibroker_segments_sealed_total` by `reason`, `manibroker_segment_partitions` (cap 4096),
`segment_queue_wait`, `cp_call`, `seal_to_ack`, `segments_in_flight`, `buffered_bytes`, `reads_paused`; `manicp_log_slots_total`,
`slot attempts/lost`, `append_seconds`, `entries_per_slot`, `entry_bytes`, `apply_lag_slots`, `standby_lag_slots`, `hole_fills_total`,
`role_changes_total`, `index_*`, `checkpoint_*`, `window_keys`; CloudWatch DynamoDB (WRU, throttles, latency) and S3 (PUT latency, 5xx);
node CPU, network and memory per role (broker, cp, loadgen), `process_*` of manicp (RSS, Pebble). Local: `collect-results.sh` then
`summarize.py` (reads `metrics/cp/` and `cp-1/`, shows the active node per stage and the kill-appender failover summary) BEFORE destroy.

## Cost (on-demand; actual 2026 numbers)

README prices (fetched 2026-10-19): fixed about $12.0/h for 6 brokers, 2 loadgens, 1 CP, NLB, volumes, IPv4; the standby adds about $0.23/h, so
about $12.3/h. w3 (40 min) cost $7-10, which this matches. w4 stack time about 2 h (see timing): **about $25** compute.
DynamoDB: index rows dominate, about 10k WRU/s at 500k+; total about 8-15M WRU at README's about $0.625/M (the 4,096 point can add 10M+): **$5-20**.
S3 and the rest under $2; the one-off warm-throughput charge is unverified. **Estimate $30-50, cap $60**; `budget_usd = 200` alert stays.

## Timing and the SSO token (about 1 h)

Needed: login, apply (15 min incl. boot and READY), 10 min checks, load about 70 min (46 min of stages + gaps), collect 5 min, destroy 10 min.
Wall time about 2 h 5 min, inside the 2.5 h budget. One token cannot cover it. Cleanest option, **one re-login at the planned pause**:
- T+0 login #1 (token to about T+60). T+0-15 apply, upload, READY. T+15-25 checks. T+25 smoke, T+30 500k, T+39 750k, ends T+48.
- T+48 PAUSE: the user runs `aws sso login` again (1 min), reads the 500k/750k panels; go/no-go for 1M. Token #2 runs to about T+110.
- T+52 1M produce-only (ends T+60), T+63 1M symmetric (ends T+71), T+74 kill test (ends T+84), T+87 `collect-results.sh`, **teardown starts T+90**
  = 20 min margin before token #2 expires (rule: >= 15 min). The 4096 stage is dropped (scope is OMB at 288 partitions only).
- The load runs detached on loadgen 0 (instance role), so the run survives an expired token; only `bench-ctl`, S3 sync and destroy need it.
- Rejected: two separate stacks (second apply costs 15 min and about $5 more); a long-lived SSO session setting (not ours to change).
- If any stage slips by more than 10 min, shorten the gaps first; the kill test (stage 5) is the first stage to drop, 4b is not.

## Teardown and verification checklist

- [ ] `results/w4/summary.md` exists locally and `terraform-outputs.json`, `build-git-sha.txt` saved (destroy force-deletes the bucket)
- [ ] Grafana panels for each stage window noted (epochs from `omb/stages.tsv`) before the data ages out
- [ ] `terraform destroy -var-file=my.tfvars` (run by the user); self-destruct guard `max_runtime_minutes = 150` is the backstop, not the plan
- [ ] README post-destroy checklist all empty: instances (incl. `cp_standby`), NLB and target groups, DynamoDB table, bucket, SG/VPC, budget
- [ ] No stray tag `run_id=w4` resources (`aws resourcegroupstaggingapi get-resources`, read-only)
- [ ] Results doc `docs/w4-...md` written from the data; decision on the S3 index (stage 5 exit criterion) recorded

## Decisions the product owner must make

1. CP size: m6in.xlarge x 2 (recommended, $0.46/h) or m6in.2xlarge.
2. Client placement: `bootstrap-mode balanced` (default; each OMB client pinned to one broker round-robin, like WarpStream's discovery) or
   `random` (stock; binomial skew across brokers, a harsher test). Partition choice is random per message either way. Recommended: balanced.
3. DECIDED (product owner): OMB with 288 partitions only, matching the WarpStream benchmark; the 4096-partition point is dropped.
4. Consumers: DECIDED (product owner). Scope is catching up with the WarpStream post (symmetric 1 GiB/s write+read, 288 partitions), so the consumer read path is REQUIRED for the headline result. PREREQUISITE, see docs/TODO.md "Consumer read path" and docs/surprises-audit.md: Fetch exists (assign(), no groups) but ignores MinBytes/MaxWait, so 64 consumers would cause ~48k Lookups/s (RCU above the 30k warm), the read path has no S3 cache (~6.5k GET/s at 1 GiB/s, above S3's 5,500/s per prefix), so 4b is BLOCKED until the long-poll Fetch, Lookup cache and S3 read cache are built; consumers also need to start at the tail. Original note:: at least one stage with consumers, the 1M symmetric stage 4b. Produce-only stages stay for diagnosis; Fetch must be confirmed working on the log store in the smoke stage (a short consume check) before spending 4b.
5. Cost cap ($60 proposed) and whether the one-time DynamoDB pre-warm charge is acceptable (not verified).
6. Go/no-go at the pause, and the go.mod `go 1.25` question for `x/text` v0.39.0 (GO-2026-5970), unrelated to the run.
7. The build toolchain for the artifact: `build-and-upload.sh` now defaults `GOTOOLCHAIN=go1.26.10` (no stdlib advisories).
