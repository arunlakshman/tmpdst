# Observability reference

Everything a run leaves behind, for reconstructing it afterwards (decisions 36, 37, 38).
Two services, each with a metrics port that serves `/metrics` and `/healthz`:

| Service | Port flag | Default | Metric prefix |
|---|---|---|---|
| `manibroker` | `-metrics-addr` | `:9100` | `manibroker_` |
| `manicp` | `-metrics-addr` | `:9101` | `manicp_` |

Common flags: `-run-id` (or `$MANIFESTER_RUN_ID`), `-log-level debug|info|warn|error`,
`-manifest-out`. Pick one run id for the whole experiment and give it to both services and
to the load generator's output file names; then `run_id` joins every log line, the
`*_build_info` series and the manifests.

## Logs

JSON, one object per line, on stderr. Every line has `time`, `level`, `msg`, `run_id`,
`service`, `version`, `git_sha`, and on the broker `broker_id`. Event lines also have
`event` (the stable name to filter on). Durations are milliseconds with 0.01 ms resolution
(`*_ms`); sizes are bytes.

Volume: one line per segment event (about 3 per segment) on the broker; manicp logs one
`cp_summary` line a second at INFO (the per-request `cp_call` line is DEBUG, because a consume
stage is tens of thousands of requests a second: audit item 9). At 10 segments a second that
is about 30 lines a second in total, a few MB an hour. Segment lines are not sampled;
if a run goes much higher, use `-log-level warn`, which keeps only failures, retries,
throttles and non-2xx lines (the metrics still count everything). Warnings from the AWS
middleware are rate limited to 20 a second per client; when lines are dropped the next one
is preceded by an `aws_warn_suppressed` line with the count.

### manibroker

| event | level | fields | meaning / how to read |
|---|---|---|---|
| `start` | INFO | `go_version`, `git_dirty`, `hostname`, `log_level`, `run_id_generated` | Process start. |
| `manifest` | INFO | `flags`, `flags_set`, `tuning`, `instance`, `region` | Effective configuration, same content as the manifest file. |
| `manifest_written` / `manifest_failed` | INFO / WARN | `dest`, `err` | Result of `-manifest-out`. |
| `segment_sealed` | INFO | `segment`, `reason`, `bytes`, `batches`, `partitions`, `open_age_ms` | The open segment was sealed (reason: size, timer, batches, partitions, stop). `bytes` is batch bytes. The DynamoDB commit costs 1 + partitions + grown topics items (decisions 39, 44), which `partitions` (`-max-segment-partitions`, default 96) keeps under the 100-item limit. `open_age_ms` is first batch to seal. |
| `segment_put_done` | INFO | `segment`, `bytes`, `batches`, `partitions`, `put_ms`, `put_attempts` | Object PUT succeeded. `put_ms` is seal to success including retries and backoff; `bytes` includes the index. |
| `segment_committed` | INFO | `segment`, `reason`, `bytes`, `batches`, `partitions`, `put_ms`, `wait_ms`, `commit_ms`, `total_ms`, `put_attempts`, `in_flight`, `buffered_bytes` | Control plane answered. `put_ms + wait_ms + commit_ms = total_ms` (seal to answer). `wait_ms` is queueing behind earlier segments; `commit_ms` is the CP round trip. This is the line to aggregate for latency percentiles per segment. |
| `segment_failed` | WARN | `segment`, `reason`, `bytes`, `batches`, `partitions`, `kafka_error_code`, `put_attempts`, `put_ms`, `total_ms` | The segment's batches got an error answer. |
| `backpressure_on` / `backpressure_off` | INFO | `buffered_bytes`, `paused_ms` (off) | Client reads paused/resumed by the produce buffer cap. |
| `aws_attempt_failed` | WARN | `aws_service=s3`, `op`, `result`, `http_status`, `code`, `throttled`, `duration_ms`, `request_bytes`, `target`, `request_id`, `err` | One S3 HTTP attempt was not 2xx, timed out or hit a network error. `result` is `http_<status>`, `timeout`, `network` or `error`; `code` is the S3 error code (SlowDown, ...); `request_id` is for AWS support. A PUT timeout shows as `result=timeout`. (A cancelled attempt is logged at DEBUG.) |
| `aws_warn_suppressed` | WARN | `suppressed` | Count of warnings dropped by the rate limit. |

### manicp

| event | level | fields | meaning / how to read |
|---|---|---|---|
| `start`, `manifest`, `manifest_written`, `manifest_failed` | | as above | |
| `cp_call` | DEBUG (WARN if HTTP 5xx) | `op`, `segment`, `outcome`, `http_status`, `attempts`, `retries`, `handler_ms`, `dynamo_ms`, `consumed_units`, `request_bytes`, `response_bytes` | One per `/v1/*` request, but only at `-log-level debug` (or when it is a 5xx, at WARN). `outcome` is `ok`, the business error (`ErrConflict`, ...) or `http_503`/`http_500`. `attempts` counts commit attempts (1 for other ops); `retries` is attempts minus one. `dynamo_ms` is the sum of the request's DynamoDB calls; `handler_ms - dynamo_ms` is CP-side compute. `segment` joins to the broker's `segment_*` lines. `consumed_units` is the capacity DynamoDB reported (0 if not reported). |
| `cp_summary` | INFO | `window_ms`, then one object per op (`commit-segment`, `lookup`, ...) with `calls`, `not_ok`, `mean_handler_ms`, `max_handler_ms` | manicp only. At most one a second, written by the first request after the interval (no timer, so an idle process logs nothing and the last partial second of a run is not written; the `manicp_*` metrics have every request). Bounds log volume at 1 line/s however many requests arrive, including 48k Lookups/s. |
| `commit_txn_canceled` | WARN | `segment`, `items`, `reasons`, `err` | A commit transaction lost a race and will be retried. `reasons` entries are `index:code:pk/sk` of the items that failed (`ConditionalCheckFailed`, `TransactionConflict`, `ThrottlingError`, `ProvisionedThroughputExceeded`, ...). Many `ThrottlingError` reasons mean the table is throttling, not racing. |
| `seq_group` | DEBUG | `segments`, `items`, `partitions`, `tx_ms`, `ok` | One line per group transaction (sequencer). |
| `seq_retry` | WARN | `reason`, `segments`, `err` | A group is retried (throttling, unknown outcome, failed read). |
| `seq_txn_canceled` | WARN | `segments`, `items`, `reasons`, `err` | A group transaction was cancelled (a lost race or a replayed key); the sequencer refreshes what the reasons name and retries. With one manicp this is rare. |
| `dynamo_call_failed` | WARN | `call`, `code`, `throttled`, `err` | A DynamoDB call failed with a retryable error (throttling, conflict, 5xx) after the SDK's own retries; the CP answers 503. |
| `aws_attempt_failed` | WARN | `aws_service=dynamo`, same fields as above | One DynamoDB HTTP attempt failed. The SDK retries these itself; each retry is a separate line. `TransactionCanceledException` attempts are DEBUG here (see `commit_txn_canceled`). |

## Metrics

### manibroker (`:9100`)

| name | type | labels | meaning |
|---|---|---|---|
| `manibroker_segment_put_seconds` | histogram | | Seal to successful PUT, retries included. |
| `manibroker_segment_commit_seconds` | histogram | | First CommitSegment to its answer. |
| `manibroker_segment_queue_wait_seconds` | histogram | | Uploaded segment waiting for earlier commits. |
| `manibroker_segment_seal_to_ack_seconds` | histogram | | Seal to answer (put + wait + commit); also observed for failed segments. |
| `manibroker_segment_open_age_seconds` | histogram | | First batch to seal. |
| `manibroker_segments_in_flight_at_seal` | histogram | | Segments not yet answered when one is sealed; above 1 means overlapping PUTs. |
| `manibroker_segments_sealed_total` | counter | `reason` (`size`, `timer`, `batches`, `partitions`, `stop`) | Seals by reason: `size` 16 MiB, `timer` window, `batches` batch cap, `partitions` partition cap, `stop` shutdown. |
| `manibroker_segment_partitions` | histogram | | Distinct topic-partitions per sealed segment (buckets 1 to 4096). |
| `manibroker_segment_sealed_batch_bytes_total` | counter | | Batch bytes sealed. |
| `manibroker_segments_committed_total`, `_segment_committed_bytes_total`, `_segments_failed_total` | counter | | Outcomes. |
| `manibroker_segment_put_attempts_total` | counter | | PUT attempts that ended in success (retries counted). |
| `manibroker_reads_paused_seconds_total`, `_reads_paused` | counter, gauge | | Backpressure time and state. |
| `manibroker_buffered_bytes`, `_segments_in_flight` | gauge | | As of the last segment event. |
| `manibroker_cp_call_seconds` | histogram | `op` | Control-plane HTTP exchange as the broker sees it. |
| `manibroker_open_connections` | gauge | | Open Kafka client connections. |
| `manibroker_build_info` | gauge (=1) | `version`, `go_version`, `git_sha`, `run_id` | Which build and run. |
| `manibroker_start_time_seconds`, `_uptime_seconds` | gauge | | Process start (unix) and age. |
| `manibroker_s3_attempt_seconds` | histogram | `op`, `result` | One S3 HTTP attempt; `result` is `ok`, `http_<status>`, `timeout`, `canceled`, `network`, `error`. Use `_count` for request counts per result and the buckets for latency. |
| `manibroker_s3_operation_seconds` | histogram | `op`, `outcome` | Whole SDK call (`ok`/`error`). The broker disables SDK retries, so this equals attempts. |
| `manibroker_s3_retries_total` | counter | `op` | SDK-level retries (zero by design; broker retries show in `segment_put_attempts_total`). |
| `manibroker_s3_request_bytes` | histogram | `op` | Request body size per attempt; `_sum` is bytes uploaded. |
| `manibroker_s3_api_errors_total` | counter | `op`, `code` | Attempts that ended in an S3 error code. |
| `manibroker_s3_throttles_total` | counter | `op`, `code` | Attempts rejected for throttling (`SlowDown`, ...). |
| `go_*`, `process_*` | | | Go runtime and process collectors (CPU, RSS, FDs, GC). |

### manicp (`:9101`)

| name | type | labels | meaning |
|---|---|---|---|
| `manicp_handler_seconds` | histogram | `op` | Whole handler time. |
| `manicp_calls_total` | counter | `op`, `outcome` | Requests by outcome (`ok`, `ErrConflict`, `http_503`, ...). |
| `manicp_request_body_bytes` | histogram | `op` | Request body size. |
| `manicp_inflight_requests` | gauge | | Requests being served. |
| `manicp_dynamodb_call_seconds` | histogram | `call` | Time of one DynamoDB SDK call as the store sees it (retries inside the SDK included). |
| `manicp_commit_prep_seconds` | histogram | `stage` | `reads` and `decide_encode` per attempt. |
| `manicp_commit_attempts_total` | counter | `op` | Commit attempts (with the sequencer: per request and per group attempt). |
| `manicp_retries_total` | counter | `op`, `reason` | Internal retries (`transaction_canceled`, `unavailable`, `conditional_check`). |
| `manicp_dynamo_attempt_seconds` | histogram | `op`, `result` | One DynamoDB HTTP attempt (SDK retries are separate samples). |
| `manicp_dynamo_operation_seconds` | histogram | `op`, `outcome` | Whole SDK call including SDK backoff. |
| `manicp_dynamo_retries_total` | counter | `op` | SDK-level retries; nonzero means DynamoDB is throttling or failing and the SDK is absorbing it. |
| `manicp_dynamo_request_bytes` | histogram | `op` | Request body size per attempt. |
| `manicp_dynamo_api_errors_total` | counter | `op`, `code` | Attempts ending in a DynamoDB error code (includes `TransactionCanceledException`, the normal race). |
| `manicp_dynamo_throttles_total` | counter | `op`, `code` | `ProvisionedThroughputExceededException`, `ThrottlingException`, `RequestLimitExceeded`. Should be 0 on on-demand tables at this load. |
| `manicp_dynamo_consumed_capacity_units_total` | counter | `op`, `kind` (`read`, `write`) | Capacity units DynamoDB reported (`ReturnConsumedCapacity=TOTAL`) for successful calls. Multiply by the unit price for the cost of the run: writes are WRU, reads are RRU (a consistent read of up to 4 KB is 1 unit; a transaction counts double). Failed calls are not reported by DynamoDB. |
| `manicp_dynamo_consumed_capacity_missing_total` | counter | `op` | Successful calls with no `ConsumedCapacity` in the response (ExtendDB, local emulators). If this grows while the units counter is flat, the server does not report capacity: compute cost from call counts instead. |
| `manicp_dynamo_txn_cancel_reasons_total` | counter | `code` | Per-item cancellation reasons. `ConditionalCheckFailed` is a lost race; `ThrottlingError`/`ProvisionedThroughputExceeded` is throttling inside a transaction. |
| `manicp_seq_queue_depth` | gauge | | Commit requests waiting for the sequencer (decision 46). Persistently above a few means the loop is the bottleneck (transaction time times group size is below the commit rate). |
| `manicp_seq_queue_wait_seconds` | histogram | | Time a request waited before its group was formed. |
| `manicp_seq_group_segments`, `manicp_seq_group_items` | histogram | | Segments and items per group transaction when it is sent (limit 100 items). Group size grows with load; items near 100 means the budget binds. |
| `manicp_seq_tx_items` | histogram | | DynamoDB items per group transaction actually sent (buckets 1 to 100). |
| `manicp_seq_tx_seconds` | histogram | `outcome` (`ok`, `canceled`, `unavailable`, `unknown`, `error`) | `TransactWriteItems` time of a group. `canceled` should stay near 0 with one manicp; `unknown` is a 5xx/timeout/lost answer (the group then re-reads before it goes on). |
| `manicp_seq_cache_total` | counter | `cache` (`end`, `topic`, `segment`), `result` (`hit`, `miss`) | In-memory state lookups. `end` misses are the first touch of a partition (a Query) and recoveries after a retry; a `segment` miss is every new key (normal: it costs nothing) and a segment hit is a replay served from memory. |
| `manicp_seq_retries_total` | counter | `reason` (`transaction_canceled`, `unavailable`, `unknown_outcome`, `read_failed`) | Group retries, one count per group. A second manicp or a stale one shows up as `transaction_canceled`. |
| `manicp_seq_inflight_groups` | gauge | | Group transactions in flight (at most `-seq-pipeline`). |
| `manicp_build_info`, `_start_time_seconds`, `_uptime_seconds` | | | As for the broker. |
| `go_*`, `process_*` | | | Runtime and process collectors. |

#### Log-backed control plane (`manicp_log_*`, `cp/logcp`; exported by `manicp -store dynamolog` since stage 4; one series per CP node, Alloy adds `host` and `cp_rank`)

| name | type | labels | meaning |
|---|---|---|---|
| `manicp_log_slot_attempts_total` | counter | `result` (`won`, `lost`, `error`) | Slot append attempts. `error` is an unknown outcome retried at the same slot. |
| `manicp_log_lost_slots_total` | counter | | Slots lost to another writer or a hole filler. Persistently nonzero: two appenders. |
| `manicp_log_slots_total` | counter | | Slots this node won; `rate()` is the slot rate (budget about 200/s). |
| `manicp_log_append_seconds` | histogram | | One slot append to a definite answer. |
| `manicp_log_entries_per_slot`, `manicp_log_entry_bytes` | histogram | | Commands grouped in a slot; encoded size (limit 350 KB). |
| `manicp_log_queue_depth`, `manicp_log_queue_wait_seconds` | gauge, histogram | | Commands waiting for a slot. |
| `manicp_log_rejected_total` | counter | `reason` (`not_active`, `overloaded`, `blocked`, `too_large`) | Refused requests. |
| `manicp_log_applied_slot`, `manicp_log_apply_lag_slots`, `manicp_log_standby_lag_slots` | gauge | | Position; slots known but not applied; same on a standby. Standby lag growing means it cannot take over cleanly. |
| `manicp_log_apply_seconds` | histogram | | One apply batch. |
| `manicp_log_hole_fills_total` | counter | | No-ops this node wrote. |
| `manicp_log_tail_reads_total` | counter | `result` | Tail reads of the replica loop. |
| `manicp_log_tail_checks_total`, `manicp_log_fresh_reads_total`, `manicp_log_fresh_wait_seconds` | counter, counter, histogram | `result` | Tail checks for fresh reads; reads served (ratio is the batching factor); their wait. |
| `manicp_log_active`, `manicp_log_role_changes_total` | gauge, counter | `to` | Advisory role. Flapping is a sign of a bad `TakeoverAfter`. |
| `manicp_log_window_keys` | gauge | | Segment keys in the idempotency window. |
| `manicp_log_blocked` | gauge | | 1: the node cannot apply (unknown format, store failure, trimmed); alert. |
| `manicp_log_checkpoint_slot`, `_checkpoint_age_seconds`, `_checkpoint_bytes`, `_checkpoint_seconds`, `_checkpoint_errors_total` | | | Checkpoints; alert on age above 3 x `CkptEvery`. |
| `manicp_log_trimmed_slots_total` | counter | | Slots tombstoned behind the checkpoint. |
| `manicp_log_index_watermark_slot`, `_index_lag_slots`, `_index_flush_seconds`, `_index_rows_total`, `_index_unprocessed_total`, `_index_throttles_total`, `_index_lost_runs_total`, `_index_errors_total` | | | R# indexer. Lag is bounded by the flush period; `lost_runs` nonzero means the index stalled past the window. |

Dashboard (Overview, row "Control plane (log store ...)", `gen_dashboards.py`): slot rate and lost slots; append latency p50/p99;
commands per slot and entry size; apply lag and standby lag; hole fills, role changes and the active gauge; queue depth, queue wait
and refused requests; index backlog, flush p99, throttles; checkpoint age, size and errors; fresh reads and tail checks. The old
sequencer row is renamed and stays empty with the log store. Alloy (`cp.alloy`) scrapes each CP host's manicp and adds `cp_rank`;
CloudWatch DynamoDB metrics (`cpcloudwatch.alloy`, now with PutItem and BatchWriteItem operations) are scraped from rank 0 only.
Log events (INFO unless noted): `log_ready` (node, rank, applied slot, role), `log_role` (active true/false, a takeover or
failback), `log_final_ckpt`, `log_final_ckpt_failed` (WARN), `log_no_checkpoint` (WARN, no `-ckpt-bucket`). The manifest
gains `store`, `rank`, `takeover_after`, `ckpt_bucket`. Suggested alerts: `manicp_log_blocked` = 1; checkpoint age above 15 min;
`manicp_log_standby_lag_slots` rising; `rate(manicp_log_lost_slots_total)` > 0 for minutes; `manicp_log_index_lag_slots` > 3 x flush
period x slot rate.

### Useful queries

- Segment ack latency p99: `histogram_quantile(0.99, rate(manibroker_segment_seal_to_ack_seconds_bucket[1m]))`.
- S3 request rate by result: `sum by (result) (rate(manibroker_s3_attempt_seconds_count[1m]))`.
- DynamoDB write units per second: `rate(manicp_dynamo_consumed_capacity_units_total{kind="write"}[1m])`.
- Any throttling: `sum(rate(manicp_dynamo_throttles_total[1m])) + sum(rate(manibroker_s3_throttles_total[1m]))`.

## Run manifest

`-manifest-out PATH` writes one JSON file at startup, never fatal (a failure is a WARN
`manifest_failed`). `PATH` is a file or `s3://bucket/key`; `{service}` and `{run_id}` are
replaced, and a trailing `/` appends `<service>-<run_id>.json`, so both services can share
`-manifest-out s3://bucket/runs/$RUN/`. S3 uses the default credential chain and the
service's region (the broker's `-s3-region`, the CP's `-region`). Fields: `run_id`,
`service`, `broker_id`, `version`, `git_sha`, `git_dirty`, `go_version`, `started_at`,
`hostname`, `pid`, `args`, `region`, `flags` (every flag, effective value), `flags_set`
(flags given explicitly), `tuning` (resolved values: segment size and window in
bytes/nanoseconds, buffer caps, memory limit and its source, table name), `env`
(`MANIFESTER_*`, `AWS_REGION`, `AWS_DEFAULT_REGION`, `AWS_ENDPOINT*`; never credentials),
`goos`, `goarch`, `num_cpu`, `gomaxprocs`, and `instance` (EC2 IMDSv2: instance id, type,
AZ, region, AMI, private IP; absent off EC2, one 300 ms probe).

## Reconstructing a run

1. Manifests give the configuration and the host.
2. `segment_committed` lines give every segment's bytes, batches, partitions and
   put/wait/commit/total ms; sum `bytes` for throughput, group by `reason` for the seal mix.
3. `cp_call` lines (join on `segment`; run manicp at `-log-level debug` for them) give the CP side: attempts, DynamoDB ms, units.
4. WARN lines list everything that went wrong; `*_throttles_total` and
   `consumed_capacity_*` give the cost and capacity picture; Prometheus scrapes of
   `:9100` and `:9101` (saved at the end of the run with `curl`) are the totals.
