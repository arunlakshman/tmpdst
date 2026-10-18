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

Volume: one line per segment event (about 3 per segment) and one per CP call. At 10 segments
a second that is about 40 lines a second in total, a few MB an hour. There is no sampling;
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
| `segment_sealed` | INFO | `segment`, `reason`, `bytes`, `batches`, `partitions`, `commit_items`, `open_age_ms` | The open segment was sealed (reason: size, timer, batches, partitions, items, stop). `bytes` is batch bytes. `commit_items` is the DynamoDB commit cost, 2 + 2 x partitions + producer states (decision 39); `items` seals before it passes `-max-segment-items`. `open_age_ms` is first batch to seal. |
| `segment_put_done` | INFO | `segment`, `bytes`, `batches`, `partitions`, `put_ms`, `put_attempts` | Object PUT succeeded. `put_ms` is seal to success including retries and backoff; `bytes` includes the index. |
| `segment_committed` | INFO | `segment`, `reason`, `bytes`, `batches`, `partitions`, `epoch`, `put_ms`, `wait_ms`, `commit_ms`, `total_ms`, `put_attempts`, `in_flight`, `buffered_bytes` | Control plane answered. `put_ms + wait_ms + commit_ms = total_ms` (seal to answer). `wait_ms` is queueing behind earlier segments; `commit_ms` is the CP round trip. This is the line to aggregate for latency percentiles per segment. |
| `segment_failed` | WARN | `segment`, `reason`, `bytes`, `batches`, `partitions`, `kafka_error_code`, `put_attempts`, `put_ms`, `total_ms` | The segment's batches got an error answer. |
| `backpressure_on` / `backpressure_off` | INFO | `buffered_bytes`, `paused_ms` (off) | Client reads paused/resumed by the produce buffer cap. |
| `aws_attempt_failed` | WARN | `aws_service=s3`, `op`, `result`, `http_status`, `code`, `throttled`, `duration_ms`, `request_bytes`, `target`, `request_id`, `err` | One S3 HTTP attempt was not 2xx, timed out or hit a network error. `result` is `http_<status>`, `timeout`, `network` or `error`; `code` is the S3 error code (SlowDown, ...); `request_id` is for AWS support. A PUT timeout shows as `result=timeout`. (A cancelled attempt is logged at DEBUG.) |
| `aws_warn_suppressed` | WARN | `suppressed` | Count of warnings dropped by the rate limit. |

### manicp

| event | level | fields | meaning / how to read |
|---|---|---|---|
| `start`, `manifest`, `manifest_written`, `manifest_failed` | | as above | |
| `cp_call` | INFO (WARN if HTTP 5xx) | `op`, `segment`, `outcome`, `http_status`, `attempts`, `retries`, `handler_ms`, `dynamo_ms`, `consumed_units`, `request_bytes`, `response_bytes` | One per `/v1/*` request. `outcome` is `ok`, the business error (`ErrFenced`, ...) or `http_503`/`http_500`. `attempts` counts commit attempts (1 for other ops); `retries` is attempts minus one. `dynamo_ms` is the sum of the request's DynamoDB calls; `handler_ms - dynamo_ms` is CP-side compute. `segment` joins to the broker's `segment_*` lines. `consumed_units` is the capacity DynamoDB reported (0 if not reported). |
| `commit_txn_canceled` | WARN | `segment`, `items`, `reasons`, `err` | A commit transaction lost a race and will be retried. `reasons` entries are `index:code:pk/sk` of the items that failed (`ConditionalCheckFailed`, `TransactionConflict`, `ThrottlingError`, `ProvisionedThroughputExceeded`, ...). Many `ThrottlingError` reasons mean the table is throttling, not racing. |
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
| `manibroker_segments_sealed_total` | counter | `reason` | Seals by reason. |
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
| `manicp_calls_total` | counter | `op`, `outcome` | Requests by outcome (`ok`, `ErrFenced`, `http_503`, ...). |
| `manicp_request_body_bytes` | histogram | `op` | Request body size. |
| `manicp_inflight_requests` | gauge | | Requests being served. |
| `manicp_dynamodb_call_seconds` | histogram | `call` | Time of one DynamoDB SDK call as the store sees it (retries inside the SDK included). |
| `manicp_commit_prep_seconds` | histogram | `stage` | `reads` and `decide_encode` per attempt. |
| `manicp_commit_attempts_total` | counter | `op` | Commit attempts. |
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
| `manicp_build_info`, `_start_time_seconds`, `_uptime_seconds` | | | As for the broker. |
| `go_*`, `process_*` | | | Runtime and process collectors. |

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
3. `cp_call` lines (join on `segment`) give the CP side: attempts, DynamoDB ms, units.
4. WARN lines list everything that went wrong; `*_throttles_total` and
   `consumed_capacity_*` give the cost and capacity picture; Prometheus scrapes of
   `:9100` and `:9101` (saved at the end of the run with `curl`) are the totals.
