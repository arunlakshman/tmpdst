# Observability

What a run leaves behind so you can reconstruct it later.

Each service has a metrics port that serves `/metrics` (Prometheus text) and
`/healthz`:

| Service | Port flag | Default | Metric prefix |
|---------|-----------|---------|---------------|
| `mdsbroker` | `-metrics-addr` | `:9100` | `mdsbroker_` |
| `mdscontrolplane` | `-metrics-addr` | `:9101` | `mdscontrolplane_` |

Empty `-metrics-addr` disables the port.

## Run id

`-run-id` (or `$MDS_RUN_ID`) stamps every log line, the `*_build_info` series,
and the optional run manifest. Use one id for the whole experiment on both
services so you can join broker and control-plane evidence.

Other shared flags: `-log-level` (`debug` / `info` / `warn` / `error`),
`-manifest-out` (file path or `s3://bucket/key`; `{service}` and `{run_id}`
expand; a trailing `/` appends `<service>-<run_id>.json`). Manifest write
failures are WARN only, never fatal.

## Logs

JSON lines on stderr. Every line has `time`, `level`, `msg`, `run_id`,
`service`, `version`, `git_sha` (and `broker_id` on the broker). Event lines
also have `event`. Durations are milliseconds (`*_ms`); sizes are bytes.

Broker volume is about one line per segment event (seal / put / commit). The
control plane emits one `cp_summary` line per second at INFO; per-request
`cp_call` is DEBUG (WARN on HTTP 5xx). At high load use `-log-level warn` to
keep failures, retries, and throttles; metrics still count everything.

Useful broker events: `segment_sealed`, `segment_put_done`, `segment_committed`,
`segment_failed`, `backpressure_on` / `backpressure_off`, `aws_attempt_failed`.
Useful CP events: `cp_call`, `cp_summary`, `aws_attempt_failed` (DynamoDB).

## Metrics (sketch)

Broker (`mdsbroker_*`): segment put / commit / queue-wait / seal-to-ack
histograms; seal reasons; buffered bytes and in-flight segments; CP call
latency; open connections; S3 attempt / throttle counters; `build_info`,
`uptime_seconds`; plus `go_*` / `process_*`.

Control plane (`mdscontrolplane_*`): handler latency and `calls_total` by op
and outcome; DynamoDB attempt / throttle / consumed-capacity series; with
`-store dynamolog` or `postgres`, also `mdscontrolplane_log_*` (slot rate, lost
slots, apply lag, role, blocked). Same `build_info` / uptime / runtime
collectors.

Example queries:

- Segment ack p99:
  `histogram_quantile(0.99, rate(mdsbroker_segment_seal_to_ack_seconds_bucket[1m]))`
- Any throttling:
  `sum(rate(mdscontrolplane_dynamo_throttles_total[1m])) + sum(rate(mdsbroker_s3_throttles_total[1m]))`

## Reconstructing a short run

1. Manifests: config and host.
2. `segment_committed` lines: bytes, batches, put/wait/commit/total ms.
3. `cp_call` at DEBUG (join on `segment`): CP and DynamoDB ms.
4. WARN lines plus scraped `:9100` / `:9101` for totals and cost.
