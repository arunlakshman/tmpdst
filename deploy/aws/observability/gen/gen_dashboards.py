#!/usr/bin/env python3
"""Generates dashboards/*.json. Run: python3 gen/gen_dashboards.py (from this dir's parent)."""
import json, os

PROM = {"type": "prometheus", "uid": "${ds_prom}"}
LOKI = {"type": "loki", "uid": "${ds_loki}"}
R = 'run_id="$run_id"'
RI = "$__rate_interval"


RB = 'run_id="$run_id", broker=~"$broker"'


def q(expr, legend="", ds=PROM, ref=None):
    # manibroker_ series carry a `broker` label (Alloy relabel from BROKER_ID); filter them by the
    # $broker variable. allValue ".*" also matches series without the label (single-broker runs).
    # CloudWatch (aws_*) series are per account/table, not per broker: only run_id.
    # manicp_ series come from the control-plane hosts and carry no broker label: only run_id.
    if "manibroker_" in expr and "aws_" not in expr:
        expr = expr.replace('run_id="$run_id"', RB)
    return {"expr": expr, "legendFormat": legend, "datasource": ds, "refId": ref or "A"}


class B:
    def __init__(self, title, uid, tags):
        self.d = {
            "uid": uid, "title": title, "tags": ["manifester"] + tags, "schemaVersion": 39, "version": 1,
            "editable": True, "timezone": "utc", "refresh": "10s", "time": {"from": "now-30m", "to": "now"},
            "graphTooltip": 1, "panels": [], "annotations": {"list": [
                {"name": "Run start/end", "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                 "enable": True, "iconColor": "blue",
                 "target": {"type": "tags", "tags": ["manifester", "run:$run_id"], "limit": 100}}]},
            "templating": {"list": [
                {"name": "ds_prom", "type": "datasource", "query": "prometheus", "label": "Prometheus", "current": {}},
                {"name": "ds_loki", "type": "datasource", "query": "loki", "label": "Loki", "current": {}},
                {"name": "run_id", "type": "query", "label": "run_id", "datasource": PROM,
                 "query": {"query": "label_values(up, run_id)", "refId": "rid"}, "refresh": 2,
                 "sort": 2, "current": {}},
                {"name": "broker", "type": "query", "label": "broker", "datasource": PROM, "includeAll": True, "multi": True,
                 "allValue": ".*", "query": {"query": 'label_values(manibroker_build_info{run_id="$run_id"}, broker)', "refId": "br"},
                 "refresh": 2, "sort": 3, "current": {}},
            ]},
        }
        self.y = 0
        self.x = 0
        self.id = 1

    def panel(self, title, targets, unit="short", w=12, h=8, kind="timeseries", desc="", extra=None):
        for i, t in enumerate(targets):
            t["refId"] = chr(ord("A") + i)
        ds = targets[0]["datasource"]
        p = {"id": self.id, "type": kind, "title": title, "description": desc, "datasource": ds,
             "gridPos": {"x": self.x, "y": self.y, "w": w, "h": h}, "targets": targets,
             "fieldConfig": {"defaults": {"unit": unit, "custom": {"fillOpacity": 8, "lineWidth": 1}}, "overrides": []},
             "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}}
        if kind == "logs":
            p["options"] = {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending", "enableLogDetails": True}
            p["fieldConfig"] = {"defaults": {}, "overrides": []}
        if extra:
            p.update(extra)
        self.d["panels"].append(p)
        self.id += 1
        self.x += w
        if self.x >= 24:
            self.x = 0
            self.y += h

    def row(self, title):
        """Collapsible-looking row header; starts a new line of panels."""
        if self.x:
            self.x = 0
            self.y += 8
        self.d["panels"].append({"id": self.id, "type": "row", "title": title, "collapsed": False,
                                 "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.id += 1
        self.y += 1

    def save(self, name):
        with open(os.path.join("dashboards", name), "w") as f:
            json.dump(self.d, f, indent=2)
            f.write("\n")


def hq(metric, qs, sel=R, by="", rng=RI):
    """histogram_quantile series for each quantile in qs."""
    g = f"le, {by}" if by else "le"
    return [q(f'histogram_quantile({x}, sum by ({g}) (rate({metric}_bucket{{{sel}}}[{rng}])))',
              f"p{int(x*100)}" + "".join(f" {{{{{k.strip()}}}}}" for k in by.split(",") if k)) for x in qs]


# ---------------------------------------------------------------- overview
b = B("Manifester - Overview", "mani-overview", ["overview"])
# ---- multi-broker block: aggregate, per broker, skew
b.panel("Aggregate throughput (all selected brokers): committed MB/s and approx msg/s",
        [q(f'sum(rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}]))', "committed bytes/s"),
         q(f'sum(rate(manibroker_segment_sealed_batch_bytes_total{{{R}}}[{RI}]))', "sealed batch bytes/s")], "Bps",
        desc="Write side, summed over the $broker selection. Read throughput has no broker metric yet; use the OMB consume rate.")
b.panel("Approx msg/s at 1 KiB per message (sealed batch bytes / 1024)",
        [q(f'sum(rate(manibroker_segment_sealed_batch_bytes_total{{{R}}}[{RI}])) / 1024', "msg/s (1 KiB records)")], "ops",
        desc="There is no per-record counter on the broker: this is exact only for the 1 KiB OMB workloads (batch framing makes it a slight overestimate). The OMB log/JSON is the source of truth for msg/s.")
b.panel("Per-broker write throughput (committed bytes/s)",
        [q(f'sum by (broker) (rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}]))', "broker {{broker}}")], "Bps")
b.panel("Skew: share of bytes per broker",
        [q(f'sum by (broker) (rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}])) / ignoring(broker) group_left sum(rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}]))', "broker {{broker}}")],
        "percentunit", desc="Even split is 1/N. Clients stay on the broker they bootstrapped from, so the split follows client placement.")
b.panel("Skew: max / min broker throughput",
        [q(f'max(sum by (broker) (rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}]))) / min(sum by (broker) (rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}])))', "max/min"),
         q(f'max(sum by (broker) (rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}]))) / avg(sum by (broker) (rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}])))', "max/mean")],
        "short", desc="1.0 is perfectly even. Needs the brokers selected to all be taking traffic (min of 0 gives +Inf).")
b.panel("Open client connections per broker", [q(f'manibroker_open_connections{{{R}}}', "broker {{broker}}")], "short",
        desc="Connections follow bootstrap choice (random among bootstrap.servers): the cause of byte skew.")
b.panel("Produce ack latency p99 per broker", hq("manibroker_segment_seal_to_ack_seconds", [0.99], by="broker"), "s")
b.panel("Segments per second per broker", [q(f'sum by (broker) (rate(manibroker_segments_committed_total{{{R}}}[{RI}]))', "broker {{broker}}")], "ops")
b.panel("Produce ack latency (seal to commit answer)", hq("manibroker_segment_seal_to_ack_seconds", [0.5, 0.99]),
        "s", desc="put + queue wait + commit. Excludes time batches sat in the open segment buffer (see segment open age); "
                  "client-observed latency comes from the load generator.")
b.panel("Segment open age (first batch to seal)", hq("manibroker_segment_open_age_seconds", [0.5, 0.99]), "s")
b.panel("Throughput: committed segment bytes/s",
        [q(f'sum(rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}]))', "committed (batch data + index)"),
         q(f'sum(rate(manibroker_segment_sealed_batch_bytes_total{{{R}}}[{RI}]))', "sealed batch bytes")], "Bps")
b.panel("Segments per second by seal reason",
        [q(f'sum by (reason) (rate(manibroker_segments_sealed_total{{{R}}}[{RI}]))', "{{reason}}"),
         q(f'sum(rate(manibroker_segments_failed_total{{{R}}}[{RI}]))', "failed")], "ops")
b.panel("Segment size (mean committed bytes per segment)",
        [q(f'sum(rate(manibroker_segment_committed_bytes_total{{{R}}}[{RI}])) / sum(rate(manibroker_segments_committed_total{{{R}}}[{RI}]))', "mean")], "bytes")
b.panel("Buffered produce bytes (unacked)", [q(f'manibroker_buffered_bytes{{{R}}}', "buffered")], "bytes")
# ---- control plane: the log store (-store dynamolog, decisions 48-53). One series per CP host (label host, cp_rank).
b.row("Control plane (log store: manicp_log_*, one series per CP node)")
b.panel("Slot rate (slots/s won) and lost slots",
        [q(f'sum by (host) (rate(manicp_log_slots_total{{{R}}}[{RI}]))', "slots/s {{host}}"),
         q(f'sum by (host) (rate(manicp_log_lost_slots_total{{{R}}}[{RI}]))', "lost slots/s {{host}}"),
         q(f'sum by (host, result) (rate(manicp_log_slot_attempts_total{{{R}, result="error"}}[{RI}]))', "unknown-outcome retries/s {{host}}")], "ops",
        desc="The active node's slot rate is bounded by -slot-rate (200/s, one hot DynamoDB key). Lost slots stay at 0 with one appender; a burst around a takeover is expected, persistent loss means two appenders.")
b.panel("Slot append latency (PutItem to definite answer, p50/p99)", hq("manicp_log_append_seconds", [0.5, 0.99], by="host"), "s",
        desc="Includes retries of unknown outcomes. This is the floor of every commit's latency.")
b.panel("Commands per slot and entry size (p50/p99)",
        hq("manicp_log_entries_per_slot", [0.5, 0.99], by="host") + hq("manicp_log_entry_bytes", [0.99], by="host"), "short",
        desc="Entries per slot grows with load (group commit); 1 at low load. Entry bytes p99 is in bytes, limit 350 KB.")
b.panel("Apply lag and standby lag (slots)",
        [q(f'max by (host) (manicp_log_apply_lag_slots{{{R}}})', "apply lag {{host}}"),
         q(f'max by (host) (manicp_log_standby_lag_slots{{{R}}})', "standby lag {{host}}")], "short",
        desc="Slots known to exist but not applied. A standby whose lag grows cannot take over cleanly.")
b.panel("Hole fills and role changes",
        [q(f'sum by (host) (rate(manicp_log_hole_fills_total{{{R}}}[{RI}]))', "hole fills/s {{host}}"),
         q(f'sum by (host, to) (increase(manicp_log_role_changes_total{{{R}}}[{RI}]))', "role -> {{to}} {{host}}"),
         q(f'max by (host) (manicp_log_active{{{R}}})', "active (1) {{host}}")], "short",
        desc="Hole fills happen when an appender dies holding a slot; role changes mark a takeover or a failback. The kill-the-appender marker.")
b.panel("Queue depth, queue wait and refused requests",
        [q(f'max by (host) (manicp_log_queue_depth{{{R}}})', "queue depth {{host}}"),
         q(f'histogram_quantile(0.99, sum by (le, host) (rate(manicp_log_queue_wait_seconds_bucket{{{R}}}[{RI}])))', "queue wait p99 {{host}}"),
         q(f'sum by (host, reason) (rate(manicp_log_rejected_total{{{R}}}[{RI}]))', "refused/s {{reason}} {{host}}")], "short",
        desc="Refusals with reason not_active on a standby are normal (brokers fail over). overloaded means the queue is full.")
b.panel("Checkpoint age (s) and size",
        [q(f'max by (host) (manicp_log_checkpoint_age_seconds{{{R}}})', "age s {{host}}"),
         q(f'max by (host) (manicp_log_checkpoint_bytes{{{R}}})', "bytes {{host}}"),
         q(f'sum by (host) (increase(manicp_log_checkpoint_errors_total{{{R}}}[{RI}]))', "errors {{host}}")], "short",
        desc="Alert on age above 3 x -ckpt-every (15 min). Only the active node checkpoints.")
b.panel("Fresh reads served and tail checks (batching factor)",
        [q(f'sum by (host) (rate(manicp_log_fresh_reads_total{{{R}}}[{RI}]))', "fresh reads/s {{host}}"),
         q(f'sum by (host) (rate(manicp_log_tail_checks_total{{{R}}}[{RI}]))', "tail checks/s {{host}}"),
         q(f'max by (host) (manicp_log_blocked{{{R}}})', "blocked {{host}}")], "short")
LOGSEL = '{run_id="$run_id", level=~"warn|warning|error|fatal|dpanic|panic"}'
b.panel("Logs: level >= warn", [q(LOGSEL, "", LOKI)], w=24, h=10, kind="logs")
b.save("overview.json")

# ---------------------------------------------------------------- pipeline
b = B("Manifester - Segment pipeline", "mani-pipeline", ["pipeline"])
for t, m, d in [("PUT (seal to object stored, retries included)", "segment_put_seconds", ""),
                ("Queue wait (commits strictly in seal order)", "segment_queue_wait_seconds", ""),
                ("Commit (first CommitSegment to answer, retries included)", "segment_commit_seconds", ""),
                ("Total (seal to ack)", "segment_seal_to_ack_seconds", "")]:
    b.panel(t, hq("manibroker_" + m, [0.5, 0.99]), "s")
b.panel("Segments in flight",
        [q(f'manibroker_segments_in_flight{{{R}}}', "in flight (gauge)"),
         q(f'histogram_quantile(0.5, sum by (le) (rate(manibroker_segments_in_flight_at_seal_bucket{{{R}}}[{RI}])))', "at seal p50"),
         q(f'histogram_quantile(0.99, sum by (le) (rate(manibroker_segments_in_flight_at_seal_bucket{{{R}}}[{RI}])))', "at seal p99")],
        desc="Above 1 means PUTs overlap.")
b.panel("Backpressure",
        [q(f'rate(manibroker_reads_paused_seconds_total{{{R}}}[{RI}])', "fraction of time reads paused"),
         q(f'manibroker_reads_paused{{{R}}}', "paused now (0/1)")], "percentunit",
        desc="Client reads are paused while unacked produce data exceeds -max-buffered.")
b.panel("Buffered produce bytes", [q(f'manibroker_buffered_bytes{{{R}}}', "buffered")], "bytes")
b.panel("Control-plane call seen by broker, p50/p99 by op",
        hq("manibroker_cp_call_seconds", [0.5, 0.99], by="op"), "s")
b.panel("Client-side produce latency (WarpStream benchmark-producer, p50/p99)",
        hq("warpstream_produce_benchmark_produce_request_duration_seconds", [0.5, 0.99], sel=R + ', job="warpstream_bench"'), "s",
        desc="Only while a warpstream benchmark-producer runs (its Prometheus port 8081). Not available with kafka-producer-perf-test.")
b.save("pipeline.json")

# --------------------------------------------------------------------- aws
b = B("Manifester - AWS (S3 and DynamoDB)", "mani-aws", ["aws"])
b.panel("S3 PUT latency (broker view, seal to stored)", hq("manibroker_segment_put_seconds", [0.5, 0.99]), "s")
b.panel("S3 latency (CloudWatch request metrics)",
        [q(f'aws_s3_first_byte_latency_average{{{R}}} / 1000', "first byte avg"),
         q(f'aws_s3_total_request_latency_average{{{R}}} / 1000', "total request avg"),
         q(f'aws_s3_total_request_latency_maximum{{{R}}} / 1000', "total request max")], "s",
        desc="Needs the bucket metrics filter (README). 1-minute CloudWatch resolution.")
b.panel("S3 errors and PUT retries",
        [q(f'sum(rate(manibroker_segment_put_attempts_total{{{R}}}[{RI}])) - sum(rate(manibroker_segment_put_seconds_count{{{R}}}[{RI}]))', "PUT retries/s (attempts - segments)"),
         q(f'sum(rate(manibroker_segments_failed_total{{{R}}}[{RI}]))', "failed segments/s"),
         q(f'sum(aws_s3_5xx_errors_sum{{{R}}}) / 60', "S3 5xx/s (CloudWatch)"),
         q(f'sum(aws_s3_4xx_errors_sum{{{R}}}) / 60', "S3 4xx/s (CloudWatch)")], "ops")
b.panel("S3 requests per second (CloudWatch)",
        [q(f'sum(aws_s3_put_requests_sum{{{R}}}) / 60', "PUT"), q(f'sum(aws_s3_get_requests_sum{{{R}}}) / 60', "GET"),
         q(f'sum(aws_s3_all_requests_sum{{{R}}}) / 60', "all")], "reqps")
b.panel("DynamoDB call latency inside manicp (p50/p99 by call)",
        hq("manicp_dynamodb_call_seconds", [0.5, 0.99], by="call"), "s")
b.panel("SuccessfulRequestLatency by operation (CloudWatch avg)",
        [q(f'aws_dynamodb_successful_request_latency_average{{{R}}} / 1000', "{{dimension_Operation}}")], "s")
b.panel("Consumed capacity (CloudWatch, units/s)",
        [q(f'sum(aws_dynamodb_consumed_write_capacity_units_sum{{{R}}}) / 60', "write"),
         q(f'sum(aws_dynamodb_consumed_read_capacity_units_sum{{{R}}}) / 60', "read"),
         q(f'max(aws_dynamodb_provisioned_write_capacity_units_average{{{R}}})', "provisioned write")], "short")
b.panel("Throttles (CloudWatch, per minute)",
        [q(f'sum(aws_dynamodb_write_throttle_events_sum{{{R}}})', "write throttle events"),
         q(f'sum(aws_dynamodb_read_throttle_events_sum{{{R}}})', "read throttle events"),
         q(f'sum by (dimension_Operation) (aws_dynamodb_throttled_requests_sum{{{R}}})', "throttled {{dimension_Operation}}")], "short")
b.panel("manicp handler time by op (p99)", hq("manicp_handler_seconds", [0.99], by="op"), "s")
# SDK-level metrics from sys/awsobs (manibroker_s3_*, manicp_dynamo_*), documented in docs/observability.md.
for pre, nm in [("manibroker_s3", "S3"), ("manicp_dynamo", "DynamoDB")]:
    b.panel(f"{nm} SDK attempt latency by op (p50/p99, result=ok)", hq(f"{pre}_attempt_seconds", [0.5, 0.99], sel=R + ', result="ok"', by="op"), "s")
    b.panel(f"{nm} SDK whole-call latency by op (retries included, p99)", hq(f"{pre}_operation_seconds", [0.99], by="op,outcome"), "s")
    b.panel(f"{nm} SDK retries, throttles and API errors",
            [q(f'sum by (op) (rate({pre}_retries_total{{{R}}}[{RI}]))', "retries {{op}}"),
             q(f'sum by (op, code) (rate({pre}_throttles_total{{{R}}}[{RI}]))', "throttle {{op}} {{code}}"),
             q(f'sum by (op, code) (rate({pre}_api_errors_total{{{R}}}[{RI}]))', "error {{op}} {{code}}")], "ops")
b.panel("DynamoDB capacity units consumed per second (manicp, successful calls)",
        [q(f'sum by (kind) (rate(manicp_dynamo_consumed_capacity_units_total{{{R}}}[{RI}]))', "{{kind}}")], "short")
b.save("aws.json")

# ------------------------------------------------------------------- hosts
H = f'{R}, host=~"$host"'
b = B("Manifester - Hosts", "mani-hosts", ["hosts"])
b.d["templating"]["list"] = [v for v in b.d["templating"]["list"] if v["name"] != "broker"]  # per-host already
b.d["templating"]["list"].append(
    {"name": "host", "type": "query", "label": "host", "datasource": PROM, "includeAll": True, "multi": True,
     "allValue": ".+", "query": {"query": 'label_values(node_uname_info{run_id="$run_id"}, host)', "refId": "h"},
     "refresh": 2, "current": {}})
b.panel("CPU busy", [q(f'1 - avg by (host) (rate(node_cpu_seconds_total{{{H}, mode="idle"}}[{RI}]))', "{{host}} total"),
                     q(f'avg by (host) (rate(node_cpu_seconds_total{{{H}, mode="iowait"}}[{RI}]))', "{{host}} iowait"),
                     q(f'avg by (host) (rate(node_cpu_seconds_total{{{H}, mode="steal"}}[{RI}]))', "{{host}} steal")], "percentunit")
b.panel("Memory used", [q(f'1 - node_memory_MemAvailable_bytes{{{H}}} / node_memory_MemTotal_bytes{{{H}}}', "{{host}}")], "percentunit")
b.panel("Network receive", [q(f'sum by (host) (rate(node_network_receive_bytes_total{{{H}, device!="lo"}}[{RI}]))', "{{host}}")], "Bps")
b.panel("Network transmit", [q(f'sum by (host) (rate(node_network_transmit_bytes_total{{{H}, device!="lo"}}[{RI}]))', "{{host}}")], "Bps")
b.panel("Process CPU (Go services)", [q(f'rate(process_cpu_seconds_total{{{H}}}[{RI}])', "{{service}} {{host}}")], "short",
        desc="Cores used; 1.0 = one core.")
b.panel("Process RSS", [q(f'process_resident_memory_bytes{{{H}}}', "{{service}} {{host}}")], "bytes")
b.panel("Go GC pause (sum rate, seconds per second)", [q(f'rate(go_gc_duration_seconds_sum{{{H}}}[{RI}])', "{{service}} {{host}}")], "s")
b.panel("Go GC pause max quantile", [q(f'go_gc_duration_seconds{{{H}, quantile="1"}}', "{{service}} {{host}}")], "s")
b.panel("Go heap in use / goroutines",
        [q(f'go_memstats_heap_inuse_bytes{{{H}}}', "heap {{service}} {{host}}"),
         q(f'go_goroutines{{{H}}}', "goroutines {{service}} {{host}}")], "short")
b.panel("GC cycles per second", [q(f'rate(go_gc_duration_seconds_count{{{H}}}[{RI}])', "{{service}} {{host}}")], "ops")
b.save("hosts.json")
