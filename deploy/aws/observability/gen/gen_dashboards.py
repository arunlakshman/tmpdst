#!/usr/bin/env python3
"""Generates dashboards/*.json. Run: python3 gen/gen_dashboards.py (from this dir's parent)."""
import json, os

PROM = {"type": "prometheus", "uid": "${ds_prom}"}
LOKI = {"type": "loki", "uid": "${ds_loki}"}
R = 'run_id="$run_id"'
RI = "$__rate_interval"


def q(expr, legend="", ds=PROM, ref=None):
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
        [q(f'rate(manibroker_segment_put_attempts_total{{{R}}}[{RI}]) - sum(rate(manibroker_segment_put_seconds_count{{{R}}}[{RI}]))', "PUT retries/s (attempts - segments)"),
         q(f'sum(rate(manibroker_segments_failed_total{{{R}}}[{RI}]))', "failed segments/s"),
         q(f'sum(aws_s3_5xx_errors_sum{{{R}}}) / 60', "S3 5xx/s (CloudWatch)"),
         q(f'sum(aws_s3_4xx_errors_sum{{{R}}}) / 60', "S3 4xx/s (CloudWatch)")], "ops")
b.panel("S3 requests per second (CloudWatch)",
        [q(f'sum(aws_s3_put_requests_sum{{{R}}}) / 60', "PUT"), q(f'sum(aws_s3_get_requests_sum{{{R}}}) / 60', "GET"),
         q(f'sum(aws_s3_all_requests_sum{{{R}}}) / 60', "all")], "reqps")
b.panel("DynamoDB call latency inside manicp (p50/p99 by call)",
        hq("manicp_dynamodb_call_seconds", [0.5, 0.99], by="call"), "s")
b.panel("TransactWriteItems latency (CloudWatch, server side)",
        [q(f'aws_dynamodb_successful_request_latency_average{{{R}, dimension_Operation="TransactWriteItems"}} / 1000', "avg"),
         q(f'aws_dynamodb_successful_request_latency_maximum{{{R}, dimension_Operation="TransactWriteItems"}} / 1000', "max")], "s")
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
b.panel("Transaction cancellations and retries",
        [q(f'sum by (op, reason) (rate(manicp_retries_total{{{R}}}[{RI}]))', "retry {{op}} {{reason}}"),
         q(f'sum by (op) (rate(manicp_commit_attempts_total{{{R}}}[{RI}]))', "attempts {{op}}"),
         q(f'sum(aws_dynamodb_transaction_conflict_sum{{{R}}}) / 60', "TransactionConflict/s (CW)"),
         q(f'sum(aws_dynamodb_conditional_check_failed_requests_sum{{{R}}}) / 60', "ConditionalCheckFailed/s (CW)")], "ops")
b.panel("CommitSegment time outside the transaction (p50/p99)", hq("manicp_commit_prep_seconds", [0.5, 0.99], by="stage"), "s")
b.panel("manicp handler time by op (p99)", hq("manicp_handler_seconds", [0.99], by="op"), "s")
# SDK-level metrics from sys/awsobs (manibroker_s3_*, manicp_dynamo_*); series appear once the binaries wire them.
for pre, nm in [("manibroker_s3", "S3"), ("manicp_dynamo", "DynamoDB")]:
    b.panel(f"{nm} SDK attempt latency by op (p50/p99, result=ok)", hq(f"{pre}_attempt_seconds", [0.5, 0.99], sel=R + ', result="ok"', by="op"), "s")
    b.panel(f"{nm} SDK whole-call latency by op (retries included, p99)", hq(f"{pre}_operation_seconds", [0.99], by="op,outcome"), "s")
    b.panel(f"{nm} SDK retries, throttles and API errors",
            [q(f'sum by (op) (rate({pre}_retries_total{{{R}}}[{RI}]))', "retries {{op}}"),
             q(f'sum by (op, code) (rate({pre}_throttles_total{{{R}}}[{RI}]))', "throttle {{op}} {{code}}"),
             q(f'sum by (op, code) (rate({pre}_api_errors_total{{{R}}}[{RI}]))', "error {{op}} {{code}}")], "ops")
b.save("aws.json")

# ------------------------------------------------------------------- hosts
H = f'{R}, host=~"$host"'
b = B("Manifester - Hosts", "mani-hosts", ["hosts"])
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
