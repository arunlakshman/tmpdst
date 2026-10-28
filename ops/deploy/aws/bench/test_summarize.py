#!/usr/bin/env python3
"""Tests for summarize.py: canned OMB JSON (testdata/omb-local-sample.json, a real OMB 0.0.1 result from a
local run against manibroker) and canned /metrics snapshots in both layouts.
Run: python3 -I -m unittest ops/deploy/aws/bench/test_summarize.py   (or python3 ops/deploy/aws/bench/test_summarize.py)"""
import argparse
import io
import json
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import summarize  # noqa: E402

START, END = 1000, 1060


def prom(segs, nbytes, puts, gets, wru, rcu, ack_sum=1.0, ack_n=10):
    return f"""# HELP x
manibroker_segments_committed_total {segs}
manibroker_segment_committed_bytes_total {nbytes}
manibroker_segment_put_attempts_total {segs}
manibroker_segment_put_seconds_sum {segs * 0.07}
manibroker_segment_put_seconds_count {segs}
manibroker_segment_queue_wait_seconds_sum 0
manibroker_segment_queue_wait_seconds_count {segs}
manibroker_segment_commit_seconds_sum {segs * 0.04}
manibroker_segment_commit_seconds_count {segs}
manibroker_segment_seal_to_ack_seconds_sum {ack_sum}
manibroker_segment_seal_to_ack_seconds_count {ack_n}
manibroker_s3_throttles_total{{op="PutObject",code="SlowDown"}} 0
manibroker_s3_attempt_seconds_count{{op="PutObject",result="ok"}} {puts}
manibroker_s3_attempt_seconds_count{{op="GetObject",result="ok"}} {gets}
manibroker_s3_request_bytes_sum{{op="PutObject"}} {nbytes}
"""


def prom_cp(wru, rcu):
    return f"""manicp_dynamo_consumed_capacity_units_total{{kind="write"}} {wru}
manicp_dynamo_consumed_capacity_units_total{{kind="read"}} {rcu}
manicp_dynamo_api_errors_total{{op="PutItem",code="ProvisionedThroughputExceededException"}} 3
manicp_dynamo_retries_total{{op="Query"}} 1
manicp_dynamodb_call_seconds_sum 2
manicp_dynamodb_call_seconds_count 100
"""


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def prom_log(scale):
    return f"""manicp_log_slots_total {700 * scale}
manicp_log_lost_slots_total {scale}
manicp_log_role_changes_total{{to="active"}} {scale}
"""


def make_run(d, nested=True, brokers=(("1", 6e9), ("2", 3e9), ("3", 1e9)), comp="none", sample_target=5000, cp=None):
    """cp: None = legacy (a manicp snapshot per broker); "log" / "nolog" = single control plane under metrics/cp/."""
    omb = os.path.join(d, "omb")
    os.makedirs(omb)
    with open(os.path.join(HERE, "testdata", "omb-local-sample.json")) as f:
        sample = f.read()
    write(os.path.join(omb, "smoke.json"), sample)
    write(os.path.join(omb, "stages.tsv"),
          "stage\tworkload\tstart\tend\tstatus\ttarget_msgs\tmsg_size\tconsumers\tcompression\tjson\n"
          f"smoke\tsmoke.yaml\t{START}\t{END}\tok\t{sample_target}\t1024\t4\t{comp}\tsmoke.json\n")
    write(os.path.join(omb, "run.json"), json.dumps({"run_id": "t", "compression": comp, "brokers": len(brokers), "partitions": 8}))
    for bid, nbytes in brokers:
        base = os.path.join(d, "metrics", bid) if nested else os.path.join(d, "metrics")
        pre = "" if nested else ""
        write(os.path.join(base, f"{pre}{START - 5}-broker.txt"), prom(0, 0, 0, 0, 0, 0, 0, 0))
        write(os.path.join(base, f"{pre}{END + 5}-broker.txt"), prom(100, nbytes, 100, 10, 0, 0, 5.0, 100))
        if cp is None:
            write(os.path.join(base, f"{START - 5}-manicp.txt"), prom_cp(0, 0))
            write(os.path.join(base, f"{END + 5}-manicp.txt"), prom_cp(20000, 500))
    if cp:
        base = os.path.join(d, "metrics", "cp")
        write(os.path.join(base, f"{START - 5}-manicp.txt"), prom_cp(0, 0) + (prom_log(0) if cp == "log" else ""))
        write(os.path.join(base, f"{END + 5}-manicp.txt"), prom_cp(20000, 700) + (prom_log(100) if cp == "log" else ""))


def report(d, **kw):
    args = argparse.Namespace(broker_hourly=kw.get("hourly", 1.0), s3_put_per_1000=0.005, s3_get_per_1000=0.0004,
                              ddb_wru_per_million=1.25, ddb_rcu_per_million=0.25)
    lines = []
    unmet = summarize.omb_report(lines.append, args, d, summarize.load_snapshots(os.path.join(d, "metrics")))
    return "\n".join(lines), unmet


class OMBTests(unittest.TestCase):
    def test_omb_stats_from_real_sample(self):
        with open(os.path.join(HERE, "testdata", "omb-local-sample.json")) as f:
            st = summarize.omb_stats(json.load(f))
        self.assertEqual(st["msg_size"], 1024)
        self.assertGreater(st["pub_rate"], 4000)
        self.assertAlmostEqual(st["pub_mibs"], st["pub_rate"] * 1024 / 1048576)
        self.assertGreater(st["p99"], 0)
        self.assertGreater(st["e99"], 0)  # consumers existed

    def test_produce_only_has_no_e2e(self):
        with open(os.path.join(HERE, "testdata", "omb-local-sample.json")) as f:
            res = json.load(f)
        res["consumeRate"] = [0.0] * len(res["consumeRate"])
        st = summarize.omb_stats(res)
        self.assertIsNone(st["e99"])

    def test_nested_layout_skew_and_cost(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d)
            text, unmet = report(d)
        self.assertIn("| 1 | 85.7 | 60.0% |", text)  # 6e9 bytes over the 70 s snapshot window
        self.assertIn("max/min", text)
        self.assertIn("6.00", text)  # 6e9 / 1e9
        self.assertTrue(any("per-broker skew" in u for u in unmet))
        self.assertTrue(any("compression.type=none" in u for u in unmet))
        self.assertIn("S3 $/GiB", text)
        # 300 PUTs, 30 GETs
        self.assertIn("| 300 | 30 |", text)

    def test_single_control_plane_with_log_metrics(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d, cp="log")
            text, _ = report(d)
        self.assertIn("Control plane (log store)", text)
        # cp is not a broker row; 3 brokers only, per-broker table has no DDB columns
        self.assertNotIn("| cp |", text)
        self.assertNotIn("| 1 | 85.7 | 60.0% | 1.43 | 70.0 | 0.0 | 40.0 | 50.0 | 1.4 | 0.1 | 0 | 0 | 0 | 0 |", text)
        # slots 70000/70s = 1000.0, lost 100, role changes 100, WRU 20000/70 = 286
        self.assertIn("| smoke | 1,000.0 | 100 | 100 |", text)
        self.assertIn("| 286 | 10 |", text)
        # cost: 20000 WRU + 700 RCU counted once (not per broker)
        self.assertIn("| 20,000 | 700 |", text)

    def test_single_control_plane_without_log_metrics(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d, cp="nolog")
            text, _ = report(d)
        self.assertIn("Control plane (log store)", text)
        self.assertIn("| smoke | - | - | - |", text)
        self.assertIn("| 20,000 | 700 |", text)

    def test_flat_legacy_layout_single_broker(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d, nested=False, brokers=(("1", 2e9),))
            text, unmet = report(d)
        self.assertIn("| 1 |", text)
        self.assertIn("needs >= 2 brokers", text)
        self.assertFalse(any("skew" in u for u in unmet))

    def test_lz4_run_has_no_compression_item(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d, comp="lz4")
            _, unmet = report(d)
        self.assertFalse(any("compression" in u for u in unmet))

    def test_missing_snapshots(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d, brokers=())
            text, unmet = report(d)
        self.assertIn("No metric snapshots", text)
        self.assertTrue(any("no server-side metric snapshots" in u for u in unmet))

    def test_no_compute_price_omits_compute(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d)
            text, _ = report(d, hourly=0.0)
        self.assertIn("(no compute)", text)

    def test_main_end_to_end(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d)
            out = os.path.join(d, "summary.md")
            old = sys.argv, sys.stdout
            sys.argv, sys.stdout = ["summarize.py", d, "--out", out, "--broker-hourly", "0.7"], io.StringIO()
            try:
                summarize.main()
            finally:
                sys.argv, sys.stdout = old
            with open(out) as f:
                body = f.read()
        self.assertIn("OpenMessaging Benchmark stages", body)
        self.assertIn("Apples-to-apples checklist", body)


FAILOVER = os.path.join(HERE, "testdata", "failover")


def failover_report(rundir):
    args = argparse.Namespace(broker_hourly=0.0, s3_put_per_1000=0.005, s3_get_per_1000=0.0004,
                              ddb_wru_per_million=1.25, ddb_rcu_per_million=0.25)
    lines = []
    unmet = summarize.omb_report(lines.append, args, rundir, summarize.load_snapshots(os.path.join(rundir, "metrics")))
    return "\n".join(lines), unmet


class ControlPlaneNodesTests(unittest.TestCase):
    def test_cp_dirs_are_not_brokers(self):
        snaps = summarize.load_snapshots(os.path.join(FAILOVER, "metrics"))
        self.assertEqual(summarize.cp_ids(snaps), ["cp", "cp-1"])
        self.assertEqual(summarize.broker_ids(snaps), ["1", "2"])

    def test_active_runs_per_node(self):
        snaps = summarize.load_snapshots(os.path.join(FAILOVER, "metrics"))
        runs = summarize.active_runs(summarize.cp_active_samples(snaps))
        self.assertEqual(runs, [("cp", 990, 1090), ("cp-1", 1100, 1180), ("cp", 1190, 1210)])

    def test_roles_table_and_failover_summary(self):
        text, unmet = failover_report(FAILOVER)
        self.assertIn("| stage | cp active/snapshots | cp-1 active/snapshots |", text)
        self.assertIn("| kill500k | 12/15 | 9/21 |", text)
        self.assertIn("- active before the kill: cp", text)
        self.assertIn("- cp snapshot gap: 70 s without snapshots", text)
        self.assertIn("takeover: cp-1 first seen active at", text)
        self.assertIn("5 s after the kill", text)
        self.assertIn("failback: cp active again", text)
        self.assertIn("between 25 and 35 s after T", text)
        self.assertIn("in stage kill500k (status ok)", text)
        self.assertIn("No ABORT file.", text)
        self.assertFalse(any("standby took over" in u or "acknowledged" in u for u in unmet))

    def test_cp_deltas_sum_over_nodes(self):
        snaps = summarize.load_snapshots(os.path.join(FAILOVER, "metrics"))
        m = summarize.cp_window(snaps, 1000, 1200)
        self.assertEqual(m["log_lost"], 2)  # cp-1 lost two slots; cp's counters did not move
        self.assertEqual(m["log_role_changes"], 2)

    def test_no_takeover_and_abort_are_reported(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copytree(FAILOVER, d, dirs_exist_ok=True)
            for f in os.listdir(os.path.join(d, "metrics", "cp-1")):
                write(os.path.join(d, "metrics", "cp-1", f), "manicp_log_active 0\n")
            write(os.path.join(d, "ABORT"), "publish p99 over 60 s\n")
            text, unmet = failover_report(d)
        self.assertIn("NO other node was seen active", text)
        self.assertIn("ABORT file: publish p99 over 60 s", text)
        self.assertTrue(any("no standby took over" in u for u in unmet))
        self.assertTrue(any("ABORT" in u for u in unmet))

    def test_no_marker_means_no_failover_section(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copytree(FAILOVER, d, dirs_exist_ok=True)
            shutil.rmtree(os.path.join(d, "markers"))
            text, _ = failover_report(d)
        self.assertIn("Control-plane nodes: active role", text)
        self.assertNotIn("### Failover", text)

    def test_single_cp_without_log_metrics_has_no_role_section(self):
        with tempfile.TemporaryDirectory() as d:
            make_run(d, cp="log")
            text, _ = report(d)
        self.assertNotIn("Control-plane nodes", text)

    def test_cp_hourly_counts_every_cp_node(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copytree(FAILOVER, d, dirs_exist_ok=True)
            args = argparse.Namespace(broker_hourly=1.0, cp_hourly=10.0, s3_put_per_1000=0.005, s3_get_per_1000=0.0004,
                                      ddb_wru_per_million=1.25, ddb_rcu_per_million=0.25)
            lines = []
            summarize.omb_report(lines.append, args, d, summarize.load_snapshots(os.path.join(d, "metrics")))
            one = "\n".join(lines)
            args.cp_hourly = 0.0
            lines = []
            summarize.omb_report(lines.append, args, d, summarize.load_snapshots(os.path.join(d, "metrics")))
        self.assertNotEqual(one, "\n".join(lines))


class HistTests(unittest.TestCase):
    def test_quantile(self):
        self.assertAlmostEqual(summarize.hist_quantile({0.1: 50, 0.2: 100, float("inf"): 100}, 0.5), 0.1)


if __name__ == "__main__":
    unittest.main()
