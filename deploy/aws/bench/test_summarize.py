#!/usr/bin/env python3
"""Tests for summarize.py: canned OMB JSON (testdata/omb-local-sample.json, a real OMB 0.0.1 result from a
local run against manibroker) and canned /metrics snapshots in both layouts.
Run: python3 -I -m unittest deploy/aws/bench/test_summarize.py   (or python3 deploy/aws/bench/test_summarize.py)"""
import argparse
import io
import json
import os
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
manicp_dynamo_api_errors_total{{op="TransactWriteItems",code="TransactionCanceledException"}} 3
manicp_dynamo_retries_total{{op="Query"}} 1
manicp_dynamodb_call_seconds_sum 2
manicp_dynamodb_call_seconds_count 100
"""


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def make_run(d, nested=True, brokers=(("1", 6e9), ("2", 3e9), ("3", 1e9)), comp="none", sample_target=5000):
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
        write(os.path.join(base, f"{START - 5}-manicp.txt"), prom_cp(0, 0))
        write(os.path.join(base, f"{END + 5}-manicp.txt"), prom_cp(20000, 500))


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


class HistTests(unittest.TestCase):
    def test_quantile(self):
        self.assertAlmostEqual(summarize.hist_quantile({0.1: 50, 0.2: 100, float("inf"): 100}, 0.5), 0.1)


if __name__ == "__main__":
    unittest.main()
