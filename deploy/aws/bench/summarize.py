#!/usr/bin/env python3
"""Summarize one benchmark run into a markdown report.

usage: summarize.py RESULTS_DIR [--out FILE]

RESULTS_DIR is ./results/<run_id> (collect-results.sh) or a --local output dir:
    loadgen/cells.tsv, loadgen/<cell>.rN.*.out|.prom   written by run-benchmark.sh
    metrics/<epoch>-{broker,manicp}.txt                Prometheus text snapshots

For each cell the snapshot at or before `start` and the first one at or after
`end` (after the tool drained) bracket the broker-side deltas, so the window can
include up to one snapshot interval (10 s on AWS) of idle time on either side;
averages are exact, rates are divided by the snapshot window.

Tool output parsed:
  kafka-producer-perf-test: interval lines and, when the tool survived the
    shutdown, the final summary line (the only source of p50/p95/p99).
  warpstream benchmark-producer: not usable against this broker (compression);
    parser is best effort and unverified and the .prom histogram snapshot for p50/p99.
Standard library only.
"""
import argparse
import collections
import math
import os
import re
import sys

NUM = r"([0-9][0-9,]*\.?[0-9]*)"


def num(x):
    return float(x.replace(",", ""))


# ---------------------------------------------------------------- prometheus
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)')
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_prom(path):
    """-> {(name, frozenset(labels.items())): value}"""
    out = {}
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("#"):
                    continue
                m = SAMPLE.match(line)
                if not m:
                    continue
                labels = frozenset(LABEL.findall(m.group(2) or ""))
                try:
                    out[(m.group(1), labels)] = float(m.group(3))
                except ValueError:
                    pass
    except OSError:
        pass
    return out


def total(snap, name, **want):
    """Sum of series `name` whose labels include all of want."""
    s = 0.0
    for (n, labels), v in snap.items():
        if n == name:
            d = dict(labels)
            if all(d.get(k) == v2 for k, v2 in want.items()):
                s += v
    return s


def delta(a, b, name, **want):
    return max(0.0, total(b, name, **want) - total(a, name, **want))


def hist_avg_ms(a, b, base):
    dc = delta(a, b, base + "_count")
    ds = delta(a, b, base + "_sum")
    return (ds / dc * 1000.0) if dc > 0 else None


def hist_quantile(buckets, q):
    """buckets: {le(float): cumulative count}. Linear interpolation like Prometheus."""
    items = sorted(buckets.items())
    if not items or items[-1][1] <= 0:
        return None
    rank = q * items[-1][1]
    prev_le, prev_c = 0.0, 0.0
    for le, c in items:
        if c >= rank:
            if math.isinf(le):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return None


def prom_hist(snap, base):
    b = collections.defaultdict(float)
    for (n, labels), v in snap.items():
        if n == base + "_bucket":
            le = dict(labels).get("le")
            if le is not None:
                b[float("inf") if le == "+Inf" else float(le)] += v
    return b


# ------------------------------------------------------------------ load tool
KAFKA_LINE = re.compile(NUM + r" records sent, " + NUM + r" records/sec \(" + NUM + r" MB/sec\), " + NUM + r" ms avg latency, " + NUM + r" ms max latency(?:, " + NUM + r" ms 50th, " + NUM + r" ms 95th, " + NUM + r" ms 99th, " + NUM + r" ms 99.9th)?")


def parse_kafka(path):
    """-> dict with interval samples and optional final summary."""
    intervals, final = [], None
    with open(path, errors="replace") as f:
        for line in f:
            m = KAFKA_LINE.search(line)
            if not m:
                continue
            g = m.groups()
            rec = dict(records=num(g[0]), rps=num(g[1]), mbps=num(g[2]), avg=num(g[3]), max=num(g[4]))
            if g[5] is not None:
                rec.update(p50=num(g[5]), p95=num(g[6]), p99=num(g[7]), p999=num(g[8]))
                final = rec
            else:
                intervals.append(rec)
    return intervals, final


def parse_warpstream(path):
    """Best effort: any line with records/s, MiB/s and min/avg/max latency."""
    intervals = []
    with open(path, errors="replace") as f:
        for line in f:
            r = re.search(NUM + r"\s*records?/s", line, re.I)
            b = re.search(NUM + r"\s*MiB/s", line, re.I)
            a = re.search(r"avg[^0-9]{0,12}" + NUM, line, re.I)
            if not (r and b):
                continue
            mx = re.search(r"max[^0-9]{0,12}" + NUM, line, re.I)
            n = re.search(NUM + r"\s*records?\b", line, re.I)
            intervals.append(dict(records=num(n.group(1)) if n else 0.0, rps=num(r.group(1)), mbps=num(b.group(1)) * 1.048576,
                                  avg=num(a.group(1)) if a else None, max=num(mx.group(1)) if mx else None))
    return intervals


def tool_stats(ldir, prefix, tool, clients):
    """-> dict(rps, mbps, avg, p50, p99, n_intervals, note)"""
    res = dict(rps=None, mbps=None, avg=None, p50=None, p99=None, note="")
    if tool == "kafka":
        rps = mbps = 0.0
        wsum = wn = 0.0
        p50s, p99s, got = [], [], 0
        for i in range(1, clients + 1):
            p = os.path.join(ldir, f"{prefix}.c{i}.out")
            if not os.path.exists(p):
                continue
            iv, fin = parse_kafka(p)
            if iv:
                got += 1
                rps += sum(x["rps"] for x in iv) / len(iv)
                mbps += sum(x["mbps"] for x in iv) / len(iv)
                for x in iv:
                    wsum += x["avg"] * x["records"]
                    wn += x["records"]
            if fin:
                p50s.append((fin["p50"], fin["records"]))
                p99s.append(fin["p99"])
        res.update(rps=rps if got else None, mbps=mbps if got else None, avg=(wsum / wn) if wn else None)
        if p50s:
            res["p50"] = sum(p * w for p, w in p50s) / sum(w for _, w in p50s)
            res["p99"] = max(p99s)
        else:
            res["note"] = "no final summary (tool killed): percentiles n/a"
        if got < clients:
            res["note"] += f" {got}/{clients} clients produced output"
    else:
        iv = []
        p = os.path.join(ldir, prefix + ".out")
        if os.path.exists(p):
            iv = parse_warpstream(p)
        if iv:
            res["rps"] = sum(x["rps"] for x in iv) / len(iv)
            res["mbps"] = sum(x["mbps"] for x in iv) / len(iv)
            av = [x["avg"] for x in iv if x["avg"] is not None]
            res["avg"] = sum(av) / len(av) if av else None
        else:
            res["note"] = "interval lines not recognized"
        snap = parse_prom(os.path.join(ldir, prefix + ".prom"))
        h = prom_hist(snap, "warpstream_produce_benchmark_produce_request_duration_seconds")
        if h:
            p50, p99 = hist_quantile(h, 0.5), hist_quantile(h, 0.99)
            res["p50"] = p50 * 1000 if p50 is not None else None
            res["p99"] = p99 * 1000 if p99 is not None else None
    return res


# -------------------------------------------------------------------- driver
def load_snapshots(mdir):
    snaps = collections.defaultdict(dict)  # service -> {ts: path}
    if os.path.isdir(mdir):
        for fn in os.listdir(mdir):
            m = re.match(r"(\d+)-(broker|manicp)\.txt$", fn)
            if m:
                snaps[m.group(2)][int(m.group(1))] = os.path.join(mdir, fn)
    return snaps


def bracket(tsmap, start, end):
    ks = sorted(tsmap)
    before = [k for k in ks if k <= start]
    after = [k for k in ks if k >= end]
    if not before or not after:
        return None
    return before[-1], after[0]


def fmt(x, nd=1, dash="-"):
    return dash if x is None else f"{x:,.{nd}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--out")
    args = ap.parse_args()
    ldir = os.path.join(args.dir, "loadgen")
    rows = []
    with open(os.path.join(ldir, "cells.tsv")) as f:
        hdr = f.readline().rstrip("\n").split("\t")
        for line in f:
            rows.append(dict(zip(hdr, line.rstrip("\n").split("\t"))))
    snaps = load_snapshots(os.path.join(args.dir, "metrics"))
    out = []
    w = out.append
    run_json = os.path.join(ldir, "run.json")
    w(f"# Benchmark summary: {os.path.basename(os.path.abspath(args.dir))}\n")
    if os.path.exists(run_json):
        w("```\n" + open(run_json).read().strip() + "\n```\n")

    per = []
    for r in rows:
        clients, size = int(r["clients"]), int(r["record_size"])
        start, end = int(r["start"]), int(r["end"])
        t = tool_stats(ldir, r["prefix"], r["tool"], clients)
        d = dict(cell=r["cell"], rep=int(r["rep"]), clients=clients, size=size, tool=r["tool"], t=t, note=t["note"])
        bb = bracket(snaps["broker"], start, end) if snaps["broker"] else None
        cc = bracket(snaps["manicp"], start, end) if snaps["manicp"] else None
        if bb:
            a, b = parse_prom(snaps["broker"][bb[0]]), parse_prom(snaps["broker"][bb[1]])
            win = max(1, bb[1] - bb[0])
            d["win"] = win
            d["seg_rate"] = delta(a, b, "manibroker_segments_committed_total") / win
            d["bytes_rate"] = delta(a, b, "manibroker_segment_committed_bytes_total") / win
            d["put"] = hist_avg_ms(a, b, "manibroker_segment_put_seconds")
            d["queue"] = hist_avg_ms(a, b, "manibroker_segment_queue_wait_seconds")
            d["commit"] = hist_avg_ms(a, b, "manibroker_segment_commit_seconds")
            d["ack"] = hist_avg_ms(a, b, "manibroker_segment_seal_to_ack_seconds")
            d["s3_thr"] = delta(a, b, "manibroker_s3_throttles_total")
            d["s3_err"] = delta(a, b, "manibroker_s3_api_errors_total") + delta(a, b, "manibroker_segments_failed_total")
            d["s3_retry"] = delta(a, b, "manibroker_segment_put_attempts_total") - delta(a, b, "manibroker_segments_committed_total")
        else:
            d["note"] += " no broker snapshots around the cell"
        if cc:
            a, b = parse_prom(snaps["manicp"][cc[0]]), parse_prom(snaps["manicp"][cc[1]])
            d["ddb_thr"] = delta(a, b, "manicp_dynamo_throttles_total")
            errs = delta(a, b, "manicp_dynamo_api_errors_total")
            canc = delta(a, b, "manicp_dynamo_api_errors_total", code="TransactionCanceledException")
            d["ddb_err"] = errs - canc
            d["ddb_cancel"] = canc
            d["ddb_retry"] = delta(a, b, "manicp_dynamo_retries_total")
            d["ddb_wru"] = delta(a, b, "manicp_dynamo_consumed_capacity_units_total", kind="write")
            d["ddb_call"] = hist_avg_ms(a, b, "manicp_dynamodb_call_seconds")
        per.append(d)

    w("## Per cell and repetition\n")
    w("Client side (from the load tool): throughput summed over clients; avg latency record-weighted; p50 record-weighted mean and p99 max over clients, from the tool's final summary line (n/a when the tool did not print one).\n")
    w("| cell | rep | clients | bytes | records/s | MB/s | avg ms | p50 ms | p99 ms | notes |")
    w("|---|---|---|---|---|---|---|---|---|---|")
    for d in per:
        t = d["t"]
        w(f"| {d['cell']} | {d['rep']} | {d['clients']} | {d['size']} | {fmt(t['rps'], 0)} | {fmt(t['mbps'], 2)} | {fmt(t['avg'])} | {fmt(t['p50'])} | {fmt(t['p99'])} | {d['note'].strip()} |")

    w("\n## Broker and control plane, same cells (deltas between snapshots)\n")
    w("Averages are exact (delta sum / delta count). `seg/s` and `MB/s` use the snapshot window. put = seal to stored in S3; queue = waiting for earlier commits; commit = CommitSegment round trip; ack = seal to answer.\n")
    w("| cell | rep | seg/s | committed MB/s | put ms | queue ms | commit ms | ack ms | S3 throttles | S3 errors | S3 retries | DDB throttles | DDB errors | DDB txn cancels | DDB retries | DDB WRU | DDB call ms |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for d in per:
        g = d.get
        w(f"| {d['cell']} | {d['rep']} | {fmt(g('seg_rate'), 2)} | {fmt(g('bytes_rate') / 1e6 if g('bytes_rate') is not None else None, 2)} | {fmt(g('put'))} | {fmt(g('queue'))} | {fmt(g('commit'))} | {fmt(g('ack'))} | {fmt(g('s3_thr'), 0)} | {fmt(g('s3_err'), 0)} | {fmt(g('s3_retry'), 0)} | {fmt(g('ddb_thr'), 0)} | {fmt(g('ddb_err'), 0)} | {fmt(g('ddb_cancel'), 0)} | {fmt(g('ddb_retry'), 0)} | {fmt(g('ddb_wru'), 0)} | {fmt(g('ddb_call'))} |")

    w("\n## Mean over repetitions\n")
    w("| cell | reps | records/s | MB/s | avg ms | p99 ms (worst rep) | put ms | commit ms | ack ms | S3 throttles+errors | DDB throttles+errors |")
    w("|---|---|---|---|---|---|---|---|---|---|---|")
    order = []
    for d in per:
        if d["cell"] not in order:
            order.append(d["cell"])

    def mean(vals):
        v = [x for x in vals if x is not None]
        return sum(v) / len(v) if v else None

    for c in order:
        ds = [d for d in per if d["cell"] == c]
        p99 = [d["t"]["p99"] for d in ds if d["t"]["p99"] is not None]
        s3 = sum((d.get("s3_thr") or 0) + (d.get("s3_err") or 0) for d in ds)
        dd = sum((d.get("ddb_thr") or 0) + (d.get("ddb_err") or 0) for d in ds)
        w(f"| {c} | {len(ds)} | {fmt(mean([d['t']['rps'] for d in ds]), 0)} | {fmt(mean([d['t']['mbps'] for d in ds]), 2)} | {fmt(mean([d['t']['avg'] for d in ds]))} | {fmt(max(p99) if p99 else None)} | {fmt(mean([d.get('put') for d in ds]))} | {fmt(mean([d.get('commit') for d in ds]))} | {fmt(mean([d.get('ack') for d in ds]))} | {fmt(s3, 0)} | {fmt(dd, 0)} |")
    w("\nDDB errors exclude `TransactionCanceledException` (a lost race, counted separately). CloudWatch-side AWS numbers are not in this report; use the Grafana AWS dashboard for the same window.")
    text = "\n".join(out) + "\n"
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    sys.stdout.write(text)


if __name__ == "__main__":
    main()
