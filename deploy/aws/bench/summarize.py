#!/usr/bin/env python3
"""Summarize one benchmark run into a markdown report.

usage: summarize.py RESULTS_DIR [--out FILE] [--broker-hourly USD] [--s3-put-per-1000 USD]
                    [--s3-get-per-1000 USD] [--ddb-wru-per-million USD] [--ddb-rcu-per-million USD]

RESULTS_DIR is ./results/<run_id> (collect-results.sh) or a --local output dir:
    loadgen/cells.tsv, loadgen/<cell>.rN.*.out|.prom   written by run-benchmark.sh (Kafka perf-test harness)
    omb/stages.tsv, omb/run.json, omb/<stage>.json     written by run-omb.sh (OpenMessaging Benchmark)
    metrics/<epoch>-{broker,manicp}.txt                Prometheus text snapshots, ONE broker (legacy layout)
    metrics/<broker_id>/<epoch>-broker.txt             one directory per broker (multi-broker layout)
    metrics/cp/<epoch>-manicp.txt                      the active control-plane manicp: DynamoDB and log-store side
    metrics/cp-<rank>/<epoch>-manicp.txt               standby control-plane nodes; every metrics/cp[-N] dir is read
    markers/kill-appender-<epoch>.txt                  written by bench-ctl.sh kill-appender (epoch = kill time); drives the failover summary
    metrics/<broker_id>/<epoch>-manicp.txt             older layout, one manicp per broker (still read)
Both layouts are read; a flat file is attributed to broker "1". With several brokers the Kafka harness
cells sum the per-broker deltas.

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
import datetime
import json
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


# ------------------------------------------------- control-plane roles and failover
SNAP_GAP = 30  # seconds without a snapshot from a node end its "active" run (snapshots come every 10 s)
MARKER = re.compile(r"kill-appender-(\d+)\.txt$")


def cp_active_samples(snaps):
    """-> {node: [(ts, active 0/1/None)]} from manicp_log_active (None: snapshot has no log-store metrics)."""
    out = {}
    for n in cp_ids(snaps):
        rows = []
        for ts, path in sorted(snaps["manicp"][n].items()):
            snap = parse_prom(path)
            has = any(k[0] == "manicp_log_active" for k in snap)
            rows.append((ts, int(total(snap, "manicp_log_active") > 0.5) if has else None))
        out[n] = rows
    return out


def active_runs(samples):
    """-> [(node, first_ts, last_ts)] runs of consecutive active snapshots per node, sorted by start."""
    runs = []
    for n, rows in samples.items():
        cur = None
        for ts, act in rows:
            if act == 1 and cur and ts - cur[2] <= SNAP_GAP:
                cur[2] = ts
            elif act == 1:
                if cur:
                    runs.append(tuple(cur))
                cur = [n, ts, ts]
            elif cur:
                runs.append(tuple(cur))
                cur = None
        if cur:
            runs.append(tuple(cur))
    return sorted(runs, key=lambda r: (r[1], r[0]))


def utc(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%H:%M:%S")


def ack_increments(snaps):
    """-> [(ts_prev, ts, increase)] of the broker seal-to-ack counter, per broker snapshot interval."""
    out = []
    for bid, tm in snaps["broker"].items():
        prev = None
        for ts in sorted(tm):
            c = total(parse_prom(tm[ts]), "manibroker_segment_seal_to_ack_seconds_count")
            if prev is not None:
                out.append((prev[0], ts, max(0.0, c - prev[1])))
            prev = (ts, c)
    return out


def failover_lines(rundir, snaps, stages, digest, samples, runs):
    """Markdown lines about each kill-appender marker (markers/kill-appender-<epoch>.txt) and the ABORT file;
    (lines, unmet). Resolution is the snapshot interval, so every time is an upper bound within about 10 s."""
    mdir = os.path.join(rundir, "markers")
    kills = sorted(int(m.group(1)) for m in (MARKER.search(f) for f in os.listdir(mdir)) if m) if os.path.isdir(mdir) else []
    lines, unmet = [], []
    inc = ack_increments(snaps)
    for t in kills:
        before = [r for r in runs if r[1] <= t]
        prior = max(before, key=lambda r: r[2])[0] if before else None
        lines.append(f"Appender kill at {utc(t)} UTC (epoch {t}):")
        lines.append(f"- active before the kill: {prior or 'unknown (no manicp_log_active before T)'}")
        down = [(n, max((ts for ts, _ in rows if ts <= t), default=None), min((ts for ts, _ in rows if ts > t), default=None))
                for n, rows in samples.items() if n == prior]
        for n, last, first in down:
            gap = f"{first - last} s without snapshots (last {utc(last)}, next {utc(first)})" if last and first else "no snapshot gap seen"
            lines.append(f"- {n} snapshot gap: {gap}")
        after = [r for r in runs if r[1] > t and r[0] != prior]
        if after:
            r = min(after, key=lambda r: r[1])
            lines.append(f"- takeover: {r[0]} first seen active at {utc(r[1])} UTC, {r[1] - t} s after the kill (at most; resolution 10 s)")
        else:
            lines.append("- takeover: NO other node was seen active after the kill")
            unmet.append(f"kill at {utc(t)} UTC: no standby took over (no manicp_log_active=1 on another node)")
        back = [r for r in runs if r[1] > t and r[0] == prior]
        lines.append(f"- failback: {prior} active again at {utc(min(back, key=lambda r: r[1])[1])} UTC" if back and prior else "- failback: not seen")
        fa = [(a, b) for a, b, d in inc if a >= t and d > 0]
        if fa:
            a, b = min(fa, key=lambda x: x[1])
            lines.append(f"- first ack after the kill: in the snapshot interval {utc(a)}-{utc(b)} UTC, i.e. between {a - t} and {b - t} s after T")
        else:
            lines.append("- first ack after the kill: NOT seen in any broker snapshot interval that starts after T")
            unmet.append(f"kill at {utc(t)} UTC: no broker acknowledged a segment after the kill")
        stage = next((st for st in stages if int(st["start"]) <= t <= int(st["end"])), None)
        if stage:
            d = digest.get(stage["stage"])
            e = f"{d['err_max']:.1f} err/s max, p99 {d['p99']:,.0f} ms" if d and d["err_max"] is not None and d["p99"] is not None else "no OMB numbers"
            lines.append(f"- in stage {stage['stage']} (status {stage['status']}): {e}")
        else:
            lines.append("- the kill is outside every OMB stage window")
        lines.append("")
    ab = os.path.join(rundir, "ABORT")
    if os.path.exists(ab):
        lines.append("ABORT file: " + read_text(ab) + "\n")
    elif kills:
        lines.append("No ABORT file.\n")
    return lines, unmet


def roles_section(w, snaps, stages, rundir, digest):
    """Which control-plane node was active per stage and in what runs; the failover summary. Returns unmet items."""
    ids = cp_ids(snaps)
    if len(ids) < 1:
        return []
    samples = cp_active_samples(snaps)
    has_log = any(a is not None for rows in samples.values() for _, a in rows)
    if not has_log and len(ids) < 2:
        return []
    runs = active_runs(samples)
    w("## Control-plane nodes: active role and failover\n")
    if not has_log:
        w("Snapshots of " + ", ".join(ids) + " carry no manicp_log_active (not the log store): roles unknown.\n")
        return []
    w("From manicp_log_active in " + ", ".join(f"metrics/{n}/" for n in ids) + " (10 s snapshots; a gap of more than %d s ends a run). `active/snapshots` counts snapshots in the stage window.\n" % SNAP_GAP)
    w("| stage | " + " | ".join(f"{n} active/snapshots" for n in ids) + " | slots/s | lost slots | role changes |")
    w("|---|" + "---|" * (len(ids) + 3))
    for st in stages:
        a, b = int(st["start"]), int(st["end"])
        cells = []
        for n in ids:
            rows = [x for ts, x in samples[n] if a <= ts <= b]
            cells.append(f"{sum(1 for x in rows if x == 1)}/{len(rows)}")
        m = cp_window(snaps, a, b) or {}
        win = max(1, m.get("win", 1))
        w(f"| {st['stage']} | " + " | ".join(cells) + f" | {fmt(m['log_slots'] / win, 1) if 'log_slots' in m else '-'} | "
          f"{fmt(m.get('log_lost'), 0) if 'log_lost' in m else '-'} | {fmt(m.get('log_role_changes'), 0) if 'log_role_changes' in m else '-'} |")
    w("\nActive runs:\n")
    w("| node | from (UTC) | to (UTC) | seconds |")
    w("|---|---|---|---|")
    for n, a, b in runs:
        w(f"| {n} | {utc(a)} | {utc(b)} | {b - a} |")
    w("")
    lines, unmet = failover_lines(rundir, snaps, stages, digest, samples, runs)
    if lines:
        w("### Failover\n")
        for ln in lines:
            w(ln)
    return unmet


# ------------------------------------------------------------------------ OMB
MIB = 1048576.0
GIB = 1073741824.0
# WarpStream's published figures (monthly, 1 GiB/s, 6 x m6in.4xlarge, AWS 1-year no-upfront), for the comparison block.
WS = dict(compute=3031.0, s3_get=508.0, s3_put=1788.0, mib_s=1024.0)


def mean(v):
    v = [x for x in v if x is not None]
    return sum(v) / len(v) if v else None


def read_text(path):
    with open(path) as f:
        return f.read().strip()


def read_tsv(path):
    rows = []
    with open(path) as f:
        hdr = f.readline().rstrip("\n").split("\t")
        for line in f:
            if line.strip():
                rows.append(dict(zip(hdr, line.rstrip("\n").split("\t"))))
    return rows


def omb_stats(res):
    """Digest of one OMB result JSON (the TestResult class: lists per 10 s interval plus aggregated*)."""
    size = float(res.get("messageSize", 0) or 0)
    pub = [x for x in res.get("publishRate", [])]
    con = [x for x in res.get("consumeRate", [])]
    err = [x for x in res.get("publishErrorRate", [])]
    back = res.get("backlog", [])
    d = dict(
        msg_size=size, producers=res.get("producersPerTopic"), consumers=res.get("consumersPerTopic"),
        partitions=res.get("partitions"),
        pub_rate=mean(pub), con_rate=mean(con), err_rate=mean(err), err_max=max(err) if err else None,
        backlog_last=back[-1] if back else None, backlog_max=max(back) if back else None,
        p_avg=res.get("aggregatedPublishLatencyAvg"), p50=res.get("aggregatedPublishLatency50pct"),
        p95=res.get("aggregatedPublishLatency95pct"), p99=res.get("aggregatedPublishLatency99pct"),
        p999=res.get("aggregatedPublishLatency999pct"), pmax=res.get("aggregatedPublishLatencyMax"),
        e_avg=res.get("aggregatedEndToEndLatencyAvg"), e50=res.get("aggregatedEndToEndLatency50pct"),
        e99=res.get("aggregatedEndToEndLatency99pct"), e999=res.get("aggregatedEndToEndLatency999pct"),
        intervals=len(pub),
    )
    d["pub_mibs"] = d["pub_rate"] * size / MIB if d["pub_rate"] is not None else None
    d["con_mibs"] = d["con_rate"] * size / MIB if d["con_rate"] is not None else None
    # an e2e histogram with no samples reports zeros: treat as absent
    if not con or (d["con_rate"] or 0) == 0:
        d["e_avg"] = d["e50"] = d["e99"] = d["e999"] = None
    return d


def usd(x, nd=2):
    return "n/a" if x is None else f"${x:,.{nd}f}"


def omb_report(w, args, rundir, snaps):
    """Append the OMB sections to w; returns the list of unmet checklist items."""
    odir = os.path.join(rundir, "omb")
    stages = read_tsv(os.path.join(odir, "stages.tsv"))
    run = {}
    if os.path.exists(os.path.join(odir, "run.json")):
        with open(os.path.join(odir, "run.json")) as f:
            run = json.load(f)
    comp = run.get("compression", stages[0].get("compression") if stages else "unknown")
    unmet = []
    w("## OpenMessaging Benchmark stages\n")
    if run:
        w("```\n" + json.dumps(run) + "\n```\n")
    if os.path.exists(os.path.join(odir, "../ABORT")):
        abort_text = read_text(os.path.join(odir, "../ABORT"))
        w("**ABORT file present**: " + abort_text + "\n")
        unmet.append("run aborted (ABORT file): " + abort_text)
    w("Client side (OMB aggregated over the measured period, warm-up excluded). Latencies in ms. "
      "Publish latency is the Kafka producer send-to-ack time (comparable with WarpStream's 'producer latency'); "
      "end-to-end is producer send to consumer receive.\n")
    w("| stage | status | target msg/s | target MiB/s | pub msg/s | pub MiB/s | cons MiB/s | err/s (max) | pub avg | p50 | p99 | p99.9 | max | e2e p50 | e2e p99 | e2e p99.9 | backlog last/max |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    digest = {}
    for st in stages:
        jp = os.path.join(odir, st["json"])
        d = None
        if os.path.exists(jp) and os.path.getsize(jp) > 0:
            with open(jp) as f:
                d = omb_stats(json.load(f))
        digest[st["stage"]] = d
        tgt = float(st["target_msgs"])
        tmib = tgt * float(st["msg_size"]) / MIB
        if d is None:
            w(f"| {st['stage']} | {st['status']} | {fmt(tgt, 0)} | {fmt(tmib)} | - | - | - | - | - | - | - | - | - | - | - | - | - |")
            continue
        w(f"| {st['stage']} | {st['status']} | {fmt(tgt, 0)} | {fmt(tmib)} | {fmt(d['pub_rate'], 0)} | {fmt(d['pub_mibs'])} | {fmt(d['con_mibs'])} | "
          f"{fmt(d['err_rate'], 1)} ({fmt(d['err_max'], 1)}) | {fmt(d['p_avg'])} | {fmt(d['p50'])} | {fmt(d['p99'])} | {fmt(d['p999'])} | {fmt(d['pmax'])} | "
          f"{fmt(d['e50'], 0)} | {fmt(d['e99'], 0)} | {fmt(d['e999'], 0)} | {fmt(d['backlog_last'], 0)} / {fmt(d['backlog_max'], 0)} |")

    # ---- per-broker server side per stage
    bids = broker_ids(snaps)
    stage_win = {}
    cp_win = {}
    w("\n## Per-broker server side, per stage\n")
    if not bids:
        w("No metric snapshots found (metrics/ empty): server-side tables, skew and cost are unavailable.\n")
    for st in stages:
        start, end = int(st["start"]), int(st["end"])
        per_b = {b: broker_window(snaps, b, start, end) for b in bids}
        per_b = {b: m for b, m in per_b.items() if m}
        stage_win[st["stage"]] = per_b
        cpw = cp_window(snaps, start, end)
        if cpw:
            cp_win[st["stage"]] = cpw
        if not per_b:
            continue
        tot = sum(m.get("bytes", 0) for m in per_b.values())
        w(f"### {st['stage']}\n")
        w("Window = snapshots around the stage (warm-up included, up to one 10 s snapshot interval of slack each side). put = seal to stored in S3; queue = waiting for earlier commits; commit = CommitSegment round trip; ack = seal to answer.\n")
        # DynamoDB columns only for the older one-manicp-per-broker layout; with the single control plane they live in the CP table.
        legacy = any("ddb_wru" in m for m in per_b.values())
        w("| broker | committed MB/s | share | seg/s | put ms | queue ms | commit ms | ack ms | S3 PUT/s | S3 GET/s | S3 thr | S3 err | S3 retry |"
          + (" DDB thr | DDB err | DDB retry | WRU/s | RCU/s |" if legacy else ""))
        w("|---|---|---|---|---|---|---|---|---|---|---|---|---|" + ("---|---|---|---|---|" if legacy else ""))
        for b in sorted(per_b, key=lambda x: (len(x), x)):
            m = per_b[b]
            win = max(1, m.get("win", 1))
            w(f"| {b} | {fmt(m.get('bytes', 0) / win / 1e6, 1)} | {fmt(100 * m.get('bytes', 0) / tot if tot else None, 1)}% | {fmt(m.get('segs', 0) / win, 2)} | "
              f"{fmt(avg_ms(m, 'put'))} | {fmt(avg_ms(m, 'queue'))} | {fmt(avg_ms(m, 'commit'))} | {fmt(avg_ms(m, 'ack'))} | "
              f"{fmt(m.get('s3_put', 0) / win, 1)} | {fmt(m.get('s3_get', 0) / win, 1)} | {fmt(m.get('s3_thr', 0), 0)} | {fmt(m.get('s3_err', 0), 0)} | {fmt(m.get('s3_retry', 0), 0)} |"
              + (f" {fmt(m.get('ddb_thr', 0), 0)} | {fmt(m.get('ddb_err', 0), 0)} | {fmt(m.get('ddb_retry', 0), 0)} | "
                 f"{fmt(m.get('ddb_wru', 0) / win, 0)} | {fmt(m.get('ddb_rcu', 0) / win, 0)} |" if legacy else ""))
        w("")

    # ---- control plane
    w("## Control plane (log store), per stage\n")
    if not cp_win:
        w("No metrics/cp/ snapshots (older run with one manicp per broker, or the CP snapshotter was down): DynamoDB numbers are in the per-broker tables when present.\n")
    else:
        w("From metrics/cp*/ (all control-plane nodes, summed; only the active one moves counters). Deltas between the snapshots around each stage. "
          "slots = LOG slots appended to DynamoDB; lost = slots another writer won; role changes = active/standby flips. "
          "`-` = the snapshots carry no manicp_log_* metrics.\n")
        w("| stage | slots/s | lost slots | role changes | DDB thr | DDB err | DDB retry | WRU/s | RCU/s | DDB call ms |")
        w("|---|---|---|---|---|---|---|---|---|---|")
        for st in stages:
            m = cp_win.get(st["stage"])
            if not m:
                continue
            win = max(1, m.get("win", 1))
            has = "log_slots" in m
            w(f"| {st['stage']} | {fmt(m['log_slots'] / win, 1) if has else '-'} | {fmt(m.get('log_lost'), 0) if has else '-'} | "
              f"{fmt(m.get('log_role_changes'), 0) if has else '-'} | "
              f"{fmt(m['ddb_thr'], 0)} | {fmt(m['ddb_err'], 0)} | {fmt(m['ddb_retry'], 0)} | "
              f"{fmt(m['ddb_wru'] / win, 0)} | {fmt(m['ddb_rcu'] / win, 0)} | {fmt(avg_ms(m, 'ddb_call'))} |")
        w("")

    unmet.extend(roles_section(w, snaps, stages, rundir, digest))

    # ---- skew
    w("## Skew across brokers\n")
    w("Each client stays on the broker it bootstrapped from (Metadata names only the answering broker, decision 29), so the split follows how clients landed, not the partition count. Share = committed bytes of the broker / all brokers.\n")
    w("| stage | brokers with data | max share | min share | max/min | max/mean | note |")
    w("|---|---|---|---|---|---|---|")
    worst_skew = None
    nb_seen = 0
    for st in stages:
        per_b = stage_win.get(st["stage"]) or {}
        by = [m.get("bytes", 0) for m in per_b.values()]
        if len(by) < 2 or sum(by) == 0:
            w(f"| {st['stage']} | {len(by)} | - | - | - | - | needs >= 2 brokers with snapshots |")
            continue
        nb_seen = max(nb_seen, len(by))
        tot = sum(by)
        mx, mn = max(by), min(by)
        ratio = mx / mn if mn > 0 else float("inf")
        worst_skew = ratio if worst_skew is None else max(worst_skew, ratio)
        w(f"| {st['stage']} | {len(by)} | {100 * mx / tot:.1f}% | {100 * mn / tot:.1f}% | {ratio:.2f} | {mx / (tot / len(by)):.2f} | even split would be {100 / len(by):.1f}% each |")
    w("")

    # ---- cost per GiB
    w("## Cost per GiB (measured counts x prices you pass)\n")
    w("Formulas: S3 = PUTs x put_price/1000 + GETs x get_price/1000; DynamoDB = WRU x wru_price/1e6 + RCU x rcu_price/1e6; compute = brokers x hourly x window hours. "
      "GiB = committed segment bytes (batch data + index) in the window. Prices used: "
      f"PUT ${args.s3_put_per_1000}/1000, GET ${args.s3_get_per_1000}/1000, WRU ${args.ddb_wru_per_million}/M, RCU ${args.ddb_rcu_per_million}/M, broker ${args.broker_hourly}/h "
      "(S3/DynamoDB defaults are us-east-1 list prices; check your region; pass --broker-hourly for compute). Warm-up is inside the window, which only dilutes fixed per-window costs.\n")
    w("| stage | GiB written | S3 PUTs | S3 GETs | WRU | RCU | S3 $/GiB | DDB $/GiB | compute $/GiB | total $/GiB | PUT $/month at 1 GiB/s | GET $/month at 1 GiB/s | DDB $/month at 1 GiB/s |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    GIB_PER_MONTH_AT_1GIBS = 86400 * 30.4167
    nbrokers = max(1, len(bids))  # the control-plane host is added separately through --cp-hourly
    cost_rows = 0
    for st in stages:
        per_b = stage_win.get(st["stage"]) or {}
        if not per_b:
            continue
        m = sum_windows(list(per_b.values()) + ([cp_win[st["stage"]]] if st["stage"] in cp_win else []))
        gib = m.get("bytes", 0) / GIB
        if gib <= 0:
            continue
        cost_rows += 1
        puts, gets = m.get("s3_put", 0), m.get("s3_get", 0)
        wru, rcu = m.get("ddb_wru", 0), m.get("ddb_rcu", 0)
        c_put = puts * args.s3_put_per_1000 / 1000
        c_get = gets * args.s3_get_per_1000 / 1000
        c_ddb = wru * args.ddb_wru_per_million / 1e6 + rcu * args.ddb_rcu_per_million / 1e6
        hours = (int(st["end"]) - int(st["start"])) / 3600.0
        c_comp = (nbrokers * args.broker_hourly + (getattr(args, "cp_hourly", 0.0) if cp_win else 0.0) * len(cp_ids(snaps))) * hours if args.broker_hourly else None
        tot = c_put + c_get + c_ddb + (c_comp or 0)
        per_gib = lambda c: (c / gib) if c is not None else None  # noqa: E731
        mo = lambda c: c / gib * GIB_PER_MONTH_AT_1GIBS  # noqa: E731
        w(f"| {st['stage']} | {fmt(gib, 1)} | {fmt(puts, 0)} | {fmt(gets, 0)} | {fmt(wru, 0)} | {fmt(rcu, 0)} | {usd(per_gib(c_put + c_get), 5)} | {usd(per_gib(c_ddb), 5)} | "
          f"{usd(per_gib(c_comp), 5)} | {usd(per_gib(tot), 5)}{'' if c_comp is not None else ' (no compute)'} | {usd(mo(c_put))} | {usd(mo(c_get))} | {usd(mo(c_ddb))} |")
    if cost_rows:
        w(f"\nWarpStream reference (their post, 1 GiB/s, 1-year reserved): compute ${WS['compute']:,.0f}/month, S3 GET ${WS['s3_get']:,.0f}, S3 PUT ${WS['s3_put']:,.0f} "
          "(plus their control-plane fee $7,541, which has no counterpart here). The 'per month at 1 GiB/s' columns scale our measured per-GiB cost to the same rate; they are only meaningful for stages near that rate and with similar compression.\n")

    # ---- checklist
    w("## Apples-to-apples checklist (items NOT met)\n")
    if comp != "lz4":
        unmet.append(f"compression.type={comp} (WarpStream: lz4 with randomBytesRatio 0.5): about 2x more bytes per msg/s on the wire and in S3")
    parts = {d["partitions"] for d in digest.values() if d}
    if parts and parts != {288}:
        unmet.append(f"partitions {sorted(parts)} (WarpStream vanity: 288)")
    if any(d and not d["consumers"] for d in digest.values()):
        unmet.append("produce-only stage(s): no consumers, so no read throughput and no end-to-end latency (WarpStream reports symmetric 1 GiB/s write+read and e2e P99 < 2 s)")
    if run and run.get("brokers") not in (None, 6):
        unmet.append(f"{run.get('brokers')} brokers (WarpStream: 6)")
    full = [s_ for s_ in stages if s_["target_msgs"] and float(s_["target_msgs"]) >= 1_000_000]
    if not full:
        unmet.append("no stage reached the 1,000,000 msg/s target (about 977 MiB/s of 1 KiB messages)")
    for st in full:
        d = digest.get(st["stage"])
        if d and d["pub_rate"] is not None and d["pub_rate"] < 0.95 * float(st["target_msgs"]):
            unmet.append(f"{st['stage']}: achieved {d['pub_rate']:,.0f} msg/s, below 95% of the target")
    for st in stages:
        d = digest.get(st["stage"])
        if d and d["p99"] is not None and d["p99"] > 1000:
            unmet.append(f"{st['stage']}: producer P99 {d['p99']:,.0f} ms (WarpStream: under 1 s)")
        if d and d["e99"] is not None and d["e99"] > 2000:
            unmet.append(f"{st['stage']}: end-to-end P99 {d['e99']:,.0f} ms (WarpStream: under 2 s)")
        if d and (d["err_max"] or 0) > 0:
            unmet.append(f"{st['stage']}: producer errors (max {d['err_max']:.1f} err/s)")
        if st["status"] != "ok":
            unmet.append(f"{st['stage']}: status {st['status']}")
    if worst_skew is not None and worst_skew > 1.5:
        unmet.append(f"per-broker skew max/min {worst_skew:.2f} (WarpStream balances connections across agents; here clients stick to their bootstrap broker)")
    if not bids:
        unmet.append("no server-side metric snapshots, so skew, S3 and DynamoDB counts and cost are missing")
    always = [
        "metadata store: DynamoDB on-demand log (log-based control plane) vs WarpStream's hosted control plane (different latency, cost and limits)",
        "single availability zone vs WarpStream's multi-AZ cost model (no inter-AZ transfer is exercised here, so their inter-AZ comparison against Kafka does not carry over)",
        "durability/availability scope: one AZ, stateless brokers; no zone-failure test (WarpStream scales 6 -> 3 agents mid-run; not done here)",
        "Fetch ignores MinBytes/MaxWait (answers immediately) and consumers use static assign() without groups or offset commits",
        "OMB 'Kafka-manifester' driver patched (fixed topic, no CreateTopics, no shuffle): see deploy/aws/bench/omb/patches",
    ]
    w("Measured from this run:\n")
    for u in unmet or ["none"]:
        w(f"- [ ] {u}")
    w("\nStructural, true for every run of this setup (state them next to any comparison):\n")
    for u in always:
        w(f"- [ ] {u}")
    w("")
    return unmet


# -------------------------------------------------------------------- driver
def load_snapshots(mdir):
    """-> {service: {broker_id: {ts: path}}}. Nested metrics/<bid>/<ts>-svc.txt and flat metrics/<ts>-svc.txt."""
    snaps = collections.defaultdict(lambda: collections.defaultdict(dict))
    if not os.path.isdir(mdir):
        return snaps
    pat = re.compile(r"(\d+)-(broker|manicp)\.txt$")
    for fn in os.listdir(mdir):
        full = os.path.join(mdir, fn)
        if os.path.isdir(full):
            for f2 in os.listdir(full):
                m = pat.match(f2)
                if m:
                    snaps[m.group(2)][fn][int(m.group(1))] = os.path.join(full, f2)
        else:
            m = pat.match(fn)
            if m:
                snaps[m.group(2)]["1"][int(m.group(1))] = full
    return snaps


def bracket(tsmap, start, end):
    ks = sorted(tsmap)
    before = [k for k in ks if k <= start]
    after = [k for k in ks if k >= end]
    if not before or not after:
        return None
    return before[-1], after[0]


def total_op(snap, name, op_prefix):
    """Sum of series `name` whose `op` label starts with op_prefix (case-insensitive)."""
    s = 0.0
    for (n, labels), v in snap.items():
        if n == name and dict(labels).get("op", "").lower().startswith(op_prefix):
            s += v
    return s


def delta_op(a, b, name, op_prefix):
    return max(0.0, total_op(b, name, op_prefix) - total_op(a, name, op_prefix))


ADDITIVE = ("win", "segs", "bytes", "put_s", "put_n", "queue_s", "queue_n", "commit_s", "commit_n", "ack_s", "ack_n",
            "s3_thr", "s3_err", "s3_retry", "s3_put", "s3_get", "s3_put_bytes",
            "ddb_thr", "ddb_err", "ddb_retry", "ddb_wru", "ddb_rcu", "ddb_call_s", "ddb_call_n",
            "log_slots", "log_lost", "log_role_changes")


def broker_window(snaps, bid, start, end):
    """Additive raw deltas for one broker between the snapshots bracketing [start, end] (None if no data)."""
    m = {}
    bb = bracket(snaps["broker"].get(bid, {}), start, end) if snaps["broker"].get(bid) else None
    if bb:
        a, b = parse_prom(snaps["broker"][bid][bb[0]]), parse_prom(snaps["broker"][bid][bb[1]])
        m["win"] = max(1, bb[1] - bb[0])
        m["segs"] = delta(a, b, "manibroker_segments_committed_total")
        m["bytes"] = delta(a, b, "manibroker_segment_committed_bytes_total")
        for key, base in (("put", "manibroker_segment_put_seconds"), ("queue", "manibroker_segment_queue_wait_seconds"),
                          ("commit", "manibroker_segment_commit_seconds"), ("ack", "manibroker_segment_seal_to_ack_seconds")):
            m[key + "_s"] = delta(a, b, base + "_sum")
            m[key + "_n"] = delta(a, b, base + "_count")
        m["s3_thr"] = delta(a, b, "manibroker_s3_throttles_total")
        m["s3_err"] = delta(a, b, "manibroker_s3_api_errors_total") + delta(a, b, "manibroker_segments_failed_total")
        m["s3_retry"] = delta(a, b, "manibroker_segment_put_attempts_total") - m["segs"]
        m["s3_put"] = delta_op(a, b, "manibroker_s3_attempt_seconds_count", "put")
        m["s3_get"] = delta_op(a, b, "manibroker_s3_attempt_seconds_count", "get")
        m["s3_put_bytes"] = delta_op(a, b, "manibroker_s3_request_bytes_sum", "put")
    cc = bracket(snaps["manicp"].get(bid, {}), start, end) if snaps["manicp"].get(bid) else None
    if cc:
        a, b = parse_prom(snaps["manicp"][bid][cc[0]]), parse_prom(snaps["manicp"][bid][cc[1]])
        m["ddb_thr"] = delta(a, b, "manicp_dynamo_throttles_total")
        m["ddb_err"] = delta(a, b, "manicp_dynamo_api_errors_total")
        m["ddb_retry"] = delta(a, b, "manicp_dynamo_retries_total")
        m["ddb_wru"] = delta(a, b, "manicp_dynamo_consumed_capacity_units_total", kind="write")
        m["ddb_rcu"] = delta(a, b, "manicp_dynamo_consumed_capacity_units_total", kind="read")
        m["ddb_call_s"] = delta(a, b, "manicp_dynamodb_call_seconds_sum")
        m["ddb_call_n"] = delta(a, b, "manicp_dynamodb_call_seconds_count")
    return m or None


CP = "cp"  # metrics/cp/: the first control-plane manicp; standbys of the log store are metrics/cp-1/, cp-2/, ...
CP_DIR = re.compile(r"cp(-\d+)?$")


def cp_ids(snaps):
    """Control-plane node dirs (cp, cp-1, ...) in rank order."""
    ids = [k for k in snaps["manicp"] if CP_DIR.match(k)]
    return sorted(ids, key=lambda x: int(x[3:]) if len(x) > 2 else -1)


def broker_ids(snaps):
    """Broker ids (manibroker snapshots, or legacy per-broker manicp dirs); the cp dirs are not brokers."""
    return sorted((set(snaps["broker"]) | set(snaps["manicp"])) - set(cp_ids(snaps)), key=lambda x: (len(x), x))


def cp_node_window(tm, start, end):
    """Deltas of ONE control-plane node's snapshots (tm = {ts: path}) around [start, end]; None without a bracket.
    manicp_log_* keys appear only when the snapshots carry the log-store metrics."""
    cc = bracket(tm, start, end) if tm else None
    if not cc:
        return None
    a, b = parse_prom(tm[cc[0]]), parse_prom(tm[cc[1]])
    m = {"win": max(1, cc[1] - cc[0])}
    m["ddb_thr"] = delta(a, b, "manicp_dynamo_throttles_total")
    m["ddb_err"] = delta(a, b, "manicp_dynamo_api_errors_total")
    m["ddb_retry"] = delta(a, b, "manicp_dynamo_retries_total")
    m["ddb_wru"] = delta(a, b, "manicp_dynamo_consumed_capacity_units_total", kind="write")
    m["ddb_rcu"] = delta(a, b, "manicp_dynamo_consumed_capacity_units_total", kind="read")
    m["ddb_call_s"] = delta(a, b, "manicp_dynamodb_call_seconds_sum")
    m["ddb_call_n"] = delta(a, b, "manicp_dynamodb_call_seconds_count")
    if any(n.startswith("manicp_log_") for (n, _l) in b):  # log store: slot rate, lost slots, role flips
        m["log_slots"] = delta(a, b, "manicp_log_slots_total")
        m["log_lost"] = delta(a, b, "manicp_log_lost_slots_total")
        m["log_role_changes"] = delta(a, b, "manicp_log_role_changes_total")
    return m


def cp_window(snaps, start, end):
    """Control-plane deltas summed over all cp nodes (only the active one moves counters; a restarted node's counters
    reset, so its delta in the stage of the restart is a lower bound). None when no node has a bracket."""
    ws = [w for w in (cp_node_window(snaps["manicp"].get(n), start, end) for n in cp_ids(snaps)) if w]
    return sum_windows(ws) if ws else None


def sum_windows(ws):
    """Sum a list of broker_window dicts (keys missing in some are skipped); win = max."""
    out = {}
    for w in ws:
        for k, v in w.items():
            out[k] = max(out.get(k, 0), v) if k == "win" else out.get(k, 0) + v
    return out


def avg_ms(m, key):
    n = m.get(key + "_n", 0)
    return m.get(key + "_s", 0) / n * 1000.0 if n > 0 else None


def fmt(x, nd=1, dash="-"):
    return dash if x is None else f"{x:,.{nd}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--out")
    ap.add_argument("--broker-hourly", type=float, default=0.0, help="USD per broker instance hour (compute cost); 0 omits compute")
    ap.add_argument("--cp-hourly", type=float, default=0.0, help="USD per hour of the single control-plane host (m6in.xlarge); added to compute when metrics/cp/ exists")
    ap.add_argument("--s3-put-per-1000", type=float, default=0.005)
    ap.add_argument("--s3-get-per-1000", type=float, default=0.0004)
    ap.add_argument("--ddb-wru-per-million", type=float, default=1.25, help="on-demand write request unit price")
    ap.add_argument("--ddb-rcu-per-million", type=float, default=0.25, help="on-demand read request unit price")
    args = ap.parse_args()
    ldir = os.path.join(args.dir, "loadgen")
    rows = []
    cells_path = os.path.join(ldir, "cells.tsv")
    if os.path.exists(cells_path):
        rows = read_tsv(cells_path)
    elif not os.path.exists(os.path.join(args.dir, "omb", "stages.tsv")):
        sys.exit(f"{args.dir}: neither loadgen/cells.tsv nor omb/stages.tsv found")
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
        ws = [x for x in (broker_window(snaps, bid, start, end) for bid in broker_ids(snaps)) if x]
        cpw = cp_window(snaps, start, end)
        if cpw:
            ws.append(cpw)
        if ws:
            m = sum_windows(ws)
            win = max(1, m.get("win", 1))
            if "segs" in m:
                d["win"] = win
                d["seg_rate"] = m["segs"] / win
                d["bytes_rate"] = m["bytes"] / win
                d["put"], d["queue"], d["commit"], d["ack"] = (avg_ms(m, k) for k in ("put", "queue", "commit", "ack"))
                d["s3_thr"], d["s3_err"], d["s3_retry"] = m["s3_thr"], m["s3_err"], m["s3_retry"]
            if "ddb_wru" in m:
                d["ddb_thr"], d["ddb_err"] = m["ddb_thr"], m["ddb_err"]
                d["ddb_retry"], d["ddb_wru"], d["ddb_call"] = m["ddb_retry"], m["ddb_wru"], avg_ms(m, "ddb_call")
        else:
            d["note"] += " no broker snapshots around the cell"
        per.append(d)

    if os.path.exists(os.path.join(args.dir, "omb", "stages.tsv")):
        omb_report(w, args, args.dir, snaps)
    if not rows:
        text = "\n".join(out) + "\n"
        if args.out:
            with open(args.out, "w") as f:
                f.write(text)
        sys.stdout.write(text)
        return
    w("## Per cell and repetition\n")
    w("Client side (from the load tool): throughput summed over clients; avg latency record-weighted; p50 record-weighted mean and p99 max over clients, from the tool's final summary line (n/a when the tool did not print one).\n")
    w("| cell | rep | clients | bytes | records/s | MB/s | avg ms | p50 ms | p99 ms | notes |")
    w("|---|---|---|---|---|---|---|---|---|---|")
    for d in per:
        t = d["t"]
        w(f"| {d['cell']} | {d['rep']} | {d['clients']} | {d['size']} | {fmt(t['rps'], 0)} | {fmt(t['mbps'], 2)} | {fmt(t['avg'])} | {fmt(t['p50'])} | {fmt(t['p99'])} | {d['note'].strip()} |")

    w("\n## Broker and control plane, same cells (deltas between snapshots)\n")
    w("Averages are exact (delta sum / delta count). `seg/s` and `MB/s` use the snapshot window. put = seal to stored in S3; queue = waiting for earlier commits; commit = CommitSegment round trip; ack = seal to answer.\n")
    w("| cell | rep | seg/s | committed MB/s | put ms | queue ms | commit ms | ack ms | S3 throttles | S3 errors | S3 retries | DDB throttles | DDB errors | DDB retries | DDB WRU | DDB call ms |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for d in per:
        g = d.get
        w(f"| {d['cell']} | {d['rep']} | {fmt(g('seg_rate'), 2)} | {fmt(g('bytes_rate') / 1e6 if g('bytes_rate') is not None else None, 2)} | {fmt(g('put'))} | {fmt(g('queue'))} | {fmt(g('commit'))} | {fmt(g('ack'))} | {fmt(g('s3_thr'), 0)} | {fmt(g('s3_err'), 0)} | {fmt(g('s3_retry'), 0)} | {fmt(g('ddb_thr'), 0)} | {fmt(g('ddb_err'), 0)} | {fmt(g('ddb_retry'), 0)} | {fmt(g('ddb_wru'), 0)} | {fmt(g('ddb_call'))} |")

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
    w("\nCloudWatch-side AWS numbers are not in this report; use the Grafana AWS dashboard for the same window.")
    text = "\n".join(out) + "\n"
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    sys.stdout.write(text)


if __name__ == "__main__":
    main()
