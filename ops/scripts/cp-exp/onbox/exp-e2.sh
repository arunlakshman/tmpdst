#!/bin/bash
# E2 zombie writer. See docs/cp-aws-experiments.md E2.
# The active node (cp0) is frozen (SIGSTOP) longer than takeover-after, so the standby
# (cp1) takes over. Then cp0 is thawed (SIGCONT) and the load keeps running.
# Safety question: can the old writer damage the log? Also: do both replicas end up equal?
set -uo pipefail
. "$(dirname "$0")/lib.sh"

R="$CPV_ROOT/results"
ADDR0=18080; MET0=19101
ADDR1=18081; MET1=19102
TAKEOVER=5
FREEZE_SECONDS=$(( TAKEOVER + 10 ))
FLAGS=(-checkpoint-every 30s -checkpoint-min-slots 200 -window-slots 1000000000 -takeover-after ${TAKEOVER}s -heartbeat 1s)
ACKED="$R/e2-acked.jsonl"

"$CPV_ROOT/scripts/reset.sh" || { echo "RESET FAILED: the experiment did not start"; exit 2; }
: >"$ACKED"

conflicts() { # conflicts METRICS_PORT : failed conditional writes (HTTP 400) seen by a node
  curl -s -m 3 "http://127.0.0.1:$1/metrics" | grep 'dynamo_attempt_seconds_count{op="PutItem",result="http_400"' | awk '{s+=$2} END {print s+0}'
}

echo "== start cp0 (rank 0) and cp1 (rank 1)"
node.sh start cp0 0 $ADDR0 $MET0 "${FLAGS[@]}"
node.sh start cp1 1 $ADDR1 $MET1 "${FLAGS[@]}"
node.sh wait-ready cp0 60 >/dev/null; node.sh wait-ready cp1 60 >/dev/null

echo "== load: 120 s against both nodes"
mdscpverify -mode load -cp 127.0.0.1:$ADDR0,127.0.0.1:$ADDR1 -duration 120s -workers 4 -run-id e2 \
  -acked-out "$ACKED" >"$R/e2-load.out" 2>"$R/e2-load.err" &
LOAD=$!

sleep 20
echo "== freeze cp0 for ${FREEZE_SECONDS} s"
node.sh freeze cp0
node.sh status | grep cp0
sleep "$FREEZE_SECONDS"
# do not read cp0's metrics here: a frozen process never answers
echo "== thaw cp0"
node.sh thaw cp0
THAW_AT=$(now_ms)
sleep 5
CONFLICTS_AFTER_THAW=$(conflicts $MET0)
CONFLICTS_CP1=$(conflicts $MET1)

wait $LOAD
SUMMARY=$(last_summary "$R/e2-load.out")
echo "== settle 10 s, then ask each node alone for the end offsets"
sleep 10
mdscpverify -mode ends -cp 127.0.0.1:$ADDR0 -acked "$ACKED" >"$R/e2-ends-cp0.txt" 2>&1; ENDS0=$?
mdscpverify -mode ends -cp 127.0.0.1:$ADDR1 -acked "$ACKED" >"$R/e2-ends-cp1.txt" 2>&1; ENDS1=$?
echo "   cp0 alone: $(tail -2 "$R/e2-ends-cp0.txt" | head -1)"
echo "   cp1 alone: $(tail -2 "$R/e2-ends-cp1.txt" | head -1)"

node.sh stop cp0; node.sh stop cp1

echo "== offline checks on the log"
mdscpinspect -check -region "$REGION" -table "$TABLE" -bucket "$BUCKET" -acked "$ACKED" >"$R/e2-inspect.txt" 2>&1
mdscpinspect -v -region "$REGION" -table "$TABLE" >"$R/e2-slots.txt" 2>&1
RUNS=$(python3 - "$R/e2-slots.txt" <<'PY'
import re, sys
runs, last, start, prev, n = [], None, 0, 0, 0
for line in open(sys.argv[1]):
    m = re.match(r"\s+slot (\d+)\s.*writer=(\S+)", line)
    if not m:
        continue
    slot, writer = int(m.group(1)), m.group(2)
    if writer != last:
        if last is not None:
            runs.append("%s[%d..%d]x%d" % (last, start, prev, n))
        last, start, n = writer, slot, 0
    n += 1
    prev = slot
runs.append("%s[%d..%d]x%d" % (last, start, prev, n))
print(" ".join(runs))
PY
)
echo "   writer runs: $RUNS"
N_CHANGES=$(( $(echo "$RUNS" | wc -w) - 1 ))
echo "   failed conditional writes: cp0 5 s after thaw=$CONFLICTS_AFTER_THAW, cp1 total=$CONFLICTS_CP1"

failed=$(json_field "$SUMMARY" 'd["failed"]'); acked=$(json_field "$SUMMARY" 'd["acked"]')
stall=$(json_field "$SUMMARY" 'd["max_stall_ms"]'); retries=$(json_field "$SUMMARY" 'd["retries"]')
errors=$(count_in_logs '"level":"ERROR"')
busy=$(decisions | grep -c 'control_io_pending' || true)
check_failed=$(grep -c '^FAIL' "$R/e2-inspect.txt" || true)
skips=$(grep -c '^SKIP' "$R/e2-inspect.txt" || true)

criterion "load: failed = 0" "$([ "$failed" = 0 ] && echo 1 || echo 0)" "acked=$acked failed=$failed retries=$retries"
criterion "longest stall <= takeover-after + 5 s" "$(le "$stall" $(( (TAKEOVER + 5) * 1000 )))" "max_stall=${stall}ms"
criterion "the standby took over (cp1 wrote slots)" "$([ -n "$(grep -m1 'writer=cp1' "$R/e2-slots.txt")" ] && echo 1 || echo 0)" "runs: $RUNS"
criterion "writer changes <= 4 (the E1 failback pattern, issue 24)" "$([ "$N_CHANGES" -le 4 ] && echo 1 || echo 0)" "changes=$N_CHANGES"
criterion "cp0 alone: live end offsets equal records acked" "$([ "$ENDS0" = 0 ] && echo 1 || echo 0)" "rc=$ENDS0"
criterion "cp1 alone: live end offsets equal records acked" "$([ "$ENDS1" = 0 ] && echo 1 || echo 0)" "rc=$ENDS1"
criterion "A1 to A5: mdscpinspect -check -acked passes" "$([ "$check_failed" = 0 ] && echo 1 || echo 0)" "failed_lines=$check_failed"
criterion "no check was skipped" "$([ "$skips" = 0 ] && echo 1 || echo 0)" "skipped=$skips"
criterion "A6: no ERROR line in node logs" "$([ "$errors" = 0 ] && echo 1 || echo 0)" "count=$errors"
criterion "A7: no checkpoint skipped for control_io_pending" "$([ "$busy" = 0 ] && echo 1 || echo 0)" "count=$busy"
verdict E2
rc=$?
ACKED_FILE="$ACKED" EXTRA_FILES="$VERDICT_FILE $R/e2-load.out $R/e2-ends-cp0.txt $R/e2-ends-cp1.txt $R/e2-inspect.txt $R/e2-slots.txt" collect.sh e2 >/dev/null 2>&1
echo "results uploaded under s3://$BUCKET/results/ (prefix e2-)"
echo "E2 DONE rc=$rc"
exit $rc
