#!/bin/bash
# E3 limits and errors. See docs/cp-aws-experiments.md E3.
#   Part A (normal table): one commit too big for a log entry must get ErrTooLarge,
#          and the log must be fine afterwards.
#   Part B (table with 1 read / 1 write unit): DynamoDB throttles. The node must not spin,
#          must not lose acked data, and must catch up when the load stops.
set -uo pipefail
. "$(dirname "$0")/lib.sh"

R="$CPV_ROOT/results"
THROTTLE_TABLE="${TABLE_PREFIX}throttle_cp"
ACKED_A="$R/e3-acked-a.jsonl"; ACKED_B="$R/e3-acked-b.jsonl"

"$CPV_ROOT/scripts/reset.sh" || { echo "RESET FAILED: the experiment did not start"; exit 2; }
"$CPV_ROOT/bin/mdscpreset" -region "$REGION" -table "$THROTTLE_TABLE" || { echo "RESET OF THE THROTTLE TABLE FAILED"; exit 2; }
: >"$ACKED_A"; : >"$ACKED_B"

attempts() { # attempts PORT : PutItem attempts so far, and how many were throttled (HTTP 400)
  curl -s -m 3 "http://127.0.0.1:$1/metrics" | awk '
    /dynamo_attempt_seconds_count\{op="PutItem"/ { all += $2; if ($0 ~ /result="http_400"/) bad += $2 }
    END { printf "%d %d\n", all, bad }'
}

echo "== part A: one oversized commit on the normal table"
node.sh start cp0 0 18080 19101 -checkpoint-every 30s -checkpoint-min-slots 200 -window-slots 1000000000
node.sh wait-ready cp0 60 >/dev/null
mdscpverify -mode big -cp 127.0.0.1:18080 -big-batches 40000 >"$R/e3-big.out" 2>&1; BIG_RC=$?
cat "$R/e3-big.out"
mdscpverify -mode load -cp 127.0.0.1:18080 -duration 10s -workers 4 -run-id e3a -acked-out "$ACKED_A" >"$R/e3-load-a.out" 2>/dev/null
SUMMARY_A=$(last_summary "$R/e3-load-a.out")
node.sh stop cp0
# no -bucket: part A runs only about 10 s, so no checkpoint exists yet, and the check would fail on that.
mdscpinspect -check -region "$REGION" -table "$TABLE" -acked "$ACKED_A" >"$R/e3-inspect-a.txt" 2>&1

echo "== part B: throttle table with 1 write unit, load 50 commits per second for 60 s"
# no checkpoint bucket here: the node would share the S3 folder with part A
node.sh start cp1 0 18081 19102 -table "$THROTTLE_TABLE" -checkpoint-bucket "" -window-slots 1000000000
node.sh wait-ready cp1 60 >/dev/null
SAMPLES="$R/e3-samples.txt"; : >"$SAMPLES"
( for _ in $(seq 1 14); do echo "$(now_ms) $(attempts 19102)" >>"$SAMPLES"; sleep 5; done ) &
SAMPLER=$!
mdscpverify -mode load -cp 127.0.0.1:18081 -duration 60s -rate 50 -workers 4 -run-id e3b -acked-out "$ACKED_B" \
  >"$R/e3-load-b.out" 2>"$R/e3-load-b.err"
SUMMARY_B=$(last_summary "$R/e3-load-b.out")
wait $SAMPLER
echo "== load stopped; waiting for the node to catch up (applied = tail), up to 30 s"
CAUGHT_UP=0
for _ in $(seq 1 30); do
  line=$(mdscpverify -mode ready -cp 127.0.0.1:18081 2>/dev/null)
  applied=$(echo "$line" | sed -n 's/.*applied_slot=\([0-9]*\).*/\1/p'); tail=$(echo "$line" | sed -n 's/.*log_tail=\([0-9]*\).*/\1/p')
  if [ -n "$applied" ] && [ "$applied" = "$tail" ]; then CAUGHT_UP=1; echo "   caught up: applied=$applied tail=$tail"; break; fi
  sleep 1
done
read -r TOTAL_ATTEMPTS THROTTLED < <(attempts 19102)
node.sh stop cp1
mdscpinspect -check -region "$REGION" -table "$THROTTLE_TABLE" -acked "$ACKED_B" >"$R/e3-inspect-b.txt" 2>&1

# highest PutItem attempts per second in any 5 s sample window
MAX_RATE=$(python3 - "$SAMPLES" <<'PY'
import sys
rows = [tuple(map(int, l.split())) for l in open(sys.argv[1]) if len(l.split()) == 3]
best = 0.0
for (t0, a0, _), (t1, a1, _) in zip(rows, rows[1:]):
    if t1 > t0:
        best = max(best, (a1 - a0) * 1000.0 / (t1 - t0))
print(round(best, 1))
PY
)
echo "   PutItem attempts in total: $TOTAL_ATTEMPTS, throttled (HTTP 400): $THROTTLED, highest rate in a 5 s window: ${MAX_RATE}/s"

acked_a=$(json_field "$SUMMARY_A" 'd["acked"]'); failed_a=$(json_field "$SUMMARY_A" 'd["failed"]')
acked_b=$(json_field "$SUMMARY_B" 'd["acked"]'); failed_b=$(json_field "$SUMMARY_B" 'd["failed"]')
fail_a=$(grep -c '^FAIL' "$R/e3-inspect-a.txt" || true); skip_a=$(grep -c '^SKIP' "$R/e3-inspect-a.txt" || true)
fail_b=$(grep -c '^FAIL' "$R/e3-inspect-b.txt" || true); skip_b=$(grep -c '^SKIP' "$R/e3-inspect-b.txt" || true)
errors_a=$(grep -c '"level":"ERROR"' "$CPV_ROOT/logs/cp0.log" || true)
errors_b=$(grep -c '"level":"ERROR"' "$CPV_ROOT/logs/cp1.log" || true)

criterion "A: oversized commit refused with ErrTooLarge, normal commit still works" "$([ "$BIG_RC" = 0 ] && echo 1 || echo 0)" "$(grep -m1 'refused' "$R/e3-big.out")"
criterion "A: 10 s load after it: failed = 0" "$([ "$failed_a" = 0 ] && [ "$acked_a" -gt 0 ] && echo 1 || echo 0)" "acked=$acked_a failed=$failed_a"
criterion "A: log checks pass, no slot over 350 KiB (A1 to A5)" "$([ "$fail_a" = 0 ] && echo 1 || echo 0)" "failed_lines=$fail_a"
criterion "A: no check was skipped" "$([ "$skip_a" = 0 ] && echo 1 || echo 0)" "skipped=$skip_a"
criterion "A6: no ERROR line in the part A node log" "$([ "$errors_a" = 0 ] && echo 1 || echo 0)" "count=$errors_a"
criterion "B: DynamoDB really throttled (HTTP 400 seen)" "$([ "${THROTTLED:-0}" -gt 0 ] && echo 1 || echo 0)" "throttled=$THROTTLED of $TOTAL_ATTEMPTS attempts"
criterion "B: no tight loop (PutItem attempts <= 200/s in every 5 s window)" "$(le "$MAX_RATE" 200)" "highest=${MAX_RATE}/s"
criterion "B: no acked commit lost, offsets right (A1 to A5 on the throttle table)" "$([ "$fail_b" = 0 ] && echo 1 || echo 0)" "acked=$acked_b failed(client gave up)=$failed_b failed_lines=$fail_b"
criterion "B: no check was skipped" "$([ "$skip_b" = 0 ] && echo 1 || echo 0)" "skipped=$skip_b"
criterion "B: node caught up (applied = tail) within 30 s of the end" "$([ "$CAUGHT_UP" = 1 ] && echo 1 || echo 0)" "caught_up=$CAUGHT_UP"
criterion "B: no ERROR line in the part B node log" "$([ "$errors_b" = 0 ] && echo 1 || echo 0)" "count=$errors_b"
verdict E3
rc=$?
ACKED_FILE="$ACKED_A" EXTRA_FILES="$VERDICT_FILE $R/e3-big.out $R/e3-load-a.out $R/e3-load-b.out $R/e3-inspect-a.txt $R/e3-inspect-b.txt $R/e3-samples.txt $ACKED_B" collect.sh e3 >/dev/null 2>&1
echo "results uploaded under s3://$BUCKET/results/ (prefix e3-)"
echo "E3 DONE rc=$rc"
exit $rc
