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
  printf '%d %d\n' "$(metric_sum "$1" 'dynamo_attempt_seconds_count{op="PutItem"')" \
    "$(metric_sum "$1" 'dynamo_attempt_seconds_count{op="PutItem",result="http_400"')"
}

echo "== part A: one oversized commit on the normal table"
node.sh start cp0 0 $ADDR0 $MET0 -checkpoint-every 30s -checkpoint-min-slots 200 -window-slots 1000000000
node.sh wait-ready cp0 60 >/dev/null
mdscpverify -mode big -cp 127.0.0.1:$ADDR0 -big-batches 40000 >"$R/e3-big.out" 2>&1; BIG_RC=$?
cat "$R/e3-big.out"
mdscpverify -mode load -cp 127.0.0.1:$ADDR0 -duration 10s -workers 4 -run-id e3a -acked-out "$ACKED_A" >"$R/e3-load-a.out" 2>/dev/null
SUMMARY_A=$(last_summary "$R/e3-load-a.out")
node.sh stop cp0
# The log-versus-acks check (mdscpinspect) was removed with the tool.

echo "== part B: throttle table with 1 write unit, load 50 commits per second for 60 s"
# no checkpoint bucket here: the node would share the S3 folder with part A
node.sh start cp1 0 $ADDR1 $MET1 -table "$THROTTLE_TABLE" -checkpoint-bucket "" -window-slots 1000000000
node.sh wait-ready cp1 60 >/dev/null
SAMPLES="$R/e3-samples.txt"; : >"$SAMPLES"
( for _ in $(seq 1 14); do echo "$(now_ms) $(attempts $MET1)" >>"$SAMPLES"; sleep 5; done ) &
SAMPLER=$!
mdscpverify -mode load -cp 127.0.0.1:$ADDR1 -duration 60s -rate 50 -workers 4 -run-id e3b -acked-out "$ACKED_B" \
  >"$R/e3-load-b.out" 2>"$R/e3-load-b.err"
SUMMARY_B=$(last_summary "$R/e3-load-b.out")
wait $SAMPLER
echo "== load stopped; waiting for the node to catch up (applied = tail), up to 30 s"
CAUGHT_UP=0
for _ in $(seq 1 30); do
  line=$(mdscpverify -mode ready -cp 127.0.0.1:$ADDR1 2>/dev/null)
  applied=$(echo "$line" | sed -n 's/.*applied_slot=\([0-9]*\).*/\1/p'); tail=$(echo "$line" | sed -n 's/.*log_tail=\([0-9]*\).*/\1/p')
  if [ -n "$applied" ] && [ "$applied" = "$tail" ]; then CAUGHT_UP=1; echo "   caught up: applied=$applied tail=$tail"; break; fi
  sleep 1
done
read -r TOTAL_ATTEMPTS THROTTLED < <(attempts $MET1)
node.sh stop cp1
# The log-versus-acks check (mdscpinspect) was removed with the tool.

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
errors_a=$(grep -c '"level":"ERROR"' "$CPV_ROOT/logs/cp0.log" || true)
errors_b=$(grep -c '"level":"ERROR"' "$CPV_ROOT/logs/cp1.log" || true)

expect "A: oversized commit refused with ErrTooLarge, normal commit still works" "$(grep -m1 'refused' "$R/e3-big.out")" [ "$BIG_RC" = 0 ]
load_a_ok=0
if [ "$failed_a" = 0 ] && [ "$acked_a" -gt 0 ]; then load_a_ok=1; fi
expect "A: 10 s load after it: failed = 0" "acked=$acked_a failed=$failed_a" [ "$load_a_ok" = 1 ]
expect "A6: no ERROR line in the part A node log" "count=$errors_a" [ "$errors_a" = 0 ]
expect "B: DynamoDB really throttled (HTTP 400 seen)" "throttled=$THROTTLED of $TOTAL_ATTEMPTS attempts" [ "${THROTTLED:-0}" -gt 0 ]
criterion "B: no tight loop (PutItem attempts <= 200/s in every 5 s window)" "$(le "$MAX_RATE" 200)" "highest=${MAX_RATE}/s"
expect "B: load failed = 0 on the throttle table" "acked=$acked_b failed=$failed_b" [ "$failed_b" = 0 ]
expect "B: node caught up (applied = tail) within 30 s of the end" "caught_up=$CAUGHT_UP" [ "$CAUGHT_UP" = 1 ]
expect "B: no ERROR line in the part B node log" "count=$errors_b" [ "$errors_b" = 0 ]
verdict E3
rc=$?
EXTRA_FILES="$VERDICT_FILE $R/e3-big.out $R/e3-load-a.out $R/e3-load-b.out $R/e3-samples.txt $ACKED_B" collect.sh e3 >/dev/null 2>&1
echo "results uploaded under s3://$BUCKET/results/ (prefix e3-)"
echo "E3 DONE rc=$rc"
exit $rc
