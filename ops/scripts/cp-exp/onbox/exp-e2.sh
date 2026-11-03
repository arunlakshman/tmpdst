#!/bin/bash
# E2 zombie writer. See docs/cp-aws-experiments.md E2.
# The active node (cp0) is frozen (SIGSTOP) longer than takeover-after, so the standby
# (cp1) takes over. Then cp0 is thawed (SIGCONT) and the load keeps running.
# Safety question: can the old writer damage the log? Also: do both replicas end up equal?
set -uo pipefail
. "$(dirname "$0")/lib.sh"

R="$CPV_ROOT/results"
FREEZE_SECONDS=$(( TAKEOVER + 10 ))
FLAGS=(-checkpoint-every 30s -window-slots 1000000000 "${NODE_FLAGS[@]}")
ACKED="$R/e2-acked.jsonl"

"$CPV_ROOT/scripts/reset.sh" || { echo "RESET FAILED: the experiment did not start"; exit 2; }
: >"$ACKED"

conflicts() { # conflicts METRICS_PORT : failed conditional writes (HTTP 400) seen by a node
  metric_sum "$1" 'dynamo_attempt_seconds_count{op="PutItem",result="http_400"'
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

# The log-versus-acks check (mdscpinspect) was removed with the tool.
echo "   failed conditional writes: cp0 5 s after thaw=$CONFLICTS_AFTER_THAW, cp1 total=$CONFLICTS_CP1"

failed=$(json_field "$SUMMARY" 'd["failed"]'); acked=$(json_field "$SUMMARY" 'd["acked"]')
stall=$(json_field "$SUMMARY" 'd["max_stall_ms"]'); retries=$(json_field "$SUMMARY" 'd["retries"]')
errors=$(count_in_logs '"level":"ERROR"')
busy=$(decisions | grep -c 'control_io_pending' || true)

expect "load: failed = 0" "acked=$acked failed=$failed retries=$retries" [ "$failed" = 0 ]
criterion "longest stall <= takeover-after + 5 s" "$(le "$stall" $(( (TAKEOVER + 5) * 1000 )))" "max_stall=${stall}ms"
expect "cp0 alone: live end offsets equal records acked" "rc=$ENDS0" [ "$ENDS0" = 0 ]
expect "cp1 alone: live end offsets equal records acked" "rc=$ENDS1" [ "$ENDS1" = 0 ]
expect "A6: no ERROR line in node logs" "count=$errors" [ "$errors" = 0 ]
expect "A7: no checkpoint skipped for control_io_pending" "count=$busy" [ "$busy" = 0 ]
verdict E2
rc=$?
EXTRA_FILES="$VERDICT_FILE $R/e2-load.out $R/e2-ends-cp0.txt $R/e2-ends-cp1.txt" collect.sh e2 >/dev/null 2>&1
echo "results uploaded under s3://$BUCKET/results/ (prefix e2-)"
echo "E2 DONE rc=$rc"
exit $rc
