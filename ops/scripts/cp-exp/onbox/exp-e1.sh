#!/bin/bash
# E1 failover and recovery. See docs/cp-aws-experiments.md E1.
#   A: kill -9 the active node (cp0) under load -> the standby (cp1) takes over
#   B: wipe cp0's disk and start it again -> it restores from the checkpoint and fails back
#   C: kill both, wipe both, start both -> full outage recovery, then a short load
set -uo pipefail
. "$(dirname "$0")/lib.sh"

R="$CPV_ROOT/results"
FLAGS=(-checkpoint-every 30s -window-slots 1000000000 "${NODE_FLAGS[@]}")
ACKED_A="$R/e1-acked-a.jsonl"; ACKED_B="$R/e1-acked-b.jsonl"; ACKED_ALL="$R/e1-acked-all.jsonl"

"$CPV_ROOT/scripts/reset.sh" || { echo "RESET FAILED: the experiment did not start"; exit 2; }
: >"$ACKED_A"; : >"$ACKED_B"

# wait_ms NAME : wait until the node answers, print milliseconds since the call
wait_ms() {
  local t0; t0=$(now_ms)
  node.sh wait-ready "$1" 180 >/dev/null 2>&1 || echo "NOT-READY" >&2
  echo $(( $(now_ms) - t0 ))
}
query_count() { metric_sum "$1" 'dynamo_attempt_seconds_count{op="Query"'; }
log_tail() { mdscpverify -mode ready -cp "127.0.0.1:$1" 2>/dev/null | sed -n 's/.*log_tail=\([0-9]*\).*/\1/p'; }

echo "== start cp0 (rank 0) and cp1 (rank 1)"
node.sh start cp0 0 $ADDR0 $MET0 "${FLAGS[@]}"
node.sh start cp1 1 $ADDR1 $MET1 "${FLAGS[@]}"
node.sh wait-ready cp0 60 >/dev/null; node.sh wait-ready cp1 60 >/dev/null

echo "== load A: 150 s against both nodes"
mdscpverify -mode load -cp 127.0.0.1:$ADDR0,127.0.0.1:$ADDR1 -duration 150s -workers 4 -run-id e1a \
  -acked-out "$ACKED_A" >"$R/e1-load-a.out" 2>"$R/e1-load-a.err" &
LOAD_A=$!

sleep 30
echo "== phase A at +30 s: kill -9 cp0"
KILL_AT=$(now_ms)
node.sh kill9 cp0
sleep 30
echo "== phase B at +60 s: wipe cp0, start it again"
node.sh wipe cp0
node.sh start cp0 0 $ADDR0 $MET0 "${FLAGS[@]}"
READY_B_MS=$(wait_ms cp0)
QUERIES_B=$(query_count $MET0)
TAIL_B=$(log_tail $ADDR0)
echo "   cp0 ready after ${READY_B_MS} ms; Query calls so far: $QUERIES_B; log tail: $TAIL_B"

wait $LOAD_A
SUMMARY_A=$(last_summary "$R/e1-load-a.out")
sleep 10
TAIL_AFTER_A=$(log_tail $ADDR1)

echo "== phase C: full outage (kill -9 both, wipe both, start both)"
node.sh kill9 cp0; node.sh kill9 cp1
node.sh wipe cp0; node.sh wipe cp1
node.sh start cp0 0 $ADDR0 $MET0 "${FLAGS[@]}"
node.sh start cp1 1 $ADDR1 $MET1 "${FLAGS[@]}"
READY_C0_MS=$(wait_ms cp0)
READY_C1_MS=$(wait_ms cp1)
echo "   after full outage: cp0 ready ${READY_C0_MS} ms, cp1 ready ${READY_C1_MS} ms"

echo "== load B: 30 s after the full outage"
mdscpverify -mode load -cp 127.0.0.1:$ADDR0,127.0.0.1:$ADDR1 -duration 30s -workers 4 -run-id e1b \
  -acked-out "$ACKED_B" >"$R/e1-load-b.out" 2>"$R/e1-load-b.err"
SUMMARY_B=$(last_summary "$R/e1-load-b.out")
cat "$ACKED_A" "$ACKED_B" >"$ACKED_ALL"

echo "== live end offsets against everything acked"
mdscpverify -mode ends -cp 127.0.0.1:$ADDR0,127.0.0.1:$ADDR1 -acked "$ACKED_ALL" >"$R/e1-ends.txt" 2>&1
ENDS_RC=$?
tail -3 "$R/e1-ends.txt"

node.sh stop cp0; node.sh stop cp1

echo "== offline checks on the log"
mdscpinspect -check -region "$REGION" -table "$TABLE" -bucket "$BUCKET" -acked "$ACKED_ALL" >"$R/e1-inspect.txt" 2>&1
mdscpinspect -v -region "$REGION" -table "$TABLE" >"$R/e1-slots.txt" 2>&1
# writer changes in slot order, up to the end of load A
TRANSITIONS=$(python3 - "$R/e1-slots.txt" "$TAIL_AFTER_A" <<'PY'
import re, sys
limit = int(sys.argv[2] or 0)
last, out = None, []
for line in open(sys.argv[1]):
    m = re.match(r"\s+slot (\d+)\s.*writer=(\S+)", line)
    if not m:
        continue
    slot, writer = int(m.group(1)), m.group(2)
    if slot > limit:
        break
    if last is not None and writer != last:
        out.append("%s->%s@%d" % (last, writer, slot))
    last = writer
print(" ".join(out) if out else "none")
PY
)
echo "   writer changes up to slot $TAIL_AFTER_A: $TRANSITIONS"
N_TRANSITIONS=$(echo "$TRANSITIONS" | wc -w)

failed_a=$(json_field "$SUMMARY_A" 'd["failed"]'); acked_a=$(json_field "$SUMMARY_A" 'd["acked"]')
stall_a=$(json_field "$SUMMARY_A" 'd["max_stall_ms"]'); retries_a=$(json_field "$SUMMARY_A" 'd["retries"]')
failed_b=$(json_field "$SUMMARY_B" 'd["failed"]'); acked_b=$(json_field "$SUMMARY_B" 'd["acked"]')
errors=$(count_in_logs '"level":"ERROR"')
busy=$(decisions | grep -c 'control_io_pending' || true)
check_failed=$(grep -c '^FAIL' "$R/e1-inspect.txt" || true)
skips=$(grep -c '^SKIP' "$R/e1-inspect.txt" || true)
replay_floor=$(( ${TAIL_B:-0} / 64 / 4 ))

expect "load A (through kill and restart): failed = 0" "acked=$acked_a failed=$failed_a retries=$retries_a" [ "$failed_a" = 0 ]
criterion "longest stall <= takeover-after + 5 s" "$(le "$stall_a" $(( (TAKEOVER + 5) * 1000 )))" "max_stall=${stall_a}ms"
expect "the standby took over (cp1 wrote slots)" "writer changes: $TRANSITIONS" [ -n "$(grep -m1 'writer=cp1' "$R/e1-slots.txt")" ]
expect "no writer ping-pong in phases A and B (<= 2 changes)" "changes=$N_TRANSITIONS" [ "$N_TRANSITIONS" -le 2 ]
criterion "phase B: cp0 ready within 60 s on a wiped disk" "$(le "$READY_B_MS" 60000)" "${READY_B_MS}ms"
expect "phase B: cp0 restored from the checkpoint (few Query calls)" "queries=$QUERIES_B, a full replay needs >= $((${TAIL_B:-0} / 64)), limit=$replay_floor" [ "$QUERIES_B" -le "$replay_floor" ]
criterion "phase C: both nodes ready within 120 s" "$(le "$(( READY_C0_MS > READY_C1_MS ? READY_C0_MS : READY_C1_MS ))" 120000)" "cp0=${READY_C0_MS}ms cp1=${READY_C1_MS}ms"
expect "load B after the full outage: failed = 0" "acked=$acked_b failed=$failed_b" [ "$failed_b" = 0 ]
expect "live end offsets equal records acked (mode ends)" "$(tail -2 "$R/e1-ends.txt" | head -1)" [ "$ENDS_RC" = 0 ]
expect "A1 to A5: mdscpinspect -check -acked passes" "failed_lines=$check_failed" [ "$check_failed" = 0 ]
expect "no check was skipped" "skipped=$skips" [ "$skips" = 0 ]
expect "A6: no ERROR line in node logs" "count=$errors" [ "$errors" = 0 ]
expect "A7: no checkpoint skipped for control_io_pending" "count=$busy" [ "$busy" = 0 ]
verdict E1
rc=$?
ACKED_FILE="$ACKED_ALL" EXTRA_FILES="$VERDICT_FILE $R/e1-load-a.out $R/e1-load-b.out $R/e1-ends.txt $R/e1-inspect.txt $R/e1-slots.txt" collect.sh e1 >/dev/null 2>&1
echo "results uploaded under s3://$BUCKET/results/ (prefix e1-)"
echo "E1 DONE rc=$rc"
exit $rc
