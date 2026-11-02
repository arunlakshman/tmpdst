#!/bin/bash
# E4 sustained load (soak). See docs/cp-aws-experiments.md E4.
# Two nodes, 100 commits per second, -window-slots 8192 so the log trim really runs.
# The load runs as 3 equal parts, so we can compare the first part with the last part.
# Once a minute we write one line per node: memory, Pebble disk size, applied slot, log tail.
#
# Env: PART_MIN = minutes per load part (default 10, so the run is 3 x 10 = 30 minutes).
set -uo pipefail
. "$(dirname "$0")/lib.sh"

R="$CPV_ROOT/results"
PART_MIN="${PART_MIN:-10}"
TOTAL_MIN=$(( PART_MIN * 3 ))
RATE=100
FLAGS=(-checkpoint-every 60s -window-slots 8192 "${NODE_FLAGS[@]}")
SAMPLES="$R/e4-samples.csv"
ACKED="$R/e4-acked.jsonl"

"$CPV_ROOT/scripts/reset.sh" || { echo "RESET FAILED: the experiment did not start"; exit 2; }
: >"$ACKED"
echo "minute,node,rss_kb,pebble_kb,applied_slot,log_tail,put_item,query" >"$SAMPLES"

rss_kb() { ps -o rss= -p "$(cat "$CPV_ROOT/run/$1.pid")" 2>/dev/null | tr -d ' '; }
pebble_kb() { du -sk "$CPV_ROOT/data/$1" 2>/dev/null | cut -f1; }
sample_node() { # sample_node MINUTE NAME GRPC METRICS
  local status applied tail
  status=$(mdscpverify -mode ready -cp "127.0.0.1:$3" 2>/dev/null)
  applied=$(echo "$status" | sed -n 's/.*applied_slot=\([0-9]*\).*/\1/p')
  tail=$(echo "$status" | sed -n 's/.*log_tail=\([0-9]*\).*/\1/p')
  local put query
  put=$(metric_sum "$4" 'dynamo_attempt_seconds_count{op="PutItem"')
  query=$(metric_sum "$4" 'dynamo_attempt_seconds_count{op="Query"')
  echo "$1,$2,$(rss_kb "$2"),$(pebble_kb "$2"),${applied:-0},${tail:-0},$put,$query" >>"$SAMPLES"
}
sample_all() {
  sample_node "$1" cp0 $ADDR0 $MET0
  sample_node "$1" cp1 $ADDR1 $MET1
}
# column MINUTE NODE COLUMN_NUMBER : one value from the samples file
column() { awk -F, -v m="$1" -v n="$2" -v c="$3" '$1 == m && $2 == n { print $c }' "$SAMPLES" | tail -1; }
# ratio_ok NEW OLD MAX : 1 when NEW <= OLD * MAX
ratio_ok() { python3 -c 'import sys; n,o,m=map(float,sys.argv[1:4]); print(1 if o>0 and n<=o*m else 0)' "$1" "$2" "$3"; }

echo "== start cp0 and cp1; $TOTAL_MIN minutes of load at $RATE commits per second"
node.sh start cp0 0 $ADDR0 $MET0 "${FLAGS[@]}"
node.sh start cp1 1 $ADDR1 $MET1 "${FLAGS[@]}"
node.sh wait-ready cp0 60 >/dev/null; node.sh wait-ready cp1 60 >/dev/null
sample_all 0

# Samples run in the background for the whole test.
(
  for minute in $(seq 1 "$TOTAL_MIN"); do
    sleep 60
    sample_all "$minute"
  done
) &
SAMPLER=$!

for part in a b c; do
  echo "== load part $part ($PART_MIN min) at $(date -u +%H:%M:%S)"
  mdscpverify -mode load -cp 127.0.0.1:$ADDR0,127.0.0.1:$ADDR1 -duration "${PART_MIN}m" -workers 4 -rate $RATE \
    -run-id "e4$part" -acked-out "$R/e4-acked-$part.jsonl" >"$R/e4-load-$part.out" 2>"$R/e4-load-$part.err"
  last_summary "$R/e4-load-$part.out"
done
wait "$SAMPLER"
sample_all "$TOTAL_MIN"   # one last sample right after the load stops (same minute label, the later line wins)
cat "$R"/e4-acked-?.jsonl >"$ACKED"

# Let the standby catch up, then read the final facts while both nodes still run.
sleep 20
CHECKPOINT_AGE=$(python3 - "$BUCKET" "$REGION" <<'PY'
import json, subprocess, sys, datetime
out = subprocess.run(["aws", "s3api", "head-object", "--bucket", sys.argv[1], "--region", sys.argv[2],
                      "--key", "cp/checkpoint/latest"], capture_output=True, text=True)
if out.returncode != 0:
    print(999999)
else:
    t = datetime.datetime.fromisoformat(json.loads(out.stdout)["LastModified"].replace("Z", "+00:00"))
    print(int((datetime.datetime.now(datetime.timezone.utc) - t).total_seconds()))
PY
)
CHECKPOINT_KB=$(aws s3api head-object --bucket "$BUCKET" --region "$REGION" --key cp/checkpoint/latest --query ContentLength --output text 2>/dev/null)
echo "   checkpoint age at the end: ${CHECKPOINT_AGE}s; size: ${CHECKPOINT_KB} bytes"
status1=$(mdscpverify -mode ready -cp 127.0.0.1:$ADDR1)
applied1=$(echo "$status1" | sed -n 's/.*applied_slot=\([0-9]*\).*/\1/p'); tail1=$(echo "$status1" | sed -n 's/.*log_tail=\([0-9]*\).*/\1/p')
echo "   cp1: $status1"
mdscpverify -mode ends -cp 127.0.0.1:$ADDR0,127.0.0.1:$ADDR1 -acked "$ACKED" >"$R/e4-ends.txt" 2>&1
ENDS_RC=$?
tail -2 "$R/e4-ends.txt"

node.sh stop cp0; node.sh stop cp1

echo "== offline checks on the log (it is trimmed, so full replay checks are skipped by design)"
mdscpinspect -check -region "$REGION" -table "$TABLE" -bucket "$BUCKET" -acked "$ACKED" >"$R/e4-inspect.txt" 2>&1
mdscpinspect -v -region "$REGION" -table "$TABLE" >"$R/e4-slots.txt" 2>&1
TRIMMED=$(grep -c '^   slot [0-9]* *1 bytes' "$R/e4-slots.txt" || true)
LIVE=$(grep -c '^   slot ' "$R/e4-slots.txt" || true)
echo "   log slots: $LIVE, of them trimmed (1 byte): $TRIMMED"

summary() { last_summary "$R/e4-load-$1.out"; }
failed=0; acked=0
for p in a b c; do
  f=$(json_field "$(summary $p)" 'd["failed"]'); a=$(json_field "$(summary $p)" 'd["acked"]')
  failed=$(( failed + f )); acked=$(( acked + a ))
done
p99_a=$(json_field "$(summary a)" 'd["latency_ms"]["p99"]'); p99_c=$(json_field "$(summary c)" 'd["latency_ms"]["p99"]')
p50_a=$(json_field "$(summary a)" 'd["latency_ms"]["p50"]'); p50_c=$(json_field "$(summary c)" 'd["latency_ms"]["p50"]')
stall_max=$(python3 -c 'import sys; print(max(float(x) for x in sys.argv[1:]))' \
  "$(json_field "$(summary a)" 'd["max_stall_ms"]')" "$(json_field "$(summary b)" 'd["max_stall_ms"]')" "$(json_field "$(summary c)" 'd["max_stall_ms"]')")
WARM=$PART_MIN
written=$(decisions | grep -c '"written":true' || true)
errors=$(count_in_logs '"level":"ERROR"')
busy=$(decisions | grep -c 'control_io_pending' || true)
check_failed=$(grep -c '^FAIL' "$R/e4-inspect.txt" || true)
skips=$(grep '^SKIP' "$R/e4-inspect.txt" | sed 's/  */ /g' | tr '\n' ';')
min_checkpoints=$(( TOTAL_MIN / 3 - 2 ))

expect "load: failed = 0 in all 3 parts" "acked=$acked failed=$failed" [ "$failed" = 0 ]
criterion "p99 of the last part <= 1.5 x the first part" "$(ratio_ok "$p99_c" "$p99_a" 1.5)" "p99 first=${p99_a}ms last=${p99_c}ms (p50 ${p50_a} -> ${p50_c})"
criterion "longest stall in any part <= 5 s (trim stall test)" "$(le "$stall_max" 5000)" "max_stall=${stall_max}ms"
for node in cp0 cp1; do
  r_warm=$(column "$WARM" $node 3); r_end=$(column "$TOTAL_MIN" $node 3)
  criterion "$node RSS grew <= 20% after warm-up" "$(ratio_ok "$r_end" "$r_warm" 1.2)" "minute $WARM=${r_warm}KB end=${r_end}KB"
  d_warm=$(column "$WARM" $node 4); d_end=$(column "$TOTAL_MIN" $node 4)
  criterion "$node Pebble size bounded (end <= 1.5 x minute $WARM)" "$(ratio_ok "$d_end" "$d_warm" 1.5)" "minute $WARM=${d_warm}KB end=${d_end}KB"
done
expect "checkpoints written at least once per 3 periods" "written=$written needed>=$min_checkpoints" [ "$written" -ge "$min_checkpoints" ]
criterion "latest checkpoint is fresh (<= 180 s old)" "$(le "$CHECKPOINT_AGE" 180)" "age=${CHECKPOINT_AGE}s"
expect "the log was trimmed (tombstones exist)" "trimmed_slots=$TRIMMED live_slots=$LIVE" [ "$TRIMMED" -gt 0 ]
criterion "standby caught up (applied within 50 of tail)" "$(le "$(( ${tail1:-0} - ${applied1:-0} ))" 50)" "applied=$applied1 tail=$tail1"
expect "live end offsets equal records acked (mode ends)" "$(tail -2 "$R/e4-ends.txt" | head -1)" [ "$ENDS_RC" = 0 ]
expect "A1, A2, A3, A5: mdscpinspect -check -acked has no FAIL" "failed_lines=$check_failed" [ "$check_failed" = 0 ]
expect "A6: no ERROR line in node logs" "count=$errors" [ "$errors" = 0 ]
expect "A7: no checkpoint skipped for control_io_pending" "count=$busy" [ "$busy" = 0 ]
echo "info: skipped checks (expected on a trimmed log): $skips" | tee -a "$VERDICT_FILE"
echo "info: DynamoDB attempts at the end (cp0 PutItem, Query; cp1 PutItem, Query):" | tee -a "$VERDICT_FILE"
tail -n 2 "$SAMPLES" | tee -a "$VERDICT_FILE"
verdict E4
rc=$?
ACKED_FILE="$ACKED" EXTRA_FILES="$VERDICT_FILE $SAMPLES $R/e4-load-a.out $R/e4-load-b.out $R/e4-load-c.out $R/e4-ends.txt $R/e4-inspect.txt" collect.sh e4 >/dev/null 2>&1
echo "results uploaded under s3://$BUCKET/results/ (prefix e4-)"
echo "E4 DONE rc=$rc"
exit $rc
