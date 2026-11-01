#!/bin/bash
# E0 baseline: one node on the box boots on an empty table and bucket with role
# credentials, serves a load, and writes checkpoints. See docs/cp-aws-experiments.md.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
NODE=cp0
GRPC=18080
METRICS=19101

"$CPV_ROOT/scripts/reset.sh" || { echo "RESET FAILED: the experiment did not start"; exit 2; }
ACKED="$CPV_ROOT/results/e0-acked.jsonl"
: >"$ACKED"

started=$(now_ms)
# -window-slots is huge so the log is NOT trimmed: the acked-key checks need the whole log.
node.sh start $NODE 0 $GRPC $METRICS -checkpoint-every 30s -checkpoint-min-slots 200 -window-slots 1000000000
node.sh wait-ready $NODE 60 || true
ready_s=$(python3 -c "print(round(($(now_ms) - $started)/1000.0, 1))")

mdscpverify -mode load -cp 127.0.0.1:$GRPC -duration 60s -workers 4 -acked-out "$ACKED" \
  >"$CPV_ROOT/results/e0-load.out" 2>"$CPV_ROOT/results/e0-load.err"
summary=$(last_summary "$CPV_ROOT/results/e0-load.out")
sleep 35 # one more checkpoint timer fire after the load

failed=$(json_field "$summary" 'd["failed"]')
acked=$(json_field "$summary" 'd["acked"]')
p50=$(json_field "$summary" 'd["latency_ms"]["p50"]')
p99=$(json_field "$summary" 'd["latency_ms"]["p99"]')
written=$(decisions | grep -c '"written":true' || true)
busy=$(decisions | grep -c 'control_io_pending' || true)

criterion "node ready within 30 s" "$(le "$ready_s" 30)" "${ready_s}s"
criterion "load: failed = 0" "$([ "$failed" = 0 ] && echo 1 || echo 0)" "acked=$acked failed=$failed"
criterion "load: p50 <= 20 ms (provisional)" "$(le "$p50" 20)" "p50=${p50}ms"
criterion "load: p99 <= 150 ms (provisional)" "$(le "$p99" 150)" "p99=${p99}ms"
criterion "at least one checkpoint written" "$([ "$written" -ge 1 ] && echo 1 || echo 0)" "written=$written"
criterion "A7: no checkpoint skipped for control_io_pending" "$([ "$busy" = 0 ] && echo 1 || echo 0)" "count=$busy"
errors=$(count_in_logs '"level":"ERROR"')
criterion "A6: no ERROR line in node logs" "$([ "$errors" = 0 ] && echo 1 || echo 0)" "count=$errors"

node.sh stop $NODE
mdscpinspect -check -region "$REGION" -table "$TABLE" -bucket "$BUCKET" -acked "$ACKED" \
  >"$CPV_ROOT/results/e0-inspect.txt" 2>&1
check_failed=$(grep -c '^FAIL' "$CPV_ROOT/results/e0-inspect.txt" || true)
criterion "A1 to A5: mdscpinspect -check -acked passes" "$([ "$check_failed" = 0 ] && echo 1 || echo 0)" "failed_lines=$check_failed"
skips=$(grep -c '^SKIP' "$CPV_ROOT/results/e0-inspect.txt" || true)
criterion "no check was skipped (a skip hides a check)" "$([ "$skips" = 0 ] && echo 1 || echo 0)" "skipped=$skips"
verdict E0
rc=$?
ACKED_FILE="$ACKED" EXTRA_FILES="$VERDICT_FILE $CPV_ROOT/results/e0-load.out $CPV_ROOT/results/e0-inspect.txt" collect.sh e0
exit $rc
