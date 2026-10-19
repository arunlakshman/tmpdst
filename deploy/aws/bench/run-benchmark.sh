#!/usr/bin/env bash
# Benchmark matrix driver. Runs ON THE LOADGEN HOST (SSM session), or locally
# with --local against a manibroker/manicp started by hand.
#
#   run-benchmark.sh [--smoke] [--local] [--tool kafka|warpstream] [--reps N]
#                    [--duration S] [--rate N] [--gap S] [--warmup S] [--cells "c1_1k c8_1k"]
#                    [--topic NAME] [--out DIR] [--run-id ID]
#
# Topic: created by the broker host at boot (see note in the script), not here.
# Matrix (default): cells c1_1k (1 client, 1 KB), c8_1k, c32_1k, c1_100b
# (1 client, 100 B); each 60 s, repeated 3 times, 15 s idle between cells,
# after a 20 s warm-up. Topic has 6 partitions.
#
# Remote mode reads /etc/profile.d/bench.sh (BOOTSTRAP, RUN_ID, BUCKET,
# AWS_REGION) and uploads everything to s3://$BUCKET/results/$RUN_ID/loadgen/.
# Run it detached so a dropped SSM session does not kill it:
#   nohup /opt/bench/run-benchmark.sh > /var/log/bench-run.log 2>&1 &
#
# Load tool: Apache Kafka's kafka-producer-perf-test (one process per client; prints
# p50/p95/p99 in rate-limited mode). The WarpStream `benchmark-producer` was
# tried locally and CANNOT be used against this broker: its producer always
# compresses and has no flag to turn that off, and the broker answers every
# produce with UNSUPPORTED_COMPRESSION_TYPE. `--tool warpstream` stays available
# only for a broker that accepts compressed batches.
# Idempotence is switched off (enable.idempotence=false): Kafka 3.8 defaults to
# true and the broker rejects idempotent batches (decision 41).
#
# Broker-side /metrics snapshots: remote mode relies on mb-snapshot.service on the
# broker host (every 10 s into s3://$BUCKET/results/$RUN_ID/metrics/); --local
# snapshots http://127.0.0.1:9100 and :9101 itself at every cell start and end.
#
# Optional Grafana annotations: GRAFANA_URL and GRAFANA_TOKEN in the environment,
# else SSM $SSM_PREFIX/grafana/url and grafana/annotation_token (remote mode).
# Missing credentials only disable annotations.
set -euo pipefail

LOCAL=0 SMOKE=0 TOOL=auto REPS=3 DURATION=60 GAP=15 WARMUP=20
CELLS="c1_1k c8_1k c32_1k c1_100b" TOPIC="" OUT="" RUN_ID="${RUN_ID:-}"
SSM_PREFIX="${SSM_PREFIX:-/manifester-bench}"
PARTITIONS=6
RATE=-1

while [ $# -gt 0 ]; do
  case "$1" in
    --local) LOCAL=1 ;;
    --smoke) SMOKE=1 ;;
    --tool) TOOL=$2; shift ;;
    --reps) REPS=$2; shift ;;
    --duration) DURATION=$2; shift ;;
    --gap) GAP=$2; shift ;;
    --warmup) WARMUP=$2; shift ;;
    --cells) CELLS=$2; shift ;;
    --topic) TOPIC=$2; shift ;;
    --rate) RATE=$2; shift ;;
    --out) OUT=$2; shift ;;
    --run-id) RUN_ID=$2; shift ;;
    -h | --help) sed -n '2,29p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
  shift
done
if [ "$SMOKE" -eq 1 ]; then # go/no-go: one client, 1 KB, 20 s, once
  REPS=1 DURATION=20 WARMUP=5 GAP=5 CELLS="c1_1k"
fi

# ---- environment -----------------------------------------------------------
if [ "$LOCAL" -eq 0 ]; then
  # shellcheck disable=SC1091
  [ -f /etc/profile.d/bench.sh ] && . /etc/profile.d/bench.sh
  : "${BOOTSTRAP:?BOOTSTRAP not set (expected from /etc/profile.d/bench.sh)}"
  : "${BUCKET:?BUCKET not set}"
  : "${RUN_ID:?RUN_ID not set}"
  export AWS_DEFAULT_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
else
  BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
  RUN_ID="${RUN_ID:-local-$(date +%H%M%S)}"
  BROKER_METRICS="${BROKER_METRICS:-http://127.0.0.1:9100/metrics}"
  CP_METRICS="${CP_METRICS:-http://127.0.0.1:9101/metrics}"
fi
export PATH="$PATH:/opt/kafka/bin:/usr/local/bin"
KAFKA_BIN="${KAFKA_BIN:-}"   # directory with kafka-topics.sh; empty: use PATH
kcmd() { if [ -n "$KAFKA_BIN" ]; then echo "$KAFKA_BIN/$1"; else echo "$1"; fi; }
TOPIC="${TOPIC:-bench-$RUN_ID}"
OUT="${OUT:-${HOME:-/tmp}/bench-out/$RUN_ID}"
mkdir -p "$OUT/loadgen" "$OUT/metrics"
LOG="$OUT/loadgen/harness.log"
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG" >&2; }

# ---- tool selection --------------------------------------------------------
[ "$TOOL" != auto ] || TOOL=kafka
case "$TOOL" in warpstream | kafka) ;; *) echo "bad --tool $TOOL" >&2; exit 2 ;; esac
log "run_id=$RUN_ID tool=$TOOL bootstrap=$BOOTSTRAP topic=$TOPIC out=$OUT reps=$REPS duration=${DURATION}s gap=${GAP}s warmup=${WARMUP}s cells=[$CELLS] local=$LOCAL smoke=$SMOKE"

# ---- Grafana annotations ---------------------------------------------------
GRAFANA_URL="${GRAFANA_URL:-}" GRAFANA_TOKEN="${GRAFANA_TOKEN:-}"
if [ "$LOCAL" -eq 0 ] && [ -z "$GRAFANA_URL" ]; then
  GRAFANA_URL=$(aws ssm get-parameter --with-decryption --name "$SSM_PREFIX/grafana/url" --query Parameter.Value --output text 2>/dev/null || true)
  GRAFANA_TOKEN=$(aws ssm get-parameter --with-decryption --name "$SSM_PREFIX/grafana/annotation_token" --query Parameter.Value --output text 2>/dev/null || true)
fi
if [ -z "$GRAFANA_URL" ] || [ -z "$GRAFANA_TOKEN" ]; then
  GRAFANA_URL=""
  log "annotations disabled (no Grafana URL/token)"
fi
annotate() { # annotate <tag> <text> [time_end_ms]
  [ -n "$GRAFANA_URL" ] || return 0
  local now end="${3:-}" body
  now=$(($(date +%s) * 1000))
  body=$(jq -nc --argjson t "$now" --arg run "run:$RUN_ID" --arg tag "$1" --arg text "$2" --arg end "$end" \
    '{time:$t, tags:["manifester",$run,$tag], text:$text} + (if $end != "" then {timeEnd:($end|tonumber)} else {} end)')
  curl -sS -m 10 -X POST "$GRAFANA_URL/api/annotations" -H "Authorization: Bearer $GRAFANA_TOKEN" \
    -H 'Content-Type: application/json' -d "$body" >/dev/null || log "annotation failed (ignored)"
}

# ---- helpers ---------------------------------------------------------------
snap_local() { # snap_local <label>: broker + manicp /metrics at this instant
  [ "$LOCAL" -eq 1 ] || return 0
  local ts
  ts=$(date +%s)
  curl -sf -m 5 "$BROKER_METRICS" >"$OUT/metrics/$ts-broker.txt" || log "broker metrics snapshot failed ($1)"
  curl -sf -m 5 "$CP_METRICS" >"$OUT/metrics/$ts-manicp.txt" || log "manicp metrics snapshot failed ($1)"
}

wait_ready() {
  local deadline=$(($(date +%s) + 300))
  log "waiting for the broker on $BOOTSTRAP (kafka-topics --list; metrics ports are loopback-only on the broker host)"
  until "$(kcmd kafka-topics.sh)" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || { log "broker not ready after 300 s"; exit 1; }
    sleep 5
  done
  log "broker answers"
}

# Saturating mode (default, --rate unset): every client sends as fast as it can for
# <seconds>, then gets SIGINT. kafka-producer-perf-test prints only interval
# lines in that case (no final summary, hence no percentiles: it is killed
# before it can drain a saturated queue), and client-side latency includes
# time spent queued in the client, so read throughput here and ack latency from
# the broker metrics. Rate-limited mode (--rate N records/s per client) sends
# N*seconds records and exits normally, so the final summary with
# p50/p95/p99 exists; use it for latency below saturation.
# run_tool <seconds> <clients> <record_size> <outprefix>: blocks for ~seconds.
# Writes <outprefix>.cK.out (kafka, one file per client) or <outprefix>.out
# plus <outprefix>.prom (warpstream, histogram scraped just before it stops).
run_tool() {
  local secs=$1 clients=$2 size=$3 prefix=$4 i
  if [ "$TOOL" = warpstream ]; then
    local first=${BOOTSTRAP%%,*} host port pid
    host=${first%:*} port=${first##*:}
    timeout -s INT -k 10 "$secs" warpstream cli benchmark-producer -bootstrap-host "$host" -bootstrap-port "$port" \
      -topic "$TOPIC" -num-clients "$clients" -record-size "$size" -num-records -1 \
      -max-records-per-second "$([ "$RATE" -gt 0 ] && echo "$RATE" || echo 1000000)" -percent-random-data 100 -prometheus-port 8081 >"$prefix.out" 2>&1 &
    pid=$!
    sleep $((secs - 2))
    curl -sf -m 3 http://127.0.0.1:8081/metrics >"$prefix.prom" || true
    wait "$pid" || true # timeout exits 124 by design
  else
    local pids=() limit=$secs nrec=2000000000
    if [ "$RATE" -gt 0 ]; then nrec=$((RATE * secs)); limit=$((secs + 60)); fi
    for i in $(seq 1 "$clients"); do
      # shellcheck disable=SC2086 # PRODUCER_PROPS is a word list on purpose
      KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx384m}" timeout -s INT -k 10 "$limit" "$(kcmd kafka-producer-perf-test.sh)" --topic "$TOPIC" --num-records "$nrec" \
        --record-size "$size" --throughput "$RATE" \
        --producer-props "bootstrap.servers=$BOOTSTRAP" acks=all enable.idempotence=false ${PRODUCER_PROPS:-batch.size=262144 linger.ms=20 buffer.memory=100663296} \
        >"$prefix.c$i.out" 2>&1 &
      pids+=("$!")
    done
    for i in "${pids[@]}"; do wait "$i" || true; done
  fi
}

# ---- main ------------------------------------------------------------------
wait_ready
# Topic creation: this broker implements neither CreateTopics nor DescribeConfigs,
# so kafka-topics.sh --create/--describe cannot be used. Topics are created through
# the control plane's HTTP API, which listens on loopback of the broker host:
# broker user-data does it at boot for bench-<run_id> with 6 partitions. In
# --local mode the harness posts to CP_URL itself. Existence is checked with
# kafka-topics --list (Metadata).
if [ "$LOCAL" -eq 1 ]; then
  curl -sf -m 10 -X POST "${CP_URL:-http://127.0.0.1:8080}/v1/create-topic" \
    -d "{\"topic\":\"$TOPIC\",\"partitions\":$PARTITIONS}" >/dev/null || { log "create-topic via CP_URL failed"; exit 1; }
fi
topic_deadline=$(($(date +%s) + 120))
until "$(kcmd kafka-topics.sh)" --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -qx "$TOPIC"; do
  [ "$(date +%s)" -lt "$topic_deadline" ] || { log "topic $TOPIC not visible after 120 s"; exit 1; }
  sleep 3
done
log "topic $TOPIC present ($PARTITIONS partitions requested)"

jq -nc --arg run "$RUN_ID" --arg tool "$TOOL" --arg topic "$TOPIC" --arg boot "$BOOTSTRAP" --arg cells "$CELLS" \
  --argjson reps "$REPS" --argjson dur "$DURATION" --argjson gap "$GAP" --argjson warm "$WARMUP" --argjson parts "$PARTITIONS" \
  '{run_id:$run,tool:$tool,topic:$topic,bootstrap:$boot,cells:($cells|split(" ")),reps:$reps,duration_s:$dur,gap_s:$gap,warmup_s:$warm,partitions:$parts}' \
  >"$OUT/loadgen/run.json"
printf 'rep\tcell\tclients\trecord_size\ttool\tstart\tload_end\tend\tprefix\n' >"$OUT/loadgen/cells.tsv"

annotate start "run $RUN_ID start: tool=$TOOL topic=$TOPIC partitions=$PARTITIONS cells=[$CELLS] reps=$REPS duration=${DURATION}s"

log "warm-up ${WARMUP}s (1 client, 1024 B), discarded"
run_tool "$WARMUP" 1 1024 "$OUT/loadgen/warmup"
sleep "$GAP"

cell_params() { # cell name -> "clients size"
  case "$1" in
    c1_1k) echo "1 1024" ;;
    c8_1k) echo "8 1024" ;;
    c32_1k) echo "32 1024" ;;
    c1_100b) echo "1 100" ;;
    c[0-9]*_[0-9]*b) # generic: c<clients>_<bytes>b
      local c=${1#c}; echo "${c%%_*} $(echo "${c#*_}" | tr -d b)" ;;
    *) echo "unknown cell $1" >&2; return 1 ;;
  esac
}

fail=0
for rep in $(seq 1 "$REPS"); do
  for cell in $CELLS; do
    read -r clients size < <(cell_params "$cell")
    prefix="$OUT/loadgen/$cell.r$rep"
    log "cell $cell rep $rep: $clients client(s), $size B, ${DURATION}s"
    snap_local "start $cell r$rep"
    start=$(date +%s)
    annotate cell_start "cell $cell rep $rep start ($clients clients, $size B)"
    run_tool "$DURATION" "$clients" "$size" "$prefix"
    end=$(date +%s)
    annotate cell_end "cell $cell rep $rep end" $((end * 1000))
    snap_local "end $cell r$rep"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$rep" "$cell" "$clients" "$size" "$TOOL" "$start" "$((start + DURATION))" "$end" "${prefix##*/}" >>"$OUT/loadgen/cells.tsv"
    # a cell whose tool produced nothing is a failed cell
    if ! grep -qi 'records' "$prefix"*.out 2>/dev/null; then log "cell $cell rep $rep produced no output"; fail=1; fi
    if [ "$LOCAL" -eq 0 ]; then
      aws s3 cp --only-show-errors --recursive "$OUT/loadgen/" "s3://$BUCKET/results/$RUN_ID/loadgen/" || log "upload failed (will retry at end)"
    fi
    log "idle ${GAP}s"
    sleep "$GAP"
  done
done

annotate end "run $RUN_ID end (fail=$fail)"
[ "$LOCAL" -eq 1 ] || sleep 12 # let the broker host's snapshotter catch the final state
if [ "$LOCAL" -eq 0 ]; then
  aws s3 sync --only-show-errors "$OUT/loadgen/" "s3://$BUCKET/results/$RUN_ID/loadgen/"
  log "uploaded to s3://$BUCKET/results/$RUN_ID/loadgen/"
fi
if [ "$SMOKE" -eq 1 ]; then
  if grep -qiE 'exception|error' "$OUT"/loadgen/c1_1k.r1.c*.out 2>/dev/null; then fail=1; fi
  if [ "$fail" -eq 0 ]; then log "SMOKE PASS (tool produced records without errors); now check the broker side, see the runbook"; else log "SMOKE FAIL"; fi
fi
log "done fail=$fail"
exit "$fail"
