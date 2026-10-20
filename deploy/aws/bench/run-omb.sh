#!/usr/bin/env bash
# OpenMessaging Benchmark coordinator: the WarpStream-style 6-broker benchmark. Runs ON LOADGEN HOST 0
# (SSM session, or detached through `bench-ctl.sh run`). Read docs/aws-benchmark-runbook.md first.
#
#   run-omb.sh [--stages LIST] [--compression auto|lz4|none] [--partitions N] [--topic NAME]
#              [--bootstrap-mode balanced|random] [--affine] [--abort-p99-ms N] [--max-minutes N] [--include-later] [--skip-preflight] [--dry-run] [--out DIR]
#
# Stages (comma list; names are workload files in omb/workloads, aliases below):
#   smoke           smoke-100k-produce-only       100k msg/s =  97.7 MiB/s, 3 min   (+1 min warm-up)
#   ramp            ramp-{100,250,500,750,1000}k-produce-only  = 97.7 .. 976.6 MiB/s, 5 min each (+1)
#   vanity-produce  vanity-1gibps-produce-only    1M msg/s  = 976.6 MiB/s, 15 min   (+2)   <- default full stage
#   vanity          vanity-1gibps                 same + 64 consumers (needs Fetch)
#   later/*         fan-out 3x, 256 producers, 1152/4608 partitions, round robin: only with --include-later
# Default: smoke,vanity-produce. A recommended careful path: smoke,ramp,vanity-produce, then vanity.
#
# --bootstrap-mode balanced (default): each OMB producer/consumer is pinned to one broker round-robin (WarpStream's service
# discovery balances connections; a manifester broker names only itself in Metadata). random: stock client behaviour
# (binomial skew, see docs/aws-benchmark-warpstream-comparison.md).
#
# Prerequisites (bench-ctl.sh does them from the workstation): topic created through the control-plane manicp with the
# right partition count (OMB cannot: no CreateTopics), OMB workers running on all loadgen hosts.
#
# --affine: EXPERIMENT (patch 0004), default off: producer k writes only partitions p % N == k % N (N = brokers), so each
# broker's segments hold a disjoint partition set. Not comparable to WarpStream; needs balanced mode.
#
# Abort rules (checked on every 10 s OMB stats line): error rate > 0.1% of publish rate, publish p99 over the
# 10 s interval > 10000 ms (--abort-p99-ms N or env OMB_ABORT_P99_MS), or (consumers > 0) backlog >= 100 K messages and growing for 2 consecutive
# intervals. On abort: OMB is killed, an ABORT file with the reason is written, results are uploaded and the
# remaining stages are skipped. Touch $OUT/STOP (or /tmp/omb-stop) to stop cleanly between stages.
#
# Output: $OUT (default ~/bench-out/$RUN_ID)/omb/<stage>.json (OMB result), <stage>.log (coordinator log),
# driver-<stage>.yaml, stages.tsv; synced to s3://$BUCKET/results/$RUN_ID/omb/ after each stage.
# manibroker /metrics snapshots come from mb-snapshot.service on every broker host (metrics/<broker id>/), manicp's from
# the single control-plane host (metrics/cp/), every 10 s.
set -euo pipefail

STAGES="smoke,vanity-produce" COMPRESSION=auto PARTITIONS=288 BMODE=balanced TOPIC="" MAX_MIN=120 LATER=0 SKIP_PRE=0 DRY=0 OUT="" AFFINE=false ABORT_P99=${OMB_ABORT_P99_MS:-10000}
while [ $# -gt 0 ]; do
  case "$1" in
    --stages) STAGES=$2; shift ;;
    --compression) COMPRESSION=$2; shift ;;
    --partitions) PARTITIONS=$2; shift ;;
    --topic) TOPIC=$2; shift ;;
    --bootstrap-mode) BMODE=$2; shift ;;
    --affine) AFFINE=true ;;
    --abort-p99-ms) ABORT_P99=$2; shift ;;
    --max-minutes) MAX_MIN=$2; shift ;;
    --include-later) LATER=1 ;;
    --skip-preflight) SKIP_PRE=1 ;;
    --dry-run) DRY=1 ;;
    --out) OUT=$2; shift ;;
    -h | --help) sed -n '2,36p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
  shift
done

# shellcheck disable=SC1091
[ -f /etc/profile.d/bench.sh ] && . /etc/profile.d/bench.sh
: "${BOOTSTRAP:?BOOTSTRAP not set (expected from /etc/profile.d/bench.sh)}"
: "${BROKER_IPS:?BROKER_IPS not set}" "${RUN_ID:?}" "${BUCKET:?}" "${LOADGEN_COUNT:?}"
export AWS_DEFAULT_REGION="${AWS_REGION:-${REGION:-${AWS_DEFAULT_REGION:-}}}"
export PATH="$PATH:/opt/kafka/bin"
OMB=${OMB:-/opt/omb}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OMBCFG=$HERE/omb
TOPIC=${TOPIC:-omb-$RUN_ID}
OUT=${OUT:-${HOME:-/root}/bench-out/$RUN_ID}
mkdir -p "$OUT/omb"
LOG=$OUT/omb/run-omb.log
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG" >&2; }
sync_up() { [ "$DRY" -eq 1 ] || aws s3 sync --only-show-errors "$OUT/omb/" "s3://$BUCKET/results/$RUN_ID/omb/" || log "upload failed (retried at end)"; }
brokers=${BROKER_IPS//,/ }
nbrokers=$(echo "$brokers" | wc -w | tr -d ' ')

# ---- stage list ----------------------------------------------------------------------------------
resolve() {
  local s
  for s in ${STAGES//,/ }; do
    case "$s" in
      smoke) echo smoke-100k-produce-only ;;
      ramp) for r in 100 250 500 750 1000; do echo "ramp-${r}k-produce-only"; done ;;
      vanity-produce) echo vanity-1gibps-produce-only ;;
      vanity) echo vanity-1gibps ;;
      *) echo "$s" ;;
    esac
  done
}
wl_file() {
  if [ -f "$OMBCFG/workloads/$1.yaml" ]; then echo "$OMBCFG/workloads/$1.yaml"
  elif [ -f "$OMBCFG/workloads/later/$1.yaml" ]; then
    [ "$LATER" -eq 1 ] || { echo "stage $1 is a 'later' workload: pass --include-later" >&2; return 1; }
    echo "$OMBCFG/workloads/later/$1.yaml"
  else echo "no workload $1" >&2; return 1; fi
}
yget() { awk -v k="$2:" '$1==k {print $2; exit}' "$1"; }
mapfile -t STAGE_LIST < <(resolve)
total_min=0
for st in "${STAGE_LIST[@]}"; do
  f=$(wl_file "$st") || exit 2
  total_min=$((total_min + $(yget "$f" testDurationMinutes) + $(yget "$f" warmupDurationMinutes) + 2))
done
log "run_id=$RUN_ID brokers=$nbrokers topic=$TOPIC partitions=$PARTITIONS loadgens=$LOADGEN_COUNT stages=[${STAGE_LIST[*]}] estimated=${total_min} min (limit $MAX_MIN)"
[ "$total_min" -le "$MAX_MIN" ] || { log "REFUSING: estimated $total_min min > --max-minutes $MAX_MIN (cost guard)"; exit 2; }

# ---- pre-flight ---------------------------------------------------------------------------------
# The single manicp (decision 46) pushes metrics/cp/<epoch>-manicp.txt every 10 s; loadgens cannot reach it on 8080
# (closed on purpose) but can list the bucket. A fresh snapshot means it is up; a missing one means no CP.
check_cp() {
  local last now age
  last=$(aws s3 ls "s3://$BUCKET/results/$RUN_ID/metrics/cp/" 2>/dev/null | awk '{print $4}' | sed -n 's/^\([0-9]*\)-manicp\.txt$/\1/p' | sort -n | tail -n 1)
  if [ -z "$last" ]; then log "  control plane: NO metrics/cp/ snapshot in s3://$BUCKET/results/$RUN_ID/ (CP host down or still booting: bench-ctl.sh check-cp)"; return 1; fi
  now=$(date +%s); age=$((now - last))
  if [ "$age" -gt 90 ]; then log "  control plane: newest manicp snapshot is ${age}s old (CP dead or stuck: see the runbook)"; return 1; fi
  log "  control plane: manicp snapshot ${age}s old (single sequencer up)"
}

preflight() {
  local ip n ids="" fail=0 out
  log "pre-flight: control plane (single manicp)"
  check_cp || fail=1
  log "pre-flight: Kafka metadata on every broker ($nbrokers)"
  for ip in $brokers; do
    if out=$(kafka-broker-api-versions.sh --bootstrap-server "$ip:9092" 2>&1 | head -n 1); then
      log "  $ip: $out"
      ids="$ids $(echo "$out" | sed -n 's/.*(id: \([0-9-]*\).*/\1/p')"
    else log "  $ip: UNREACHABLE"; fail=1; fi
    if ! kafka-topics.sh --bootstrap-server "$ip:9092" --list 2>/dev/null | grep -qx "$TOPIC"; then
      log "  $ip: topic $TOPIC NOT visible (create it: bench-ctl.sh create-topic $PARTITIONS)"; fail=1
    else
      n=$(kafka-get-offsets.sh --bootstrap-server "$ip:9092" --topic "$TOPIC" --time -1 2>/dev/null | grep -c "^$TOPIC:" || true)  # Kafka 3.8: GetOffsetShell moved packages; use the wrapper
      log "  $ip: topic $TOPIC has $n partitions"
      [ "$n" -eq "$PARTITIONS" ] || { log "  $ip: expected $PARTITIONS partitions"; fail=1; }
    fi
  done
  if [ "$(echo "$ids" | tr ' ' '\n' | sed '/^$/d' | sort | uniq -d | wc -l)" -gt 0 ]; then
    log "  WARNING: duplicate broker ids ($ids): clients merge brokers with the same node id"; fail=1
  fi
  [ "$fail" -eq 0 ] || { log "pre-flight FAILED"; return 1; }
  log "pre-flight ok"
}

# lz4 probe: one 1 KiB record with compression.type=lz4. A broker without compression support (before decision 45)
# answers UNSUPPORTED_COMPRESSION_TYPE; the default `auto` then falls back to none with a loud warning.
probe_lz4() {
  local o
  o=$(timeout 60 kafka-producer-perf-test.sh --topic "$TOPIC" --num-records 1 --record-size 1024 --throughput -1 \
    --producer-props "bootstrap.servers=${BOOTSTRAP}" acks=all enable.idempotence=false compression.type=lz4 \
    max.block.ms=15000 delivery.timeout.ms=20000 request.timeout.ms=10000 retries=0 2>&1 || true)
  if echo "$o" | grep -qiE 'UNSUPPORTED_COMPRESSION|exception|error'; then return 1; fi
  echo "$o" | grep -q '1 records sent'
}

if [ "$SKIP_PRE" -eq 0 ] && [ "$DRY" -eq 0 ]; then preflight || exit 1; fi
case "$COMPRESSION" in
  auto)
    if [ "$DRY" -eq 1 ]; then COMP=none
    elif probe_lz4; then COMP=lz4; log "lz4 pre-flight: the brokers ACCEPT lz4 -> compression.type=lz4 (default, as WarpStream)"
    else COMP=none; log "lz4 pre-flight: the brokers REJECT lz4 batches -> compression.type=none"; fi ;;
  lz4 | none) COMP=$COMPRESSION ;;
  *) echo "bad --compression $COMPRESSION" >&2; exit 2 ;;
esac
if [ "$COMP" = none ]; then
  log "####################################################################################"
  log "## LOUD WARNING: compression.type=none. WarpStream ran lz4 + randomBytesRatio 0.5.   ##"
  log "## ~2x more bytes cross the network and land in S3 per msg/s than in their run:      ##"
  log "## THIS RUN IS NOT APPLES-TO-APPLES (docs/aws-benchmark-warpstream-comparison.md).   ##"
  log "####################################################################################"
fi

# ---- workers -------------------------------------------------------------------------------------
workers=""
# Worker IPs are not in bench.sh: bench-ctl.sh run passes LOADGEN_IPS (private IPs, loadgen index order).
LOADGEN_IPS=${LOADGEN_IPS:-}
if [ -z "$LOADGEN_IPS" ] && [ -f "$HOME/loadgen-ips" ]; then LOADGEN_IPS=$(tr '\n' ',' <"$HOME/loadgen-ips"); fi
if [ -z "$LOADGEN_IPS" ]; then
  # the coordinator's own IP is always one; every worker host must be listed
  log "LOADGEN_IPS unset: pass LOADGEN_IPS=ip0,ip1 (bench-ctl.sh run does) or put one ip per line in ~/loadgen-ips"
  [ "$LOADGEN_COUNT" -eq 1 ] || exit 2
  LOADGEN_IPS=127.0.0.1
fi
for ip in ${LOADGEN_IPS//,/ }; do workers="$workers${workers:+,}http://$ip:8080"; done
nworkers=$(echo "$workers" | tr ',' '\n' | wc -l | tr -d ' ')
cworkers=$((nworkers - nworkers / 2))
[ "$nworkers" -gt 1 ] || cworkers=1
if [ "$nworkers" -gt 1 ] && [ "$DRY" -eq 0 ]; then
  for ip in ${LOADGEN_IPS//,/ }; do
    timeout 3 bash -c "exec 3<>/dev/tcp/$ip/8080" 2>/dev/null || { log "worker $ip:8080 not reachable: run bench-ctl.sh start-workers"; exit 1; }
  done
  log "workers reachable: $workers (producers: first $((nworkers / 2)), consumers: last $cworkers)"
fi

# ---- abort watcher: reads the coordinator log, prints a reason and exits 3 if a rule trips -----
# OMB line: Pub rate R msg/s / M MB/s | Pub err E err/s | Cons rate C msg/s / M MB/s | Backlog: B K | Pub Latency (ms) avg: A - 50%: X - 99%: P - 99.9%: Q - Max: Z
REASON=
check_line() { # check_line <consumers> <line>; returns 3 and sets REASON; state in globals GROW/PREVB
  local cons=$1 line=$2 rate err back p99
  rate=$(echo "$line" | sed -n 's/.*Pub rate *\([0-9.]*\) msg\/s.*/\1/p')
  err=$(echo "$line" | sed -n 's/.*Pub err *\([0-9.]*\) err\/s.*/\1/p')
  back=$(echo "$line" | sed -n 's/.*Backlog: *\([0-9.-]*\) K.*/\1/p')
  p99=$(echo "$line" | sed -n 's/.*Pub Latency (ms) avg: *[0-9.]* - 50%: *[0-9.]* - 99%: *\([0-9.]*\).*/\1/p')
  [ -n "$rate" ] || return 0
  if awk -v r="$rate" -v e="$err" 'BEGIN{exit !((r>0 && e/r>0.001) || (r==0 && e>0))}'; then REASON="error rate $err err/s at publish rate $rate msg/s (> 0.1%)"; return 3; fi
  if [ -n "$p99" ] && awk -v p="$p99" -v lim="$ABORT_P99" 'BEGIN{exit !(p>lim)}'; then REASON="publish p99 ${p99} ms > ${ABORT_P99} ms"; return 3; fi
  if [ "$cons" -gt 0 ] && [ -n "$back" ]; then
    if awk -v b="$back" -v p="${PREVB:-0}" 'BEGIN{exit !(b>=100 && b>p)}'; then GROW=$((GROW + 1)); else GROW=0; fi
    PREVB=$back
    if [ "$GROW" -ge 2 ]; then REASON="backlog ${back} K growing for $GROW consecutive intervals"; return 3; fi
  fi
  return 0
}

rm -f "$OUT/ABORT"
printf 'stage\tworkload\tstart\tend\tstatus\ttarget_msgs\tmsg_size\tconsumers\tcompression\tjson\n' >"$OUT/omb/stages.tsv"
jq -nc --arg run "$RUN_ID" --arg topic "$TOPIC" --argjson parts "$PARTITIONS" --arg comp "$COMP" --argjson nb "$nbrokers" \
  --arg boot "$BOOTSTRAP" --arg bmode "$BMODE" --argjson lg "$nworkers" --arg affine "$AFFINE" --arg stages "${STAGE_LIST[*]}" \
  '{run_id:$run,topic:$topic,partitions:$parts,compression:$comp,brokers:$nb,loadgens:$lg,bootstrap:$boot,bootstrap_mode:$bmode,affine:($affine=="true"),stages:($stages|split(" "))}' >"$OUT/omb/run.json"

status=0
for st in "${STAGE_LIST[@]}"; do
  f=$(wl_file "$st")
  if [ -e "$OUT/STOP" ] || [ -e /tmp/omb-stop ]; then log "STOP file present: not starting $st"; break; fi
  cons=$(yget "$f" consumerPerSubscription); target=$(yget "$f" producerRate); msz=$(yget "$f" messageSize)
  drv=$OUT/omb/driver-$st.yaml
  bash "$OMBCFG/render-driver.sh" "$BOOTSTRAP" "$COMP" "$drv" "$TOPIC" "$cons" "$cworkers" "$([ "$BMODE" = balanced ] && echo true || echo false)" "$AFFINE" 2>/dev/null
  mib=$(awk -v r="$target" -v m="$msz" 'BEGIN{printf "%.1f", r*m/1048576}')
  log "stage $st: $target msg/s = $mib MiB/s, consumers/subscription=$cons, compression=$COMP, affine=$AFFINE, abort p99 > ${ABORT_P99} ms"
  if [ "$DRY" -eq 1 ]; then log "dry run: would run benchmark with driver $drv"; continue; fi
  cd "$OMB"
  start=$(date +%s)
  args=(--drivers "$drv" --output "$OUT/omb/$st.json")
  [ "$nworkers" -le 1 ] || args+=(--workers "$workers")
  HEAP_OPTS="${OMB_COORD_HEAP:--Xms2g -Xmx2g}" JAVA_TOOL_OPTIONS="-Dmani.worker.index=0 -Dmani.loadgen.index=0" \
    bin/benchmark "${args[@]}" "$f" >"$OUT/omb/$st.log" 2>&1 &
  pid=$!
  GROW=0 PREVB=0 off=0 reason=""
  while kill -0 "$pid" 2>/dev/null; do
    sleep 5
    new=$(tail -c +$((off + 1)) "$OUT/omb/$st.log" | grep -a 'Pub rate' || true)
    off=$(wc -c <"$OUT/omb/$st.log")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      check_line "$cons" "$line" || { reason=$REASON; break; }
    done <<<"$new"
    if [ -n "$reason" ]; then
      log "ABORT: $reason (stage $st)"
      echo "$(date -u +%FT%TZ) stage=$st $reason" >"$OUT/ABORT"
      pkill -f io.openmessaging.benchmark.Benchmark || true
      break
    fi
  done
  wait "$pid" 2>/dev/null || true
  end=$(date +%s)
  if [ -n "$reason" ]; then res=ABORT; status=1
  elif [ -s "$OUT/omb/$st.json" ]; then res=ok
  else res=FAILED; status=1; log "stage $st produced no result JSON; tail of its log:"; tail -n 15 "$OUT/omb/$st.log" | tee -a "$LOG" >&2; fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$st" "$(basename "$f")" "$start" "$end" "$res" "$target" "$msz" "$cons" "$COMP" "$st.json" >>"$OUT/omb/stages.tsv"
  if [ "$res" = ok ] && [ "$st" = smoke-100k-produce-only ]; then
    ach=$(jq '[.publishRate[]] | add / length' "$OUT/omb/$st.json")
    if awk -v a="$ach" -v t="$target" 'BEGIN{exit !(a < 0.8*t)}'; then
      log "SMOKE FAIL: achieved $ach msg/s < 80% of $target"; echo "smoke achieved $ach < 80% of $target" >"$OUT/ABORT"; res=ABORT; status=1
    else log "SMOKE PASS: achieved $ach msg/s of $target"; fi
  fi
  log "stage $st finished: $res"
  sleep 12 # let the brokers' snapshotter capture the final state
  sync_up
  [ "$res" = ok ] || break
  sleep 20 # idle gap between stages
done

sync_up
log "done status=$status (ABORT file: $([ -e "$OUT/ABORT" ] && cat "$OUT/ABORT" || echo none))"
exit "$status"
