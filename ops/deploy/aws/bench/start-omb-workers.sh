#!/usr/bin/env bash
# Start (or restart) the OMB worker JVM on THIS loadgen host. Run on every loadgen host, as root:
#   sudo /opt/bench/start-omb-workers.sh [start|stop|status]
# bench-ctl.sh start-workers does that for all loadgen hosts from the operator workstation through
# SSM send-command (there is no SSH). The worker listens on :8080 (HTTP, coordinator -> worker) and
# :9090 (Prometheus stats); the loadgen SG allows both between loadgen hosts.
#
# OMB gives the FIRST half of the workers (by --workers order) to producers and the LAST half
# (W - W/2) to consumers; the Kafka driver patch needs this host's index among the consumer workers,
# passed as -Dmds.worker.index.
set -euo pipefail
cmd=${1:-start}
# shellcheck disable=SC1091
[ -f /etc/profile.d/bench.sh ] && . /etc/profile.d/bench.sh
: "${LOADGEN_INDEX:?}" "${LOADGEN_COUNT:?}"
OMB=${OMB:-/opt/omb}
LOG=${OMB_WORKER_LOG:-/var/log/omb-worker.log}
PIDF=/var/run/omb-worker.pid

stop() {
  if [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then
    kill "$(cat "$PIDF")" || true
    for _ in $(seq 1 20); do kill -0 "$(cat "$PIDF")" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$(cat "$PIDF")" 2>/dev/null || true
  fi
  rm -f "$PIDF"
}
case "$cmd" in
  stop) stop; echo "stopped"; exit 0 ;;
  status)
    if [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then echo "worker $LOADGEN_INDEX running pid $(cat "$PIDF")"; else echo "worker $LOADGEN_INDEX NOT running"; exit 1; fi
    exit 0 ;;
  start) ;;
  *) echo "usage: $0 [start|stop|status]" >&2; exit 2 ;;
esac
[ -x "$OMB/bin/benchmark-worker" ] || { echo "$OMB/bin/benchmark-worker missing: upload artifacts/omb/openmessaging-benchmark.tar.gz (ops/scripts/build-omb.sh) and re-extract" >&2; exit 1; }
stop
producers=$((LOADGEN_COUNT / 2))
idx=$((LOADGEN_INDEX - producers))
[ "$idx" -ge 0 ] || idx=0   # a producer-only worker never builds consumers
# Heap: 64 producers x buffer.memory 128 MB can need 8 GB across the producer workers.
export HEAP_OPTS="${OMB_WORKER_HEAP:--Xms16g -Xmx16g}"
export JVM_OPTS="${JVM_OPTS:-} -Dmds.worker.index=$idx -Dmds.loadgen.index=$LOADGEN_INDEX"
cd "$OMB"
nohup bin/benchmark-worker --port 8080 --stats-port 9090 >>"$LOG" 2>&1 &
echo $! >"$PIDF"
sleep 3
kill -0 "$(cat "$PIDF")" 2>/dev/null || { echo "worker died at start; tail of $LOG:" >&2; tail -n 20 "$LOG" >&2; exit 1; }
echo "worker $LOADGEN_INDEX/$LOADGEN_COUNT started pid $(cat "$PIDF") (consumer-worker index $idx, heap $HEAP_OPTS), log $LOG"
