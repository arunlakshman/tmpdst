#!/usr/bin/env bash
# Start, stop and check control plane nodes.
# On the box it lives in /opt/cpv/scripts. It also runs on a laptop for tests.
#
# Usage:
#   node.sh start NAME RANK GRPC_PORT METRICS_PORT [extra mdscontrolplane flags]
#   node.sh stop NAME              send SIGTERM, wait up to 10 s
#   node.sh kill9 NAME             send SIGKILL (no clean shutdown)
#   node.sh freeze NAME            send SIGSTOP (process stays, does nothing)
#   node.sh thaw NAME              send SIGCONT
#   node.sh wipe NAME              delete the Pebble dir (refused while alive)
#   node.sh state NAME             print running, frozen or stopped
#   node.sh status                 table of all nodes
#   node.sh wait-ready NAME [sec]  wait until the node answers (default 60 s)
#
# Extra flags go after the standard ones. Go's flag package keeps the last
# value, so an extra flag wins. Example (no AWS, in memory only):
#   node.sh start a0 0 17001 17101 -store mem
#
# Env (optional):
#   CPV_ROOT  root directory, default /opt/cpv
#   BIN_DIR   binaries directory, default $CPV_ROOT/bin
set -euo pipefail
unset AWS_PROFILE AWS_DEFAULT_PROFILE   # the box uses its IAM role, not a profile

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
. "$SCRIPT_DIR/defaults.sh"
BIN_DIR="${BIN_DIR:-$CPV_ROOT/bin}"

# The values in $CPV_ROOT/env replace the defaults when that file exists.
if [[ -f $CPV_ROOT/env ]]; then
  . "$CPV_ROOT/env"
fi

RUN_DIR="$CPV_ROOT/run"
LOG_DIR="$CPV_ROOT/logs"
DATA_DIR="$CPV_ROOT/data"

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  cat >&2 <<'EOF'
usage:
  node.sh start NAME RANK GRPC_PORT METRICS_PORT [extra mdscontrolplane flags]
  node.sh stop NAME
  node.sh kill9 NAME
  node.sh freeze NAME
  node.sh thaw NAME
  node.sh wipe NAME
  node.sh state NAME
  node.sh status
  node.sh wait-ready NAME [seconds]
EOF
}

# check_name NAME: the name is used in file names, so keep it simple.
check_name() {
  if ! [[ ${1:-} =~ ^[a-z][a-z0-9-]{0,30}$ ]]; then
    die "bad node name '${1:-}': use lowercase letters, digits and dashes"
  fi
}

# check_number LABEL VALUE: the value must be digits only.
check_number() {
  if ! [[ ${2:-} =~ ^[0-9]+$ ]]; then
    die "$1 must be a number, got '${2:-}'"
  fi
}

pid_file() {
  echo "$RUN_DIR/$1.pid"
}

conf_file() {
  echo "$RUN_DIR/$1.conf"
}

# read_pid NAME: print the saved pid of node NAME. Fails if there is none.
read_pid() {
  local file pid
  file=$(pid_file "$1")
  if [[ ! -f $file ]]; then
    return 1
  fi
  pid=$(cat "$file")
  if ! [[ $pid =~ ^[0-9]+$ ]]; then
    return 1
  fi
  echo "$pid"
}

# node_state NAME: print running, frozen or stopped.
# A process that has exited, or is a zombie, counts as stopped.
node_state() {
  local pid pstat
  if ! pid=$(read_pid "$1"); then
    echo stopped
    return 0
  fi
  pstat=$(ps -o stat= -p "$pid" 2>/dev/null || true)
  pstat=${pstat// /}
  case $pstat in
    "")  echo stopped ;;
    Z*)  echo stopped ;;
    T*)  echo frozen ;;
    *)   echo running ;;
  esac
}

# read_conf NAME: load the rank and ports that "start" saved.
# Sets NODE_RANK, NODE_GRPC_PORT and NODE_METRICS_PORT.
read_conf() {
  local file
  file=$(conf_file "$1")
  if [[ ! -f $file ]]; then
    die "node $1 was never started"
  fi
  read -r NODE_RANK NODE_GRPC_PORT NODE_METRICS_PORT < "$file"
}

# metrics_ok: true when the metrics page has the uptime line.
metrics_ok() {
  local body
  body=$(curl -s --max-time 2 "http://127.0.0.1:$NODE_METRICS_PORT/metrics" || true)
  case $body in
    *mdscontrolplane_uptime_seconds*) return 0 ;;
  esac
  return 1
}

# tcp_open PORT: true when something accepts connections on 127.0.0.1:PORT.
# The gRPC listener opens only after the node has finished starting, so
# this means the node really serves.
tcp_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

cmd_start() {
  local name=${1:-} rank=${2:-} grpc=${3:-} metrics=${4:-} state bin log pid args
  check_name "$name"
  check_number "rank" "$rank"
  check_number "grpc port" "$grpc"
  check_number "metrics port" "$metrics"
  shift 4

  state=$(node_state "$name")
  if [[ $state != stopped ]]; then
    die "node $name is already $state"
  fi
  bin="$BIN_DIR/mdscontrolplane"
  if [[ ! -x $bin ]]; then
    die "no binary at $bin"
  fi
  mkdir -p "$RUN_DIR" "$LOG_DIR" "$DATA_DIR"
  printf '%s %s %s\n' "$rank" "$grpc" "$metrics" > "$(conf_file "$name")"
  log="$LOG_DIR/$name.log"
  echo "=== node.sh start $name $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$log"

  # The standard flags come first. Extra flags come last, so they win.
  args=(
    -store dynamolog
    -region "$REGION"
    -table-prefix "$TABLE_PREFIX"
    -checkpoint-bucket "$BUCKET"
    -log-ttl 24h
    -node-name "$name"
    -rank "$rank"
    -addr "0.0.0.0:$grpc"
    -metrics-addr "127.0.0.1:$metrics"
    -pebble-dir "$DATA_DIR/$name"
    -manifest-out "$RUN_DIR/$name.manifest.json"
    -run-id "cpv1-$name"
  )
  args+=("$@")

  # setsid (Linux only) starts the node in its own session, so it keeps
  # running after the SSM command ends. Output goes to the log file, and
  # stdin is closed, so the caller does not wait for the node.
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup "$bin" "${args[@]}" </dev/null >>"$log" 2>&1 &
  else
    nohup "$bin" "${args[@]}" </dev/null >>"$log" 2>&1 &
  fi
  pid=$!
  echo "$pid" > "$(pid_file "$name")"

  sleep 0.5
  if [[ $(node_state "$name") == stopped ]]; then
    die "node $name exited right after start; see $log"
  fi
  echo "started $name: pid $pid, grpc port $grpc, metrics port $metrics, log $log"
}

# wait_stopped NAME TRIES : check the node every 0.1 s, up to TRIES times.
# Returns 0 when it is stopped, 1 when it still runs after the last try.
wait_stopped() {
  local i
  for i in $(seq 1 "$2"); do
    if [[ $(node_state "$1") == stopped ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

cmd_stop() {
  local name=${1:-} state pid
  check_name "$name"
  state=$(node_state "$name")
  if [[ $state == stopped ]]; then
    echo "$name is already stopped"
    return 0
  fi
  pid=$(read_pid "$name")
  # A frozen process does not act on SIGTERM until it runs again, so wake it first.
  if [[ $state == frozen ]]; then
    kill -CONT "$pid" 2>/dev/null || true
  fi
  kill -TERM "$pid" 2>/dev/null || true
  if wait_stopped "$name" 100; then
    rm -f "$(pid_file "$name")"
    echo "stopped $name"
    return 0
  fi
  die "$name is still running 10 s after SIGTERM. To force it: node.sh kill9 $name"
}

cmd_kill9() {
  local name=${1:-} pid
  check_name "$name"
  if [[ $(node_state "$name") == stopped ]]; then
    die "$name is not running"
  fi
  pid=$(read_pid "$name")
  kill -KILL "$pid" 2>/dev/null || true
  if wait_stopped "$name" 50; then
    rm -f "$(pid_file "$name")"
    echo "killed $name (SIGKILL)"
    return 0
  fi
  die "$name did not die after SIGKILL"
}

cmd_freeze() {
  local name=${1:-} state pid
  check_name "$name"
  state=$(node_state "$name")
  if [[ $state != running ]]; then
    die "$name is $state, not running, so it cannot be frozen"
  fi
  pid=$(read_pid "$name")
  kill -STOP "$pid"
  echo "froze $name (SIGSTOP)"
}

cmd_thaw() {
  local name=${1:-} state pid
  check_name "$name"
  state=$(node_state "$name")
  if [[ $state != frozen ]]; then
    die "$name is $state, not frozen"
  fi
  pid=$(read_pid "$name")
  kill -CONT "$pid"
  echo "thawed $name (SIGCONT)"
}

cmd_wipe() {
  local name=${1:-} state
  check_name "$name"
  state=$(node_state "$name")
  if [[ $state != stopped ]]; then
    die "refusing to wipe $name: it is $state. Stop it first."
  fi
  rm -rf -- "$DATA_DIR/$name"
  echo "wiped $DATA_DIR/$name"
}

cmd_state() {
  check_name "${1:-}"
  node_state "$1"
}

cmd_status() {
  local f name state pid count=0
  shopt -s nullglob
  printf '%-12s %-5s %-8s %-8s %-10s %-10s\n' NAME RANK PID STATE GRPC METRICS
  for f in "$RUN_DIR"/*.conf; do
    name=$(basename "$f" .conf)
    read_conf "$name"
    state=$(node_state "$name")
    pid="-"
    if [[ $state != stopped ]]; then
      pid=$(read_pid "$name")
    fi
    printf '%-12s %-5s %-8s %-8s %-10s %-10s\n' "$name" "$NODE_RANK" "$pid" \
      "$state" "$NODE_GRPC_PORT" "$NODE_METRICS_PORT"
    count=$((count + 1))
  done
  if [[ $count -eq 0 ]]; then
    echo "(no nodes yet)"
  fi
}

# verify_ready: asks mdscpverify for "-mode ready" on the node's gRPC port.
# Returns 0 when it passes, 1 when it fails (not ready yet), and 2 when this
# mdscpverify build has no ready mode (the metrics and TCP checks decide).
verify_ready() {
  local out
  if [[ ! -x $BIN_DIR/mdscpverify ]]; then
    return 2
  fi
  out=$("$BIN_DIR/mdscpverify" -mode ready -cp "127.0.0.1:$NODE_GRPC_PORT" 2>&1) && return 0
  case $out in
    *"unknown -mode"*) return 2 ;;
  esac
  return 1
}

cmd_wait_ready() {
  local name=${1:-} secs=${2:-60} deadline state rc
  check_name "$name"
  check_number "seconds" "$secs"
  read_conf "$name"

  deadline=$(( $(date +%s) + secs ))
  while :; do
    state=$(node_state "$name")
    if [[ $state == stopped ]]; then
      die "$name exited while waiting. See $LOG_DIR/$name.log"
    fi
    if [[ $state == running ]] && metrics_ok && tcp_open "$NODE_GRPC_PORT"; then
      rc=0
      verify_ready || rc=$?
      if [[ $rc -eq 0 || $rc -eq 2 ]]; then
        echo "$name is ready"
        return 0
      fi
    fi
    if [[ $(date +%s) -ge $deadline ]]; then
      die "$name is not ready after $secs s. See $LOG_DIR/$name.log"
    fi
    sleep 1
  done
}

cmd=${1:-}
if [[ $# -gt 0 ]]; then
  shift
fi
case $cmd in
  start)      cmd_start "$@" ;;
  stop)       cmd_stop "$@" ;;
  kill9)      cmd_kill9 "$@" ;;
  freeze)     cmd_freeze "$@" ;;
  thaw)       cmd_thaw "$@" ;;
  wipe)       cmd_wipe "$@" ;;
  state)      cmd_state "$@" ;;
  status)     cmd_status ;;
  wait-ready) cmd_wait_ready "$@" ;;
  *)          usage; exit 2 ;;
esac
