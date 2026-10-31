#!/usr/bin/env bash
# End-to-end check: run mdscontrolplane, three mdsbrokers and a local S3 server (minio,
# from mise.toml) from this tree, and drive the brokers with the Apache Kafka
# 3.9.1 CLI tools (fetched by fetch-kafka-cli.sh). The CLI tools need Java (see
# mise.toml).
# Not part of `make build`: run it with `make e2e-kafka-cli`. Pass flags with
# `make e2e-kafka-cli E2E_ARGS=-v`, or run this script directly. See --help.
set -euo pipefail
cd "$(dirname "$0")/../.."

usage() {
  cat <<'EOF'
Usage: ops/scripts/e2e-kafka-cli.sh [-v] [--keep]
       ops/scripts/e2e-kafka-cli.sh -h

Runs mdscontrolplane, three mdsbrokers and a local minio (S3) from this tree, then runs
the checks in order with the Apache Kafka 3.9.1 CLI tools. Each check prints
PASS or FAIL. Needs Java 21 (see mise.toml).

Options:
  -v, --verbose  For each check, print its header, the Kafka CLI command line
                 (with the bootstrap address), the CLI stdout and stderr, the
                 numbers the check compares, and the time it took. Also prints
                 the ports and log files at start, and a summary table at the
                 end. Same as E2E_VERBOSE=1.
  --keep         Keep the temp dir (binaries and logs) even when all checks
                 pass. A failed run always keeps it.
  -h, --help     Print this help and exit.
  Any other flag prints this help and exits 2.

Logs: a new temp dir under ${TMPDIR:-/tmp} named e2e-kafka-cli.XXXXXX. It holds
mdscontrolplane.log, minio.log, broker-<id>.log, and one output file per CLI call. When
the run fails (or with --keep), the script prints the paths of the main logs.
EOF
}

# Flags. Verbose can also come from the environment (E2E_VERBOSE=1).
VERBOSE=0
KEEP=0
if [[ ${E2E_VERBOSE:-} == 1 ]]; then
  VERBOSE=1
fi
while [[ $# -gt 0 ]]; do
  case $1 in
    -v | --verbose) VERBOSE=1 ;;
    --keep) KEEP=1 ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

# Every run gets its own temp dir for binaries and logs.
TMP=$(mktemp -d "${TMPDIR:-/tmp}/e2e-kafka-cli.XXXXXX")
CP_LOG=$TMP/mdscontrolplane.log
S3_LOG=$TMP/minio.log
CP_PID=
S3_PID=
CP_PORT=
S3_PORT=
BROKER3_METRICS_PORT=
CONSUMER_PID=
GROUP_PIDS=()
BIN_DIR=
FAILED=0
CHECK_NAMES=()
CHECK_RESULTS=()
CHECK_SECS=()

# The three brokers have ids 1, 2 and 3. Index i-1 holds broker i.
BROKER_PIDS=("" "" "")
BROKER_PORTS=("" "" "")

# Fixed S3 keys for the local minio. They only protect a test server.
S3_USER=e2eadmin
S3_SECRET=e2e-secret-key

# Stop the brokers first (they talk to the control plane), then mdscontrolplane and minio.
stop_stack() {
  local pid i
  if [[ -n $CONSUMER_PID ]] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill -TERM "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
  for pid in "${GROUP_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  for i in 0 1 2; do
    pid=${BROKER_PIDS[i]}
    if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$CP_PID" "$S3_PID"; do
    if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}

cleanup() {
  local status=$?
  stop_stack
  if [[ $status -eq 0 && $KEEP -eq 0 ]]; then
    rm -rf "$TMP"
  else
    echo "Logs and binaries kept in: $TMP" >&2
    echo "  mdscontrolplane:     $CP_LOG" >&2
    echo "  minio:      $S3_LOG" >&2
    echo "  mdsbroker: $TMP/broker-<id>.log" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

# Verbose output goes to stderr. The checks use command substitution for some
# values, and that only captures stdout, so the detail stays out of the values.

# Print a line in verbose mode only.
vlog() {
  if [[ $VERBOSE == 1 ]]; then
    echo "$@" >&2
  fi
  return 0
}

# Print a Kafka CLI command line in verbose mode, with a two space indent.
show_cmd() {
  if [[ $VERBOSE == 1 ]]; then
    echo "  \$ $*" >&2
  fi
  return 0
}

# Print file $2 under the label $1, each line indented by two spaces (verbose only).
show_file() {
  local label=$1 file=$2
  if [[ $VERBOSE != 1 ]]; then
    return 0
  fi
  echo "  $label:" >&2
  if [[ -s $file ]]; then
    sed 's/^/    /' "$file" >&2
  else
    echo "    (none)" >&2
  fi
  return 0
}

# Run one Kafka CLI command. $1 is the file for its stdout and $2 the file for
# its stderr. If $2 is the same as $1, both go to that one file. The rest is the
# command. Returns the command's exit code, so the caller decides if it failed.
# In verbose mode it prints the command line first, then the output.
run_cli() {
  local out=$1 err=$2 rc=0
  shift 2
  show_cmd "$@"
  if [[ $err == "$out" ]]; then
    "$@" >"$out" 2>&1 || rc=$?
    show_file "output" "$out"
  else
    "$@" >"$out" 2>"$err" || rc=$?
    show_file "stdout" "$out"
    show_file "stderr" "$err"
  fi
  return "$rc"
}

# Print a free TCP port on 127.0.0.1.
free_port() {
  python3 -I -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

# Wait until 127.0.0.1:port accepts a connection. Fails if pid exits first.
wait_for_port() {
  local port=$1 pid=$2 i
  for i in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "process $pid exited before port $port opened" >&2
      return 1
    fi
    sleep 0.5
  done
  echo "port $port did not open in 30s" >&2
  return 1
}

build_binaries() {
  go build -o "$TMP/mdscontrolplane" ./cmd/mdscontrolplane || return 1
  go build -o "$TMP/mdsbroker" ./cmd/mdsbroker || return 1
}

# The minio binary: on PATH, else the one mise installs (see mise.toml).
minio_bin() {
  command -v minio 2>/dev/null || mise which minio
}

# Start a local S3 server for the brokers' shared object store.
start_s3() {
  local bin
  bin=$(minio_bin) || {
    echo "minio not found: run mise install (see mise.toml)" >&2
    return 1
  }
  S3_PORT=$(free_port) || return 1
  mkdir -p "$TMP/s3data"
  MINIO_ROOT_USER=$S3_USER MINIO_ROOT_PASSWORD=$S3_SECRET MINIO_UPDATE=off MINIO_BROWSER=off \
    "$bin" server "$TMP/s3data" --address "127.0.0.1:$S3_PORT" --console-address "127.0.0.1:$(free_port)" \
    >"$S3_LOG" 2>&1 &
  S3_PID=$!
  wait_for_port "$S3_PORT" "$S3_PID"
}

# Start broker $1 (id) on its own port. Extra flags come after $1.
# All brokers share one S3 store and one control plane.
start_broker() {
  local id=$1 port
  shift
  port=${BROKER_PORTS[id - 1]}
  AWS_ACCESS_KEY_ID=$S3_USER AWS_SECRET_ACCESS_KEY=$S3_SECRET \
    "$TMP/mdsbroker" -objects s3 -s3-endpoint "http://127.0.0.1:$S3_PORT" -s3-path-style \
    -broker-id "$id" -listen "127.0.0.1:$port" -cp-url "127.0.0.1:$CP_PORT" \
    -lease-interval 1s -metrics-addr "" "$@" \
    >"$TMP/broker-$id.log" 2>&1 &
  BROKER_PIDS[id - 1]=$!
  wait_for_port "$port" "${BROKER_PIDS[id - 1]}"
}

# Build, start minio, mdscontrolplane (in-memory store), and three mdsbrokers. Broker 1
# creates the bucket. Every broker uses the same bucket.
start_stack() {
  build_binaries || return 1
  CP_PORT=$(free_port) || return 1
  local i
  for i in 0 1 2; do
    BROKER_PORTS[i]=$(free_port) || return 1
  done
  start_s3 || return 1

  "$TMP/mdscontrolplane" -store mem -addr "127.0.0.1:$CP_PORT" -metrics-addr "" -broker-lease-ttl 5s \
    >"$CP_LOG" 2>&1 &
  CP_PID=$!
  wait_for_port "$CP_PORT" "$CP_PID" || return 1

  start_broker 1 -s3-create-bucket || return 1
  start_broker 2 || return 1
  # Broker 3 serves the idle consumer, so its metrics count that consumer's Lookups.
  BROKER3_METRICS_PORT=$(free_port) || return 1
  start_broker 3 -metrics-addr "127.0.0.1:$BROKER3_METRICS_PORT" || return 1
}

# Run kafka-topics.sh against broker 1 (the bootstrap broker). $1 and $2 are the
# stdout and stderr files, as for run_cli.
kafka_topics() {
  local out=$1 err=$2
  shift 2
  run_cli "$out" "$err" "$BIN_DIR/kafka-topics.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[0]}" "$@"
}

TOPIC=e2e-basic
PARTS=3

# Print the ids of the brokers that Metadata lists, sorted, space separated.
# The bootstrap is the broker on port $1. kafka-broker-api-versions.sh asks
# for the cluster and prints one line per broker it gets back.
listed_ids() {
  local port=$1 out="$TMP/apiversions-$1.out"
  run_cli "$out" "$TMP/apiversions-$1.err" "$BIN_DIR/kafka-broker-api-versions.sh" \
    --bootstrap-server "127.0.0.1:$port" || true
  sed -n 's/.*(id: \([0-9]*\) .*/\1/p' "$out" | sort -n | tr '\n' ' ' | sed 's/ *$//'
}

# Metadata from broker 1 lists all three brokers. The live set fills within a
# heartbeat or two after start, so poll for up to 20s.
check_three_brokers_listed() {
  local got i
  for i in $(seq 1 40); do
    got=$(listed_ids "${BROKER_PORTS[0]}")
    if [[ $got == "1 2 3" ]]; then
      vlog "  broker ids listed by broker 1: [$got] (want [1 2 3])"
      return 0
    fi
    sleep 0.5
  done
  echo "metadata from broker 1 lists [$got], want [1 2 3]" >&2
  return 1
}

# Create the topic with Kafka CreateTopics (kafka-topics.sh --create).
check_create_topic() {
  kafka_topics "$TMP/create.out" "$TMP/create.out" --create --topic "$TOPIC" \
    --partitions "$PARTS" --replication-factor 3 || {
    echo "--create failed, see $TMP/create.out" >&2
    return 1
  }
}

# The topic is in the topic list.
check_list_topic() {
  kafka_topics "$TMP/list.out" "$TMP/list.out" --list || {
    echo "--list failed, see $TMP/list.out" >&2
    return 1
  }
  if ! grep -qx "$TOPIC" "$TMP/list.out"; then
    echo "--list does not show $TOPIC, see $TMP/list.out" >&2
    return 1
  fi
  vlog "  topic $TOPIC is in the list"
}

# The topic has 3 partitions, one row per partition.
check_describe_topic() {
  kafka_topics "$TMP/describe.out" "$TMP/describe.out" --describe --topic "$TOPIC" || {
    echo "--describe failed, see $TMP/describe.out" >&2
    return 1
  }
  local rows
  rows=$(grep -cE 'Partition: [0-9]+' "$TMP/describe.out" || true)
  vlog "  partition rows: $rows (want $PARTS)"
  if ! grep -q "PartitionCount: $PARTS" "$TMP/describe.out" || [[ $rows != "$PARTS" ]]; then
    echo "--describe does not show $PARTS partitions ($rows rows), see $TMP/describe.out" >&2
    return 1
  fi
}

# A second create must fail: the topic already exists (TOPIC_ALREADY_EXISTS).
check_create_again_fails() {
  if kafka_topics "$TMP/create-again.out" "$TMP/create-again.out" --create --topic "$TOPIC" \
    --partitions "$PARTS" --replication-factor 3; then
    echo "second --create of $TOPIC succeeded, want TOPIC_ALREADY_EXISTS" >&2
    return 1
  fi
  if ! grep -q "already exists" "$TMP/create-again.out"; then
    echo "second --create failed without 'already exists', see $TMP/create-again.out" >&2
    return 1
  fi
}

# Produce 100 lines through broker 1. Read them back from all partitions through
# broker 3, so the data crosses brokers. The round robin partitioner spreads the
# lines over the partitions. Each partition is read by assign (--partition), since
# group consume is not served yet.
check_produce_consume() {
  local sent=$TMP/sent.txt got=$TMP/got.txt n p count
  local produce_to=127.0.0.1:${BROKER_PORTS[0]}
  local consume_from=127.0.0.1:${BROKER_PORTS[2]}

  for n in $(seq 1 100); do
    echo "line $n"
  done >"$sent"

  run_cli "$TMP/producer.out" "$TMP/producer.err" "$BIN_DIR/kafka-console-producer.sh" \
    --bootstrap-server "$produce_to" \
    --topic "$TOPIC" --producer-property enable.idempotence=false \
    --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
    <"$sent" || {
    echo "producer failed, see $TMP/producer.err" >&2
    return 1
  }

  : >"$got"
  for p in $(seq 0 $((PARTS - 1))); do
    run_cli "$TMP/consume-$p.out" "$TMP/consumer-$p.err" "$BIN_DIR/kafka-console-consumer.sh" \
      --bootstrap-server "$consume_from" \
      --topic "$TOPIC" --partition "$p" --offset earliest \
      --timeout-ms 5000 || {
      echo "consumer of partition $p failed, see $TMP/consumer-$p.err" >&2
      return 1
    }
    cat "$TMP/consume-$p.out" >>"$got"
  done

  count=$(wc -l <"$got" | tr -d ' ')
  vlog "  lines read back: $count (want 100)"
  if [[ $count != 100 ]]; then
    echo "expected 100 lines back, got $count" >&2
    return 1
  fi
  if ! diff <(sort "$sent") <(sort "$got") >"$TMP/diff.txt"; then
    echo "lines read back differ from lines sent, see $TMP/diff.txt" >&2
    return 1
  fi
}

# Print how many control plane Lookups broker 3 has made (its metrics: one
# cp_call_seconds sample per gRPC call). Prints 0 before the first one.
cp_lookups() {
  local n
  n=$(curl -s "http://127.0.0.1:$BROKER3_METRICS_PORT/metrics" |
    sed -n 's/^mdsbroker_cp_call_seconds_count{op="Lookup"} //p' | head -n 1) || true
  echo "${n:-0}"
}

# Print the time in seconds, with decimals.
now_s() {
  python3 -I -c 'import time; print(time.time())'
}

LONGPOLL=e2e-longpoll

# A console consumer on an empty partition starts at the latest offset, so it
# prints only what is produced after it starts. Its Fetch waits at the broker
# (MaxWait 500 ms, MinBytes 1), so the new lines must show within 2 s after the
# producer returns (its acks). The consumer keeps running for the idle check.
check_long_poll_wakes() {
  local sent=$TMP/longpoll-sent.txt out=$TMP/longpoll.out base i n t0 t1
  kafka_topics "$TMP/longpoll-create.out" "$TMP/longpoll-create.out" --create \
    --topic "$LONGPOLL" --partitions 1 --replication-factor 3 || {
    echo "--create $LONGPOLL failed, see $TMP/longpoll-create.out" >&2
    return 1
  }
  show_cmd "$BIN_DIR/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[2]}" \
    --topic "$LONGPOLL" --partition 0 --offset latest "(runs in the background)"
  "$BIN_DIR/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[2]}" \
    --topic "$LONGPOLL" --partition 0 --offset latest >"$out" 2>"$TMP/longpoll.err" &
  CONSUMER_PID=$!

  # The consumer lists its offsets (one Lookup) and then sends its first Fetch
  # (one more). Wait for both, so its start point is set before the produce.
  base=$(cp_lookups)
  vlog "  broker 3 Lookups before the consumer started: $base"
  for i in $(seq 1 60); do
    if (( $(cp_lookups) >= base + 2 )); then
      break
    fi
    sleep 0.5
  done
  if (( $(cp_lookups) < base + 2 )); then
    echo "consumer made no Fetch within 30 s (broker 3 Lookups: $base before it started)" >&2
    return 1
  fi
  sleep 3

  for n in 1 2 3 4 5; do
    echo "longpoll line $n"
  done >"$sent"
  run_cli "$TMP/longpoll-producer.out" "$TMP/longpoll-producer.err" \
    "$BIN_DIR/kafka-console-producer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[0]}" \
    --topic "$LONGPOLL" --producer-property enable.idempotence=false \
    <"$sent" || {
    echo "producer failed, see $TMP/longpoll-producer.err" >&2
    return 1
  }

  t0=$(now_s)
  for i in $(seq 1 20); do # 20 x 0.1 s = 2 s
    if [[ $(wc -l <"$out" | tr -d ' ') -ge 5 ]]; then
      break
    fi
    sleep 0.1
  done
  t1=$(now_s)
  n=$(wc -l <"$out" | tr -d ' ')
  show_file "background consumer stdout" "$out"
  show_file "background consumer stderr" "$TMP/longpoll.err"
  vlog "  lines seen within 2 s: $n (want 5)"
  if [[ $n -lt 5 ]]; then
    echo "consumer printed $n lines within 2 s after the producer returned, want 5, see $out" >&2
    return 1
  fi
  if ! diff "$sent" <(head -n 5 "$out") >"$TMP/longpoll-diff.txt"; then
    echo "lines seen differ from lines produced, see $TMP/longpoll-diff.txt" >&2
    return 1
  fi
  echo "5 lines seen $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%d", (b - a) * 1000 }') ms after the produce"
}

# One idle consumer on one partition: a Fetch waits 500 ms and is polled every
# 200 ms, so about 6 Lookups a second. Over 5 s that is about 30. The limit is 60.
check_idle_lookups() {
  local before after n
  if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
    echo "consumer $CONSUMER_PID is not running" >&2
    return 1
  fi
  before=$(cp_lookups)
  sleep 5
  after=$(cp_lookups)
  n=$((after - before))
  echo "$n Lookups in 5 s from one idle consumer on one partition (limit 60)"
  if [[ $n -gt 60 ]]; then
    echo "idle consumer made $n Lookups in 5 s, want at most 60" >&2
    return 1
  fi
}

# Produce the lines of file $2 to topic $1 through broker 1. The round robin
# partitioner spreads the lines over the partitions. $3 names the output files.
produce_file() {
  run_cli "$TMP/$3.out" "$TMP/$3.err" "$BIN_DIR/kafka-console-producer.sh" \
    --bootstrap-server "127.0.0.1:${BROKER_PORTS[0]}" \
    --topic "$1" --producer-property enable.idempotence=false \
    --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
    <"$2"
}

GROUP_TOPIC=e2e-groups

# A group consumer reads 100 lines through broker 1 and commits them. The
# consumer commits when it exits (auto commit on close).
check_group_consume() {
  local sent=$TMP/group-sent.txt got=$TMP/group-g1.txt n count
  kafka_topics "$TMP/group-create.out" "$TMP/group-create.out" --create --topic "$GROUP_TOPIC" \
    --partitions "$PARTS" --replication-factor 3 || {
    echo "--create $GROUP_TOPIC failed, see $TMP/group-create.out" >&2
    return 1
  }
  for n in $(seq 1 100); do
    echo "group line $n"
  done >"$sent"
  produce_file "$GROUP_TOPIC" "$sent" group-produce || {
    echo "producer failed, see $TMP/group-produce.err" >&2
    return 1
  }
  run_cli "$got" "$TMP/group-g1.err" "$BIN_DIR/kafka-console-consumer.sh" \
    --bootstrap-server "127.0.0.1:${BROKER_PORTS[0]}" \
    --topic "$GROUP_TOPIC" --group g1 --from-beginning --max-messages 100 --timeout-ms 40000 \
    --consumer-property auto.commit.interval.ms=500 || {
    echo "group consumer failed, see $TMP/group-g1.err" >&2
    return 1
  }
  count=$(wc -l <"$got" | tr -d ' ')
  vlog "  lines read by group g1: $count (want 100)"
  if [[ $count != 100 ]]; then
    echo "group consumer printed $count lines, want 100, see $got" >&2
    return 1
  fi
  if ! diff <(sort "$sent") <(sort "$got") >"$TMP/group-diff.txt"; then
    echo "lines read by the group differ from lines sent, see $TMP/group-diff.txt" >&2
    return 1
  fi
}

# Describe g1 through broker 3: the 3 partitions hold 100 committed in total,
# and the lag is 0.
check_group_offsets() {
  local out=$TMP/group-describe-g1.out sums
  run_cli "$out" "$out" "$BIN_DIR/kafka-consumer-groups.sh" \
    --bootstrap-server "127.0.0.1:${BROKER_PORTS[2]}" \
    --describe --group g1 || {
    echo "--describe g1 failed, see $out" >&2
    return 1
  }
  sums=$(awk -v t="$GROUP_TOPIC" '$2 == t && $3 ~ /^[0-9]+$/ { cur += $4; lag += $6; rows++ } END { print cur + 0, lag + 0, rows + 0 }' "$out")
  vlog "  committed sum, lag sum, partition rows: [$sums] (want [100 0 3])"
  if [[ $sums != "100 0 3" ]]; then
    echo "describe g1: current offset, lag, partitions = [$sums], want [100 0 3], see $out" >&2
    return 1
  fi
}

# The list shows g1 once. Broker 2 asks every broker, as the admin client does.
check_group_listed() {
  local n
  run_cli "$TMP/group-list.out" "$TMP/group-list.out" "$BIN_DIR/kafka-consumer-groups.sh" \
    --bootstrap-server "127.0.0.1:${BROKER_PORTS[1]}" \
    --list || {
    echo "--list failed, see $TMP/group-list.out" >&2
    return 1
  }
  n=$(grep -cx g1 "$TMP/group-list.out" || true)
  vlog "  lines that are exactly g1: $n (want 1)"
  if [[ $n != 1 ]]; then
    echo "--list shows g1 $n times, want 1, see $TMP/group-list.out" >&2
    return 1
  fi
}

# After a restart (a new consumer process with the same group), only the 20 new
# lines come back, not the 100 committed before.
check_group_restart() {
  local sent=$TMP/group-again.txt got=$TMP/group-g1-again.txt n count
  for n in $(seq 1 20); do
    echo "again line $n"
  done >"$sent"
  produce_file "$GROUP_TOPIC" "$sent" group-again-produce || {
    echo "producer failed, see $TMP/group-again-produce.err" >&2
    return 1
  }
  run_cli "$got" "$TMP/group-g1-again.err" "$BIN_DIR/kafka-console-consumer.sh" \
    --bootstrap-server "127.0.0.1:${BROKER_PORTS[1]}" \
    --topic "$GROUP_TOPIC" --group g1 --max-messages 20 --timeout-ms 40000 \
    --consumer-property auto.commit.interval.ms=500 || {
    echo "group consumer after restart failed, see $TMP/group-g1-again.err" >&2
    return 1
  }
  count=$(wc -l <"$got" | tr -d ' ')
  vlog "  lines read after the restart: $count (want 20)"
  if [[ $count != 20 ]]; then
    echo "group consumer after restart printed $count lines, want 20, see $got" >&2
    return 1
  fi
  if ! diff <(sort "$sent") <(sort "$got") >"$TMP/group-again-diff.txt"; then
    echo "lines after restart differ from the new lines, see $TMP/group-again-diff.txt" >&2
    return 1
  fi
}

# Print the members of group $1 as seen from broker port $2, then the summary:
# the member count and the partitions they hold, sorted, one entry per partition.
group_summary() {
  local group=$1 port=$2 out=$TMP/members-$1.out
  run_cli "$out" "$out" "$BIN_DIR/kafka-consumer-groups.sh" \
    --bootstrap-server "127.0.0.1:$port" \
    --describe --group "$group" --members --verbose || true
  # A member row is: group, consumer id, host, client id, #partitions, ASSIGNMENT
  # where ASSIGNMENT is "(0,1,2)". Members without a partition show "-".
  awk -v g="$group" '
    $1 == g && $NF ~ /^\(.*\)$/ {
      members++
      s = $NF
      gsub(/[()]/, "", s)
      n = split(s, p, ",")
      for (i = 1; i <= n; i++) if (p[i] != "") print "PART", p[i]
    }
    END { print "MEMBERS", members + 0 }
  ' "$out" | sort -k1,1 -k2,2n | awk '
    /^MEMBERS/ { members = $2 }
    /^PART/ { parts = parts (parts == "" ? "" : ",") $2 }
    END { print members + 0, parts }
  '
}

# Wait until group $1 shows $2 members and partitions $3 (for example "2 0,1,2").
wait_group_summary() {
  local group=$1 port=$2 want=$3 got i
  for i in $(seq 1 45); do
    got=$(group_summary "$group" "$port")
    if [[ $got == "$want" ]]; then
      vlog "  group $group via port $port: [$got] (want [$want])"
      return 0
    fi
    sleep 2
  done
  echo "group $group shows [$got], want [$want], see $TMP/members-$group.out" >&2
  return 1
}

# Two members share the 3 partitions: start one, then a second (through broker
# 3), and each partition is held once. Then kill -9 the second: after the
# 6 s session timeout the first owns all 3.
check_group_rebalance() {
  local a=$TMP/group2-a.out b=$TMP/group2-b.out pid_b
  show_cmd "$BIN_DIR/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[0]}" \
    --topic "$GROUP_TOPIC" --group g2 --consumer-property session.timeout.ms=6000 \
    "(runs in the background)"
  "$BIN_DIR/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[0]}" \
    --topic "$GROUP_TOPIC" --group g2 --consumer-property session.timeout.ms=6000 \
    >"$a" 2>"$TMP/group2-a.err" &
  GROUP_PIDS+=("$!")
  wait_group_summary g2 "${BROKER_PORTS[0]}" "1 0,1,2" || return 1

  show_cmd "$BIN_DIR/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[2]}" \
    --topic "$GROUP_TOPIC" --group g2 --consumer-property session.timeout.ms=6000 \
    "(runs in the background)"
  "$BIN_DIR/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:${BROKER_PORTS[2]}" \
    --topic "$GROUP_TOPIC" --group g2 --consumer-property session.timeout.ms=6000 \
    >"$b" 2>"$TMP/group2-b.err" &
  pid_b=$!
  GROUP_PIDS+=("$pid_b")
  wait_group_summary g2 "${BROKER_PORTS[2]}" "2 0,1,2" || return 1

  kill -9 "$pid_b" 2>/dev/null || true
  wait "$pid_b" 2>/dev/null || true
  wait_group_summary g2 "${BROKER_PORTS[0]}" "1 0,1,2" || return 1
  show_file "consumer a stdout" "$a"
  show_file "consumer b stdout" "$b"
}

# Kill broker 2 with SIGKILL. Its lease runs out after the 5s TTL. After the
# TTL and 2s more, Metadata from broker 1 lists only brokers 1 and 3.
check_lost_broker_leaves() {
  kill -9 "${BROKER_PIDS[1]}" 2>/dev/null || true
  wait "${BROKER_PIDS[1]}" 2>/dev/null || true
  BROKER_PIDS[1]=
  sleep 7
  local got
  got=$(listed_ids "${BROKER_PORTS[0]}")
  vlog "  broker ids listed by broker 1 after broker 2 died: [$got] (want [1 3])"
  if [[ $got != "1 3" ]]; then
    echo "after broker 2 died, metadata lists [$got], want [1 3]" >&2
    return 1
  fi
}

# Run one named check. Prints PASS or FAIL and counts failures. In verbose mode
# it also prints the header, the time the check took, and keeps a row for the
# summary table.
run_check() {
  local name=$1 fn=$2 t0 t1 secs result
  vlog "== check: $name =="
  t0=$(now_s)
  if "$fn"; then
    result=PASS
  else
    result=FAIL
    FAILED=$((FAILED + 1))
  fi
  t1=$(now_s)
  secs=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }')
  CHECK_NAMES+=("$name")
  CHECK_RESULTS+=("$result")
  CHECK_SECS+=("$secs")
  if [[ $VERBOSE == 1 ]]; then
    echo "$result $name ($secs s)"
  else
    echo "$result $name"
  fi
}

# Print the summary table: check name, PASS or FAIL, seconds. Verbose only.
print_summary() {
  local i
  if [[ $VERBOSE != 1 ]]; then
    return 0
  fi
  echo "summary:"
  printf '  %-78s %-6s %8s\n' "check" "result" "seconds"
  for i in "${!CHECK_NAMES[@]}"; do
    printf '  %-78s %-6s %8s\n' "${CHECK_NAMES[i]}" "${CHECK_RESULTS[i]}" "${CHECK_SECS[i]}"
  done
}

main() {
  local t0 t1 secs i
  if ! command -v java >/dev/null 2>&1; then
    echo "java not found: the Kafka CLI needs Java 21 (see mise.toml)" >&2
    exit 1
  fi

  # Last line of the fetch output is the bin dir.
  BIN_DIR=$(bash ops/scripts/fetch-kafka-cli.sh | tail -n 1)

  t0=$(now_s)
  if ! start_stack; then
    echo "FAIL start stack"
    exit 1
  fi
  echo "stack up: control plane 127.0.0.1:$CP_PORT, s3 127.0.0.1:$S3_PORT," \
    "brokers ${BROKER_PORTS[0]}, ${BROKER_PORTS[1]}, ${BROKER_PORTS[2]}"
  if [[ $VERBOSE == 1 ]]; then
    t1=$(now_s)
    secs=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }')
    vlog "  stack started in $secs s"
    vlog "  mdscontrolplane: port $CP_PORT, log $CP_LOG"
    vlog "  minio: port $S3_PORT, log $S3_LOG"
    for i in 0 1 2; do
      vlog "  mdsbroker $((i + 1)): port ${BROKER_PORTS[i]}, log $TMP/broker-$((i + 1)).log"
    done
  fi

  run_check "metadata from broker 1 lists all 3 brokers" check_three_brokers_listed
  run_check "create topic with 3 partitions and replication factor 3" check_create_topic
  run_check "list shows the topic" check_list_topic
  run_check "describe shows 3 partitions" check_describe_topic
  run_check "create again fails with TOPIC_ALREADY_EXISTS" check_create_again_fails
  run_check "produce via broker 1, read 100 lines via broker 3" check_produce_consume
  run_check "long poll: consumer on an empty partition sees 5 new lines within 2 s" check_long_poll_wakes
  run_check "idle consumer makes at most 60 Lookups in 5 s" check_idle_lookups
  run_check "group consume: 100 lines with --group g1 via broker 1" check_group_consume
  run_check "describe g1 via broker 3: 100 committed, lag 0" check_group_offsets
  run_check "--list shows g1 once via broker 2" check_group_listed
  run_check "group restart: only the 20 new lines" check_group_restart
  run_check "two members share 3 partitions; kill -9 one, the other owns all 3" check_group_rebalance
  run_check "after broker 2 dies, metadata lists only brokers 1 and 3" check_lost_broker_leaves

  print_summary
  if [[ $FAILED -ne 0 ]]; then
    exit 1
  fi
  echo "all checks passed"
}

main
