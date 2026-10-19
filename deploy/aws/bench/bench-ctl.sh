#!/usr/bin/env bash
# Operator-workstation control for the 6-broker OMB run. Everything goes through SSM send-command (no SSH).
# Needs: aws CLI v2 with credentials, jq, terraform outputs (broker_instance_ids, loadgen_instance_ids).
#
#   bench-ctl.sh [--tf-dir DIR] [--region R] [--run-id ID] <command> [args]
#
#   create-topic [PARTITIONS=288] [TOPIC]   POST /v1/create-topic on broker 0's loopback manicp (any manicp works:
#                                           all share the DynamoDB table); then lists the topic on that broker.
#   start-workers                           run /opt/bench/start-omb-workers.sh on every loadgen host
#   stop-workers
#   worker-status
#   run [run-omb.sh args]                   start run-omb.sh DETACHED on loadgen 0 (log /var/log/omb-run.log)
#   tail [N=40]                             last N lines of that log
#   abort                                   touch the STOP file and kill the coordinator on loadgen 0
#
# Override instance lists with BROKER_INSTANCE_IDS / LOADGEN_INSTANCE_IDS (space separated).
# Why through the broker host and not the NLB for topic creation: the NLB only forwards Kafka (9092) and
# manicp listens on loopback; the loadgen role has no ssm:SendCommand. OMB itself cannot create topics.
set -euo pipefail
TF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../terraform" && pwd)
REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-}}
RUN_ID=${RUN_ID:-}
while [ $# -gt 0 ]; do
  case "$1" in
    --tf-dir) TF_DIR=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --run-id) RUN_ID=$2; shift 2 ;;
    *) break ;;
  esac
done
cmd=${1:-}; [ $# -eq 0 ] || shift
[ -n "$cmd" ] || { sed -n '2,19p' "$0"; exit 2; }
[ -z "$REGION" ] || export AWS_DEFAULT_REGION=$REGION

tfout() { (cd "$TF_DIR" && terraform output -json "$1" 2>/dev/null) || true; }
BROKER_INSTANCE_IDS=${BROKER_INSTANCE_IDS:-$(tfout broker_instance_ids | jq -r '.[]?' | tr '\n' ' ')}
LOADGEN_INSTANCE_IDS=${LOADGEN_INSTANCE_IDS:-$(tfout loadgen_instance_ids | jq -r '.[]?' | tr '\n' ' ')}
if [ -z "$BROKER_INSTANCE_IDS" ] || [ -z "$LOADGEN_INSTANCE_IDS" ]; then
  echo "no instance ids: set BROKER_INSTANCE_IDS and LOADGEN_INSTANCE_IDS or use --tf-dir" >&2; exit 1
fi
# first id of each list (do not use `set --` here: it would clobber the command's own arguments "$@")
BROKER0=${BROKER_INSTANCE_IDS%% *}
LOADGEN0=${LOADGEN_INSTANCE_IDS%% *}

# ssm_run <instance ids (space separated)> <shell command>: waits, prints each instance's output, fails if any failed.
ssm_run() {
  local ids=$1 script=$2 cid st rc=0 params
  params=$(jq -nc --arg c "$script" '{commands:[$c]}')
  # shellcheck disable=SC2086
  cid=$(aws ssm send-command --document-name AWS-RunShellScript --instance-ids $ids --parameters "$params" \
    --timeout-seconds 600 --query Command.CommandId --output text)
  for _ in $(seq 1 120); do
    sleep 3
    st=$(aws ssm list-command-invocations --command-id "$cid" --query 'CommandInvocations[].Status' --output text)
    case " $st " in *" Pending "* | *" InProgress "* | *" Delayed "*) continue ;; esac
    break
  done
  aws ssm list-command-invocations --command-id "$cid" --details \
    --query 'CommandInvocations[].[InstanceId,Status,CommandPlugins[0].Output]' --output text
  case "$st" in *Failed* | *TimedOut* | *Cancelled*) rc=1 ;; esac
  return "$rc"
}

loadgen_ips() {
  # shellcheck disable=SC2086
  aws ec2 describe-instances --instance-ids $LOADGEN_INSTANCE_IDS \
    --query "Reservations[].Instances[].[Tags[?Key=='LoadgenIndex']|[0].Value,PrivateIpAddress]" --output text |
    sort -k1,1n | awk '{print $2}' | tr '\n' ',' | sed 's/,$//'
}

case "$cmd" in
  create-topic)
    parts=${1:-288}
    topic=${2:-omb-${RUN_ID:?--run-id or RUN_ID, or pass the topic name}}
    echo "creating $topic with $parts partitions through manicp on $BROKER0"
    # Broker hosts have no Kafka CLI (only loadgens do), so verify through manicp's describe-topics; run-omb.sh pre-flight
    # re-checks over the Kafka port on every broker. ErrInvalid on create means the topic already exists with other settings.
    ssm_run "$BROKER0" "set -e; curl -sf -m 20 -X POST http://127.0.0.1:8080/v1/create-topic -d '{\"topic\":\"$topic\",\"partitions\":$parts}'; echo; curl -sf -m 20 -X POST http://127.0.0.1:8080/v1/describe-topics -d '{\"topics\":[\"$topic\"]}' | head -c 300; echo" ||
      { echo "create-topic failed" >&2; exit 1; }
    ;;
  start-workers) ssm_run "$LOADGEN_INSTANCE_IDS" "/opt/bench/start-omb-workers.sh start" ;;
  stop-workers) ssm_run "$LOADGEN_INSTANCE_IDS" "/opt/bench/start-omb-workers.sh stop" ;;
  worker-status) ssm_run "$LOADGEN_INSTANCE_IDS" "/opt/bench/start-omb-workers.sh status" || true ;;
  run)
    ips=$(loadgen_ips)
    echo "loadgen private IPs (sorted by the LoadgenIndex tag): $ips"
    args=$(printf '%q ' "$@")
    ssm_run "$LOADGEN0" "export LOADGEN_IPS='$ips'; nohup /opt/bench/run-omb.sh $args >/var/log/omb-run.log 2>&1 & echo started pid \$!"
    echo "follow with: $0 tail"
    ;;
  tail) ssm_run "$LOADGEN0" "tail -n ${1:-40} /var/log/omb-run.log" ;;
  abort) ssm_run "$LOADGEN0" "touch /tmp/omb-stop; pkill -f io.openmessaging.benchmark.Benchmark || true; echo stop requested" ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
