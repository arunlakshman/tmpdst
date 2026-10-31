#!/usr/bin/env bash
# Operator-workstation control for the 6-broker OMB run. Everything goes through SSM send-command (no SSH).
# Needs: aws CLI v2 with credentials, jq, terraform outputs (cp_instance_id, cp_private_ip, broker_instance_ids, loadgen_instance_ids).
#
#   bench-ctl.sh [--tf-dir DIR] [--region R] [--run-id ID] <command> [args]
#
#   create-topic [PARTITIONS=288] [TOPIC]   POST /v1/create-topic on the control-plane host's loopback mdscontrolplane (the ONE
#                                           mdscontrolplane, decision 46) through SSM send-command; then describes the topic.
#   check-cp                                verify the single control plane: exactly one mdscontrolplane process on the CP host,
#                                           healthz ok, no mdscontrolplane on any broker, every broker reaches CP:8080
#   cp-roles                                print /healthz and the X-CP-Role header (active|standby) of every CP host (log store)
#   kill-appender [DOWN_SECONDS=60]         SIGKILL mdscontrolplane on the rank-0 CP host, keep it down DOWN_SECONDS, then start it again
#                                           (it fails back as the preferred appender). Runs detached on the host; prints the time and
#                                           writes results/RUN/markers/kill-appender-<epoch>.txt for summarize.py's failover summary.
#   start-workers                           run /opt/bench/start-omb-workers.sh on every loadgen host
#   stop-workers
#   worker-status
#   run [run-omb.sh args]                   start run-omb.sh DETACHED on loadgen 0 (log /var/log/omb-run.log)
#   broker-args [FLAGS...]                  append FLAGS (e.g. -segment-window 1s) to every mdsbroker's command line through a
#                                           systemd drop-in and restart mdsbroker; no FLAGS removes the drop-in (defaults)
#   tail [N=40]                             last N lines of that log
#   abort                                   touch the STOP file and kill the coordinator on loadgen 0
#
# Override with CP_INSTANCE_ID / CP_PRIVATE_IP / BROKER_INSTANCE_IDS / LOADGEN_INSTANCE_IDS (ids space separated).
# Why through the CP host's SSM and not over the network: the CP security group admits 8080 from the brokers only
# (loadgen -> CP stays closed) and the loadgen role has no ssm:SendCommand. OMB itself cannot create topics.
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
[ -n "$cmd" ] || { sed -n '2,25p' "$0"; exit 2; }
[ -z "$REGION" ] || export AWS_DEFAULT_REGION=$REGION

tfout() { (cd "$TF_DIR" && terraform output -json "$1" 2>/dev/null) || true; }
BROKER_INSTANCE_IDS=${BROKER_INSTANCE_IDS:-$(tfout broker_instance_ids | jq -r '.[]?' | tr '\n' ' ')}
LOADGEN_INSTANCE_IDS=${LOADGEN_INSTANCE_IDS:-$(tfout loadgen_instance_ids | jq -r '.[]?' | tr '\n' ' ')}
if [ -z "$BROKER_INSTANCE_IDS" ] || [ -z "$LOADGEN_INSTANCE_IDS" ]; then
  echo "no instance ids: set BROKER_INSTANCE_IDS and LOADGEN_INSTANCE_IDS or use --tf-dir" >&2; exit 1
fi
CP_INSTANCE_ID=${CP_INSTANCE_ID:-$(tfout cp_instance_id | jq -r '.// empty' 2>/dev/null || true)}
CP_PRIVATE_IP=${CP_PRIVATE_IP:-$(tfout cp_private_ip | jq -r '.// empty' 2>/dev/null || true)}
# hot-standby CP hosts of the log store (empty with the old store or cp_standby_count = 0)
CP_STANDBY_INSTANCE_IDS=${CP_STANDBY_INSTANCE_IDS:-$(tfout cp_standby_instance_ids | jq -r '.[]?' 2>/dev/null | tr '\n' ' ' || true)}
CP_ALL_IDS="$CP_INSTANCE_ID $CP_STANDBY_INSTANCE_IDS"
# first id of each list (do not use `set --` here: it would clobber the command's own arguments "$@")
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
    [ -n "$CP_INSTANCE_ID" ] || { echo "no control-plane instance: set CP_INSTANCE_ID or use --tf-dir (terraform output cp_instance_id)" >&2; exit 1; }
    echo "creating $topic with $parts partitions through the control-plane mdscontrolplane on $CP_INSTANCE_ID"
    # Hosts have no Kafka CLI (only loadgens do), so verify through mdscontrolplane's describe-topics; run-omb.sh pre-flight
    # re-checks over the Kafka port on every broker. ErrInvalid on create means the topic already exists with other settings.
    ssm_run "$CP_INSTANCE_ID" "set -e; curl -sf -m 20 -X POST http://127.0.0.1:8080/v1/create-topic -d '{\"topic\":\"$topic\",\"partitions\":$parts}'; echo; curl -sf -m 20 -X POST http://127.0.0.1:8080/v1/describe-topics -d '{\"topics\":[\"$topic\"]}' | head -c 300; echo" ||
      { echo "create-topic failed" >&2; exit 1; }
    ;;
  check-cp)
    [ -n "$CP_INSTANCE_ID" ] || { echo "no control-plane instance: set CP_INSTANCE_ID or use --tf-dir" >&2; exit 1; }
    echo "== control-plane host $CP_INSTANCE_ID: exactly one mdscontrolplane, healthy"
    ssm_run "$CP_INSTANCE_ID" "n=\$(pgrep -x mdscontrolplane | wc -l); echo mdscontrolplane processes: \$n; systemctl is-active mdscontrolplane mb-snapshot alloy; curl -sf -m 5 http://127.0.0.1:8080/healthz && echo healthz ok; [ \$n -eq 1 ]" ||
      { echo "check-cp FAILED on the control-plane host" >&2; exit 1; }
    for sb in $CP_STANDBY_INSTANCE_IDS; do
      echo "== standby control-plane host $sb: one mdscontrolplane, healthy"
      ssm_run "$sb" "n=\$(pgrep -x mdscontrolplane | wc -l); echo mdscontrolplane processes: \$n; systemctl is-active mdscontrolplane mb-snapshot alloy; curl -sf -m 5 http://127.0.0.1:8080/healthz && echo healthz ok; [ \$n -eq 1 ]" ||
        { echo "check-cp FAILED on standby $sb" >&2; exit 1; }
    done
    echo "== broker hosts: no mdscontrolplane, CP reachable${CP_PRIVATE_IP:+ at $CP_PRIVATE_IP:8080}"
    ssm_run "$BROKER_INSTANCE_IDS" "echo mdscontrolplane processes: \$(pgrep -x mdscontrolplane | wc -l); systemctl is-active mdsbroker; curl -sf -m 5 http://${CP_PRIVATE_IP:-CP_IP_UNKNOWN}:8080/healthz && echo cp reachable" ||
      { echo "check-cp: a broker cannot reach the CP (or runs mdscontrolplane)" >&2; exit 1; }
    ;;
  cp-roles)
    # shellcheck disable=SC2086
    ssm_run "$CP_ALL_IDS" "curl -s -m 5 -D - -o /dev/null http://127.0.0.1:8080/healthz | grep -i -E '^(HTTP|x-cp-role)'; systemctl is-active mdscontrolplane" ;;
  kill-appender)
    down=${1:-60}
    [ -n "$CP_INSTANCE_ID" ] || { echo "no control-plane instance: set CP_INSTANCE_ID or use --tf-dir" >&2; exit 1; }
    date -u +"kill at %Y-%m-%dT%H:%M:%SZ (note it for the report)"
    ssm_run "$CP_INSTANCE_ID" "nohup bash -c '. /etc/mds/snapshot.env; t=\$(date +%s); echo kill_epoch=\$t | aws s3 cp - s3://\$BUCKET/results/\$RUN_ID/markers/kill-appender-\$t.txt --only-show-errors --region \$AWS_REGION || true; systemctl kill -s SIGKILL mdscontrolplane; systemctl stop mdscontrolplane; sleep $down; systemctl start mdscontrolplane' >/var/log/kill-appender.log 2>&1 & echo killer started" ;;
  broker-args)
    # Flags later on the command line override earlier ones (Go flag package), so appending is enough.
    flags=$*
    if [ -z "$flags" ]; then
      ssm_run "$BROKER_INSTANCE_IDS" "rm -rf /etc/systemd/system/mdsbroker.service.d; systemctl daemon-reload; systemctl restart mdsbroker; sleep 3; systemctl is-active mdsbroker; pgrep -a mdsbroker | cut -c1-400"
    else
      ssm_run "$BROKER_INSTANCE_IDS" "set -e; o=\$(grep '^ExecStart=' /etc/systemd/system/mdsbroker.service); mkdir -p /etc/systemd/system/mdsbroker.service.d; printf '[Service]\nExecStart=\n%s %s\n' \"\$o\" '$flags' > /etc/systemd/system/mdsbroker.service.d/bench-args.conf; systemctl daemon-reload; systemctl restart mdsbroker; sleep 3; systemctl is-active mdsbroker; pgrep -a mdsbroker | cut -c1-400"
    fi
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
