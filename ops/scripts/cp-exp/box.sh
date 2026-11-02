#!/usr/bin/env bash
# Run a shell command on the test box through AWS SSM (no SSH).
# The command runs as ec2-user in a login shell (bash -lc).
#
# Usage:
#   box.sh 'shell command'
#   box.sh -f local_script_file
#
# Env (optional):
#   INSTANCE_ID       instance id. Default: terraform output instance_id
#                     in ops/deploy/aws/cp-verify
#   TIMEOUT_SECONDS   time limit on the box, default 600
#   AWS_PROFILE_NAME  laptop AWS profile, default in onbox/defaults.sh
#
# Output limit: SSM returns at most 24000 characters of stdout and of
# stderr. Longer output is cut. Then read the full output in S3 with
# fetch-results.sh.
set -euo pipefail
# shellcheck disable=SC1091
. "$(dirname "$0")/onbox/defaults.sh"

PROFILE="$AWS_PROFILE_NAME"
TIMEOUT="${TIMEOUT_SECONDS:-600}"
SSM_LIMIT=24000

usage() {
  cat >&2 <<'EOF'
usage:
  box.sh 'shell command'
  box.sh -f local_script_file
EOF
}

if [[ $# -eq 2 && $1 == "-f" ]]; then
  if [[ ! -f $2 ]]; then
    echo "error: no such file: $2" >&2
    exit 2
  fi
  script=$(cat "$2")
elif [[ $# -eq 1 && $1 != "-f" ]]; then
  script=$1
else
  usage
  exit 2
fi

if ! [[ $TIMEOUT =~ ^[0-9]+$ ]]; then
  echo "error: TIMEOUT_SECONDS must be a number, got '$TIMEOUT'" >&2
  exit 2
fi

if [[ -n ${INSTANCE_ID:-} ]]; then
  instance=$INSTANCE_ID
else
  # Go to the repo root. This script is in ops/scripts/cp-exp/.
  repo=$(cd "$(dirname "$0")/../../.." && pwd)
  instance=$(terraform -chdir="$repo/ops/deploy/aws/cp-verify" output -raw instance_id 2>/dev/null || true)
  if [[ -z $instance ]]; then
    echo "error: no instance id. Set INSTANCE_ID, or check that 'terraform output instance_id' works in ops/deploy/aws/cp-verify" >&2
    exit 1
  fi
fi

# Send the script as base64. Quotes and special characters then survive
# the trip through SSM and the JSON.
encoded=$(printf '%s' "$script" | base64 | tr -d '\n')

# The box runs: sudo -u ec2-user bash -lc "<decoded script>"
remote="sudo -u ec2-user bash -lc \"\$(echo $encoded | base64 -d)\""

# Escape double quotes, so the text can sit inside a JSON string.
json_string() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr -d '\n'
}

params="{\"commands\":[\"$(json_string "$remote")\"],\"executionTimeout\":[\"$TIMEOUT\"]}"

if ! cmd_id=$(aws ssm send-command --profile "$PROFILE" --region "$REGION" \
    --instance-ids "$instance" --document-name AWS-RunShellScript \
    --comment "cp-exp box.sh" --parameters "$params" \
    --query 'Command.CommandId' --output text 2>&1); then
  echo "error: send-command failed: $cmd_id" >&2
  exit 1
fi
echo "command id: $cmd_id on $instance"

# get_field FIELD: read one field of the command result from SSM.
get_field() {
  aws ssm get-command-invocation --profile "$PROFILE" --region "$REGION" \
    --command-id "$cmd_id" --instance-id "$instance" \
    --query "$1" --output text
}

# Wait until the command is done. SSM always ends a command at its own
# timeout, so the extra 300 s is only a safety stop.
deadline=$(( $(date +%s) + TIMEOUT + 300 ))
status=Pending
while :; do
  if out=$(get_field Status 2>&1); then
    status=$out
  elif [[ $out == *InvocationDoesNotExist* ]]; then
    status=Pending   # SSM has not created the result yet
  else
    echo "error: get-command-invocation failed: $out" >&2
    exit 1
  fi
  case $status in
    Pending|InProgress|Delayed|Cancelling) ;;
    *) break ;;
  esac
  if [[ $(date +%s) -ge $deadline ]]; then
    echo "error: gave up waiting; last status was $status" >&2
    exit 1
  fi
  sleep 3
done

stdout=$(get_field StandardOutputContent)
stderr=$(get_field StandardErrorContent)
code=$(get_field ResponseCode)

echo "status: $status, exit code: $code"
echo "--- stdout ---"
printf '%s\n' "$stdout"
echo "--- stderr ---" >&2
printf '%s\n' "$stderr" >&2

if [[ ${#stdout} -ge $SSM_LIMIT || ${#stderr} -ge $SSM_LIMIT ]]; then
  echo "warning: the output is at the SSM limit of $SSM_LIMIT characters, so it was probably cut." >&2
  echo "         Read the full output in S3: ops/scripts/cp-exp/fetch-results.sh EXPERIMENT_NAME" >&2
fi

# Exit with the remote exit code. A bad or missing code counts as failure.
rc=$code
if ! [[ $rc =~ ^[0-9]+$ ]] || [[ $rc -gt 255 ]]; then
  rc=1
fi
if [[ $status != Success && $rc -eq 0 ]]; then
  rc=1
fi
exit "$rc"
