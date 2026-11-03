#!/usr/bin/env bash
# Collect the results of one experiment on the box, then upload them to S3.
# Run it on the box, from /opt/cpv/scripts.
#
# It makes /opt/cpv/results/NAME-UTCSTAMP/ with:
#   logs/              a copy of every node log in /opt/cpv/logs
#   manifests/         the run manifest of every node
#   metrics-NODE.txt   the /metrics page of every running node
#   node-status.txt    output of node.sh status
#   extra/             copies of the files listed in EXTRA_FILES
#   meta.txt           when, where and what was collected
# Then it copies the folder to s3://BUCKET/results/NAME-UTCSTAMP/.
#
# Usage: collect.sh EXPERIMENT_NAME
# Env (optional):
#   CPV_ROOT     default /opt/cpv
#   BIN_DIR      default $CPV_ROOT/bin
#   EXTRA_FILES  space separated list of more files to copy in
set -euo pipefail
unset AWS_PROFILE AWS_DEFAULT_PROFILE   # the box uses its IAM role, not a profile

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
. "$SCRIPT_DIR/defaults.sh"
BIN_DIR="${BIN_DIR:-$CPV_ROOT/bin}"

# The values in $CPV_ROOT/env replace the defaults when that file exists.
if [[ -f $CPV_ROOT/env ]]; then
  . "$CPV_ROOT/env"
fi

# node.sh reads these too, so pass them on.
export CPV_ROOT BIN_DIR

name=${1:-}
if ! [[ $name =~ ^[a-z][a-z0-9-]{0,40}$ ]]; then
  echo "usage: collect.sh EXPERIMENT_NAME (lowercase letters, digits, dashes)" >&2
  exit 2
fi

stamp=$(date -u +%Y%m%dT%H%M%SZ)
run_name="$name-$stamp"
out="$CPV_ROOT/results/$run_name"
mkdir -p "$out/logs" "$out/manifests" "$out/extra"
shopt -s nullglob

for f in "$CPV_ROOT"/logs/*.log; do
  cp "$f" "$out/logs/"
done

for f in "$CPV_ROOT"/run/*.manifest.json; do
  cp "$f" "$out/manifests/"
done

# Scrape the metrics of every running node. Frozen and stopped nodes are skipped.
for f in "$CPV_ROOT"/run/*.conf; do
  node=$(basename "$f" .conf)
  read -r _rank _grpc metrics_port < "$f"
  if [[ $("$SCRIPT_DIR/node.sh" state "$node") == running ]]; then
    curl -s --max-time 5 "http://127.0.0.1:$metrics_port/metrics" > "$out/metrics-$node.txt" \
      || echo "scrape failed (curl exit $?)" >> "$out/metrics-$node.txt"
  fi
done

"$SCRIPT_DIR/node.sh" status > "$out/node-status.txt" 2>&1 || true

# The log-versus-acks check (mdscpinspect) was removed with the tool.

missing=""
# shellcheck disable=SC2086
for f in ${EXTRA_FILES:-}; do
  if [[ -f $f ]]; then
    cp "$f" "$out/extra/"
  else
    missing="$missing $f"
  fi
done

{
  echo "experiment: $name"
  echo "run: $run_name"
  echo "utc: $stamp"
  echo "host: $(hostname)"
  echo "kernel: $(uname -a)"
  echo "missing extra files:${missing:- none}"
  echo "node states:"
  "$SCRIPT_DIR/node.sh" status
} > "$out/meta.txt" 2>&1

echo "local copy: $out"
aws s3 cp --recursive --region "$REGION" "$out/" "s3://$BUCKET/results/$run_name/"
echo "uploaded to: s3://$BUCKET/results/$run_name/"
echo "local copy: $out"
