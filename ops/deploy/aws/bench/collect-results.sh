#!/usr/bin/env bash
# Pull a run's results from S3 to ./results/<run_id> and summarize them.
# Run on the operator workstation BEFORE terraform destroy (the bucket is force-destroyed).
#   usage: collect-results.sh <bucket> <run_id> [region]
# Result layout: results/<run_id>/{loadgen,omb,metrics}/ plus the service manifests and markers/kill-appender-<epoch>.txt (bench-ctl kill-appender). metrics/ holds either
# <broker_id>/<epoch>-broker.txt per broker plus cp/ (and cp-1/ ... for log-store standbys) <epoch>-mdscontrolplane.txt from the control-plane hosts
# (decision 46), or the older flat <epoch>-*.txt / <broker_id>/<epoch>-mdscontrolplane.txt layouts (one mdscontrolplane per broker).
# Price knobs for the cost-per-GiB table are passed to summarize.py through SUMMARIZE_ARGS, e.g.
#   SUMMARIZE_ARGS='--broker-hourly 0.9 --s3-put-per-1000 0.0055' collect-results.sh ...
set -euo pipefail
bucket=${1:?bucket (terraform output -raw bucket)}
run=${2:?run_id}
region=${3:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}
[ -z "$region" ] || export AWS_DEFAULT_REGION=$region
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
dest=${RESULTS_DIR:-./results}/$run
mkdir -p "$dest"
aws s3 sync --only-show-errors "s3://$bucket/results/$run/" "$dest/"
# Evidence that dies with the stack: git sha of the uploaded build and the Terraform outputs.
aws s3 cp --only-show-errors "s3://$bucket/artifacts/READY" "$dest/build-git-sha.txt" || echo "warning: could not copy artifacts/READY" >&2
tfdir=${TF_DIR:-$here/../terraform}
(cd "$tfdir" && terraform output -json) >"$dest/terraform-outputs.json" 2>/dev/null || echo "warning: terraform output failed (set TF_DIR); save it by hand before destroy" >&2
echo "synced to $dest: $(find "$dest" -type f | wc -l | tr -d ' ') files"
[ -f "$dest/loadgen/cells.tsv" ] || [ -f "$dest/omb/stages.tsv" ] || { echo "no loadgen/cells.tsv and no omb/stages.tsv: did run-benchmark.sh or run-omb.sh finish?" >&2; exit 1; }
# shellcheck disable=SC2086 # SUMMARIZE_ARGS is a word list on purpose
python3 -I "$here/summarize.py" "$dest" --out "$dest/summary.md" ${SUMMARIZE_ARGS:-} >/dev/null
echo "summary: $dest/summary.md"
