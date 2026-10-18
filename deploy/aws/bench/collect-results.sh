#!/usr/bin/env bash
# Pull a run's results from S3 to ./results/<run_id> and summarize them.
# Run on the operator workstation BEFORE terraform destroy (the bucket is force-destroyed).
#   usage: collect-results.sh <bucket> <run_id> [region]
# Result layout: results/<run_id>/{loadgen,metrics}/ plus the two service manifests.
set -euo pipefail
bucket=${1:?bucket (terraform output -raw bucket)}
run=${2:?run_id}
region=${3:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}
[ -z "$region" ] || export AWS_DEFAULT_REGION=$region
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
dest=${RESULTS_DIR:-./results}/$run
mkdir -p "$dest"
aws s3 sync --only-show-errors "s3://$bucket/results/$run/" "$dest/"
echo "synced to $dest: $(find "$dest" -type f | wc -l | tr -d ' ') files"
[ -f "$dest/loadgen/cells.tsv" ] || { echo "no loadgen/cells.tsv: did run-benchmark.sh finish?" >&2; exit 1; }
python3 -I "$here/summarize.py" "$dest" --out "$dest/summary.md" >/dev/null
echo "summary: $dest/summary.md"
