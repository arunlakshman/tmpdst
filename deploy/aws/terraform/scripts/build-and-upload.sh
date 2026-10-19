#!/usr/bin/env bash
# Build linux/amd64 binaries (matches the x86_64 instance types) and upload the artifact contract to
# the run bucket. Run from anywhere; needs go, aws CLI credentials.
#   usage: build-and-upload.sh <bucket> <region>
#
# Contract (read by the instances' user-data):
#   s3://BUCKET/artifacts/manifester-linux-amd64.tar.gz   manibroker + manicp at tarball root
#   s3://BUCKET/artifacts/observability/                  deploy/aws/observability minus dashboards/ and gen/ (install-alloy.sh, alloy/*)
#   s3://BUCKET/artifacts/bench/                          deploy/aws/bench (run-benchmark.sh, ...)
#   s3://BUCKET/artifacts/omb/openmessaging-benchmark.tar.gz   from build/omb/ (repo root), only if it exists locally
#   s3://BUCKET/artifacts/READY                           written last; instances wait for it
set -euo pipefail
bucket=${1:?bucket}
region=${2:?region}
root=$(cd "$(dirname "$0")/../../../.." && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

cd "$root"
for c in manibroker manicp; do
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$out/$c" "./cmd/$c"
done
tar -czf "$out/manifester-linux-amd64.tar.gz" -C "$out" manibroker manicp

export AWS_DEFAULT_REGION=$region
aws s3 rm "s3://$bucket/artifacts/READY" >/dev/null 2>&1 || true
aws s3 cp "$out/manifester-linux-amd64.tar.gz" "s3://$bucket/artifacts/manifester-linux-amd64.tar.gz"
aws s3 sync --delete --exclude 'dashboards/*' --exclude 'gen/*' --exclude 'README.md' "$root/deploy/aws/observability/" "s3://$bucket/artifacts/observability/"
aws s3 sync --delete "$root/deploy/aws/bench/" "s3://$bucket/artifacts/bench/"
if [ -f "$root/build/omb/openmessaging-benchmark.tar.gz" ]; then
  aws s3 cp "$root/build/omb/openmessaging-benchmark.tar.gz" "s3://$bucket/artifacts/omb/openmessaging-benchmark.tar.gz"
else
  echo "note: build/omb/openmessaging-benchmark.tar.gz not found; skipping OMB upload" >&2
fi
{ git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown; } | aws s3 cp - "s3://$bucket/artifacts/READY"
echo "uploaded; instances will pick it up"
