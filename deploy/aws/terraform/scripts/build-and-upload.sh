#!/usr/bin/env bash
# Build linux/amd64 binaries (matches c7i) and upload the artifact contract to
# the run bucket. Run from anywhere; needs go, aws CLI credentials.
#   usage: build-and-upload.sh <bucket> <region>
#
# Contract (read by the instances' user-data):
#   s3://BUCKET/artifacts/manifester-linux-amd64.tar.gz   manibroker + manicp at tarball root
#   s3://BUCKET/artifacts/alloy/*                         deploy/aws/observability/alloy/* (optional)
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
if [ -d "$root/deploy/aws/observability/alloy" ]; then
  aws s3 sync --delete "$root/deploy/aws/observability/alloy/" "s3://$bucket/artifacts/alloy/"
else
  echo "warning: deploy/aws/observability/alloy missing; Alloy will not start" >&2
fi
echo "$(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)" | aws s3 cp - "s3://$bucket/artifacts/READY"
echo "uploaded; instances will pick it up"
