#!/usr/bin/env bash
# Build linux/amd64 binaries (matches the x86_64 instance types) and upload the artifact contract to
# the run bucket. Run from anywhere; needs go, aws CLI credentials.
#   usage: build-and-upload.sh <bucket> <region>
#
# Contract (read by the instances' user-data):
#   s3://BUCKET/artifacts/mds-linux-amd64.tar.gz   mdsbroker + mdscontrolplane at tarball root
#   s3://BUCKET/artifacts/observability/                  ops/deploy/aws/observability minus dashboards/ and gen/ (install-alloy.sh, alloy/*)
#   s3://BUCKET/artifacts/bench/                          ops/deploy/aws/bench (run-benchmark.sh, ...)
#   s3://BUCKET/artifacts/omb/openmessaging-benchmark.tar.gz   from build/omb/ (repo root), only if it exists locally
#   s3://BUCKET/artifacts/READY                           written last; instances wait for it
set -euo pipefail
bucket=${1:?bucket}
region=${2:?region}
root=$(cd "$(dirname "$0")/../../../../.." && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

cd "$root"
# Newest toolchain so the binaries carry no stdlib advisories (go.mod keeps its older minimum; override via env).
export GOTOOLCHAIN=${GOTOOLCHAIN:-go1.26.10}
for c in mdsbroker mdscontrolplane; do
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$out/$c" "./cmd/$c"
done
tar -czf "$out/mds-linux-amd64.tar.gz" -C "$out" mdsbroker mdscontrolplane

export AWS_DEFAULT_REGION=$region
aws s3 rm "s3://$bucket/artifacts/READY" >/dev/null 2>&1 || true
aws s3 cp "$out/mds-linux-amd64.tar.gz" "s3://$bucket/artifacts/mds-linux-amd64.tar.gz"
aws s3 sync --delete --exclude 'dashboards/*' --exclude 'gen/*' --exclude 'README.md' "$root/ops/deploy/aws/observability/" "s3://$bucket/artifacts/observability/"
aws s3 sync --delete "$root/ops/deploy/aws/bench/" "s3://$bucket/artifacts/bench/"
if [ -f "$root/build/omb/openmessaging-benchmark.tar.gz" ]; then
  aws s3 cp "$root/build/omb/openmessaging-benchmark.tar.gz" "s3://$bucket/artifacts/omb/openmessaging-benchmark.tar.gz"
else
  echo "note: build/omb/openmessaging-benchmark.tar.gz not found; skipping OMB upload" >&2
fi
# Kafka CLI tarball cache (archive.apache.org is ~40 KB/s): build/kafka/kafka_2.13-<ver>.tgz, optional.
for t in "$root"/build/kafka/kafka_2.13-*.tgz; do
  [ -f "$t" ] && aws s3 cp "$t" "s3://$bucket/artifacts/kafka/$(basename "$t")"
done
{ git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown; } | aws s3 cp - "s3://$bucket/artifacts/READY"
echo "uploaded; instances will pick it up"
