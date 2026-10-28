#!/usr/bin/env bash
# Render driver-kafka-manifester.yaml from the template.
#   render-driver.sh <bootstrap-list> <lz4|none> <out-file> <topic> [consumers-per-subscription=0] [consumer-workers=1] [balance=true|false] [affine=true|false]
# Compression defaults to lz4 (as WarpStream; the broker accepts it since decision 45); `none` prints a warning.
set -euo pipefail
boot=${1:?bootstrap list ip:9092,ip:9092}
comp=${2:-lz4}
out=${3:?output file}
topic=${4:?pre-created topic name}
consumers=${5:-0}
cworkers=${6:-1}
balance=${7:-true}
affine=${8:-false}
case "$affine" in true | false) ;; *) echo "affine must be true or false, got $affine" >&2; exit 2 ;; esac
[ "$affine" = false ] || [ "$balance" = true ] || { echo "affine=true needs balance=true" >&2; exit 2; }
case "$comp" in lz4 | none) ;; *) echo "compression must be lz4 or none, got $comp" >&2; exit 2 ;; esac
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
sed -e "s|@@BOOTSTRAP@@|$boot|g" -e "s|@@COMPRESSION@@|$comp|g" \
  -e "s|@@TOPIC@@|$topic|g" -e "s|@@CONSUMERS@@|$((consumers > 0 ? consumers : 1))|g" -e "s|@@CONSUMER_WORKERS@@|$cworkers|g" -e "s|@@BALANCE@@|$balance|g" -e "s|@@AFFINE@@|$affine|g" "$here/driver-kafka-manifester.yaml.tmpl" >"$out"
if [ "$comp" = none ]; then
  {
    echo '################################################################################'
    echo '## WARNING: compression.type=none. WarpStream ran lz4 with randomBytesRatio 0.5. ##'
    echo '## Bytes on the wire and in S3 are ~2x higher than theirs for the same msg/s:     ##'
    echo '## this run is NOT apples-to-apples (docs/aws-benchmark-warpstream-comparison.md). ##'
    echo '################################################################################'
  } >&2
fi
