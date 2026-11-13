#!/usr/bin/env bash
# Render driver-kafka.yaml from the template.
#   render-driver.sh <bootstrap-list> <lz4|none> <out-file>
# Compression defaults to lz4 (as WarpStream; the broker accepts it since decision 45); `none` prints a warning.
set -euo pipefail
boot=${1:?bootstrap list ip:9092,ip:9092}
comp=${2:-lz4}
out=${3:?output file}
case "$comp" in lz4 | none) ;; *) echo "compression must be lz4 or none, got $comp" >&2; exit 2 ;; esac
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
sed -e "s|@@BOOTSTRAP@@|$boot|g" -e "s|@@COMPRESSION@@|$comp|g" "$here/driver-kafka.yaml.tmpl" >"$out"
if [ "$comp" = none ]; then
  {
    echo '################################################################################'
    echo '## WARNING: compression.type=none. WarpStream ran lz4 with randomBytesRatio 0.5. ##'
    echo '## Bytes on the wire and in S3 are ~2x higher than theirs for the same msg/s:     ##'
    echo '## this run is NOT apples-to-apples (docs/aws-benchmark-warpstream-comparison.md). ##'
    echo '################################################################################'
  } >&2
fi
