#!/usr/bin/env bash
# Cross compile the three control plane tools for the test box.
# Target: Linux on arm64 (Amazon Linux 2023 on Graviton).
# Output: build/linux-arm64/ at the repo root (git ignores build/).
#
# Usage: ops/scripts/cp-exp/build.sh
set -euo pipefail

# Go to the repo root. This script is in ops/scripts/cp-exp/.
cd "$(dirname "$0")/../../.."

OUT_DIR="build/linux-arm64"
TOOLS="mdscontrolplane mdscpverify mdscpinspect"

if ! command -v go >/dev/null 2>&1; then
  echo "error: go is not installed or not on PATH" >&2
  exit 1
fi

# Start clean, so an old binary is never pushed by mistake.
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

# CGO is off, so the binaries need no C libraries on the box.
export GOOS=linux GOARCH=arm64 CGO_ENABLED=0

for tool in $TOOLS; do
  echo "building $tool for linux/arm64"
  go build -trimpath -o "$OUT_DIR/$tool" "./cmd/$tool"
done

echo
echo "built files in $OUT_DIR:"
for f in "$OUT_DIR"/*; do
  size=$(wc -c < "$f" | tr -d ' ')
  echo "  $size bytes  $f"
done
