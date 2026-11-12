#!/usr/bin/env bash
# CI gate for the deterministic packages of dst: no goroutines, channels, map
# ranges, package-level state, or imports outside ops/determinism-allow.txt.
# Not in the gate (they use goroutines, wall time or files on purpose):
#   campaign (parallel seeds), syncclock, realclock, golden (files), model and
#   mutant (testing), examples.
set -euo pipefail
cd "$(dirname "$0")/../.."

pkgs=(
  ./rng
  ./sched
  ./trace
  ./assert
  ./fault
  ./clock
  ./check
)

echo "== determinismcheck"
go run github.com/manifesterio/mds/dst/cmd/determinismcheck -allow ops/determinism-allow.txt "${pkgs[@]}"
echo "determinism gate passed"
