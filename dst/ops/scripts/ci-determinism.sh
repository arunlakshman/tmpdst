#!/usr/bin/env bash
# CI gate for the deterministic packages of dst: no goroutines, channels, map
# ranges, package-level state, or imports outside ops/determinism-allow.txt.
# Not in the gate (they use goroutines, wall time or files on purpose):
#   campaign (parallel seeds), live (the production loop: real time, goroutines,
#   channels), syncclock, realclock, golden (files), model and mutant (testing),
#   examples.
# The dst v2 packages machine and kernel are in the gate.
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
  ./check/faulty
  ./machine
  ./kernel
)

echo "== determinismcheck"
go run github.com/manifesterio/mds/dst/cmd/determinismcheck -allow ops/determinism-allow.txt "${pkgs[@]}"
echo "determinism gate passed"
