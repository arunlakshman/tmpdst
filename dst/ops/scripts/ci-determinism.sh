#!/usr/bin/env bash
# CI gate for the deterministic packages of dst: no goroutines, channels, map
# ranges, package-level state, or imports outside ops/tools/determinismcheck/allow.txt.
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
go run ./ops/tools/determinismcheck "${pkgs[@]}"
echo "determinism gate passed"
