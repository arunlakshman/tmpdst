#!/usr/bin/env bash
# CI gate for the deterministic packages (core/, sim/). See docs/decisions.md (I).
#   1. detcheck: exact-path import allow-list plus banned constructs.
#   2. Every third-party module they depend on has a reviewed audit record
#      for the exact version in go.mod (so a version bump forces a re-audit).
#   3. Determinism canary: each seed runs twice and must match (enabled once
#      the simulator is implemented).
set -euo pipefail
cd "$(dirname "$0")/.."

pkgs=(./core/... ./sim/... ./toys3/core/... ./toys3/sim/...)

echo "== detcheck"
go run ./tools/detcheck "${pkgs[@]}"

echo "== dependency audit records"
missing=0
while read -r modver; do
  [[ -z $modver ]] && continue
  record="docs/dep-audits/$modver.md"
  if [[ -f $record ]]; then
    echo "ok       $modver"
  else
    echo "MISSING  $record  (run scripts/audit-dep.sh $modver)"
    missing=1
  fi
done < <(go list -deps -f '{{if and .Module (not .Module.Main)}}{{.Module.Path}}@{{.Module.Version}}{{end}}' "${pkgs[@]}" | sort -u)
[[ $missing == 0 ]]

echo "== determinism canary"
if go list ./sim/... 2>/dev/null | xargs -r go test -list 'TestDeterminismCanary' 2>/dev/null | grep -q '^TestDeterminismCanary$'; then
  go test ./sim/... -run '^TestDeterminismCanary$' -count=1
else
  echo "skipped: TestDeterminismCanary not implemented yet"
fi

echo "determinism gate passed"
