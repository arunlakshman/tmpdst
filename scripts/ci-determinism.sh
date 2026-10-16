#!/usr/bin/env bash
# CI gate for the deterministic packages (core/, sim/, format/, veritas/ except
# its sys adapters in veritas/vsys, and every toys3 package
# except prod and cmd). See docs/decisions.md (I).
#   1. detcheck: exact-path import allow-list plus banned constructs.
#   2. Every third-party module they depend on has a reviewed audit record
#      for the exact version in go.mod (so a version bump forces a re-audit).
#   3. Determinism canary: each seed runs twice and must match, in every
#      simulator package that implements TestDeterminismCanary (sim/, toys3/sim/).
set -euo pipefail
cd "$(dirname "$0")/.."

# toys3: every package is deterministic except the production runtime and the
# commands. Derived, so a new pure package cannot be left out of the gate.
pkgs=(./core/... ./sim/... ./format/... ./kwire ./veritas ./veritas/veritastest)
while read -r p; do
  pkgs+=("$p")
done < <(go list ./toys3/... | grep -v -e '/toys3/prod$' -e '/toys3/cmd/')

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
for simpkg in ./sim/... ./toys3/sim/...; do
  # Capture first: piping into grep -q would SIGPIPE go test under pipefail.
  tests=$(go list "$simpkg" 2>/dev/null | xargs -r go test -list 'TestDeterminismCanary' 2>/dev/null || true)
  if grep -q '^TestDeterminismCanary$' <<<"$tests"; then
    go test "$simpkg" -run '^TestDeterminismCanary$' -count=1
  else
    echo "skipped $simpkg: TestDeterminismCanary not implemented yet"
  fi
done

echo "determinism gate passed"
