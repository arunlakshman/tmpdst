#!/usr/bin/env bash
# CI gate for the deterministic packages (internal/core, internal/verify/simulator, internal/wire/segmentformat, internal/verify/speccheck except
# its system adapters in internal/verify/speccheck/specsystem, plus internal/wire/kafkawire, internal/broker, and
# internal/controlplane/statemachine). See docs/decisions.md (I, 55, 56).
#   1. determinismcheck: exact-path import allow-list plus banned constructs.
#   2. Every third-party module they depend on has a reviewed audit record
#      for the exact version in go.mod (so a version bump forces a re-audit).
#   3. Determinism canary: each seed runs twice and must match, in every
#      simulator package that implements TestDeterminismCanary (internal/verify/simulator).
set -euo pipefail
cd "$(dirname "$0")/../.."

pkgs=(./internal/core/... ./internal/verify/simulator/... ./internal/wire/segmentformat/... ./internal/wire/kafkawire ./internal/broker/... ./internal/verify/speccheck ./internal/verify/speccheck/checker ./internal/controlplane/statemachine/...)

echo "== detcheck"
go run ./ops/tools/determinismcheck "${pkgs[@]}"

echo "== dependency audit records"
missing=0
while read -r modver; do
  [[ -z $modver ]] && continue
  record="docs/dep-audits/$modver.md"
  if [[ -f $record ]]; then
    echo "ok       $modver"
  else
    echo "MISSING  $record  (run ops/scripts/audit-dep.sh $modver)"
    missing=1
  fi
done < <(go list -deps -f '{{if and .Module (not .Module.Main)}}{{.Module.Path}}@{{.Module.Version}}{{end}}' "${pkgs[@]}" | sort -u)
[[ $missing == 0 ]]

echo "== determinism canary"
for simpkg in ./internal/verify/simulator/...; do
  # Capture first: piping into grep -q would SIGPIPE go test under pipefail.
  tests=$(go list "$simpkg" 2>/dev/null | xargs -r go test -list 'TestDeterminismCanary' 2>/dev/null || true)
  if grep -q '^TestDeterminismCanary$' <<<"$tests"; then
    go test "$simpkg" -run '^TestDeterminismCanary$' -count=1
  else
    echo "skipped $simpkg: TestDeterminismCanary not implemented yet"
  fi
done

echo "determinism gate passed"
