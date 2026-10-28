#!/usr/bin/env bash
# CI gate for the deterministic packages (internal/core, internal/verify/sim, internal/wire/format, internal/verify/veritas except
# its sys adapters in internal/verify/veritas/vsys, plus internal/wire/kwire, internal/broker, and
# internal/controlplane/machine). See docs/decisions.md (I, 55, 56).
#   1. detcheck: exact-path import allow-list plus banned constructs.
#   2. Every third-party module they depend on has a reviewed audit record
#      for the exact version in go.mod (so a version bump forces a re-audit).
#   3. Determinism canary: each seed runs twice and must match, in every
#      simulator package that implements TestDeterminismCanary (internal/verify/sim).
set -euo pipefail
cd "$(dirname "$0")/../.."

pkgs=(./internal/core/... ./internal/verify/sim/... ./internal/wire/format/... ./internal/wire/kwire ./internal/broker/... ./internal/verify/veritas ./internal/verify/veritas/veritastest ./internal/controlplane/machine/...)

echo "== detcheck"
go run ./ops/tools/detcheck "${pkgs[@]}"

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
for simpkg in ./internal/verify/sim/...; do
  # Capture first: piping into grep -q would SIGPIPE go test under pipefail.
  tests=$(go list "$simpkg" 2>/dev/null | xargs -r go test -list 'TestDeterminismCanary' 2>/dev/null || true)
  if grep -q '^TestDeterminismCanary$' <<<"$tests"; then
    go test "$simpkg" -run '^TestDeterminismCanary$' -count=1
  else
    echo "skipped $simpkg: TestDeterminismCanary not implemented yet"
  fi
done

echo "determinism gate passed"
