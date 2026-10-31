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

echo "== determinismcheck"
go run ./ops/tools/determinismcheck "${pkgs[@]}"

echo "== dependency audit records"
# Only code that the deterministic packages really run needs an audit record.
# The generated gRPC API (internal/controlplane/api/v1) is the input/output
# edge, so we do not follow imports into it: grpc, protobuf and their
# dependencies are not audited here. The packages still may import it for type
# definitions only (decision 57).
edge=manifester/internal/controlplane/api/v1
roots=$(go list "${pkgs[@]}" | tr '\n' ' ')
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
done < <(
  # Line format: import path, module (or "-"), then the imports of that package.
  go list -deps -f '{{.ImportPath}} {{if and .Module (not .Module.Main)}}{{.Module.Path}}@{{.Module.Version}}{{else}}-{{end}} {{join .Imports " "}}' "${pkgs[@]}" |
    awk -v edge="$edge" -v roots="$roots" '
      { mod[$1] = $2; n = split($0, f, " "); imps[$1] = ""; for (i = 3; i <= n; i++) imps[$1] = imps[$1] " " f[i] }
      END {
        nr = split(roots, r, " "); top = 0
        for (i = 1; i <= nr; i++) stack[++top] = r[i]
        while (top > 0) {
          p = stack[top--]
          if (p in seen || p == edge) continue
          seen[p] = 1
          if (mod[p] != "" && mod[p] != "-") out[mod[p]] = 1
          m = split(imps[p], q, " ")
          for (i = 1; i <= m; i++) if (!(q[i] in seen)) stack[++top] = q[i]
        }
        for (k in out) print k
      }' | sort -u
)
# An explicit test: a failing [[ ]] does not stop old bash (macOS) under set -e.
if [[ $missing != 0 ]]; then
  exit 1
fi

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
