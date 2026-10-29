#!/usr/bin/env bash
# Audit a Go module/package for determinism before the deterministic core
# (internal/core, internal/verify/sim) may import it. Prints a review report; it does not decide.
#
# Usage: scripts/audit-dep.sh <module>[@version] [package-pattern]
#   scripts/audit-dep.sh github.com/twmb/franz-go/pkg/kmsg@v1.14.0
#   scripts/audit-dep.sh github.com/foo/bar@latest github.com/foo/bar/codec
#
# Works in a throwaway module; this repository is never modified.
# Follow-up steps are in .claude/skills/audit-go-dependency/SKILL.md.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  sed -n '2,9p' "$0"; exit 2
fi

repo=$(cd "$(dirname "$0")/../.." && pwd)
mod_at=$1
mod=${mod_at%@*}
pattern=${2:-$mod/...}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cd "$work"
go mod init audit >/dev/null 2>&1
go get "$mod_at" >/dev/null 2>&1 || { echo "go get $mod_at failed" >&2; go get "$mod_at"; exit 1; }
version=$(go list -m -f '{{.Version}}' "$mod")

# detcheck must be built with a toolchain at least as new as the one the audited
# module requires, or it cannot read the compiled standard library it imports.
need_go=$(go mod edit -json | sed -n 's/.*"Go": *"\([^"]*\)".*/\1/p')
repo_go=$(cd "$repo" && go mod edit -json | sed -n 's/.*"Go": *"\([^"]*\)".*/\1/p')
toolchain=$(printf '%s\n%s\n' "$need_go" "$repo_go" | sort -V | tail -1)
(cd "$repo" && GOTOOLCHAIN="go$toolchain+auto" go build -o "$work/detcheck" ./ops/tools/determinismcheck)
export GOTOOLCHAIN="go$toolchain+auto"
dir=$(go list -m -f '{{.Dir}}' "$mod")

section() { printf '\n== %s\n' "$1"; }

echo "# Determinism audit: $mod@$version"
echo "source: $dir"
echo "requires go: $need_go (detcheck built with go$toolchain)"
echo "packages: $pattern"

section "Direct imports (non-test)"
go list -f '{{.ImportPath}}:{{range .Imports}}
    {{.}}{{end}}' "$pattern"

section "Third-party packages pulled in (each needs its own audit record)"
go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{if .Module}}  ({{.Module.Path}}@{{.Module.Version}}){{end}}{{end}}' "$pattern" | sed '/^$/d'

section "detcheck findings"
set +e
"$work/detcheck" "$pattern" 2>&1
rc=$?
set -e
case $rc in
  0) echo "(none)";;
  3) ;;  # diagnostics reported above
  *) echo "detcheck failed with exit $rc" >&2; exit $rc;;
esac

section "Manual-review hints (aliasing, unsafe, reflection)"
pkg_dirs=$(go list -f '{{.Dir}}' "$pattern")
# shellcheck disable=SC2086
grep -n -E 'unsafe\.|reflect\.|\[[^]]*:[^]]*:[^]]*\]|sync\.Pool|time\.|rand\.' \
  $(find $pkg_dirs -maxdepth 1 -name '*.go' ! -name '*_test.go') 2>/dev/null \
  | sed "s|$dir/||" | head -200 || true

section "Reviewer checklist"
cat <<'CHECK'
[ ] Every detcheck finding is explained as harmless, or the dependency is rejected.
[ ] Decoded/returned []byte and strings: copied, or aliasing the input? (3-index slices, unsafe)
[ ] No hidden defaults: timestamps, random IDs, checksums filled in implicitly?
[ ] No global mutable state: pools, caches, registries written at runtime?
[ ] Encode twice / decode twice gives byte-identical / deeply equal results.
[ ] Every third-party package listed above has its own audit record.
[ ] Record written: docs/dep-audits/<module>@<version>.md
[ ] Exact import path(s) added to ops/tools/determinismcheck/allow.txt
CHECK
