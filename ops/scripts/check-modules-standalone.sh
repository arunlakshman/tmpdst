#!/usr/bin/env bash
# Verifies each Go module builds and stays tidy with GOWORK=off.
#
# Catches:
#   - go.mod / go.sum drift (hand edits, workspace-only resolution)
#   - missing generated files (run root `make generate` first)
#
# Usage: check-modules-standalone.sh [root]
#   root is the repo root. Default: the folder above this script.
# Exit 0 when every module is tidy and builds. Exit 1 otherwise.

set -u

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
modules="dst shared controlplane broker"

failures=0

for module in $modules; do
	dir="$root/$module"
	if [ ! -f "$dir/go.mod" ]; then
		echo "missing go.mod for module $module (expected $dir/go.mod)"
		failures=$((failures + 1))
		continue
	fi

	tmp="$(mktemp -d)"
	cp "$dir/go.mod" "$tmp/go.mod"
	if [ -f "$dir/go.sum" ]; then
		cp "$dir/go.sum" "$tmp/go.sum"
	fi

	echo "check-modules-standalone: $module (GOWORK=off go mod tidy)"
	if ! (cd "$dir" && GOWORK=off go mod tidy); then
		echo "$module: go mod tidy failed"
		cp "$tmp/go.mod" "$dir/go.mod"
		if [ -f "$tmp/go.sum" ]; then
			cp "$tmp/go.sum" "$dir/go.sum"
		fi
		rm -rf "$tmp"
		failures=$((failures + 1))
		continue
	fi

	untidy=0
	if ! cmp -s "$dir/go.mod" "$tmp/go.mod"; then
		untidy=1
	fi
	if [ -f "$tmp/go.sum" ] || [ -f "$dir/go.sum" ]; then
		if ! cmp -s "$dir/go.sum" "$tmp/go.sum" 2>/dev/null; then
			untidy=1
		fi
	fi

	if [ "$untidy" -ne 0 ]; then
		echo "$module: go.mod / go.sum are not tidy (GOWORK=off go mod tidy would change them)"
		diff -u "$tmp/go.mod" "$dir/go.mod" || true
		if [ -f "$tmp/go.sum" ] || [ -f "$dir/go.sum" ]; then
			diff -u "$tmp/go.sum" "$dir/go.sum" || true
		fi
		# Leave the tidied files in place so the next edit/commit can keep them.
		rm -rf "$tmp"
		failures=$((failures + 1))
		continue
	fi

	rm -rf "$tmp"

	echo "check-modules-standalone: $module (GOWORK=off go build ./...)"
	if ! (cd "$dir" && GOWORK=off go build ./...); then
		echo "$module: GOWORK=off go build ./... failed"
		echo "If generated files are missing, run root: make generate"
		failures=$((failures + 1))
		continue
	fi
done

if [ "$failures" -gt 0 ]; then
	echo "standalone module check failed: $failures problem(s)."
	exit 1
fi

echo "standalone module check is fine (GOWORK=off tidy + build for dst, shared, controlplane, broker)"
