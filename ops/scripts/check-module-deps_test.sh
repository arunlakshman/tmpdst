#!/usr/bin/env bash
# One test case for check-module-deps.sh.
# It makes a temp root with four go.mod files. The shared go.mod requires
# controlplane, which is to its right. The guard must fail and name both modules.

set -u

here="$(cd "$(dirname "$0")" && pwd)"
guard="$here/check-module-deps.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mk() {
	mkdir -p "$tmp/$1"
	printf 'module github.com/manifesterio/mds/%s\n\ngo 1.26.0\n\n%s\n' "$1" "$2" >"$tmp/$1/go.mod"
}

mk dst ''
mk shared 'require github.com/manifesterio/mds/controlplane v0.0.0'
mk controlplane 'require github.com/manifesterio/mds/dst v0.0.0'
mk broker 'require github.com/manifesterio/mds/controlplane v0.0.0'

out="$(bash "$guard" "$tmp" 2>&1)"
code=$?

echo "$out"

if [ "$code" -eq 0 ]; then
	echo "FAIL: the guard passed, but shared requires controlplane"
	exit 1
fi

want="shared must not depend on controlplane (found in shared/go.mod)"
if ! printf '%s\n' "$out" | grep -qF "$want"; then
	echo "FAIL: the message does not name both modules. Want: $want"
	exit 1
fi

echo "PASS: the guard fails on a wrong require and names both modules"
