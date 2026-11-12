#!/usr/bin/env bash
# Checks the dependency order of the four Go modules in this repo.
#
# The order is:  dst <- shared <- controlplane <- broker
# A module may require only the modules to its left:
#   dst          requires none of the others
#   shared       may require dst
#   controlplane may require dst and shared
#   broker       may require any of them
#
# The script reads the require lines of each go.mod. Both forms count:
# the single line form and the require ( ... ) block. Direct and indirect
# lines both count. Replace lines do not count.
#
# Usage: check-module-deps.sh [root]
#   root is the repo root. Default: the folder above this script.
# Exit 0 when the order is kept. Exit 1 and one message per problem otherwise.

set -u

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
prefix="github.com/manifesterio/mds"

# Modules in order, left to right. The position is the rank.
modules="dst shared controlplane broker"

rank_of() {
	local i=0
	local m
	for m in $modules; do
		if [ "$m" = "$1" ]; then
			echo "$i"
			return 0
		fi
		i=$((i + 1))
	done
	echo "-1"
}

# Print the module path of every require line in a go.mod file.
required_paths() {
	awk '
		/^[ \t]*require[ \t]*\(/ { inblock = 1; next }
		inblock && /^[ \t]*\)/ { inblock = 0; next }
		inblock { print $1; next }
		/^[ \t]*require[ \t]+/ { print $2 }
	' "$1"
}

failures=0

for module in $modules; do
	gomod="$root/$module/go.mod"
	if [ ! -f "$gomod" ]; then
		echo "missing go.mod for module $module (expected $gomod)"
		failures=$((failures + 1))
		continue
	fi

	module_rank=$(rank_of "$module")

	for path in $(required_paths "$gomod"); do
		case "$path" in
		"$prefix"/*) ;;
		*) continue ;;
		esac
		required="${path#"$prefix"/}"

		# Only the four module names count. Other paths under the prefix are ignored.
		required_rank=$(rank_of "$required")
		if [ "$required_rank" = "-1" ]; then
			continue
		fi

		if [ "$required_rank" -gt "$module_rank" ]; then
			echo "$module must not depend on $required (found in $module/go.mod)"
			failures=$((failures + 1))
		fi
	done
done

if [ "$failures" -gt 0 ]; then
	echo "module dependency order check failed: $failures problem(s)."
	echo "The order is dst, shared, controlplane, broker. A module may depend only on the modules before it."
	exit 1
fi

echo "module dependency order is fine (dst <- shared <- controlplane <- broker)"
