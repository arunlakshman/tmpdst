#!/usr/bin/env bash
# Fails if any tracked code file contains a non-ASCII byte (smart quotes,
# em dashes, section signs, zero-width or bidi characters, ...). Prints
# file:line for each hit so a failure is directly fixable.
set -euo pipefail
cd "$(dirname "$0")/.."
hits=$(git ls-files '*.go' '*.sh' '*.py' '*.yml' '*.yaml' 'Makefile' \
  | xargs env LC_ALL=C grep -nI '[^[:print:][:space:]]' || true)
if [[ -n $hits ]]; then
  echo "non-ASCII characters in code files (use plain ASCII):"
  echo "$hits"
  exit 1
fi
