#!/bin/bash
# Empty the log table and the checkpoint prefix so an experiment starts clean.
# It keeps the table and the bucket. It keeps bin/, scripts/ and results/ in the bucket.
set -euo pipefail
CPV_ROOT="${CPV_ROOT:-/opt/cpv}"
# shellcheck disable=SC1091
. "$CPV_ROOT/env"
unset AWS_PROFILE
export AWS_DEFAULT_REGION="$REGION"
TABLE="${1:-${TABLE_PREFIX}cp}"

if "$CPV_ROOT/scripts/node.sh" status 2>/dev/null | grep -q ' running \| frozen '; then
  echo "refusing to reset: a node is still running or frozen" >&2
  exit 1
fi

"$CPV_ROOT/bin/mdscpreset" -region "$REGION" -table "$TABLE" -bucket "$BUCKET" -prefix cp/

rm -rf "$CPV_ROOT"/data/* "$CPV_ROOT"/logs/*.log "$CPV_ROOT"/run/*.pid 2>/dev/null || true
echo "reset done: table empty, s3 prefix cp/ empty, local data and logs cleared"
