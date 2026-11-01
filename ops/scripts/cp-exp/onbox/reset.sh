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

python3 - "$TABLE" <<'PY'
import json, subprocess, sys
table = sys.argv[1]
def aws(*a):
    return subprocess.run(["aws", *a], check=True, capture_output=True, text=True).stdout
keys = []
start = None
while True:
    cmd = ["dynamodb", "scan", "--table-name", table, "--projection-expression", "pk, sk", "--output", "json"]
    if start:
        cmd += ["--exclusive-start-key", json.dumps(start)]
    out = json.loads(aws(*cmd))
    keys += out["Items"]
    start = out.get("LastEvaluatedKey")
    if not start:
        break
for i in range(0, len(keys), 25):
    chunk = [{"DeleteRequest": {"Key": k}} for k in keys[i:i + 25]]
    aws("dynamodb", "batch-write-item", "--request-items", json.dumps({table: chunk}))
print("table %s: deleted %d items" % (table, len(keys)))
PY

aws s3 rm "s3://$BUCKET/cp/" --recursive --quiet || true
rm -rf "$CPV_ROOT"/data/* "$CPV_ROOT"/logs/*.log "$CPV_ROOT"/run/*.pid 2>/dev/null || true
echo "reset done: table empty, s3 prefix cp/ empty, local data and logs cleared"
