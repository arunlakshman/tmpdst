#!/usr/bin/env bash
# Push dashboards/*.json to Grafana via the HTTP API.
#   GRAFANA_URL        e.g. https://myorg.grafana.net
#   GRAFANA_API_TOKEN  service account token with dashboards:write (Editor)
#   GRAFANA_FOLDER_UID optional folder uid (default: General)
# Usage: apply-dashboards.sh [--dry-run] [file.json ...]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

dry=0
files=()
for a in "$@"; do
  case "$a" in
    --dry-run) dry=1 ;;
    -h | --help) sed -n '2,8p' "$0"; exit 0 ;;
    *) files+=("$a") ;;
  esac
done
[ "${#files[@]}" -gt 0 ] || files=(dashboards/*.json)

if [ "$dry" -eq 0 ]; then
  : "${GRAFANA_URL:?GRAFANA_URL is required}"
  : "${GRAFANA_API_TOKEN:?GRAFANA_API_TOKEN is required}"
fi
GRAFANA_URL="${GRAFANA_URL:-https://grafana.example.invalid}"
folder="${GRAFANA_FOLDER_UID:-}"
out=$(mktemp)
trap 'rm -f "$out"' EXIT

for f in "${files[@]}"; do
  # Validate and wrap: {dashboard, overwrite, folderUid}; id is dropped so uid decides identity.
  body=$(python3 - "$f" "$folder" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d.pop("id", None)
out = {"dashboard": d, "overwrite": True, "message": "apply-dashboards.sh"}
if sys.argv[2]:
    out["folderUid"] = sys.argv[2]
print(json.dumps(out))
PY
  )
  uid=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["uid"])' "$f")
  if [ "$dry" -eq 1 ]; then
    echo "[dry-run] POST $GRAFANA_URL/api/dashboards/db  <- $f (uid=$uid, ${#body} bytes)"
    continue
  fi
  code=$(printf '%s' "$body" | curl -sS -o "$out" -w '%{http_code}' \
    -X POST "$GRAFANA_URL/api/dashboards/db" \
    -H "Authorization: Bearer $GRAFANA_API_TOKEN" -H 'Content-Type: application/json' --data-binary @-)
  if [ "$code" != 200 ]; then
    echo "FAILED $f: HTTP $code" >&2; cat "$out" >&2; echo >&2; exit 1
  fi
  echo "ok $f -> $GRAFANA_URL/d/$uid"
done
