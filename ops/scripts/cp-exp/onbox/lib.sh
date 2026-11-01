#!/bin/bash
# Shared helpers for the experiment scripts. Source this file; do not run it.
# Each experiment prints one PASS or FAIL line per success criterion, then a verdict.
set -uo pipefail

CPV_ROOT="${CPV_ROOT:-/opt/cpv}"
# shellcheck disable=SC1091
. "$CPV_ROOT/env"
export AWS_DEFAULT_REGION="$REGION"
unset AWS_PROFILE
export PATH="$CPV_ROOT/bin:$CPV_ROOT/scripts:$PATH"
TABLE="${TABLE_PREFIX}cp"

FAILED=0
VERDICT_FILE="$CPV_ROOT/results/verdict.txt"
: >"$VERDICT_FILE"

# criterion NAME OK(0 or 1) DETAIL
criterion() {
  local status=PASS
  if [ "$2" != "1" ]; then status=FAIL; FAILED=$((FAILED + 1)); fi
  printf '%s  %-58s %s\n' "$status" "$1" "$3" | tee -a "$VERDICT_FILE"
}

verdict() {
  echo "== $1: $FAILED criterion(s) failed" | tee -a "$VERDICT_FILE"
  [ "$FAILED" -eq 0 ]
}

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# json_field FILE_OR_STRING PYTHON_EXPR_ON_d  e.g. json_field "$line" 'd["acked"]'
json_field() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }

# last_summary FILE : the last JSON line of a load run
last_summary() { grep '^{' "$1" | tail -1; }

# ge A B -> prints 1 when A <= B for numbers (use: criterion ... "$(le 12 20)")
le() { python3 -c 'import sys; print(1 if float(sys.argv[1]) <= float(sys.argv[2]) else 0)' "$1" "$2"; }

# count_in_logs PATTERN : lines matching in all node logs
count_in_logs() { cat "$CPV_ROOT"/logs/*.log 2>/dev/null | grep -c -- "$1" || true; }

# checkpoint decisions written / skipped for one reason
decisions() { cat "$CPV_ROOT"/logs/*.log 2>/dev/null | grep checkpoint_decision || true; }
