#!/usr/bin/env bash
# Download experiment results from S3 into build/results/ at the repo root.
#
# Usage:
#   fetch-results.sh                 list the result folders in S3
#   fetch-results.sh EXPERIMENT      download every result folder whose name
#                                    starts with EXPERIMENT. Folders are named
#                                    NAME-UTCSTAMP, e.g. kill9-20261101T120000Z
# Env (optional):
#   BUCKET            default mds-cpverify-cpv1-478483354994
#   AWS_PROFILE_NAME  laptop AWS profile, default hydsandbox
set -euo pipefail

# Go to the repo root. This script is in ops/scripts/cp-exp/.
cd "$(dirname "$0")/../../.."

BUCKET="${BUCKET:-mds-cpverify-cpv1-478483354994}"
PROFILE="${AWS_PROFILE_NAME:-hydsandbox}"
REGION="ap-south-2"
DEST="build/results"

if [[ $# -eq 0 ]]; then
  aws s3 ls "s3://$BUCKET/results/" --profile "$PROFILE" --region "$REGION"
  exit 0
fi

name=$1
if ! [[ $name =~ ^[A-Za-z][A-Za-z0-9-]*$ ]]; then
  echo "error: bad experiment name '$name'" >&2
  exit 2
fi

mkdir -p "$DEST"
aws s3 sync "s3://$BUCKET/results/" "$DEST/" --profile "$PROFILE" --region "$REGION" \
  --exclude '*' --include "$name*"

echo "downloaded to: $PWD/$DEST"
shopt -s nullglob
for d in "$DEST"/"$name"*; do
  echo "  $d"
done
