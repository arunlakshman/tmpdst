#!/usr/bin/env bash
# Upload the built binaries and the on-box scripts to S3.
# The box pulls them with onbox/install.sh.
#
# Usage: ops/scripts/cp-exp/push.sh
# Env (optional):
#   BUCKET            default in onbox/defaults.sh
#   AWS_PROFILE_NAME  laptop AWS profile, default in onbox/defaults.sh
set -euo pipefail
# shellcheck disable=SC1091
. "$(dirname "$0")/onbox/defaults.sh"

# Go to the repo root. This script is in ops/scripts/cp-exp/.
cd "$(dirname "$0")/../../.."

PROFILE="$AWS_PROFILE_NAME"
BIN_SRC="build/linux-arm64"
SCRIPTS_SRC="ops/scripts/cp-exp/onbox"

if ! command -v aws >/dev/null 2>&1; then
  echo "error: aws CLI is not installed or not on PATH" >&2
  exit 1
fi

# Refuse to push when the build is missing.
for tool in mdscontrolplane mdscpverify mdscpreset; do
  if [[ ! -f "$BIN_SRC/$tool" ]]; then
    echo "error: $BIN_SRC/$tool is missing. Run ops/scripts/cp-exp/build.sh first." >&2
    exit 1
  fi
done

echo "uploading binaries to s3://$BUCKET/bin/"
aws s3 sync "$BIN_SRC/" "s3://$BUCKET/bin/" --profile "$PROFILE" --region "$REGION"

echo "uploading on-box scripts to s3://$BUCKET/scripts/"
aws s3 sync "$SCRIPTS_SRC/" "s3://$BUCKET/scripts/" --profile "$PROFILE" --region "$REGION"

echo
echo "done. The first time (or after a change), on the box run install.sh:"
echo "  ops/scripts/cp-exp/box.sh \"rm -rf /tmp/cpv-bootstrap && aws s3 cp --recursive s3://$BUCKET/scripts/ /tmp/cpv-bootstrap/ --region $REGION && bash /tmp/cpv-bootstrap/install.sh\""
