#!/usr/bin/env bash
# Show what the control plane stored in DynamoDB and S3 (cp-verify stack), decoded.
#   ops/scripts/cp-inspect.sh            summary
#   ops/scripts/cp-inspect.sh -v         one line per log slot
#   ops/scripts/cp-inspect.sh -slot 17   everything inside slot 17
# Needs: aws sso login --profile hydsandbox
set -euo pipefail
cd "$(dirname "$0")/../.."
PROFILE="${AWS_PROFILE_NAME:-hydsandbox}"
eval "$(aws configure export-credentials --profile "$PROFILE" --format env)"
go run ./cmd/mdscpinspect -region ap-south-2 -table mds_cpv1_cp \
  -bucket mds-cpverify-cpv1-478483354994 "$@"
