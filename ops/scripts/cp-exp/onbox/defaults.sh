#!/bin/bash
# The one place for the defaults of the control plane test scripts:
# region, table prefix, bucket, root folder on the box, and laptop AWS profile.
# Source this file; do not run it. It lives in onbox/, so push.sh uploads it,
# and install.sh puts it next to the on-box scripts.
#
# Overridable from the environment: BUCKET, CPV_ROOT, AWS_PROFILE_NAME.
# REGION and TABLE_PREFIX are fixed: the table and the bucket are made there.

REGION="ap-south-2"
TABLE_PREFIX="mds_cpv1_"
BUCKET="${BUCKET:-mds-cpverify-cpv1-478483354994}"
CPV_ROOT="${CPV_ROOT:-/opt/cpv}"
AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-hydsandbox}"
