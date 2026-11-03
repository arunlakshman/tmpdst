#!/usr/bin/env bash
# Install the control plane test kit on the box. Run it on the box, as ec2-user.
#   - binaries from s3://BUCKET/bin/      to $CPV_ROOT/bin/
#   - scripts from s3://BUCKET/scripts/   to $CPV_ROOT/scripts/
#   - writes $CPV_ROOT/env (region, table prefix, bucket)
# Safe to run again. Files are replaced by rename, so a node that is
# running keeps its old binary until it is restarted.
#
# Run it from a folder that also holds defaults.sh (push.sh prints the
# command: it copies the whole scripts folder to /tmp/cpv-bootstrap first).
#
# Usage: install.sh
# Env (optional): BUCKET, CPV_ROOT
set -euo pipefail
unset AWS_PROFILE AWS_DEFAULT_PROFILE   # the box uses its IAM role, not a profile

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
if [[ ! -f $SCRIPT_DIR/defaults.sh ]]; then
  echo "error: defaults.sh is not next to install.sh in $SCRIPT_DIR. Use the command from push.sh." >&2
  exit 1
fi
. "$SCRIPT_DIR/defaults.sh"

if ! command -v aws >/dev/null 2>&1; then
  echo "error: aws CLI not found" >&2
  exit 1
fi

# make_dir DIR: create DIR and make it writable by this user (sudo if needed).
make_dir() {
  if mkdir -p "$1" 2>/dev/null && [[ -w $1 ]]; then
    return 0
  fi
  sudo mkdir -p "$1"
  sudo chown "$(id -u):$(id -g)" "$1"
}

for d in "$CPV_ROOT" "$CPV_ROOT/bin" "$CPV_ROOT/scripts" "$CPV_ROOT/data" \
         "$CPV_ROOT/logs" "$CPV_ROOT/run" "$CPV_ROOT/results"; do
  make_dir "$d"
done

shopt -s nullglob

# fetch_into PREFIX DIR: copy every object under s3://BUCKET/PREFIX/ into DIR.
# New files are moved in with rename (mv), so running programs are not hurt.
fetch_into() {
  local prefix=$1 dir=$2 stage="$2.new" f
  rm -rf "$stage"
  aws s3 cp --recursive "s3://$BUCKET/$prefix/" "$stage/" --region "$REGION"
  for f in "$stage"/*; do
    chmod 0755 "$f"
    mv -f "$f" "$dir/$(basename "$f")"
  done
  rmdir "$stage"
}

fetch_into bin "$CPV_ROOT/bin"
fetch_into scripts "$CPV_ROOT/scripts"

for t in mdscontrolplane mdscpverify; do
  if [[ ! -x $CPV_ROOT/bin/$t ]]; then
    echo "error: $t is missing from $CPV_ROOT/bin after the download" >&2
    exit 1
  fi
done

# The env file is read by node.sh and collect.sh.
tmp="$CPV_ROOT/env.tmp"
cat > "$tmp" <<EOF
REGION=$REGION
TABLE_PREFIX=$TABLE_PREFIX
BUCKET=$BUCKET
EOF
mv -f "$tmp" "$CPV_ROOT/env"

echo "installed into $CPV_ROOT:"
ls -l "$CPV_ROOT/bin" "$CPV_ROOT/scripts"
echo "env:"
cat "$CPV_ROOT/env"

# Show which role the box uses. This check does not stop the install.
aws sts get-caller-identity --region "$REGION" --output text || echo "warning: could not read the caller identity"
