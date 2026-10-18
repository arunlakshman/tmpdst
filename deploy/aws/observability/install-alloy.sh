#!/usr/bin/env bash
# Idempotent Alloy install + config for an EC2 host. Target: Amazon Linux 2023
# (dnf, systemd, aws cli v2 preinstalled). Run as root from user-data:
#
#   RUN=bench-0042 ROLE=broker OBS_DIR=/opt/manifester/deploy/aws/observability \
#     bash /opt/manifester/deploy/aws/observability/install-alloy.sh
#
# Inputs (environment):
#   RUN       run id; also the SSM path component (required)
#   ROLE      broker | loadgen (required)
#   OBS_DIR   directory holding alloy/ (default: this script's directory)
#   ALLOY_VERSION  rpm version to pin (default below)
#   SSM_PREFIX     default /manifester/$RUN
#   AWS_REGION     default: instance region from IMDS
#
# SSM parameters read with the instance role (SecureString for secrets):
#   $SSM_PREFIX/grafana/prom_url  prom_user  loki_url  loki_user  api_key
#   $SSM_PREFIX/aws/ddb_table  s3_bucket  s3_metrics_filter_id   (broker only)
# Instance role needs ssm:GetParametersByPath on $SSM_PREFIX/* and kms:Decrypt
# on the key if SecureStrings use a CMK.
set -euo pipefail

: "${RUN:?RUN is required}"
: "${ROLE:?ROLE is required (broker|loadgen)}"
case "$ROLE" in broker | loadgen) ;; *) echo "ROLE must be broker or loadgen" >&2; exit 2 ;; esac
ALLOY_VERSION="${ALLOY_VERSION:-1.10.1}"
SSM_PREFIX="${SSM_PREFIX:-/manifester/$RUN}"
OBS_DIR="${OBS_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

if [ -z "${AWS_REGION:-}" ]; then
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
  AWS_REGION=$(curl -fsS -H "X-aws-ec2-metadata-token: $tok" http://169.254.169.254/latest/meta-data/placement/region)
fi
export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

# --- package (grafana rpm repo, version pinned) ------------------------------
if ! rpm -q "alloy-${ALLOY_VERSION}" >/dev/null 2>&1; then
  cat >/etc/yum.repos.d/grafana.repo <<'REPO'
[grafana]
name=grafana
baseurl=https://rpm.grafana.com
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://rpm.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
REPO
  dnf install -y "alloy-${ALLOY_VERSION}" || dnf install -y "alloy-${ALLOY_VERSION}-1"
fi
id alloy >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin alloy
usermod -aG systemd-journal alloy
install -d -o alloy -g alloy -m 0750 /var/lib/alloy/data
install -d -m 0755 /etc/alloy

# --- config files ------------------------------------------------------------
install -m 0644 "$OBS_DIR/alloy/config.alloy" /etc/alloy/config.alloy
install -m 0644 "$OBS_DIR/alloy/$ROLE.alloy" "/etc/alloy/$ROLE.alloy"
# drop the other role's file if the host was re-purposed
for r in broker loadgen; do [ "$r" = "$ROLE" ] || rm -f "/etc/alloy/$r.alloy"; done
install -m 0644 "$OBS_DIR/alloy/alloy.service" /etc/systemd/system/alloy.service

# --- secrets from SSM -> /etc/alloy/alloy.env (0600, never in the repo) -------
ssm() { # ssm <relative-name> [default]; fails if missing and no default
  local v
  if v=$(aws ssm get-parameter --with-decryption --name "$SSM_PREFIX/$1" --query Parameter.Value --output text 2>/dev/null); then
    printf '%s' "$v"
  elif [ "$#" -ge 2 ]; then
    printf '%s' "$2"
  else
    echo "missing SSM parameter $SSM_PREFIX/$1" >&2; return 1
  fi
}
umask 077
tmp=$(mktemp /etc/alloy/alloy.env.XXXXXX)
{
  echo "RUN_ID=$RUN"
  echo "HOST_ROLE=$ROLE"
  echo "HOST_NAME=$(hostname -s)"
  echo "AWS_REGION=$AWS_REGION"
  echo "GRAFANA_PROM_URL=$(ssm grafana/prom_url)"
  echo "GRAFANA_PROM_USER=$(ssm grafana/prom_user)"
  echo "GRAFANA_LOKI_URL=$(ssm grafana/loki_url)"
  echo "GRAFANA_LOKI_USER=$(ssm grafana/loki_user)"
  echo "GRAFANA_API_KEY=$(ssm grafana/api_key)"
  if [ "$ROLE" = broker ]; then
    echo "DDB_TABLE=$(ssm aws/ddb_table)"
    echo "S3_BUCKET=$(ssm aws/s3_bucket)"
    echo "S3_METRICS_FILTER_ID=$(ssm aws/s3_metrics_filter_id manifester-bench)"
  fi
} >"$tmp"
chown root:root "$tmp"; chmod 0600 "$tmp"
mv -f "$tmp" /etc/alloy/alloy.env   # systemd reads it as root before dropping to User=

# --- start -------------------------------------------------------------------
systemctl daemon-reload
systemctl enable alloy >/dev/null 2>&1 || true
systemctl restart alloy
sleep 2
systemctl is-active --quiet alloy || { journalctl -u alloy -n 30 --no-pager >&2; exit 1; }
echo "alloy $ALLOY_VERSION active (run=$RUN role=$ROLE)"
