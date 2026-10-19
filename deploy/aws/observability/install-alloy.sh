#!/usr/bin/env bash
# Idempotent Alloy install + config for an EC2 host. Target: Amazon Linux 2023
# (dnf, systemd, aws cli v2 preinstalled). Run as root from user-data:
#
#   RUN=bench-0042 ROLE=broker OBS_DIR=/opt/manifester/deploy/aws/observability \
#     bash /opt/manifester/deploy/aws/observability/install-alloy.sh
#
# Inputs (environment):
#   RUN       run id, becomes the run_id label (required)
#   ROLE      broker | cp | loadgen (required)
#   OBS_DIR   directory holding alloy/ (default: this script's directory)
#   ALLOY_VERSION  rpm version to pin (default below)
#   SSM_PREFIX     default /manifester-bench (the Terraform allowed_ssm_param_prefix, no trailing /)
#   PARAM_PROM_URL PARAM_PROM_USER PARAM_PROM_KEY PARAM_LOKI_URL PARAM_LOKI_USER PARAM_LOKI_KEY PARAM_API_KEY
#                  full SSM names; defaults $SSM_PREFIX/grafana/{remote_write_url,user,prom_api_key,loki_url,loki_user,loki_api_key}
#                  PARAM_API_KEY (grafana/api_key) is a fallback used for whichever key is missing
#   DDB_TABLE (cp only) S3_BUCKET S3_METRICS_FILTER_ID (broker only); when unset read from SSM
#                  $SSM_PREFIX/aws/{ddb_table,s3_bucket,s3_metrics_filter_id} (filter default manifester-bench)
#   BROKER_ID      broker only: 1..N, becomes the `broker` label on manibroker/manicp series (default 1)
#   PRIVATE_IP     accepted, informational
#   AWS_REGION     default: instance region from IMDS
#
# SSM parameters read with the instance role (SecureString for secrets):
#   grafana/remote_write_url  user  prom_api_key  loki_url  loki_user  loki_api_key   (under $SSM_PREFIX;
#   a single combined api_key works when the two keys are absent)
# Instance role needs ssm:GetParametersByPath on $SSM_PREFIX/* and kms:Decrypt
# on the key if SecureStrings use a CMK.
set -euo pipefail

: "${RUN:?RUN is required}"
: "${ROLE:?ROLE is required (broker|cp|loadgen)}"
case "$ROLE" in broker | cp | loadgen) ;; *) echo "ROLE must be broker, cp or loadgen" >&2; exit 2 ;; esac
ALLOY_VERSION="${ALLOY_VERSION:-1.10.1}"
SSM_PREFIX="${SSM_PREFIX:-/manifester-bench}"
SSM_PREFIX="${SSM_PREFIX%/}"
PARAM_PROM_URL="${PARAM_PROM_URL:-$SSM_PREFIX/grafana/remote_write_url}"
PARAM_PROM_USER="${PARAM_PROM_USER:-$SSM_PREFIX/grafana/user}"
PARAM_PROM_KEY="${PARAM_PROM_KEY:-$SSM_PREFIX/grafana/prom_api_key}"
PARAM_LOKI_KEY="${PARAM_LOKI_KEY:-$SSM_PREFIX/grafana/loki_api_key}"
PARAM_API_KEY="${PARAM_API_KEY:-$SSM_PREFIX/grafana/api_key}" # combined fallback
PARAM_LOKI_URL="${PARAM_LOKI_URL:-$SSM_PREFIX/grafana/loki_url}"
PARAM_LOKI_USER="${PARAM_LOKI_USER:-$SSM_PREFIX/grafana/loki_user}"
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
for r in broker cp loadgen; do [ "$r" = "$ROLE" ] || rm -f "/etc/alloy/$r.alloy"; done
install -m 0644 "$OBS_DIR/alloy/alloy.service" /etc/systemd/system/alloy.service

# --- secrets from SSM -> /etc/alloy/alloy.env (0600, never in the repo) -------
ssm() { # ssm <full-name> [default]; fails if missing and no default
  local v
  if v=$(aws ssm get-parameter --with-decryption --name "$1" --query Parameter.Value --output text 2>/dev/null); then
    printf '%s' "$v"
  elif [ "$#" -ge 2 ]; then
    printf '%s' "$2"
  else
    echo "missing SSM parameter $1" >&2; return 1
  fi
}
prom_url=$(ssm "$PARAM_PROM_URL")
prom_user=$(ssm "$PARAM_PROM_USER")
loki_url=$(ssm "$PARAM_LOKI_URL")
loki_user=$(ssm "$PARAM_LOKI_USER")
fallback_key=$(ssm "$PARAM_API_KEY" "")
prom_key=$(ssm "$PARAM_PROM_KEY" "$fallback_key")
loki_key=$(ssm "$PARAM_LOKI_KEY" "$fallback_key")
[ -n "$prom_key" ] && [ -n "$loki_key" ] || { echo "no Grafana API key: need $PARAM_PROM_KEY and $PARAM_LOKI_KEY, or $PARAM_API_KEY" >&2; exit 1; }
if [ "$ROLE" = cp ]; then
  ddb_table=${DDB_TABLE:-$(ssm "$SSM_PREFIX/aws/ddb_table")}
fi
if [ "$ROLE" = broker ]; then
  s3_bucket=${S3_BUCKET:-$(ssm "$SSM_PREFIX/aws/s3_bucket")}
  s3_filter=${S3_METRICS_FILTER_ID:-$(ssm "$SSM_PREFIX/aws/s3_metrics_filter_id" manifester-bench)}
fi
umask 077
tmp=$(mktemp /etc/alloy/alloy.env.XXXXXX)
{
  echo "RUN_ID=$RUN"
  echo "HOST_ROLE=$ROLE"
  echo "HOST_NAME=$(hostname -s)"
  echo "AWS_REGION=$AWS_REGION"
  echo "GRAFANA_PROM_URL=$prom_url"
  echo "GRAFANA_PROM_USER=$prom_user"
  echo "GRAFANA_LOKI_URL=$loki_url"
  echo "GRAFANA_LOKI_USER=$loki_user"
  echo "GRAFANA_PROM_API_KEY=$prom_key"
  echo "GRAFANA_LOKI_API_KEY=$loki_key"
  if [ "$ROLE" = cp ]; then
    echo "DDB_TABLE=$ddb_table"
  fi
  if [ "$ROLE" = broker ]; then
    echo "BROKER_ID=${BROKER_ID:-1}"
    echo "S3_BUCKET=$s3_bucket"
    echo "S3_METRICS_FILTER_ID=$s3_filter"
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
