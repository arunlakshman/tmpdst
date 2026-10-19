# Shared bootstrap (rendered by templatefile, injected into both user-data scripts).
BUCKET=${bucket}
REGION=${region}
RUN_ID=${run_id}
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION

# Self-destruct guard: with instance_initiated_shutdown_behavior=terminate this
# terminates the instance ${max_runtime_minutes} min after boot.
shutdown -h +${max_runtime_minutes} "bench max_runtime_minutes reached" || echo "could not schedule shutdown" >&2

imds() {
  local tok
  tok=$(curl -sf -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -sf -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/meta-data/$1"
}

retry() { # retry <attempts> <cmd...>
  local n=$1; shift
  local i
  for i in $(seq 1 "$n"); do "$@" && return 0; sleep 5; done
  return 1
}

wait_for_artifacts() {
  local deadline=$(( $(date +%s) + ${artifact_wait_minutes} * 60 ))
  until aws s3api head-object --bucket "$BUCKET" --key artifacts/READY >/dev/null 2>&1; do
    if [ "$(date +%s)" -gt "$deadline" ]; then
      echo "artifacts/READY not found in s3://$BUCKET after ${artifact_wait_minutes} min; upload with scripts/build-and-upload.sh" >&2
      return 1
    fi
    sleep 15
  done
}

# Alloy: install-alloy.sh (deploy/aws/observability) is the single installer. The
# operator's build-and-upload.sh publishes that directory to
# s3://BUCKET/artifacts/observability/. It writes /etc/alloy/alloy.env from SSM.
install_alloy() { # install_alloy <role> [extra VAR=value ...]
  local role=$1; shift
  mkdir -p /opt/manifester-obs
  aws s3 sync "s3://$BUCKET/artifacts/observability/" /opt/manifester-obs/
  if [ ! -f /opt/manifester-obs/install-alloy.sh ]; then
    echo "no artifacts/observability/: Alloy not installed" >&2
    return 0
  fi
  env RUN="$RUN_ID" ROLE="$role" OBS_DIR=/opt/manifester-obs ALLOY_VERSION=${alloy_version} \
    SSM_PREFIX=${ssm_prefix} \
    PARAM_PROM_URL=${grafana_url_param} PARAM_PROM_USER=${grafana_user_param} \
    PARAM_API_KEY=${grafana_api_key_param} \
    PARAM_PROM_KEY=${grafana_prom_key_param} PARAM_LOKI_KEY=${grafana_loki_key_param} PARAM_LOKI_URL=${grafana_loki_url_param} \
    PARAM_LOKI_USER=${grafana_loki_user_param} S3_BUCKET="$BUCKET" \
    S3_METRICS_FILTER_ID=${metrics_filter_id} "$@" \
    bash /opt/manifester-obs/install-alloy.sh
}
