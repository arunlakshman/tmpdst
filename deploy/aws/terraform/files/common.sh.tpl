# Shared bootstrap (rendered by templatefile, injected into both user-data scripts).
BUCKET=${bucket}
REGION=${region}
RUN_ID=${run_id}
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION

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

install_node_exporter() {
  local v=${node_exporter_version}
  curl -fsSL "https://github.com/prometheus/node_exporter/releases/download/v$v/node_exporter-$v.linux-amd64.tar.gz" | tar -xz -C /tmp
  install -m 0755 "/tmp/node_exporter-$v.linux-amd64/node_exporter" /usr/local/bin/node_exporter
  useradd --system --no-create-home --shell /sbin/nologin node_exporter 2>/dev/null || true
  cat > /etc/systemd/system/node_exporter.service <<UNIT
[Unit]
Description=node_exporter
After=network-online.target
Wants=network-online.target

[Service]
User=node_exporter
ExecStart=/usr/local/bin/node_exporter --web.listen-address=127.0.0.1:9110
Restart=always

[Install]
WantedBy=multi-user.target
UNIT
}

# Alloy contract: operator uploads deploy/aws/observability/alloy/* to
# s3://BUCKET/artifacts/alloy/ ; Alloy runs on that directory. Credentials and
# labels arrive as environment variables in /etc/alloy/env (see README).
install_alloy() { # install_alloy <role>
  local role=$1 v=${alloy_version}
  dnf install -y unzip >/dev/null
  curl -fsSL -o /tmp/alloy.zip "https://github.com/grafana/alloy/releases/download/v$v/alloy-linux-amd64.zip"
  unzip -o /tmp/alloy.zip -d /tmp/alloy-bin
  install -m 0755 /tmp/alloy-bin/alloy-linux-amd64 /usr/local/bin/alloy

  mkdir -p /etc/alloy/conf.d /var/lib/alloy
  if ! aws s3 cp --recursive "s3://$BUCKET/artifacts/alloy/" /etc/alloy/conf.d/ || [ -z "$(ls -A /etc/alloy/conf.d)" ]; then
    echo "no Alloy config under artifacts/alloy/: Alloy not started" >&2
    return 0
  fi

  getp() { aws ssm get-parameter --with-decryption --name "$1" --query Parameter.Value --output text; }
  umask 077
  cat > /etc/alloy/env <<ENV
GRAFANA_REMOTE_WRITE_URL=$(getp ${grafana_url_param})
GRAFANA_USER=$(getp ${grafana_user_param})
GRAFANA_API_KEY=$(getp ${grafana_api_key_param})
RUN_ID=$RUN_ID
INSTANCE_ROLE=$role
AWS_REGION=$REGION
S3_BUCKET=$BUCKET
ENV
  [ "$role" = broker ] && echo "DYNAMODB_TABLE=$DYNAMO_TABLE" >> /etc/alloy/env
  umask 022

  cat > /etc/systemd/system/alloy.service <<UNIT
[Unit]
Description=Grafana Alloy
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=/etc/alloy/env
ExecStart=/usr/local/bin/alloy run --server.http.listen-addr=127.0.0.1:12345 --storage.path=/var/lib/alloy /etc/alloy/conf.d
Restart=always

[Install]
WantedBy=multi-user.target
UNIT
  systemctl enable --now alloy.service
}
