#!/bin/bash
# mds-bench broker host ${broker_id}: mdsbroker + metrics snapshotter + Alloy.
# The control plane runs on dedicated hosts (rank 0 = preferred appender, then hot standbys): -cp-url is the list ${cp_urls}.
# cpclient uses the first URL and moves to the next only on a connect error or HTTP 503 (decision 51); a commit
# that moves carries its segment key, so the replay is safe. With the old store the list has one URL.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

BROKER_ID=${broker_id}
${common}

dnf install -y tar gzip >/dev/null
wait_for_artifacts

mkdir -p /opt/mds /etc/mds
aws s3 cp "s3://$BUCKET/artifacts/mds-linux-amd64.tar.gz" /tmp/mds.tar.gz
tar -xzf /tmp/mds.tar.gz -C /opt/mds
chmod 0755 /opt/mds/mdsbroker
useradd --system --no-create-home --shell /sbin/nologin mds 2>/dev/null || true

PRIVATE_IP=$(imds local-ipv4)
INSTANCE_ID=$(imds instance-id)

cat > /etc/mds/host.env <<ENV
BROKER_ID=$BROKER_ID
PRIVATE_IP=$PRIVATE_IP
INSTANCE_ID=$INSTANCE_ID
ENV

CP_URL=${cp_urls}
CP_FIRST=$${CP_URL%%,*}
# mdsbroker writes its manifest as mdsbroker-<run_id>.json (trailing /).
# Logs are JSON on stderr, which journald keeps and Alloy ships to Loki.
cat > /etc/mds/mdsbroker.env <<ENV
AWS_REGION=$REGION
ENV

# The control-plane hosts boot in parallel: wait (bounded, 10 min) until the first (rank 0) mdscontrolplane answers before starting the
# broker. On timeout the unit is started anyway; Restart=on-failure keeps retrying until the CP is up.
cp_deadline=$(( $(date +%s) + 600 ))
until curl -sf -m 3 "$CP_FIRST/healthz" >/dev/null; do
  if [ "$(date +%s)" -gt "$cp_deadline" ]; then echo "control plane $CP_FIRST not healthy after 10 min; starting broker anyway" >&2; break; fi
  sleep 3
done

cat > /etc/systemd/system/mdsbroker.service <<UNIT
[Unit]
Description=mdsbroker (Kafka on S3)
After=network-online.target
Wants=network-online.target

[Service]
User=mds
EnvironmentFile=/etc/mds/mdsbroker.env
ExecStart=/opt/mds/mdsbroker -listen :9092 -advertise $PRIVATE_IP:9092 -broker-id $BROKER_ID -node-id broker-$BROKER_ID -cp-url $CP_URL -objects s3 -s3-bucket $BUCKET -s3-region $REGION -run-id $RUN_ID -manifest-out s3://$BUCKET/results/$RUN_ID/ -log-level info -metrics-addr 127.0.0.1:9100 ${broker_extra_args}
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

# Metrics snapshotter: the SG opens no metrics ports and loadgen has no
# SSM send-command right, so this host pushes /metrics of mdsbroker to
# s3://BUCKET/results/RUN/metrics/<broker id>/<epoch>-broker.txt every 10 s (mdscontrolplane's go to metrics/cp/ from the CP host).
# summarize.py picks the snapshots around each cell by timestamp.
cat > /usr/local/bin/mb-snapshot.sh <<'SNAP'
#!/bin/bash
set -u
. /etc/mds/snapshot.env
while true; do
  ts=$(date +%s)
  for pair in broker:9100; do
    name=$${pair%%:*}; port=$${pair##*:}
    curl -sf -m 3 "http://127.0.0.1:$port/metrics" | aws s3 cp - "s3://$BUCKET/results/$RUN_ID/metrics/$BROKER_ID/$ts-$name.txt" --only-show-errors || true
  done
  sleep 10
done
SNAP
chmod 0755 /usr/local/bin/mb-snapshot.sh
cat > /etc/mds/snapshot.env <<ENV
BUCKET=$BUCKET
RUN_ID=$RUN_ID
AWS_REGION=$REGION
BROKER_ID=$BROKER_ID
ENV
cat > /etc/systemd/system/mb-snapshot.service <<UNIT
[Unit]
Description=push /metrics snapshots to S3
After=mdsbroker.service

[Service]
ExecStart=/usr/local/bin/mb-snapshot.sh
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now mdsbroker.service mb-snapshot.service
install_alloy broker BROKER_ID=$BROKER_ID PRIVATE_IP=$PRIVATE_IP
touch /var/lib/bench-bootstrap.done
