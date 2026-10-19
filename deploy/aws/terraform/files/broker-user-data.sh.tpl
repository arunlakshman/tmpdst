#!/bin/bash
# manifester-bench broker host ${broker_id}: manibroker + metrics snapshotter + Alloy.
# The ONE manicp (single sequencer, decision 46) runs on the dedicated control-plane host ${cp_private_ip}:8080.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

BROKER_ID=${broker_id}
${common}

dnf install -y tar gzip >/dev/null
wait_for_artifacts

mkdir -p /opt/manifester /etc/manifester
aws s3 cp "s3://$BUCKET/artifacts/manifester-linux-amd64.tar.gz" /tmp/manifester.tar.gz
tar -xzf /tmp/manifester.tar.gz -C /opt/manifester
chmod 0755 /opt/manifester/manibroker
useradd --system --no-create-home --shell /sbin/nologin manifester 2>/dev/null || true

PRIVATE_IP=$(imds local-ipv4)
INSTANCE_ID=$(imds instance-id)

cat > /etc/manifester/host.env <<ENV
BROKER_ID=$BROKER_ID
PRIVATE_IP=$PRIVATE_IP
INSTANCE_ID=$INSTANCE_ID
ENV

CP_URL=http://${cp_private_ip}:8080
# manibroker writes its manifest as manibroker-<run_id>.json (trailing /).
# Logs are JSON on stderr, which journald keeps and Alloy ships to Loki.
cat > /etc/manifester/manibroker.env <<ENV
AWS_REGION=$REGION
ENV

# The control-plane host boots in parallel: wait (bounded, 10 min) until its manicp answers before starting the
# broker. On timeout the unit is started anyway; Restart=on-failure keeps retrying until the CP is up.
cp_deadline=$(( $(date +%s) + 600 ))
until curl -sf -m 3 "$CP_URL/healthz" >/dev/null; do
  if [ "$(date +%s)" -gt "$cp_deadline" ]; then echo "control plane $CP_URL not healthy after 10 min; starting broker anyway" >&2; break; fi
  sleep 3
done

cat > /etc/systemd/system/manibroker.service <<UNIT
[Unit]
Description=manibroker (Kafka on S3)
After=network-online.target
Wants=network-online.target

[Service]
User=manifester
EnvironmentFile=/etc/manifester/manibroker.env
ExecStart=/opt/manifester/manibroker -listen :9092 -advertise $PRIVATE_IP:9092 -broker-id $BROKER_ID -node-id broker-$BROKER_ID -cp-url $CP_URL -objects s3 -s3-bucket $BUCKET -s3-region $REGION -run-id $RUN_ID -manifest-out s3://$BUCKET/results/$RUN_ID/ -log-level info -metrics-addr 127.0.0.1:9100 ${broker_extra_args}
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

# Metrics snapshotter: the SG opens no metrics ports and loadgen has no
# SSM send-command right, so this host pushes /metrics of manibroker to
# s3://BUCKET/results/RUN/metrics/<broker id>/<epoch>-broker.txt every 10 s (manicp's go to metrics/cp/ from the CP host).
# summarize.py picks the snapshots around each cell by timestamp.
cat > /usr/local/bin/mb-snapshot.sh <<'SNAP'
#!/bin/bash
set -u
. /etc/manifester/snapshot.env
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
cat > /etc/manifester/snapshot.env <<ENV
BUCKET=$BUCKET
RUN_ID=$RUN_ID
AWS_REGION=$REGION
BROKER_ID=$BROKER_ID
ENV
cat > /etc/systemd/system/mb-snapshot.service <<UNIT
[Unit]
Description=push /metrics snapshots to S3
After=manibroker.service

[Service]
ExecStart=/usr/local/bin/mb-snapshot.sh
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now manibroker.service mb-snapshot.service
install_alloy broker BROKER_ID=$BROKER_ID PRIVATE_IP=$PRIVATE_IP
touch /var/lib/bench-bootstrap.done
