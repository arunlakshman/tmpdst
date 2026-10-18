#!/bin/bash
# manifester-bench broker host: manibroker + manicp + node_exporter + Alloy.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

DYNAMO_TABLE=${table_name}
${common}

dnf install -y tar gzip >/dev/null
wait_for_artifacts

mkdir -p /opt/manifester /etc/manifester
aws s3 cp "s3://$BUCKET/artifacts/manifester-linux-amd64.tar.gz" /tmp/manifester.tar.gz
tar -xzf /tmp/manifester.tar.gz -C /opt/manifester
chmod 0755 /opt/manifester/manibroker /opt/manifester/manicp
useradd --system --no-create-home --shell /sbin/nologin manifester 2>/dev/null || true

PRIVATE_IP=$(imds local-ipv4)
INSTANCE_ID=$(imds instance-id)

# Flags -run-id, -manifest-out and -log-level are added to cmd/manibroker
# concurrently; if the uploaded binary predates them, the unit fails fast.
cat > /etc/manifester/manicp.env <<ENV
AWS_REGION=$REGION
ENV
cat > /etc/manifester/manibroker.env <<ENV
AWS_REGION=$REGION
ENV

cat > /etc/systemd/system/manicp.service <<UNIT
[Unit]
Description=manicp (control plane, DynamoDB)
After=network-online.target
Wants=network-online.target

[Service]
User=manifester
EnvironmentFile=/etc/manifester/manicp.env
ExecStart=/opt/manifester/manicp -addr 127.0.0.1:8080 -store dynamo -region $REGION -table-prefix ${table_prefix} -metrics-addr 127.0.0.1:9101 ${cp_extra_args}
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

cat > /etc/systemd/system/manibroker.service <<UNIT
[Unit]
Description=manibroker (Kafka on S3)
After=manicp.service network-online.target
Wants=manicp.service network-online.target

[Service]
User=manifester
EnvironmentFile=/etc/manifester/manibroker.env
ExecStart=/opt/manifester/manibroker -listen :9092 -advertise $PRIVATE_IP:9092 -broker-id 1 -node-id broker-1 -cp-url http://127.0.0.1:8080 -objects s3 -s3-bucket $BUCKET -s3-region $REGION -run-id $RUN_ID -manifest-out s3://$BUCKET/results/$RUN_ID/manifest.json -log-level info -metrics-addr 127.0.0.1:9100 ${broker_extra_args}
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

install_node_exporter
systemctl daemon-reload
systemctl enable --now node_exporter.service manicp.service manibroker.service
install_alloy broker
touch /var/lib/bench-bootstrap.done
