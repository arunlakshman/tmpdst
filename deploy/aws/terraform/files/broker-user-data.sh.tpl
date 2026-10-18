#!/bin/bash
# manifester-bench broker host: manibroker + manicp + metrics snapshotter + Alloy.
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

# Both services write their manifest as <service>-<run_id>.json (trailing /).
# Logs are JSON on stderr, which journald keeps and Alloy ships to Loki.
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
ExecStart=/opt/manifester/manicp -addr 127.0.0.1:8080 -store dynamo -region $REGION -table-prefix ${table_prefix} -metrics-addr 127.0.0.1:9101 -run-id $RUN_ID -manifest-out s3://$BUCKET/results/$RUN_ID/ -log-level info ${cp_extra_args}
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
ExecStart=/opt/manifester/manibroker -listen :9092 -advertise $PRIVATE_IP:9092 -broker-id 1 -node-id broker-1 -cp-url http://127.0.0.1:8080 -objects s3 -s3-bucket $BUCKET -s3-region $REGION -run-id $RUN_ID -manifest-out s3://$BUCKET/results/$RUN_ID/ -log-level info -metrics-addr 127.0.0.1:9100 ${broker_extra_args}
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

# Metrics snapshotter: the SG opens no metrics ports and loadgen has no
# SSM send-command right, so this host pushes /metrics of both services to
# s3://BUCKET/results/RUN/metrics/<epoch>-{broker,manicp}.txt every 10 s.
# summarize.py picks the snapshots around each cell by timestamp.
cat > /usr/local/bin/mb-snapshot.sh <<'SNAP'
#!/bin/bash
set -u
. /etc/manifester/snapshot.env
while true; do
  ts=$(date +%s)
  for pair in broker:9100 manicp:9101; do
    name=$${pair%%:*}; port=$${pair##*:}
    curl -sf -m 3 "http://127.0.0.1:$port/metrics" | aws s3 cp - "s3://$BUCKET/results/$RUN_ID/metrics/$ts-$name.txt" --only-show-errors || true
  done
  sleep 10
done
SNAP
chmod 0755 /usr/local/bin/mb-snapshot.sh
cat > /etc/manifester/snapshot.env <<ENV
BUCKET=$BUCKET
RUN_ID=$RUN_ID
AWS_REGION=$REGION
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
systemctl enable --now manicp.service manibroker.service mb-snapshot.service
# The broker implements neither CreateTopics nor DescribeConfigs, so
# kafka-topics.sh cannot create the topic. Create it via the control plane
# (loopback-only), 6 partitions; the harness only waits for it.
until curl -sf http://127.0.0.1:8080/healthz >/dev/null; do sleep 2; done
retry 20 curl -sf -X POST http://127.0.0.1:8080/v1/create-topic -d '{"topic":"bench-'"$RUN_ID"'","partitions":6}'

install_alloy broker DDB_TABLE=$DYNAMO_TABLE
touch /var/lib/bench-bootstrap.done
