#!/bin/bash
# manifester-bench control-plane host: the ONE manicp (single sequencer, decision 46), metrics snapshotter, Alloy.
# Runs nothing else. Brokers reach it at http://<this private ip>:8080; topic creation happens here on loopback.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

DYNAMO_TABLE=${table_name}
${common}

dnf install -y tar gzip >/dev/null
wait_for_artifacts

mkdir -p /opt/manifester /etc/manifester
aws s3 cp "s3://$BUCKET/artifacts/manifester-linux-amd64.tar.gz" /tmp/manifester.tar.gz
tar -xzf /tmp/manifester.tar.gz -C /opt/manifester
chmod 0755 /opt/manifester/manicp
useradd --system --no-create-home --shell /sbin/nologin manifester 2>/dev/null || true

PRIVATE_IP=$(imds local-ipv4)

cat > /etc/manifester/manicp.env <<ENV
AWS_REGION=$REGION
ENV

# Real AWS DynamoDB: no -dynamo-endpoint. Manifest is written as manicp-<run_id>.json (trailing /).
# Optional extra flags (e.g. sequencer tuning) come from the cp_extra_args variable.
cat > /etc/systemd/system/manicp.service <<UNIT
[Unit]
Description=manicp (control plane, DynamoDB)
After=network-online.target
Wants=network-online.target

[Service]
User=manifester
EnvironmentFile=/etc/manifester/manicp.env
ExecStart=/opt/manifester/manicp -addr :8080 -store dynamo -region $REGION -table-prefix ${table_prefix} -metrics-addr 127.0.0.1:9101 -run-id $RUN_ID -manifest-out s3://$BUCKET/results/$RUN_ID/ -log-level info ${cp_extra_args}
Restart=always
RestartSec=1
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

# Metrics snapshotter (no metrics ports are open): manicp /metrics -> s3://BUCKET/results/RUN/metrics/cp/<epoch>-manicp.txt every 10 s.
cat > /usr/local/bin/mb-snapshot.sh <<'SNAP'
#!/bin/bash
set -u
. /etc/manifester/snapshot.env
while true; do
  ts=$(date +%s)
  curl -sf -m 3 "http://127.0.0.1:9101/metrics" | aws s3 cp - "s3://$BUCKET/results/$RUN_ID/metrics/cp/$ts-manicp.txt" --only-show-errors || true
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
Description=push manicp /metrics snapshots to S3
After=manicp.service

[Service]
ExecStart=/usr/local/bin/mb-snapshot.sh
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now manicp.service mb-snapshot.service

# The broker implements neither CreateTopics nor DescribeConfigs, so kafka-topics.sh cannot create the topic.
# Create it here on loopback (idempotent on identical settings). bench-ctl.sh create-topic does the same for other topics.
until curl -sf http://127.0.0.1:8080/healthz >/dev/null; do sleep 2; done
retry 20 curl -sf -X POST http://127.0.0.1:8080/v1/create-topic -d '{"topic":"bench-'"$RUN_ID"'","partitions":${initial_topic_partitions}}'

install_alloy cp DDB_TABLE=$DYNAMO_TABLE PRIVATE_IP=$PRIVATE_IP
touch /var/lib/bench-bootstrap.done
