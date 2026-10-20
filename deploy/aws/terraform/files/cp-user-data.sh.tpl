#!/bin/bash
# manifester-bench control-plane host, rank ${cp_rank} (store ${cp_store}): manicp, metrics snapshotter, Alloy.
# Runs nothing else. Log store: rank 0 is the preferred appender, higher ranks are hot standbys that tail the log
# (decisions 48-53); brokers hold the URL list. Old store: exactly one host (the single sequencer, decision 46).
# Brokers reach it at http://<this private ip>:8080; topic creation (rank 0 only) happens here on loopback.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

DYNAMO_TABLE=${table_name}
CP_RANK=${cp_rank}
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

# Rank 0 keeps the historic names (results/<run>/manicp-<run>.json, metrics/cp/); standbys get their own.
if [ "$CP_RANK" = 0 ]; then
  MANIFEST_OUT=s3://$BUCKET/results/$RUN_ID/
  SNAP_DIR=cp
else
  MANIFEST_OUT=s3://$BUCKET/results/$RUN_ID/cp-$CP_RANK/
  SNAP_DIR=cp-$CP_RANK
fi
%{ if cp_store == "log" ~}
# Log store: DynamoDB log + Pebble state under /var/lib/manicp (StateDirectory) + checkpoints in the run bucket under cp/.
# The table (with TTL and warm throughput) is created by Terraform, so no -create-tables.
STORE_ARGS="-store dynamolog -table ${table_name} -ckpt-bucket $BUCKET -ckpt-prefix cp/ -rank $CP_RANK -node-name cp-$CP_RANK -takeover-after ${cp_takeover_after} -pebble-dir /var/lib/manicp/pebble"
%{ else ~}
STORE_ARGS="-store dynamo"
%{ endif ~}

# Real AWS DynamoDB: no -dynamo-endpoint. Manifest is written as manicp-<run_id>.json (trailing /).
# Optional extra flags (e.g. slot rate, sequencer tuning) come from the cp_extra_args variable.
cat > /etc/systemd/system/manicp.service <<UNIT
[Unit]
Description=manicp (control plane, rank $CP_RANK)
After=network-online.target
Wants=network-online.target

[Service]
User=manifester
EnvironmentFile=/etc/manifester/manicp.env
StateDirectory=manicp
ExecStart=/opt/manifester/manicp -addr :8080 $STORE_ARGS -region $REGION -table-prefix ${table_prefix} -metrics-addr 127.0.0.1:9101 -run-id $RUN_ID -manifest-out $MANIFEST_OUT -log-level info ${cp_extra_args}
Restart=always
RestartSec=1
TimeoutStopSec=40
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

# Metrics snapshotter (no metrics ports are open): manicp /metrics -> s3://BUCKET/results/RUN/metrics/<cp|cp-RANK>/<epoch>-manicp.txt every 10 s.
cat > /usr/local/bin/mb-snapshot.sh <<'SNAP'
#!/bin/bash
set -u
. /etc/manifester/snapshot.env
while true; do
  ts=$(date +%s)
  curl -sf -m 3 "http://127.0.0.1:9101/metrics" | aws s3 cp - "s3://$BUCKET/results/$RUN_ID/metrics/$SNAP_DIR/$ts-manicp.txt" --only-show-errors || true
  sleep 10
done
SNAP
chmod 0755 /usr/local/bin/mb-snapshot.sh
cat > /etc/manifester/snapshot.env <<ENV
BUCKET=$BUCKET
RUN_ID=$RUN_ID
AWS_REGION=$REGION
SNAP_DIR=$SNAP_DIR
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
if [ "$CP_RANK" = 0 ]; then
  retry 20 curl -sf -X POST http://127.0.0.1:8080/v1/create-topic -d '{"topic":"bench-'"$RUN_ID"'","partitions":${initial_topic_partitions}}'
fi

# CloudWatch (DynamoDB table metrics) is scraped from rank 0 only, so the series are not duplicated.
install_alloy cp DDB_TABLE=$DYNAMO_TABLE PRIVATE_IP=$PRIVATE_IP CP_RANK=$CP_RANK
touch /var/lib/bench-bootstrap.done
