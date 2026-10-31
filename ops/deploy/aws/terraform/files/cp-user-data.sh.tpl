#!/bin/bash
# mds-bench control-plane host, rank ${cp_rank} (log store): mdscontrolplane, metrics snapshotter, Alloy.
# Runs nothing else. Rank 0 is the preferred appender, higher ranks are hot standbys that tail the log
# (decisions 48-53); brokers hold the URL list.
# Brokers reach it at http://<this private ip>:8080; topic creation (rank 0 only) happens here on loopback.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

DYNAMO_TABLE=${table_name}
CP_RANK=${cp_rank}
${common}

dnf install -y tar gzip >/dev/null
wait_for_artifacts

mkdir -p /opt/mds /etc/mds
aws s3 cp "s3://$BUCKET/artifacts/mds-linux-amd64.tar.gz" /tmp/mds.tar.gz
tar -xzf /tmp/mds.tar.gz -C /opt/mds
chmod 0755 /opt/mds/mdscontrolplane
useradd --system --no-create-home --shell /sbin/nologin mds 2>/dev/null || true

PRIVATE_IP=$(imds local-ipv4)

cat > /etc/mds/mdscontrolplane.env <<ENV
AWS_REGION=$REGION
ENV

# Rank 0 keeps the historic names (results/<run>/mdscontrolplane-<run>.json, metrics/cp/); standbys get their own.
if [ "$CP_RANK" = 0 ]; then
  MANIFEST_OUT=s3://$BUCKET/results/$RUN_ID/
  SNAP_DIR=cp
else
  MANIFEST_OUT=s3://$BUCKET/results/$RUN_ID/cp-$CP_RANK/
  SNAP_DIR=cp-$CP_RANK
fi
# DynamoDB log + Pebble state under /var/lib/mdscontrolplane (StateDirectory) + checkpoints in the run bucket under cp/.
# The table (with TTL and warm throughput) is created by Terraform, so no -create-tables.
STORE_ARGS="-store dynamolog -table ${table_name} -checkpoint-bucket $BUCKET -checkpoint-prefix cp/ -rank $CP_RANK -node-name cp-$CP_RANK -takeover-after ${cp_takeover_after} -pebble-dir /var/lib/mdscontrolplane/pebble"

# Real AWS DynamoDB: no -dynamo-endpoint. Manifest is written as mdscontrolplane-<run_id>.json (trailing /).
# Optional extra flags (e.g. slot rate, log-k) come from the cp_extra_args variable.
cat > /etc/systemd/system/mdscontrolplane.service <<UNIT
[Unit]
Description=mdscontrolplane (control plane, rank $CP_RANK)
After=network-online.target
Wants=network-online.target

[Service]
User=mds
EnvironmentFile=/etc/mds/mdscontrolplane.env
StateDirectory=mdscontrolplane
ExecStart=/opt/mds/mdscontrolplane -addr :8080 $STORE_ARGS -region $REGION -table-prefix ${table_prefix} -metrics-addr 127.0.0.1:9101 -run-id $RUN_ID -manifest-out $MANIFEST_OUT -log-level info ${cp_extra_args}
Restart=always
RestartSec=1
TimeoutStopSec=40
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

# Metrics snapshotter (no metrics ports are open): mdscontrolplane /metrics -> s3://BUCKET/results/RUN/metrics/<cp|cp-RANK>/<epoch>-mdscontrolplane.txt every 10 s.
cat > /usr/local/bin/mb-snapshot.sh <<'SNAP'
#!/bin/bash
set -u
. /etc/mds/snapshot.env
while true; do
  ts=$(date +%s)
  curl -sf -m 3 "http://127.0.0.1:9101/metrics" | aws s3 cp - "s3://$BUCKET/results/$RUN_ID/metrics/$SNAP_DIR/$ts-mdscontrolplane.txt" --only-show-errors || true
  sleep 10
done
SNAP
chmod 0755 /usr/local/bin/mb-snapshot.sh
cat > /etc/mds/snapshot.env <<ENV
BUCKET=$BUCKET
RUN_ID=$RUN_ID
AWS_REGION=$REGION
SNAP_DIR=$SNAP_DIR
ENV
cat > /etc/systemd/system/mb-snapshot.service <<UNIT
[Unit]
Description=push mdscontrolplane /metrics snapshots to S3
After=mdscontrolplane.service

[Service]
ExecStart=/usr/local/bin/mb-snapshot.sh
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now mdscontrolplane.service mb-snapshot.service

# The broker implements neither CreateTopics nor DescribeConfigs, so kafka-topics.sh cannot create the topic.
# Create it here on loopback (idempotent on identical settings). bench-ctl.sh create-topic does the same for other topics.
until curl -sf http://127.0.0.1:8080/healthz >/dev/null; do sleep 2; done
if [ "$CP_RANK" = 0 ]; then
  retry 20 curl -sf -X POST http://127.0.0.1:8080/v1/create-topic -d '{"topic":"bench-'"$RUN_ID"'","partitions":${initial_topic_partitions}}'
fi

# CloudWatch (DynamoDB table metrics) is scraped from rank 0 only, so the series are not duplicated.
install_alloy cp DDB_TABLE=$DYNAMO_TABLE PRIVATE_IP=$PRIVATE_IP CP_RANK=$CP_RANK
touch /var/lib/bench-bootstrap.done
