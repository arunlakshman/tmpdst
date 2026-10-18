#!/bin/bash
# manifester-bench load generator: WarpStream CLI (preferred), Apache Kafka CLI
# tools (fallback), node_exporter, Alloy.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

${common}

BROKER=${broker_private_ip}:9092
dnf install -y tar gzip java-17-amazon-corretto-headless >/dev/null

# Apache Kafka CLI tools (kafka-topics.sh, kafka-producer-perf-test.sh, ...).
KV=${kafka_tools_version}
curl -fsSL "https://archive.apache.org/dist/kafka/$KV/kafka_2.13-$KV.tgz" | tar -xz -C /opt
ln -sfn "/opt/kafka_2.13-$KV" /opt/kafka

# WarpStream CLI: official installer (curl https://console.warpstream.com/install.sh | sh).
# Best effort: the Kafka tools above remain if it fails. Usage:
#   warpstream cli benchmark-producer -bootstrap-host H -bootstrap-port 9092 -topic T -num-clients 1
export HOME=/opt/warpstream
mkdir -p "$HOME"
if curl -fsSL https://console.warpstream.com/install.sh | sh; then
  WS=$(find "$HOME" /usr/local/bin -name warpstream -type f 2>/dev/null | head -1 || true)
  [ -n "$WS" ] && ln -sfn "$WS" /usr/local/bin/warpstream
else
  echo "warpstream install failed; use /opt/kafka/bin tools" >&2
fi

cat > /etc/profile.d/bench.sh <<ENV
export BOOTSTRAP=$BROKER
export BOOTSTRAP_HOST=${broker_private_ip}
export RUN_ID=$RUN_ID
export BUCKET=$BUCKET
export AWS_REGION=$REGION
export PATH=\$PATH:/opt/kafka/bin
ENV

install_node_exporter
systemctl daemon-reload
systemctl enable --now node_exporter.service
wait_for_artifacts || true   # Alloy config comes from the same upload
install_alloy loadgen
touch /var/lib/bench-bootstrap.done
