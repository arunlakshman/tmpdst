#!/bin/bash
# manifester-bench load generator: WarpStream CLI (preferred), Apache Kafka CLI
# tools (fallback), bench harness, Alloy.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

${common}

BROKER=${broker_private_ip}:9092
dnf install -y tar gzip java-17-amazon-corretto-headless jq >/dev/null

# Apache Kafka CLI tools (kafka-topics.sh, kafka-producer-perf-test.sh, ...).
KV=${kafka_tools_version}
curl -fsSL "https://archive.apache.org/dist/kafka/$KV/kafka_2.13-$KV.tgz" | tar -xz -C /opt
ln -sfn "/opt/kafka_2.13-$KV" /opt/kafka

# WarpStream CLI: same tarball the official installer (console.warpstream.com/install.sh)
# downloads, fetched directly because that script edits shell profiles and exits
# non-zero without one. Best effort: the Kafka tools above remain if it fails.
#   warpstream cli benchmark-producer -bootstrap-host H -bootstrap-port 9092 -topic T -num-clients 1
mkdir -p /opt/warpstream
if curl -fsSL "https://warpstream-public-us-east-1.s3.amazonaws.com/warpstream_agent_releases/warpstream_agent_linux_amd64_latest.tar.gz" | tar -xz -C /opt/warpstream; then
  mv /opt/warpstream/warpstream_agent_linux_amd64 /usr/local/bin/warpstream || echo "warpstream binary name differs; skipped" >&2
else
  echo "warpstream install failed; use /opt/kafka/bin tools" >&2
fi

cat > /etc/profile.d/bench.sh <<ENV
export BOOTSTRAP=$BROKER
export BOOTSTRAP_HOST=${broker_private_ip}
export RUN_ID=$RUN_ID
export BUCKET=$BUCKET
export AWS_REGION=$REGION
export SSM_PREFIX=${ssm_prefix}
export PATH=\$PATH:/opt/kafka/bin
ENV

wait_for_artifacts || true   # bench harness and Alloy config come from the same upload
mkdir -p /opt/bench
aws s3 sync "s3://$BUCKET/artifacts/bench/" /opt/bench/ || true
# S3 does not keep mode bits: make the harness scripts executable.
chmod -R a+rX /opt/bench
chmod a+x /opt/bench/*.sh /opt/bench/*.py 2>/dev/null || true
install_alloy loadgen
touch /var/lib/bench-bootstrap.done
