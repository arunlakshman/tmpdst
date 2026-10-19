#!/bin/bash
# manifester-bench load generator ${loadgen_index}: Java 17, Kafka CLI tools, WarpStream CLI
# (best effort), OpenMessaging Benchmark (if uploaded), bench harness, Alloy.
set -euxo pipefail
exec > >(tee -a /var/log/bench-bootstrap.log) 2>&1

${common}

BROKER_IPS=${broker_ips}
BOOTSTRAP_NLB=${nlb_bootstrap}
dnf install -y tar gzip java-17-amazon-corretto-headless jq >/dev/null

# Comma-separated ip:9092 of every broker.
BOOTSTRAP=""
for ip in $${BROKER_IPS//,/ }; do BOOTSTRAP="$${BOOTSTRAP:+$BOOTSTRAP,}$ip:9092"; done

# Apache Kafka CLI tools (kafka-topics.sh, kafka-producer-perf-test.sh, ...).
KV=${kafka_tools_version}
curl -fsSL "https://archive.apache.org/dist/kafka/$KV/kafka_2.13-$KV.tgz" | tar -xz -C /opt
ln -sfn "/opt/kafka_2.13-$KV" /opt/kafka

# WarpStream CLI: same tarball the official installer (console.warpstream.com/install.sh)
# downloads, fetched directly because that script edits shell profiles and exits
# non-zero without one. Best effort: the Kafka tools above remain if it fails.
mkdir -p /opt/warpstream
if curl -fsSL "https://warpstream-public-us-east-1.s3.amazonaws.com/warpstream_agent_releases/warpstream_agent_linux_amd64_latest.tar.gz" | tar -xz -C /opt/warpstream; then
  mv /opt/warpstream/warpstream_agent_linux_amd64 /usr/local/bin/warpstream || echo "warpstream binary name differs; skipped" >&2
else
  echo "warpstream install failed; use /opt/kafka/bin tools" >&2
fi

cat > /etc/profile.d/bench.sh <<ENV
export BOOTSTRAP=$BOOTSTRAP
export BOOTSTRAP_NLB=$BOOTSTRAP_NLB
export BROKER_IPS=$BROKER_IPS
export BOOTSTRAP_HOST=$${BROKER_IPS%%,*}
export LOADGEN_INDEX=${loadgen_index}
export LOADGEN_COUNT=${loadgen_count}
export RUN_ID=$RUN_ID
export BUCKET=$BUCKET
export TABLE=${table_name}
export REGION=$REGION
export AWS_REGION=$REGION
export SSM_PREFIX=${ssm_prefix}
export PATH=\$PATH:/opt/kafka/bin
ENV

wait_for_artifacts || true   # bench harness, OMB and Alloy config come from the same upload
mkdir -p /opt/bench
aws s3 sync "s3://$BUCKET/artifacts/bench/" /opt/bench/ || true
# S3 does not keep mode bits: make the harness scripts executable.
chmod -R a+rX /opt/bench
chmod a+x /opt/bench/*.sh /opt/bench/*.py 2>/dev/null || true

# OpenMessaging Benchmark distribution (built by the bench build script); optional.
mkdir -p /opt/omb
if aws s3 cp "s3://$BUCKET/artifacts/omb/openmessaging-benchmark.tar.gz" /tmp/omb.tar.gz; then
  tar -xzf /tmp/omb.tar.gz -C /opt/omb
  # a single top-level directory (e.g. openmessaging-benchmark-0.0.1-SNAPSHOT/) is flattened into /opt/omb
  if [ "$(ls -A /opt/omb | wc -l)" = 1 ] && [ -d "/opt/omb/$(ls -A /opt/omb)" ]; then
    top=/opt/omb/$(ls -A /opt/omb)
    mv "$top"/* /opt/omb/ && rmdir "$top"
  fi
  echo "OMB extracted to /opt/omb"
else
  echo "no artifacts/omb/openmessaging-benchmark.tar.gz: continuing without OMB" >&2
fi

install_alloy loadgen
touch /var/lib/bench-bootstrap.done
