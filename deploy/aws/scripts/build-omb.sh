#!/usr/bin/env bash
# Build the public OpenMessaging Benchmark (OMB), unmodified, with Podman.
#
#   deploy/aws/scripts/build-omb.sh [--keep-src]
#
# No patches. The stock Kafka driver talks to mdsbroker (CreateTopics, consumer groups, Fetch all work).
# Output: build/omb/openmessaging-benchmark.tar.gz = upstream's own package/target/*-bin.tar.gz, extracted
# on the load generator hosts to /opt/omb (bin/benchmark, bin/benchmark-worker, lib/, driver-kafka/, workloads/).
# Podman and a pinned Maven image are the only tools needed (no local Java or Maven).
# Never runs "podman system prune"; the Maven cache lives in the named volume mds-omb-m2.
set -euo pipefail

OMB_REPO=${OMB_REPO:-https://github.com/openmessaging/benchmark}
OMB_COMMIT=${OMB_COMMIT:-5b1fa70951a323da26bd587174b58bb2c65b0b5c} # master of 2026-04-07; OMB has no release tags
MAVEN_IMAGE=${MAVEN_IMAGE:-docker.io/library/maven:3.9.9-eclipse-temurin-17}

root=$(cd "$(dirname "$0")/../../.." && pwd)
out=$root/build/omb
src=$out/src
KEEP=0
[ "${1:-}" != "--keep-src" ] || KEEP=1

command -v podman >/dev/null || { echo "podman not found (see mise.toml / README)" >&2; exit 1; }
podman info >/dev/null 2>&1 || { echo "podman machine not running (macOS: podman machine start)" >&2; exit 1; }
command -v git >/dev/null || { echo "git not found" >&2; exit 1; }

mkdir -p "$out"
if [ ! -d "$src/.git" ]; then
  git clone --quiet "$OMB_REPO" "$src"
fi
git -C "$src" fetch --quiet origin "$OMB_COMMIT" 2>/dev/null || true
git -C "$src" checkout --quiet --force "$OMB_COMMIT"
git -C "$src" clean --quiet -fdx

podman volume create mds-omb-m2 >/dev/null
# --userns=keep-id runs as your user inside the machine; :Z keeps SELinux labels happy.
podman run --rm --userns=keep-id -e HOME=/tmp -e MAVEN_CONFIG=/tmp/m2 \
  -v mds-omb-m2:/tmp/m2 -v "$src":/src:Z -w /src "$MAVEN_IMAGE" \
  mvn -B -ntp -q -Dmaven.repo.local=/tmp/m2/repository -DskipTests -Dspotless.check.skip=true \
    -Dcheckstyle.skip=true -Dlicense.skip=true -Dspotbugs.skip=true -Drat.skip=true -Denforcer.skip=true clean package

tarball=$(ls "$src"/package/target/openmessaging-benchmark-*-bin.tar.gz)
cp "$tarball" "$out/openmessaging-benchmark.tar.gz"
echo "$OMB_COMMIT" >"$out/OMB_COMMIT"
[ "$KEEP" -eq 1 ] || rm -rf "$src"
echo "built $out/openmessaging-benchmark.tar.gz ($(du -h "$out/openmessaging-benchmark.tar.gz" | cut -f1)), OMB commit $OMB_COMMIT"
