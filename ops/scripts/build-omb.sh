#!/usr/bin/env bash
# Build the OpenMessaging Benchmark (OMB) artifact used by ops/deploy/aws/bench/run-omb.sh.
#
#   scripts/build-omb.sh [--keep-src]
#
# Output: build/omb/openmessaging-benchmark.tar.gz, extracted on loadgen hosts to /opt/omb:
#   bin/benchmark  bin/benchmark-worker  lib/*.jar  workloads/  driver-kafka/*.yaml
# The tarball is built with Docker (pinned maven image, no local Java/Maven needed) from a
# pinned OMB commit plus ops/deploy/aws/bench/omb/patches/*.patch (the manifester broker has no
# CreateTopics and no consumer groups, see docs/aws-benchmark-warpstream-comparison.md).
# Only the Kafka driver is built (patch 0002 drops the other drivers from benchmark-framework: their
# mixed dependency trees (e.g. two jackson-databind versions) break the classpath). Never runs docker prune; the Maven cache lives in the
# named volume manifester-omb-m2 (remove it by hand if you want it gone).
set -euo pipefail

OMB_REPO=${OMB_REPO:-https://github.com/openmessaging/benchmark}
OMB_COMMIT=${OMB_COMMIT:-5b1fa70951a323da26bd587174b58bb2c65b0b5c} # master of 2026-04-07; OMB publishes no release tags
MAVEN_IMAGE=${MAVEN_IMAGE:-maven:3.9.9-eclipse-temurin-17}

root=$(cd "$(dirname "$0")/../.." && pwd)
out=$root/build/omb
src=$out/src
KEEP=0
[ "${1:-}" != "--keep-src" ] || KEEP=1

command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "docker daemon not running (macOS: open -a Docker)" >&2; exit 1; }
command -v git >/dev/null || { echo "git not found" >&2; exit 1; }

mkdir -p "$out"
if [ ! -d "$src/.git" ]; then
  git clone --quiet "$OMB_REPO" "$src"
fi
git -C "$src" fetch --quiet origin "$OMB_COMMIT" 2>/dev/null || true
git -C "$src" checkout --quiet --force "$OMB_COMMIT"
git -C "$src" clean --quiet -fdx -e target
for p in "$root"/ops/deploy/aws/bench/omb/patches/*.patch; do
  echo "applying $(basename "$p")"
  git -C "$src" apply "$p"
done

# A fresh named volume is root-owned; the build runs as the invoking user.
docker volume create manifester-omb-m2 >/dev/null
docker run --rm -u 0 -v manifester-omb-m2:/m2 "$MAVEN_IMAGE" chmod 777 /m2

stage=$out/stage
rm -rf "$stage"
mkdir -p "$stage"
# Runs as the invoking user so files are not root-owned; HOME=/tmp keeps Maven's settings writable.
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -e MAVEN_CONFIG=/tmp/m2 \
  -v manifester-omb-m2:/tmp/m2 -v "$src":/src -v "$stage":/stage -w /src "$MAVEN_IMAGE" \
  bash -euc '
    MVN="mvn -B -ntp -q -Dmaven.repo.local=/tmp/m2/repository -DskipTests -Dmaven.test.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dlicense.skip=true -Dspotbugs.skip=true -Dmaven.javadoc.skip=true -Drat.skip=true -Denforcer.skip=true"
    mkdir -p /stage/lib
    $MVN -pl benchmark-framework,driver-kafka -am package dependency:copy-dependencies -DoutputDirectory=/stage/lib -DincludeScope=runtime -DexcludeGroupIds=io.openmessaging.benchmark
    for m in driver-api benchmark-framework driver-kafka; do
      cp "$m"/target/*.jar /stage/lib/
    done
    rm -f /stage/lib/*-sources.jar /stage/lib/*-tests.jar
    cp -r bin workloads payload /stage/
    mkdir -p /stage/driver-kafka
    cp driver-kafka/*.yaml /stage/driver-kafka/
  '

[ -x "$stage/bin/benchmark-worker" ] || chmod +x "$stage"/bin/*
ls "$stage"/lib/*kafka-clients*.jar >/dev/null
ls "$stage"/lib/benchmark-framework*.jar >/dev/null
tar -C "$stage" -czf "$out/openmessaging-benchmark.tar.gz" .
echo "$OMB_COMMIT" >"$out/OMB_COMMIT"
[ "$KEEP" -eq 1 ] || rm -rf "$stage"
echo "built $out/openmessaging-benchmark.tar.gz ($(du -h "$out/openmessaging-benchmark.tar.gz" | cut -f1)), OMB commit $OMB_COMMIT"
echo "upload: aws s3 cp $out/openmessaging-benchmark.tar.gz s3://<bucket>/artifacts/omb/openmessaging-benchmark.tar.gz"
