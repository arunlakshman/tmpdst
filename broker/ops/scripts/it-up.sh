#!/usr/bin/env bash
# Start the integration backends and print the env to run the tests with.
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose -f ops/deploy/docker-compose.yml up -d --wait
echo "export MDS_DYNAMO_ENDPOINT=http://127.0.0.1:18080"
echo "export MDS_S3_ENDPOINT=http://127.0.0.1:8333"
