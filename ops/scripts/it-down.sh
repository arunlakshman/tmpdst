#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose -f ops/deploy/docker-compose.yml down -v
