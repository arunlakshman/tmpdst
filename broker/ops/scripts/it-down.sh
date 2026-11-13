#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
podman compose -f ../deploy/container/broker-compose.yml down -v
