#!/usr/bin/env bash
# Runs the Postgres-backed log tests against a throwaway PostgreSQL in Docker.
# It starts one container on a free local port, waits until Postgres answers,
# sets MDS_TEST_POSTGRES_DSN, runs go test, and then removes the container.
# The container is removed even when a test fails or you press Ctrl+C.
#
# Needs Docker (Docker Desktop or Engine). Not part of `make build`: run it with
# `make it-postgres`. The Postgres tests skip when MDS_TEST_POSTGRES_DSN is not set.
#
# Environment variables:
#   POSTGRES_IMAGE  Image to run. Default: postgres:17.11-alpine
#   POSTGRES_KEEP=1 Do not remove the container at the end. It prints the
#                   container name and the DSN, so you can connect with psql.
set -euo pipefail
cd "$(dirname "$0")/../.."

usage() {
  cat <<'EOF'
Usage: ops/scripts/postgres-test.sh [go test args...]
       ops/scripts/postgres-test.sh -h

Starts a throwaway PostgreSQL in Docker, runs go test with MDS_TEST_POSTGRES_DSN
set to that server, then removes the container.

With no arguments it runs:
  go test -count=1 -timeout 300s ./internal/controlplane/executor/ ./cmd/mdscontrolplane/

Environment:
  POSTGRES_IMAGE   Image to run (default postgres:17.11-alpine)
  POSTGRES_KEEP=1  Keep the container after the run and print its name and DSN.

Exit status: the exit status of go test, 3 if Docker is missing or not running.
EOF
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac

POSTGRES_IMAGE=${POSTGRES_IMAGE:-postgres:17.11-alpine}
POSTGRES_KEEP=${POSTGRES_KEEP:-0}
NAME="mds-pg-test-$$"
CONTAINER_STARTED=0
DSN=""

# Stops the container we started. It was run with --rm, so stop also removes it.
# Keeps the exit status of the script.
cleanup() {
  local status=$?
  if [[ $CONTAINER_STARTED == 1 ]]; then
    if [[ $POSTGRES_KEEP == 1 ]]; then
      echo "POSTGRES_KEEP=1: container $NAME is kept running."
      echo "DSN: $DSN"
      echo "Remove it later with: docker stop $NAME"
    else
      docker stop "$NAME" >/dev/null 2>&1 || true
    fi
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

# (a) Docker must exist and the daemon must answer.
if ! command -v docker >/dev/null 2>&1; then
  echo "postgres-test: docker is not installed. Install Docker Desktop or Engine first." >&2
  exit 3
fi
if ! docker info >/dev/null 2>&1; then
  echo "postgres-test: the Docker daemon does not answer. Start Docker and try again." >&2
  exit 3
fi

# (b) Pick a free local TCP port, so we never clash with a running Postgres.
if ! command -v python3 >/dev/null 2>&1; then
  echo "postgres-test: python3 is needed to pick a free port." >&2
  exit 3
fi
PORT=$(python3 -c '
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
')

# (c) Start the container. Only 127.0.0.1 can reach it.
echo "Starting $POSTGRES_IMAGE as $NAME on 127.0.0.1:$PORT ..."
docker run -d --rm --name "$NAME" \
  -e POSTGRES_PASSWORD=mds \
  -e POSTGRES_DB=mds \
  -p "127.0.0.1:$PORT:5432" \
  "$POSTGRES_IMAGE" >/dev/null
CONTAINER_STARTED=1
DSN="postgres://postgres:mds@127.0.0.1:$PORT/mds?sslmode=disable"

# (e) Wait until Postgres is ready, at most 60 seconds. The check uses TCP inside
# the container, because the image's first start runs a temporary server that
# only listens on the Unix socket.
ready=0
for _ in $(seq 1 60); do
  if docker exec "$NAME" pg_isready -h 127.0.0.1 -p 5432 -U postgres -d mds >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ $ready != 1 ]]; then
  echo "postgres-test: PostgreSQL in $NAME was not ready after 60 seconds. Last log lines:" >&2
  docker logs --tail 20 "$NAME" >&2 || true
  exit 1
fi
echo "PostgreSQL is ready."

# (f) Run the tests against that server.
export MDS_TEST_POSTGRES_DSN="$DSN"
if [[ $# -eq 0 ]]; then
  set -- ./internal/controlplane/executor/ ./cmd/mdscontrolplane/
fi

# (g) Keep go test's exit status. The EXIT trap then removes the container.
status=0
go test -count=1 -timeout 300s "$@" || status=$?
exit "$status"
