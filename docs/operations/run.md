# Run

How to run `mdscontrolplane` and `mdsbroker` as local binaries. For containers
and kind+helm, see [deploy.md](deploy.md) and `deploy/container/README.md`.

Build first: from the repo root, `mise install`, `make generate`, `make build`.
Binaries land in `bin/mdscontrolplane` and `bin/mdsbroker`.

## Minimal local pair (mem)

State is in memory and is lost on stop. Good for smoke tests only.

```sh
bin/mdscontrolplane -store mem -addr :8080 -metrics-addr :9101

bin/mdsbroker \
  -objects mem \
  -cp-url http://127.0.0.1:8080 \
  -listen 127.0.0.1:9092 \
  -advertise 127.0.0.1:9092 \
  -metrics-addr :9100
```

Point a Kafka client at `127.0.0.1:9092`. Metrics: `curl -s localhost:9100/metrics`
and `curl -s localhost:9101/metrics`. Health: `.../healthz` on the same ports.

Give both processes the same `-run-id` (or `$MDS_RUN_ID`) when you care about
joining logs and metrics later. See [observability.md](observability.md).

## Control plane stores (`-store`)

| Value | Role |
|-------|------|
| `mem` | In-process log. Demo and tests. Lost on exit. Default. |
| `dynamolog` | DynamoDB log; optional S3 checkpoints. |
| `postgres` | PostgreSQL log; optional S3 checkpoints. |

Shared useful flags (all stores): `-addr` (default `:8080`), `-metrics-addr`
(default `:9101`), `-log-level`, `-run-id`, `-manifest-out`,
`-broker-lease-ttl` (default 5s).

### dynamolog

Needs AWS credentials (or a local endpoint). Typical flags:

- `-dynamo-endpoint` empty = AWS; set for DynamoDB Local / compatible servers
- `-region`, `-table-prefix` (default `mds_`), `-table` (default `<prefix>cp`)
- `-create-tables` for development
- `-checkpoint-bucket` / `-checkpoint-prefix` (checkpoints off if bucket empty)
- `-node-name`, `-rank` (0 = preferred appender), `-takeover-after`, `-heartbeat`
- `-pebble-dir` for on-disk state (empty = temp dir, rebuilt each start)

### postgres

PostgreSQL only (tested on 17.x). Put the DSN in the environment so it does not
show in `ps`:

```sh
export MDS_POSTGRES_DSN='postgres://mds_user@db.example.internal:5432/mds'

bin/mdscontrolplane \
  -store postgres \
  -postgres-prefix mds_ \
  -create-tables \
  -checkpoint-bucket my-checkpoints \
  -checkpoint-prefix cp/ \
  -node-name cp-0 \
  -rank 0
```

- `-postgres-dsn` or `$MDS_POSTGRES_DSN`. The DSN is never logged or written to
  the run manifest (only host/port/database).
- `-postgres-prefix` (default `mds_`): tables `<prefix>log` and `<prefix>meta`.
- `-create-tables` creates them if missing (dev).
- Checkpoint / rank / takeover flags match dynamolog. Checkpoints still go to S3
  when `-checkpoint-bucket` is set.

All nodes share one database and prefix; the epoch picks the single writer.
**Do not point the control plane at a read replica.** Reads must hit the primary.

Integration tests: `make it-postgres` (needs Docker/Podman; see module Makefile).

## Broker object store (`-objects`)

| Value | Role |
|-------|------|
| `s3` | Default. Real S3 or compatible (MinIO, SeaweedFS). |
| `mem` | In-process. Single broker. Lost on exit. |

S3 flags: `-s3-endpoint`, `-s3-bucket` (default `mds`), `-s3-prefix`,
`-s3-region`, `-s3-path-style`, `-s3-create-bucket`.

Other common flags: `-listen` (default `127.0.0.1:9092`), `-advertise`,
`-broker-id`, `-cp-url` (one URL or a comma-separated list for failover),
`-metrics-addr` (default `:9100`), `-log-level`, `-run-id`, `-manifest-out`,
`-lease-interval` (default 1s; keep CP `-broker-lease-ttl` about 3x this).

## Production shape

Use separate processes (or images) with a durable store and a real object store.
Do not use `mem` or the `mds-all` demo image in production. See [deploy.md](deploy.md).
