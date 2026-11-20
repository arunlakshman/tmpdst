# Operations

Run binaries, read metrics/logs, and deploy containers. Demo quickstart:
[getting-started.md](getting-started.md). Build: [contribute/build.md](contribute/build.md).

## Local binaries

```sh
mise install && make generate && make build
# bin/mdscontrolplane  bin/mdsbroker
```

Minimal mem pair (state lost on stop):

```sh
bin/mdscontrolplane -store mem -addr :8080 -metrics-addr :9101

bin/mdsbroker \
  -objects mem \
  -cp-url http://127.0.0.1:8080 \
  -listen 127.0.0.1:9092 \
  -advertise 127.0.0.1:9092 \
  -metrics-addr :9100
```

Give both the same `-run-id` (or `$MDS_RUN_ID`) when joining logs later.

### Control plane stores (`-store`)

| Value | Role |
|-------|------|
| `mem` | In-process. Demo/tests. Default. |
| `dynamolog` | DynamoDB log; optional S3 checkpoints. |
| `postgres` | PostgreSQL log; optional S3 checkpoints. |

Shared flags: `-addr` (`:8080`), `-metrics-addr` (`:9101`), `-log-level`,
`-run-id`, `-manifest-out`, `-broker-lease-ttl` (default 5s).

**dynamolog:** `-dynamo-endpoint` (empty = AWS), `-region`, `-table-prefix` /
`-table`, `-create-tables`, `-checkpoint-bucket` / `-checkpoint-prefix`,
`-node-name`, `-rank` (0 = preferred appender), `-takeover-after`, `-heartbeat`,
`-pebble-dir`.

**postgres:** DSN via `$MDS_POSTGRES_DSN` or `-postgres-dsn` (never logged).
`-postgres-prefix` (default `mds_`), `-create-tables`. Checkpoint/rank flags
match dynamolog. All nodes share one DB; do not point at a read replica.
`make it-postgres` needs Podman/Docker.

### Broker objects (`-objects`)

| Value | Role |
|-------|------|
| `s3` | Default. Real S3 or compatible. |
| `mem` | In-process. Single broker. |

S3: `-s3-endpoint`, `-s3-bucket`, `-s3-prefix`, `-s3-region`, `-s3-path-style`,
`-s3-create-bucket`. Other: `-listen`, `-advertise`, `-broker-id`, `-cp-url`
(comma list for failover), `-metrics-addr` (`:9100`), `-lease-interval`
(keep CP `-broker-lease-ttl` about 3x this).

Production: separate processes/images, durable store, real object store. Not
`mem` or `mds-all`.

## Observability

| Service | Port flag | Default | Metric prefix |
|---------|-----------|---------|---------------|
| `mdsbroker` | `-metrics-addr` | `:9100` | `mdsbroker_` |
| `mdscontrolplane` | `-metrics-addr` | `:9101` | `mdscontrolplane_` |

Empty `-metrics-addr` disables the port. `/metrics` and `/healthz` on each.

`-run-id` / `$MDS_RUN_ID` stamps logs, `*_build_info`, and optional
`-manifest-out`. JSON logs on stderr (`run_id`, `service`, `version`, `git_sha`).
Broker: ~one line per segment event. CP: `cp_summary` each second at INFO;
`cp_call` at DEBUG. High load: `-log-level warn`.

Useful broker events: `segment_sealed`, `segment_put_done`, `segment_committed`,
`segment_failed`, `backpressure_on` / `off`. CP: `cp_call`, `cp_summary`,
`aws_attempt_failed`.

## Containers and cluster

| Image | Use |
|-------|-----|
| `mdscontrolplane:dev` | CP only (prod shape) |
| `mdsbroker:dev` | broker only (prod shape) |
| `mds-all:dev` | CP + broker, mem (**demo only**) |

From repo root (Podman): `make image-cp`, `make image-broker`, `make image-all`.
Detail: `deploy/container/README.md`. Chart: `deploy/helm/mdscontrolplane`,
`deploy/README.md`.

kind + helm sketch:

```sh
helm upgrade --install cp deploy/helm/mdscontrolplane \
  --kube-context kind-mds --wait --timeout 3m
```

`deploy/container/broker-compose.yml` is only for IT backends (`ops/scripts/it-up.sh`),
not a product deploy.
