# Deploy

Container and cluster packaging lives under `deploy/`. This page is the map;
build and run detail is in `deploy/container/README.md` and `deploy/README.md`.

## Images

| Image | File | Use |
|-------|------|-----|
| `mdscontrolplane:dev` | `deploy/container/mdscontrolplane.Containerfile` | Control plane only (production shape) |
| `mdsbroker:dev` | `deploy/container/mdsbroker.Containerfile` | Broker only (production shape) |
| `mds-all:dev` | `deploy/container/mds-all.Containerfile` | CP + broker, mem stores (**demo / quickstart only**) |

Build from the **repo root** (so `dst` / `shared` / `controlplane` / `broker` are
in context). Repo standard is **Podman**, not Docker.

```sh
make image-cp       # mdscontrolplane:dev
make image-broker   # mdsbroker:dev
make image-all      # mds-all:dev
```

Or `podman build -f deploy/container/<file> -t <tag> .`.

Generated Go (`*.pb.go`, `*_gen.go`) is not in git; each image runs `buf generate`
inside the build.

## mds-all vs separate images

- **mds-all**: one container, `-store mem` and `-objects mem`. State is lost on
  stop. Use for a five-minute try. Not for production.
- **Separate images**: run CP and broker as two containers/processes, wire a
  durable store (`dynamolog` or `postgres`) and a real object store (`s3`).
  That is the production shape.

Quickstart for mds-all (ports 9092 / 8080 / 9100 / 9101) is in
`deploy/container/README.md`. Operator flags for stores and objects are in
[run.md](run.md).

## kind + Helm (control plane)

Needs Podman, kind, and helm. From the repo root: build the CP image, load it
into kind (`KIND_EXPERIMENTAL_PROVIDER=podman`), then:

```sh
helm upgrade --install cp deploy/helm/mdscontrolplane \
  --kube-context kind-mds --wait --timeout 3m
```

Default store in the chart is `mem` (`replicaCount: 1`). Change
`controlPlane.store` and `controlPlane.extraArgs` in
`deploy/helm/mdscontrolplane/values.yaml` for dynamolog or postgres.

Full command sequence, port-forward for metrics, and Podman/kind notes:
`deploy/container/README.md`. Chart overview: `deploy/README.md`.

## Unrelated compose file

`deploy/container/broker-compose.yml` starts DynamoDB-compatible and S3 test
backends for integration tests (`ops/scripts/it-up.sh`). It is not a product
deploy.
