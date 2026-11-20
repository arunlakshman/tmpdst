# Container images

This folder holds Containerfiles for local and cluster runs. The repo standard is
**Podman** (not Docker). Build every image from the **root of the monorepo** so
`dst` / `shared` / `controlplane` / `broker` are on the context. The root
`.dockerignore` applies; module-local `.dockerignore` files do not.

Generated Go (`*.pb.go`, `*_gen.go`) is not in git. Each image runs `buf generate`
inside the build.

| Image | File | Use |
|---|---|---|
| `mdscontrolplane:dev` | `mdscontrolplane.Containerfile` | Control plane only (prod shape) |
| `mdsbroker:dev` | `mdsbroker.Containerfile` | Broker only (prod shape) |
| `mds-all:dev` | `mds-all.Containerfile` | CP + broker, mem stores (**demo / quickstart only**) |

Makefile helpers from the repo root: `make image-cp`, `make image-broker`, `make image-all`.

`broker-compose.yml` is unrelated: it starts DynamoDB-compatible and S3 test
backends for integration tests (`ops/scripts/it-up.sh`).

## Five-step quickstart (mds-all)

One container runs the control plane (`-store mem`) and the broker (`-objects mem`).
State is lost when the container stops. For real deploys, use the separate images
below.

1. Install [Podman](https://podman.io/) and start a machine if you are on macOS:
   `podman machine start`.
2. From the repo root, build the demo image:

        podman build -f deploy/container/mds-all.Containerfile -t mds-all:dev .

   Or: `make image-all`.
3. Run it and publish the Kafka and control-plane ports:

        podman run --rm --name mds-all \
          -p 9092:9092 -p 8080:8080 -p 9100:9100 -p 9101:9101 \
          mds-all:dev

4. Point a Kafka client at `127.0.0.1:9092` (the image advertises that address).
5. Produce and consume as you would with any Kafka broker. Stop with Ctrl-C
   (or `podman stop mds-all`).

Smoke check without a Kafka client: after step 3, `curl -s localhost:9101/metrics`
and `curl -s localhost:9100/metrics` should return Prometheus text, and something
should accept TCP on `localhost:9092`.

## Production shape: separate CP and broker images

Do not run `mds-all` in production. Build and run the two product images, wire
a real store (not `mem`), and put a real object store behind the broker.

Control plane:

    podman build -f deploy/container/mdscontrolplane.Containerfile \
      -t mdscontrolplane:dev .
    # or: make image-cp
    podman run --rm -p 8080:8080 -p 9101:9101 mdscontrolplane:dev \
      -store mem -addr :8080

Broker (needs a reachable control plane; default `-objects` is `s3`):

    podman build -f deploy/container/mdsbroker.Containerfile -t mdsbroker:dev .
    # or: make image-broker
    podman run --rm -p 9092:9092 -p 9100:9100 mdsbroker:dev \
      -objects mem -cp-url http://host.containers.internal:8080 \
      -listen 0.0.0.0:9092 -advertise 127.0.0.1:9092

For cluster deploy of the control plane, keep reading (kind + helm).

## Deploy the control plane to a local kind cluster

Needs podman, kind and helm. The Containerfile makes the generated files itself
(they are not in git). `dst` is a folder of this repo (go.mod has a replace to it).
Run the commands below from the root of the repo.

    podman build -f deploy/container/mdscontrolplane.Containerfile \
      -t docker.io/library/mdscontrolplane:dev .
    export KIND_EXPERIMENTAL_PROVIDER=podman
    kind create cluster --name mds
    podman save docker.io/library/mdscontrolplane:dev -o mds-image.tar
    kind load image-archive mds-image.tar --name mds
    helm upgrade --install cp deploy/helm/mdscontrolplane \
      --kube-context kind-mds --wait --timeout 3m
    kubectl --context kind-mds get pods

The pod is Ready when the gRPC health check passes (the chart uses a gRPC probe).
Metrics: `kubectl --context kind-mds port-forward svc/cp-mdscontrolplane 9101` then open
`localhost:9101/metrics`. The gRPC API is on the same Service, port 8080.

The default store is `mem`, so keep `replicaCount: 1`. Change `controlPlane.store`
and `controlPlane.extraArgs` in `values.yaml` for dynamolog or postgres.

The root filesystem is read-only. `tmpVolume.enabled` (default true) mounts an emptyDir at
`/tmp`; the dynamolog and postgres stores need it when `-pebble-dir` is not set.

Clean up: `helm uninstall cp --kube-context kind-mds`, then `kind delete cluster --name mds`.

### kind notes under Podman

1. Use `podman` directly in scripts. A `docker` alias in `~/.zshrc` is not seen by
   non-interactive shells. Tag with the full name the chart expects
   (`mdscontrolplane:dev` means `docker.io/library/mdscontrolplane:dev`).
2. `kind load docker-image` fails under podman with "not present locally"; use
   `podman save` + `kind load image-archive` as above.
3. If `kubectl` is missing, run it inside the node:
   `podman exec mds-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf get pods`.

## What was verified

### Images (2026-11-20)

macOS Apple silicon, podman 6.2.0. From the repo root, with the root `.dockerignore`:

- `podman build` for `mdscontrolplane:dev`, `mdsbroker:dev`, and `mds-all:dev` succeeded.
- CP smoke: `podman run --rm mdscontrolplane:dev --help`; run with `-store mem` and
  published ports, logs show listening on :8080 / :9101; `curl /metrics` returns
  Prometheus text.
- Broker smoke: `podman run --rm mdsbroker:dev --help`; run with `-objects mem` and
  a CP URL, logs show "mdsbroker listening".
- mds-all smoke: both processes listen; CP and broker `/metrics` answer; TCP accept
  on :9092.

### kind + helm (2026-11-06 / 2026-11-07)

Earlier checks used Docker Desktop and Podman with kind/helm for the control plane
chart only (mem store, one replica). See git history of this file for the long form
of that runbook if you need the exact timings.

## What was NOT verified

- Full Kafka produce/consume through `mds-all` against an external client in CI.
- The dynamolog and postgres stores in the cluster. No DynamoDB, S3 or PostgreSQL was
  deployed or reached from the pod for those paths in the kind checks.
- More than one replica, leader election between pods, and failover.
- Ingress, TLS, NetworkPolicy and any `service.type` other than ClusterIP.
- Other CPU architectures beyond arm64 for the new broker / mds-all images.
