# Deploy the control plane to a local kind cluster

Needs docker, kind and helm. The Dockerfile makes the generated files itself (they are
not in git). `dst` is a folder of this repo (go.mod has a replace to it). Run the
commands below from the root of the repo.

    docker build -f controlplane/deploy/docker/mdscontrolplane.Dockerfile -t mdscontrolplane:dev .
    kind create cluster --name mds
    kind load docker-image mdscontrolplane:dev --name mds
    helm upgrade --install cp controlplane/deploy/helm/mdscontrolplane --kube-context kind-mds --wait --timeout 3m
    kubectl --context kind-mds get pods

The pod is Ready when the gRPC health check passes (the chart uses a gRPC probe).
Metrics: `kubectl --context kind-mds port-forward svc/cp-mdscontrolplane 9101` then open
`localhost:9101/metrics`. The gRPC API is on the same Service, port 8080.

The default store is `mem`, so keep `replicaCount: 1`. Change `controlPlane.store`
and `controlPlane.extraArgs` in `values.yaml` for dynamolog or postgres.

The root filesystem is read-only. `tmpVolume.enabled` (default true) mounts an emptyDir at
`/tmp`; the dynamolog and postgres stores need it when `-pebble-dir` is not set.

Clean up: `helm uninstall cp --kube-context kind-mds`, then `kind delete cluster --name mds`.

## What was verified (2026-11-06)

Run on macOS (Apple silicon, arm64) with Docker Desktop (engine 27.3.1), kind v0.31.0,
helm v4.1.0 and kubectl client v1.30.2. The commands above ran as written.

- Image build: `docker build` succeeded on the first try (about 35 s with warm
  layer cache from an earlier attempt; the Go build step took 29 s). The image is
  11.3 MB of content (49.7 MB disk usage in Docker Desktop's view), arm64.
- Image alone: `docker run --rm mdscontrolplane:dev --help` prints the flags. Run with the
  mem store and ports published, the logs show "listening" on :8080 and the metrics on
  :9101, `curl /metrics` returns Prometheus text, and a small Go client got health
  `SERVING` from the standard gRPC health service.
- kind and helm: `kind create cluster` took about 13 s, `kind load docker-image` loaded the
  image, `helm lint` passed, `helm upgrade --install ... --wait` finished in about 11 s.
  The pod was 1/1 Running with 0 restarts, and the gRPC readiness and liveness probes passed.
- Through `kubectl port-forward svc/cp-mdscontrolplane 8080 9101`: `/metrics` answered
  (138 lines, including `mdscontrolplane_log_epoch 1`), and the generated gRPC client
  made real calls to the deployed pod: health, GetStatus (role "active", epoch 1),
  CreateTopic, DescribeTopics, CommitSegment and FindSegments, all returned the expected
  results. With the mem store there is no election log line; the active role shows in
  GetStatus and in the epoch metric.
- Chart upgrade: a second `helm upgrade` (with the `/tmp` volume added) rolled the pod and
  it became Ready again. `helm uninstall` and `kind delete cluster` worked.
- The `/tmp` fix: before the chart change, `docker run --read-only mdscontrolplane:dev
  -store dynamolog -dynamo-endpoint http://127.0.0.1:1` failed with
  `mkdir /tmp/mdscontrolplane-pebble-...: read-only file system`. With
  `--tmpfs /tmp` the same command got past that point (it then retried the unreachable
  Dynamo endpoint, as expected).

## Using podman instead of docker

Checked on 2026-11-07 with podman 6.1.4 (machine: rootless, 2 CPUs, 2 GiB), kind v0.31.0
and helm v4.1.0. The same Dockerfile and chart work. Three things differ from the docker
steps above:

1. Use `podman` directly in scripts. A `docker` alias in `~/.zshrc` is not seen by
   non-interactive shells. Build with the full image name, so it matches what the chart
   asks for (`mdscontrolplane:dev` means `docker.io/library/mdscontrolplane:dev`):

        podman build -f controlplane/deploy/docker/mdscontrolplane.Dockerfile \
          -t docker.io/library/mdscontrolplane:dev .

2. Tell kind to use podman, and load the image from an archive. `kind load docker-image`
   fails under podman with "not present locally".

        export KIND_EXPERIMENTAL_PROVIDER=podman
        kind create cluster --name mds
        podman save docker.io/library/mdscontrolplane:dev -o mds-image.tar
        kind load image-archive mds-image.tar --name mds

3. Install and check as before (`helm upgrade --install cp controlplane/deploy/helm/mdscontrolplane
   --kube-context kind-mds --wait`). If `kubectl` is missing, run it inside the node:
   `podman exec mds-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf get pods`.

What ran: a cold image build (about 3.5 minutes, 36 MB image), the standalone container
(serves, `/metrics` answers), a kind cluster (ready in 16 s), the helm install with
`--wait` (pod 1/1 Ready, 0 restarts, gRPC probe passed), `/metrics` from inside the node
(138 lines), then helm uninstall and `kind delete cluster`. The real gRPC calls were not
repeated under podman; they were checked with docker above.

## What was NOT verified

- The dynamolog and postgres stores in the cluster. No DynamoDB, S3 or PostgreSQL was
  deployed or reached from the pod. The `/tmp` fix was checked only with the docker
  `--read-only` experiment above, not with a full deploy of those stores.
- More than one replica, leader election between pods, and failover. Only one pod with the
  mem store ran.
- Ingress, TLS, NetworkPolicy and any `service.type` other than ClusterIP.
- Restart or upgrade behaviour with data: the mem store loses its state when the pod restarts.
- Other CPU architectures (only arm64 was built) and pulling the image from a registry
  (it was loaded into kind with `kind load`; `pullPolicy` is `IfNotPresent`).
- `helm lint` printed only the info note that the chart has no icon; no other chart linters ran.
