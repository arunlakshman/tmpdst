# mds

MDS is a Kafka-protocol broker and a control plane, written in Go. It also has a library for
deterministic simulation testing. All code is in this one repo.

## Modules

The repo has four Go modules. Each module is a folder with its own `go.mod`.

- `dst/` - the deterministic simulation library. It has no product words (no Kafka, no S3).
- `shared/` - code that both products use and that has no product words (AWS metrics, HTTP settings, run info).
- `controlplane/` - the control plane: metadata, topics, consumer groups, and the replicated log that stores them.
- `broker/` - the Kafka-protocol broker. It talks to the control plane through its client.

## Dependency picture

    dst <- shared <- controlplane <- broker

A module may import only the modules to its left. `make check-deps` checks this rule.

## Build

1. Install [mise](https://mise.jdx.dev).
2. From the repo root:

        mise install      # go, golangci-lint, buf, java, minio; also installs the pre-commit hook
        make generate     # required before a standalone broker/controlplane build (pb.go not in git)
        make build        # the full gate for all four modules, then the two binaries

The root `Makefile` only calls the `Makefile` of each module, in the order above.
`make build` (and `make all`) also runs `make check-modules-standalone`: each module must
`go mod tidy` cleanly and `go build ./...` with `GOWORK=off`.

Other root targets: `make generate`, `make test`, `make it`, `make sim`, `make sim-campaign`,
`make determinism`, `make install-hooks`, `make check-deps`, `make check-modules-standalone`.

`make it` runs control-plane then broker Podman integration tests (scoped packages only, not
ordinary unit tests). It needs Podman (DynamoDB Local, PostgreSQL and SeaweedFS via
`shared/testinfra`). Module `make build` (and thus the commit hook) also runs those Podman
ITs: controlplane and broker each depend on `it`.

The commit hook (`ops/githooks/pre-commit`) runs `make build` and therefore needs Podman.

## Binaries

`make build` writes two binaries to `bin/`:

- `bin/mdscontrolplane` - the control plane
- `bin/mdsbroker` - the broker

## Containers (quickstart)

Demo (one container, in-memory stores): see the five-step quickstart in
`deploy/container/README.md` (`make image-all`, then `podman run ... mds-all:dev`).
Production uses separate `mdscontrolplane` and `mdsbroker` images from the same folder.

## Read more

Each module has its own `README.md`.

## Roadmap

- network model - not started
- disk and crash-state model - not started
- linearizability checker - not started
