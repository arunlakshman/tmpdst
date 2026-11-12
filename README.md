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
        make build        # the full gate for all four modules, then the two binaries

The root `Makefile` only calls the `Makefile` of each module, in the order above.

Other root targets: `make generate`, `make test`, `make sim`, `make sim-campaign`,
`make determinism`, `make install-hooks`, `make check-deps`.

The commit hook (`ops/githooks/pre-commit`) runs `make build`.

## Binaries

`make build` writes two binaries to `bin/`:

- `bin/mdscontrolplane` - the control plane
- `bin/mdsbroker` - the broker

## Read more

Each module has its own `README.md`.

## Roadmap

- network model - not started
- disk and crash-state model - not started
- linearizability checker - not started
