# Build

How to build and gate the monorepo. Module folders (`dst/`, `shared/`,
`controlplane/`, `broker/`) each have a `go.mod` and a `Makefile`. Root
`make build` drives them in dependency order, then writes the binaries.

## Prerequisites

1. Install [mise](https://mise.jdx.dev).
2. From the repo root:

```sh
mise install      # go, golangci-lint, buf, java, minio; installs the pre-commit hook
make generate     # required before a standalone broker/controlplane build (pb.go not in git)
make build        # full gate for all modules, then bin/mdscontrolplane and bin/mdsbroker
```

Without mise: put Go and golangci-lint on PATH, then `make install-hooks` and
`make build`.

## What `make build` covers

Root `Makefile` calls each module's gate (`dst` -> `shared` -> `controlplane` ->
`broker`), then `make check-modules-standalone`, then the two binaries under
`bin/`.

Typical module steps: fmt, ASCII check, vet, lint, compile, unit tests, then
module-specific sim / IT / risk / determinism. See each module `README.md`.

Other root targets: `make test`, `make it`, `make sim`, `make sim-campaign`,
`make determinism`, `make check-deps`, `make check-modules-standalone`.

## Podman

Broker and control-plane integration tests use Podman (DynamoDB Local,
PostgreSQL, SeaweedFS via `shared/testinfra`). Root `make it` runs those
scoped IT packages. Module `make build` (and thus the commit hook) also runs
them where the module depends on `it`.

On macOS: `podman machine start` before committing or running `make it`.

## Pre-commit hook

`mise install` links `ops/githooks/pre-commit`, which runs `make build` (via
`mise exec` when mise is present). Bypass one commit when you must:

```sh
SKIP_BUILD_HOOK=1 git commit ...
```

## Standalone modules (`GOWORK=off`)

Each module must tidy and build outside `go.work`:

```sh
make check-modules-standalone
# equivalent idea per module:
#   GOWORK=off go mod tidy && GOWORK=off go build ./...
```

For a standalone broker or controlplane build: run root `make generate` first
(generated files are not in git), then `make -C broker build` or
`make -C controlplane build`.

## Related

- [determinism.md](determinism.md) — import allow-list and audit records
- [risk-gate.md](risk-gate.md) — change-risk score at end of broker `make build`
- Module READMEs: `dst/README.md`, `controlplane/README.md`, `broker/README.md`
