# broker

The Kafka-protocol broker (Go). It is a module of the MDS monorepo. It uses the control plane
through the client in `controlplane/client`. It imports `dst`, `shared` and `controlplane`.

## Bootstrap

1. Install [mise](https://mise.jdx.dev).
2. From the repo root (the folder with `go.work`):

```bash
mise install
```

That installs the pinned toolchain from `mise.toml` (`go`, `golangci-lint`,
`buf`, `java`, `minio`) and runs `make install-hooks` (mise `postinstall`), which links
`ops/githooks/pre-commit` into `.git/hooks/pre-commit`.

Without mise:

```bash
make install-hooks   # uses ambient go / golangci-lint on PATH
```

## Build gate

`make build` in this folder is the full gate, in order. From the repo root, `make build` runs it
after the other modules and writes `bin/mdsbroker`. The control plane proto is in `controlplane/api`.

Standalone (outside `go.work`): run root `make generate` first, then `make -C broker build`.
Root `make check-modules-standalone` verifies `GOWORK=off go mod tidy` and `go build ./...`.

| Step | What |
|------|------|
| fmt-check | `gofmt -l` must be empty |
| ascii-check | `../ops/scripts/ascii-check.sh` |
| vet | `go vet ./...` |
| lint | `golangci-lint run ./...` (see `.golangci.yml`) |
| compile | `go build ./...` |
| test | unit packages (not the simulation tree `internal/verify/simulator`, not `test/integrationtest`) |
| sim | `go test ./internal/verify/simulator/...`, then `go run ./cmd/mdssimulator -scenario kafka -seeds 300` |
| it | `go test -tags integration ./test/integrationtest/` (Podman + Dynamo/S3 + real mdscontrolplane) |
| risk | change risk score <= 20 (`docs/contribute/risk-gate.md`) |

Every `git commit` runs that gate via the pre-commit hook (with mise:
`mise exec -- make build`). Podman must be running. Bypass once:

```bash
SKIP_BUILD_HOOK=1 git commit ...
```

## Other targets

```bash
make test-all      # unit + sim + it (no lint/risk; needs Podman)
make it            # Podman IT: Dynamo/S3 + real mdscontrolplane (also in build)
make lint
make risk-strict
```

`make it` requires Podman and never skips. Integration tests are under
`//go:build integration`. The import rules for the deterministic packages
are checked by `go run github.com/manifesterio/mds/dst/cmd/determinismcheck` (see `../ops/broker/scripts/ci-determinism.sh`).

## Docs

Start at [`docs/README.md`](../docs/README.md). Read more:

- [`docs/what-works-in-0.1.0.md`](../docs/what-works-in-0.1.0.md) - supported Kafka APIs and gaps
- [`docs/operations/observability.md`](../docs/operations/observability.md) - metrics, logs, `run_id`
- [`docs/contribute/risk-gate.md`](../docs/contribute/risk-gate.md) - change risk score threshold and baseline
