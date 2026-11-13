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

| Step | What |
|------|------|
| fmt-check | `gofmt -l` must be empty |
| ascii-check | `ops/scripts/ascii-check.sh` |
| vet | `go vet ./...` |
| lint | `golangci-lint run ./...` (see `.golangci.yml`) |
| compile | `go build ./...` |
| test | unit packages (not the simulation trees `internal/verify/simulation` and `internal/verify/simulator`, not `test/integrationtest`) |
| sim | `go test ./internal/verify/simulation/... ./internal/verify/simulator/...`, then `go run ./cmd/mdssimulator -scenario kafka -seeds 300` |
| it-hermetic | `go test ./test/integrationtest/` (in-process memstore) |
| risk | change risk score <= 20 (`docs/risk-gate.md`) |

Every `git commit` runs that gate via the pre-commit hook (with mise:
`mise exec -- make build`). Bypass once:

```bash
SKIP_BUILD_HOOK=1 git commit ...
```

## Other targets

```bash
make test-all      # unit + sim + hermetic it (no lint/risk)
make lint
make risk-strict
```

Podman fullstack tests under `test/integrationtest/` skip unless
`MDS_DYNAMO_ENDPOINT` and `MDS_S3_ENDPOINT` are set
(`ops/scripts/it-up.sh`). The import rules for the deterministic packages
are checked by `go run github.com/manifesterio/mds/dst/cmd/determinismcheck` (see `ops/scripts/ci-determinism.sh`).

## Docs

- `docs/cp-extraction.md` - old note on the control plane move (stale; the control plane is now the `controlplane` module)
- `docs/decisions.md` - design decisions
- `docs/risk-gate.md` - change risk score threshold and baseline
- `docs/lint-baseline.md` - golangci-lint policy
- `docs/aws-benchmark-runbook.md` - AWS bench / deploy
