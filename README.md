# manifester

Kafka-protocol brokers and a log-backed control plane (Go).

## Bootstrap

1. Install [mise](https://mise.jdx.dev).
2. From the repo root:

```bash
mise install
```

That installs the pinned toolchain from `mise.toml` (`go`, `golangci-lint`) and
runs `make install-hooks` (mise `postinstall`), which links
`scripts/githooks/pre-commit` into `.git/hooks/pre-commit`.

Without mise:

```bash
make install-hooks   # uses ambient go / golangci-lint on PATH
```

## Build gate

`make` / `make build` is the full gate, in order:

| Step | What |
|------|------|
| fmt-check | `gofmt -l` must be empty |
| ascii-check | `scripts/ascii-check.sh` |
| vet | `go vet ./...` |
| lint | `golangci-lint run ./...` (see `.golangci.yml`) |
| compile | `go build ./...` |
| test | unit packages (not `sim/`, not `it/`) |
| sim | `go test ./sim/...` |
| it-hermetic | `go test ./it/` (in-process memstore) |
| crap | CRAP score ≤ 20 (`docs/crap-gate.md`) |

Every `git commit` runs that gate via the pre-commit hook (with mise:
`mise exec -- make build`). Bypass once:

```bash
SKIP_BUILD_HOOK=1 git commit ...
```

## Other targets

```bash
make test-all      # unit + sim + hermetic it (no lint/crap)
make test-race     # go test -race ./... (slow; not in build)
make lint
make crap-strict
```

Docker fullstack tests under `it/` skip unless
`MANIFESTER_DYNAMO_ENDPOINT` and `MANIFESTER_S3_ENDPOINT` are set
(`scripts/it-up.sh`). Determinism is a separate gate:
`scripts/ci-determinism.sh`.

## Docs

- `docs/decisions.md` — design decisions
- `docs/crap-gate.md` — CRAP threshold and baseline
- `docs/lint-baseline.md` — golangci-lint policy
- `docs/aws-benchmark-runbook.md` — AWS bench / deploy
