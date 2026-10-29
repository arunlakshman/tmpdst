# manifester

Kafka-protocol brokers and a log-backed control plane (Go).

## Bootstrap

1. Install [mise](https://mise.jdx.dev).
2. From the repo root:

```bash
mise install
```

That installs the pinned toolchain from `mise.toml` (`go`, `golangci-lint`,
`buf`) and runs `make install-hooks` (mise `postinstall`), which links
`ops/scripts/githooks/pre-commit` into `.git/hooks/pre-commit`.

Without mise:

```bash
make install-hooks   # uses ambient go / golangci-lint on PATH
```

## Build gate

`make` / `make build` is the full gate, in order:

| Step | What |
|------|------|
| proto | `make proto` regenerates ignored `*.pb.go` via mise `buf` |
| fmt-check | `gofmt -l` must be empty |
| ascii-check | `ops/scripts/ascii-check.sh` |
| vet | `go vet ./...` |
| lint | `golangci-lint run ./...` (see `.golangci.yml`) |
| compile | `go build ./...` |
| test | unit packages (not `internal/verify/sim`, not `test/integrationtest`) |
| sim | `go test ./internal/verify/sim/...` |
| it-hermetic | `go test ./test/integrationtest/` (in-process memstore) |
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

Docker fullstack tests under `test/integrationtest/` skip unless
`MANIFESTER_DYNAMO_ENDPOINT` and `MANIFESTER_S3_ENDPOINT` are set
(`ops/scripts/it-up.sh`). The import rules for the deterministic packages
are checked by `go run ./ops/tools/determinismcheck <packages>`.

## Docs

- `docs/decisions.md` — design decisions
- `docs/crap-gate.md` — CRAP threshold and baseline
- `docs/lint-baseline.md` — golangci-lint policy
- `docs/aws-benchmark-runbook.md` — AWS bench / deploy
