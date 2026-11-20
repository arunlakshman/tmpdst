# Contributing to MDS

## Prerequisites

- [mise](https://mise.jdx.dev) (toolchain: Go, golangci-lint, buf, and related tools)
- [Podman](https://podman.io/) for the full commit hook and integration tests (`make it`, module `make build` on controlplane/broker)

## Build

From the repo root:

```sh
mise install       # toolchain + pre-commit hook
make generate      # required before standalone broker/controlplane builds (pb.go not in git)
make build         # full gate: all modules, then bin/mdscontrolplane and bin/mdsbroker
```

The commit hook runs `make build` and therefore needs Podman. To skip the hook for a one-off commit (for example docs-only):

```sh
SKIP_BUILD_HOOK=1 git commit ...
```

## Docs for contributors

| Topic | Doc |
|-------|-----|
| Build, hooks, standalone modules | [docs/contribute/build.md](docs/contribute/build.md) |
| Determinism, allow-list, audits | [docs/contribute/determinism.md](docs/contribute/determinism.md) |
| Risk-score gate | [docs/contribute/risk-gate.md](docs/contribute/risk-gate.md) |

## Pull requests

- Keep source and comments ASCII unless a file already uses non-ASCII for a reason.
- Format and tests for code you touch (`make build` / module gates).
- Do not commit secrets, credentials, or local env files.
