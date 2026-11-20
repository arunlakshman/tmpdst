# Contributing

Thank you for helping. This file explains how the repo is laid out and where tests go.

## Repo layout

| Folder | What it is |
|---|---|
| `dst/` | Deterministic simulation testing library |
| `shared/` | Small code used by more than one module |
| `broker/` | The broker |
| `controlplane/` | The control plane |
| `deploy/` | Helm charts (`helm/`) and container build files (`container/`) |
| `docs/` | Design docs, one folder per module |
| `ops/` | Repo tooling: scripts, git hooks, shared Makefile rules (`ops/make/common.mk`) |

Each module has its own `go.mod`. Run `make` commands inside the module, or the root `Makefile` for all modules.

## Where tests go

Use the same rule in every module.

1. **Unit tests** sit next to the code, as `*_test.go` in the same folder.
2. **Slow tests, simulation tests, and tests across many packages** go in `<module>/test/`.
   Example: `broker/test/integrationtest`, `controlplane/test/conformance`.
3. **Test data files** go in a `testdata/` folder beside the test that reads them.
   Go tools skip `testdata/` folders, so never put Go packages there.
4. **Test helper packages** (code other tests import) are normal packages, not `testdata/`.
   Example: `dst/assert`, `dst/golden`, `dst/live`.
5. **Examples** go in `examples/`. They must compile in CI.
6. **Architecture tests** stay in the package they guard.
   Example: `controlplane/internal/controlplane/architecture_test.go`.

## Before you send a pull request

- Run `make` at the root. It runs the checks, the tests, the simulations, and the determinism check.
- Run `make install-hooks` once to enable the pre-commit hook.
- Keep names, comments, and docs in plain, simple English.
- Every new file needs the Apache-2.0 license, which covers the whole repo (see `LICENSE`).

## Reporting a security problem

Do not open a public issue. Read `SECURITY.md`.
