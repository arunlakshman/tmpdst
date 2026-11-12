# Known issues (found during the monorepo move, not fixed on purpose)

## 1. Broker determinism gate is red

- Command: `bash broker/ops/scripts/ci-determinism.sh`
- Error: `broker/internal/core/operation_runner.go:3:8: import "context" is not on the deterministic allow-list`
- It was already red before the move (commit a056a4b of the broker repo). It is not part of `make build`, so nothing was blocked.
- Fix options for later: add `context` to `broker/ops/determinism-allow.txt`, or remove the `context` import from `operation_runner.go`.
- Why not fixed now: the monorepo plan allows no behavior change (decision 5).

## 2. Lint rules that enforce module order are not proven

- T0.8 added depguard rules to dst, shared and controlplane `.golangci.yml`. The linter accepts them, but nobody has seen one fire.
- The shell check `ops/scripts/check-module-deps.sh` (first step of `make build`) is the check that really enforces the order.

## 3. Container files are not tested

- The control plane Dockerfile (root build context, copies dst, shared and controlplane) was changed in T0.2, T0.5 and T0.6 and has not been built.
- `.dockerignore` in `controlplane/` no longer applies to a root-context build.

## 4. Go module files

- `broker/go.mod` was edited by hand in T0.6, and it was already not tidy before (x/tools, x/mod, x/sync, dynamodb). Check each module with `GOWORK=off go build ./...` before a release.
- Standalone `make -C broker build` needs the control plane generated files. Run root `make generate` first.

## 5. `make it` does not pass on Podman yet

- Run on 2026-11-12 with a temporary `docker` link to podman (the `docker` command is only a shell alias, so `make` cannot see it).
- The dynamodb-local container did not start in 300 seconds: `create container: context deadline exceeded`. Other packages passed.
- Not checked further. Possible causes: Podman VM has 2 GiB of memory, slow image pull, or testcontainers settings for Podman.
- The Makefile messages still say "Docker".
