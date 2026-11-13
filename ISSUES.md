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

## 5. `make it` on Podman needs two local settings

- Result on 2026-11-12: `make it` PASSES (exit 0, all packages ok) with the two settings below.
- The `docker` command must exist. On this Mac it is only a shell alias, so `make` cannot see it. Put a link named `docker` to `podman` early in PATH.
- Set `TESTCONTAINERS_RYUK_DISABLED=true`. Without it the test library looks for a network named `bridge`, and Podman calls its default network `podman`.
- Pull the images first (dynamodb-local 3.1.0, seaweedfs 4.48, postgres 17.11-alpine): the first pull is slow and the tests give up after 300 seconds.
- Later: make this work without these settings, and change the "Docker" words in the Makefile messages to Podman.

## 6. cpsim never reaches one declared path

- cpsim never reaches "logcp-sim: idempotent replay after takeover". It needs a client retry that is answered after a takeover. Result on 2026-11-13: 0 hits in 2000 seeds of every scenario, and 0 hits in 10000 more `chaos` seeds.
- It stays declared in the code but is not expected (`assert.NewReachableNotExpected`), so the cpsim report still says "all 27 expected paths were reached", as before. "put refused at or below the floor" and "replayed key answered from the window" are reached but are also not expected, to keep the expected set equal to the old hand list of 27 names.
- A gap stays a gap (decision 5): no scenario or fault was added for it. Later: a scenario that retries a write after the writer changed.
