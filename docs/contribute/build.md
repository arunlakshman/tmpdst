# Build

How to build and gate the monorepo. Modules (`dst/`, `shared/`, `controlplane/`,
`broker/`) each have a `go.mod` and `Makefile`. Root `make build` drives them in
order, then writes `bin/mdscontrolplane` and `bin/mdsbroker`.

## Prerequisites

```sh
mise install      # toolchain + pre-commit hook
make generate     # pb.go / gen files are not in git
make build        # full gate, then binaries
```

Without mise: Go and golangci-lint on PATH, then `make install-hooks` and
`make build`.

## What `make build` covers

`dst` -> `shared` -> `controlplane` -> `broker`, then
`make check-modules-standalone`, then binaries. Module steps: fmt, ASCII, vet,
lint, compile, unit tests, then sim / IT / risk / determinism as wired.

Other roots: `make test`, `make it`, `make sim`, `make sim-campaign`,
`make determinism`, `make check-deps`.

## Podman and the hook

Broker/controlplane ITs need Podman (`shared/testinfra`). On macOS:
`podman machine start`. The pre-commit hook runs `make build`. Bypass once:

```sh
SKIP_BUILD_HOOK=1 git commit ...
```

## Standalone modules (`GOWORK=off`)

```sh
make check-modules-standalone
```

Standalone broker/CP: root `make generate`, then `make -C broker build` (or
controlplane).

## Risk gate (broker)

Broker `make build` ends with `ops/tools/riskscore` (stdlib only). It fails when
a function is too hard to change safely.

```
risk(m) = comp(m)^2 * (1 - cov(m))^3 + comp(m)        cov in 0..1
```

Default threshold **20**. Override: `make build RISK_THRESHOLD=15`.

```sh
make risk           # gate on merged coverprofiles
make risk-strict    # ignore baseline; fail on all offenders
make risk-baseline  # rewrite ops/broker/risk-baseline.json (review the diff)
```

Baseline ratchet: `ops/broker/risk-baseline.json`. Fails on new offenders over
threshold, or baselined scores that got worse past tolerance (default 0.5).
Improvements never fail. Commit baseline diffs deliberately.

Determinism gate is separate: [determinism.md](determinism.md).
