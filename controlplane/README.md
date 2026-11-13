# controlplane

The control plane of MDS: metadata, topics, consumer groups, and the replicated log that stores them.

This is a module of the MDS monorepo. It talks to the broker only through its API (`api/`).
It imports `dst` and `shared`. It never imports `broker`.

## Build

    mise install      # at the repo root: go, golangci-lint, buf; installs the pre-commit hook
    make build        # in this folder, or `make -C controlplane build` from the root; generate, fmt, ascii, vet, lint, modernize, compile, unit tests, determinism, simulation

The generated Go files (`*.pb.go`, and the `*_gen.go` files of `contract`, `wire` and
`domain/rebalance`) are not in git. `make generate` writes them, and `make build` runs it first.
The simulation library is the `dst` module of this repo (see `go.work` at the root).
A new dependency of a deterministic package needs an audit record (`docs/dep-audits/README.md`).

Run: `go run ./cmd/mdscontrolplane`. From the repo root, `make build` also writes `bin/mdscontrolplane`. Podman and Helm: see `deploy/README.md`.

## Make targets

- `make build` - the full gate (above). It needs no Podman.
- `make generate` - the generated files from the `.proto` files.
- `make test-race` - the tests under the race detector (`RACE_RUNS=3` repeats them).
- `make sim-campaign` - the simulation campaign, 300 seeds of each scenario (`SIM_SEEDS=5000` for more). See `docs/simulation.md`.
- `make mutants` - plant each bug of `ops/mutants.txt` and require that a test fails.
- `make fuzz` - fuzz the decoders (`FUZZTIME=10s` each).
- `make spec-long` - the conformance tests with 2000 checks each. See `spec/README.md`.
- `make it` - the end-to-end tests on the real binary, against DynamoDB Local, PostgreSQL and SeaweedFS (S3) in Podman. It needs Podman, fails without it, and never skips.

## Layout

See `internal/controlplane/doc.go` for the package map and the mental model, and `docs/glossary.md` for the terms. `docs/rebalance.md` is the group protocol design; `docs/kafka-differences.md` lists where it differs from Apache Kafka.

- `cmd/mdscontrolplane` - the binary
- `cmd/cpsim` - the simulation campaign runner
- `api` - the .proto files and the generated Go (the broker imports it)
- `contract` - the public vocabulary: operations, responses, `ErrCode` (the broker imports it)
- `internal/controlplane` - the control plane: `wire`, `domain/{topic,group,rebalance,broker}`, `metadb` (with `engine`), `replica`, `executor`, `server`
- `internal/runtime` - the event and value types and the clock interface
- `spec` - the small independent model of the control plane, used by tests and by the simulator (public: the broker will import it)
- `internal/verify/simulator` - deterministic simulation of the replicated control plane
- `internal/testinfra` - containers for the integration tests
- `test/conformance` - the model conformance tests
- `test/integration` - end-to-end tests on the real binary (build tag `integration`)
- `ops` - tools and scripts (`mutate`, `protoc-gen-mdswire`, the mutant list). The determinism tool is `dst/cmd/determinismcheck`.
