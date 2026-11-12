# mds-controlplane

The control plane of MDS: metadata, topics, consumer groups, and the replicated log that stores them.

It was extracted from the MDS broker repository. It talks to the broker only through its API.

## Build

    mise install      # go, golangci-lint, buf; installs the pre-commit hook
    make build        # generate, fmt, ascii, vet, lint, modernize, compile, unit tests, determinism, simulation

The generated Go files (`*.pb.go`, and the `*_gen.go` files of `contract`, `wire` and
`domain/rebalance`) are not in git. `make generate` writes them, and `make build` runs it first.
The simulation library `github.com/manifesterio/dst` is a public repo with no tags. `go.mod`
pins it to a pseudo-version of its `main` tip, so the build fetches it through the normal Go
proxy and needs no credentials. Moving to a newer dst commit needs a new audit record
(`docs/dep-audits/README.md`).

Run: `go run ./cmd/mdscontrolplane`. Docker and Helm: see `deploy/README.md`.

## Make targets

- `make build` - the full gate (above). It needs no Docker.
- `make generate` - the generated files from the `.proto` files.
- `make test-race` - the tests under the race detector (`RACE_RUNS=3` repeats them).
- `make sim-campaign` - the simulation campaign, 300 seeds of each scenario (`SIM_SEEDS=5000` for more). See `docs/simulation.md`.
- `make mutants` - plant each bug of `ops/mutants.txt` and require that a test fails.
- `make fuzz` - fuzz the decoders (`FUZZTIME=10s` each).
- `make spec-long` - the conformance tests with 2000 checks each. See `internal/spec/README.md`.
- `make it` - the end-to-end tests on the real binary, against DynamoDB Local, PostgreSQL and SeaweedFS (S3) in Docker. It needs Docker, fails without it, and never skips.

## Layout

See `internal/controlplane/doc.go` for the package map and the mental model, and `docs/glossary.md` for the terms. `docs/rebalance.md` is the group protocol design; `docs/kafka-differences.md` lists where it differs from Apache Kafka.

- `cmd/mdscontrolplane` - the binary
- `cmd/cpsim` - the simulation campaign runner
- `internal/controlplane` - the control plane: `api` (.proto), `contract`, `wire`, `domain/{topic,group,rebalance,broker}`, `metadb` (with `engine`), `replica`, `executor`, `server`
- `internal/runtime` - the event and value types and the clock interface
- `internal/system` - system adapters (clock, S3, `live` run loop)
- `internal/spec` - the small independent model of the control plane, used by tests and by the simulator
- `internal/verify/simulator` - deterministic simulation of the replicated control plane
- `internal/testinfra` - containers for the integration tests
- `test/conformance` - the model conformance tests
- `test/integration` - end-to-end tests on the real binary (build tag `integration`)
- `ops` - tools and scripts (`determinismcheck`, `mutate`, `protoc-gen-mdswire`, the mutant list)
