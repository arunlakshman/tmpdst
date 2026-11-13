# Build for the MDS control plane. `make` / `make build` is the full gate:
#   generate (buf: the *.pb.go files and protoc-gen-mdswire), fmt, ascii, vet, lint, modernize, compile, unit tests, determinism, simulation.
# Toolchain: `mise install` at the repo root (pins in the root mise.toml). The commit hook is
# installed by the root Makefile (make install-hooks).

BUF             ?= buf
PROTOC_GEN_GO   ?= protoc-gen-go
PROTOC_GEN_GRPC ?= protoc-gen-go-grpc
PROTO_GO_VER    ?= v1.35.2
PROTO_GRPC_VER  ?= v1.5.1
# Seeds per scenario for make sim-campaign (see docs/simulation.md).
SIM_SEEDS       ?= 300

.PHONY: it build compile test proto proto-tools fmt-check vet lint modernize-check modernize \
	ascii-check determinism sim sim-campaign spec-long test-race generate mutants fuzz

build: generate fmt-check ascii-check vet lint modernize-check compile test determinism sim sim-campaign

# Generate Go from api/**/*.proto (shared, service, log, options):
# the *.pb.go files next to them, and, by the plugin ops/tools/protoc-gen-mdswire
# (buf runs it with go run), the contract value types and the wire converters.
# None of it is in git.
proto: proto-tools
	@if command -v mise >/dev/null 2>&1; then \
		cd api && mise exec -- $(BUF) generate; \
	elif command -v $(BUF) >/dev/null 2>&1; then \
		cd api && $(BUF) generate; \
	else \
		echo "buf not found: run \`mise install\` (pins buf in mise.toml)" >&2; \
		exit 1; \
	fi

proto-tools:
	@command -v $(PROTOC_GEN_GO) >/dev/null || go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTO_GO_VER)
	@command -v $(PROTOC_GEN_GRPC) >/dev/null || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTO_GRPC_VER)

# Determinism gate for internal/runtime, the contract and the deterministic domain packages.
determinism:
	bash ops/scripts/ci-determinism.sh

# All generated Go: the *.pb.go files and the contract, rebalance and wire *_gen.go
# files. One buf run writes all of them (see make proto). None of it is in git,
# and the module does not compile without it.
generate: proto

compile:
	go build ./...

test:
	go test ./...

# The test suite under the race detector. It is not part of make build (it is
# slow). RACE_RUNS repeats it, and RACE_OUT keeps the full output of every run in
# a file, so a rare failure names its test. Example: make test-race RACE_RUNS=30
RACE_RUNS ?= 1
RACE_OUT  ?= build/test-race
test-race:
	@mkdir -p $(RACE_OUT)
	@fail=0; for i in $$(seq 1 $(RACE_RUNS)); do \
	  if go test -race -count=1 ./... > $(RACE_OUT)/run-$$i.txt 2>&1; then echo "run $$i: ok"; \
	  else echo "run $$i: FAILED (see $(RACE_OUT)/run-$$i.txt)"; fail=1; fi; \
	done; exit $$fail

# Fuzz the decoders for FUZZTIME each (not part of make build; the seed corpus
# runs in make test). Go allows one -fuzz target per run, so this loops.
FUZZTIME ?= 10s
FUZZ_TARGETS = \
	./internal/controlplane/metadb:FuzzDecode \
	./internal/controlplane/domain/topic:FuzzDecodeBatchRun \
	./internal/controlplane/domain/topic:FuzzParseDuplicateCheck \
	./internal/controlplane/domain/group:FuzzDecodeHeader \
	./internal/controlplane/domain/group:FuzzDecodeMember \
	./internal/controlplane/domain/group:FuzzDecodeOffset
fuzz:
	@for t in $(FUZZ_TARGETS); do \
	  echo "fuzz $${t#./internal/controlplane/}"; \
	  go test -run='^$$' -fuzz="^$${t#*:}\$$" -fuzztime=$(FUZZTIME) "$${t%%:*}" || exit 1; \
	done

# Deterministic simulation of the replicated control plane: its own tests, then
# a campaign of SIM_SEEDS seeds for each scenario. A failure prints the command
# that runs the seed again.
sim:
	go test ./internal/verify/simulator/...

sim-campaign:
	go run ./cmd/cpsim -seeds $(SIM_SEEDS) -scenario all

# The conformance tests (test/conformance: the metadb, the replica with
# crashes and failure injection, the gRPC stack) and the test doubles, with 2000
# checks each (rapid default is 100). Not part of build. To replay one failure,
# see spec/README.md.
spec-long:
	go test ./test/conformance -count=1 -timeout 60m -rapid.checks=2000

# Plant each bug of ops/mutants.txt in a copy of the tree and run its test
# command. A bug the tests do not see fails the target, and so does a mutant whose
# old text is no longer in the code exactly once. Not part of build.
# One mutant: make mutants MUTANT=writer-offsets
MUTANT ?=
mutants:
	go run ./ops/tools/mutate $(if $(MUTANT),-only $(MUTANT))

# Integration tests: real DynamoDB Local, PostgreSQL and SeaweedFS (S3) in Podman, started by
# internal/testinfra. test/integration builds the mdscontrolplane binary once and runs it as
# child processes against them. It fails when Podman is not running and never skips. Not part of build.
it:
	@podman info >/dev/null 2>&1 || { echo "make it: Podman is not running (or the Podman socket is not reachable). Start Podman and run again; the integration tests are never skipped." >&2; exit 1; }
	go test -tags integration -count=1 -timeout 15m ./...

include ../make/common.mk
