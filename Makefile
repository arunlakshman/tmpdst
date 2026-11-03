# Project build. `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, modernize-check, compile, unit tests, sim, hermetic it,
#   Kafka CLI e2e, change risk score.
# Docker fullstack test/integrationtest tests still Skip unless MDS_*_ENDPOINT are set.
# The import check for deterministic packages is the determinismcheck tool:
# go run ./ops/tools/determinismcheck <packages>
# Toolchain + commit hook: `mise install` (pins in mise.toml; postinstall runs
# make install-hooks). Without mise: `make install-hooks` alone.

COVER_DIR     ?= build
COVER_UNIT    ?= $(COVER_DIR)/cover-unit.out
COVER_SIM     ?= $(COVER_DIR)/cover-sim.out
COVER_IT      ?= $(COVER_DIR)/cover-it.out
# Optional extra coverprofile(s), comma separated, merged into the risk score input
# (e.g. coverage from Docker-backed integration tests).
RISK_MERGE    ?=
# Fail threshold; also settable as RISK_THRESHOLD in the environment.
RISK_THRESHOLD ?= 20
RISK_FLAGS    ?=
# Seeds per scenario for the sim campaign.
SIM_SEEDS     ?= 300

# Everything except internal/verify/simulator and test/integrationtest (those have their own targets).
UNIT_PKGS = $(shell go list ./... | grep -vE '/verify/simulator(/|$$)|/test/integrationtest$$')

export RISK_THRESHOLD

# Proto plugins (pin versions; local buf, no BSR). Override via env if needed.
# buf itself comes from mise (`mise install`); plugins still go-install.
BUF            ?= buf
PROTOC_GEN_GO  ?= protoc-gen-go
PROTOC_GEN_GRPC ?= protoc-gen-go-grpc
PROTO_GO_VER   ?= v1.35.2
PROTO_GRPC_VER ?= v1.5.1
# Image for make it-postgres (pinned; see ops/scripts/postgres-test.sh).
POSTGRES_IMAGE ?= postgres:17.11-alpine
# Extra go test args for make it-postgres, e.g. PG_TEST_ARGS="-v ./internal/controlplane/executor/"
PG_TEST_ARGS  ?=

.PHONY: build compile test sim it-hermetic test-all sim-campaign \
	ascii-check lint modernize-check modernize fmt-check vet risk risk-baseline risk-strict \
	install-hooks proto proto-tools e2e-kafka-cli it-postgres

build: proto fmt-check ascii-check vet lint modernize-check compile test sim it-hermetic e2e-kafka-cli risk

# Generate Go from internal/controlplane/api/v1/*.proto into ignored *.pb.go files.
# Prefer mise-provided buf; plugins via `make proto-tools` (go install).
proto: proto-tools
	@if command -v mise >/dev/null 2>&1; then \
		cd internal/controlplane/api && mise exec -- $(BUF) generate; \
	elif command -v $(BUF) >/dev/null 2>&1; then \
		cd internal/controlplane/api && $(BUF) generate; \
	else \
		echo "buf not found: run \`mise install\` (pins buf in mise.toml)" >&2; \
		exit 1; \
	fi

proto-tools:
	@command -v $(PROTOC_GEN_GO) >/dev/null || go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTO_GO_VER)
	@command -v $(PROTOC_GEN_GRPC) >/dev/null || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTO_GRPC_VER)

# Symlink ops/scripts/githooks/* into .git/hooks so every commit runs `make build`.
# Does not change git config. Prefer `mise install` (runs this via postinstall).
# Bypass once: SKIP_BUILD_HOOK=1 git commit ...
install-hooks:
	@mkdir -p .git/hooks
	@ln -sfn ../../ops/scripts/githooks/pre-commit .git/hooks/pre-commit
	@chmod +x ops/scripts/githooks/pre-commit
	@echo "installed .git/hooks/pre-commit -> ops/scripts/githooks/pre-commit"

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

# Effective Go rules via .golangci.yml. Any finding fails the build (exit 1).
# See docs/lint-baseline.md.
lint:
	golangci-lint run ./...

# Built-in go fix modernizers (range-int, waitgroup.Go, errors.AsType, etc.).
# Diff-only: fails the gate if the tree is not already modernized; does not rewrite.
modernize-check:
	go fix -diff ./...

# Apply the same modernizers locally (review the diff before committing).
modernize:
	go fix ./...

# No non-ASCII characters in code files (ops/scripts/ascii-check.sh).
ascii-check:
	@ops/scripts/ascii-check.sh

compile:
	go build ./...

$(COVER_DIR)/.gitignore:
	@mkdir -p $(COVER_DIR) && echo '*' > $@

# Unit packages only (not verify/simulator, not test/integrationtest).
test: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_UNIT) $(UNIT_PKGS)

# Deterministic simulation packages, then a seed campaign for the broker
# (scenario kafka) and for the log-backed control plane (scenario kafka-log).
sim: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_SIM) ./internal/verify/simulator/...
	@$(MAKE) --no-print-directory sim-campaign

# Seed campaigns through cmd/mdssimulator. More seeds: make sim-campaign SIM_SEEDS=2000
sim-campaign:
	go run ./cmd/mdssimulator -scenario kafka -seeds $(SIM_SEEDS)
	go run ./cmd/mdssimulator -scenario kafka-log -seeds $(SIM_SEEDS)
	go run ./cmd/mdssimulator -scenario kafka-log-freeze -seeds $(SIM_SEEDS)

# Hermetic integration tests (in-process memstore). Fullstack Docker tests
# in the same package Skip unless MDS_DYNAMO_ENDPOINT + MDS_S3_ENDPOINT are set.
it-hermetic: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_IT) ./test/integrationtest/

# Log tests against a real PostgreSQL in Docker (needs Docker). Not part of build: the Postgres cases skip when MDS_TEST_POSTGRES_DSN is unset.
it-postgres:
	POSTGRES_IMAGE=$(POSTGRES_IMAGE) bash ops/scripts/postgres-test.sh $(PG_TEST_ARGS)

# All tests without lint/compile/risk.
test-all: test sim it-hermetic


# Risk gate: merge unit + sim + hermetic-it coverprofiles.
risk:
	@test -f $(COVER_UNIT) || $(MAKE) test
	@test -f $(COVER_SIM) || $(MAKE) sim
	@test -f $(COVER_IT) || $(MAKE) it-hermetic
	go run ./cmd/riskscore -profile $(COVER_UNIT) \
		-merge-profile $(COVER_SIM),$(COVER_IT)$(if $(RISK_MERGE),$(comma)$(RISK_MERGE)) \
		$(RISK_FLAGS)

comma := ,

# E2E: mdscontrolplane + mdsbroker driven by the Apache Kafka 3.9.1 CLI (needs Java and minio, see mise.toml; part of build).
e2e-kafka-cli:
	bash ops/scripts/e2e-kafka-cli.sh $(E2E_ARGS)

# Full offender list, ignoring the baseline.
risk-strict:
	@$(MAKE) risk RISK_FLAGS="-strict -top 0 $(RISK_FLAGS)"

# Deliberate, reviewable: rewrite ops/risk-baseline.json from the current tree.
risk-baseline:
	@test -f $(COVER_UNIT) || $(MAKE) test
	@test -f $(COVER_SIM) || $(MAKE) sim
	@test -f $(COVER_IT) || $(MAKE) it-hermetic
	go run ./cmd/riskscore -profile $(COVER_UNIT) \
		-merge-profile $(COVER_SIM),$(COVER_IT)$(if $(RISK_MERGE),$(comma)$(RISK_MERGE)) \
		-update-baseline
