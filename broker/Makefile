# Project build. `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, modernize-check, compile, unit tests, sim, hermetic it,
#   Kafka CLI e2e, change risk score.
# Docker fullstack test/integrationtest tests still Skip unless MDS_*_ENDPOINT are set.
# The import check for deterministic packages is the determinismcheck tool:
# go run ./ops/tools/determinismcheck <packages>
# Toolchain: `mise install` at the repo root (pins in the root mise.toml). The commit hook is
# installed by the root Makefile (make install-hooks).

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

# Image for make it-postgres (pinned; see ops/scripts/postgres-test.sh).
POSTGRES_IMAGE ?= postgres:17.11-alpine
# Extra go test args for make it-postgres (CP Postgres tests moved to mds-controlplane; see docs/cp-extraction.md).
PG_TEST_ARGS  ?=

.PHONY: build compile test sim it-hermetic test-all sim-campaign \
	ascii-check lint modernize-check modernize fmt-check vet risk risk-baseline risk-strict \
	e2e-kafka-cli it-postgres

build: fmt-check ascii-check vet lint modernize-check compile test sim it-hermetic e2e-kafka-cli risk

compile:
	go build ./...

$(COVER_DIR)/.gitignore:
	@mkdir -p $(COVER_DIR) && echo '*' > $@

# Unit packages only (not verify/simulator, not test/integrationtest).
test: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_UNIT) $(UNIT_PKGS)

# Deterministic simulation packages, then a seed campaign for the broker
# (scenario kafka). kafka-log lives in mds-controlplane after CP extraction.
sim: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_SIM) ./internal/verify/simulator/...
	@$(MAKE) --no-print-directory sim-campaign

# Seed campaigns through cmd/mdssimulator. More seeds: make sim-campaign SIM_SEEDS=2000
sim-campaign:
	go run ./cmd/mdssimulator -scenario kafka -seeds $(SIM_SEEDS)

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

# E2E: ../controlplane + mdsbroker driven by the Apache Kafka 3.9.1 CLI
# (needs Java and minio, see mise.toml; part of build). Override with MDS_CONTROLPLANE_DIR.
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

include ../make/common.mk
