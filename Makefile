# Project build. `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, compile, unit tests, sim, hermetic it, CRAP.
# Docker fullstack it/ tests still Skip unless MANIFESTER_*_ENDPOINT are set.
# Determinism gate is separate: scripts/ci-determinism.sh
# (to include it, add `./scripts/ci-determinism.sh` as a step before `crap`).

COVER_DIR     ?= build
COVER_UNIT    ?= $(COVER_DIR)/cover-unit.out
COVER_SIM     ?= $(COVER_DIR)/cover-sim.out
COVER_IT      ?= $(COVER_DIR)/cover-it.out
# Optional extra coverprofile(s), comma separated, merged into the CRAP input
# (e.g. coverage from Docker-backed integration tests).
CRAP_MERGE    ?=
# Fail threshold; also settable as CRAP_THRESHOLD in the environment.
CRAP_THRESHOLD ?= 20
CRAP_FLAGS    ?=

# Everything except sim/ and it/ (those have their own targets).
UNIT_PKGS = $(shell go list ./... | grep -vE '/sim(/|$$)|/it$$')

export CRAP_THRESHOLD

.PHONY: build compile test sim it-hermetic test-all test-race \
	ascii-check lint fmt-check vet crap crap-baseline crap-strict

build: fmt-check ascii-check vet lint compile test sim it-hermetic crap

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

# Effective Go rules via .golangci.yml (terse; capped per linter). See docs/lint-baseline.md.
lint:
	golangci-lint run ./...

# No non-ASCII characters in code files (scripts/ascii-check.sh).
ascii-check:
	@scripts/ascii-check.sh

compile:
	go build ./...

$(COVER_DIR)/.gitignore:
	@mkdir -p $(COVER_DIR) && echo '*' > $@

# Unit packages only (not sim/, not it/).
test: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_UNIT) $(UNIT_PKGS)

# Deterministic simulation packages.
sim: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_SIM) ./sim/...

# Hermetic integration tests (in-process memstore). Fullstack Docker tests
# in the same package Skip unless MANIFESTER_DYNAMO_ENDPOINT + MANIFESTER_S3_ENDPOINT are set.
it-hermetic: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_IT) ./it/

# All tests without lint/compile/crap.
test-all: test sim it-hermetic

# Race detector: too slow for `build` (>10 min on this tree), run on demand.
test-race:
	go test -race ./...

# CRAP gate: merge unit + sim + hermetic-it coverprofiles.
crap:
	@test -f $(COVER_UNIT) || $(MAKE) test
	@test -f $(COVER_SIM) || $(MAKE) sim
	@test -f $(COVER_IT) || $(MAKE) it-hermetic
	go run ./cmd/crapscore -profile $(COVER_UNIT) \
		-merge-profile $(COVER_SIM),$(COVER_IT)$(if $(CRAP_MERGE),$(comma)$(CRAP_MERGE)) \
		$(CRAP_FLAGS)

comma := ,

# Full offender list, ignoring the baseline.
crap-strict:
	@$(MAKE) crap CRAP_FLAGS="-strict -top 0 $(CRAP_FLAGS)"

# Deliberate, reviewable: rewrite crap-baseline.json from the current tree.
crap-baseline:
	@test -f $(COVER_UNIT) || $(MAKE) test
	@test -f $(COVER_SIM) || $(MAKE) sim
	@test -f $(COVER_IT) || $(MAKE) it-hermetic
	go run ./cmd/crapscore -profile $(COVER_UNIT) \
		-merge-profile $(COVER_SIM),$(COVER_IT)$(if $(CRAP_MERGE),$(comma)$(CRAP_MERGE)) \
		-update-baseline
