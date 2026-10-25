# Project build. `make build` is the full gate; the CRAP gate is its last step.
# See docs/crap-gate.md. Determinism gate is separate: scripts/ci-determinism.sh
# (to include it, add `./scripts/ci-determinism.sh` as a step before `crap` in
# the build recipe).

COVER_PROFILE ?= build/cover.out
# Optional extra coverprofile(s), comma separated, merged into the CRAP input
# (e.g. coverage from Docker-backed integration tests).
CRAP_MERGE    ?=
# Fail threshold; also settable as CRAP_THRESHOLD in the environment.
CRAP_THRESHOLD ?= 30
CRAP_FLAGS    ?=

export CRAP_THRESHOLD

.PHONY: build test-race lint fmt-check vet compile test crap crap-baseline crap-strict

build: fmt-check vet lint compile test crap

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

# Effective Go rules via .golangci.yml (terse; capped per linter). See docs/lint-baseline.md.
lint:
	golangci-lint run ./...

compile:
	go build ./...

test:
	@mkdir -p $(dir $(COVER_PROFILE)) && echo '*' > $(dir $(COVER_PROFILE)).gitignore
	go test -coverprofile=$(COVER_PROFILE) ./...

# Race detector: too slow for `build` (>10 min on this tree), run on demand.
test-race:
	go test -race ./...

# CRAP gate alone (reuses an existing profile; run `make test` first if absent).
crap:
	@test -f $(COVER_PROFILE) || $(MAKE) test
	go run ./cmd/crapscore -profile $(COVER_PROFILE) $(if $(CRAP_MERGE),-merge-profile $(CRAP_MERGE)) $(CRAP_FLAGS)

# Full offender list, ignoring the baseline.
crap-strict:
	@$(MAKE) crap CRAP_FLAGS="-strict -top 0 $(CRAP_FLAGS)"

# Deliberate, reviewable: rewrite crap-baseline.json from the current tree.
crap-baseline:
	@test -f $(COVER_PROFILE) || $(MAKE) test
	go run ./cmd/crapscore -profile $(COVER_PROFILE) $(if $(CRAP_MERGE),-merge-profile $(CRAP_MERGE)) -update-baseline
