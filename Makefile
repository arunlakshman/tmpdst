# Project build. `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, compile, unit tests, sim, hermetic it, CRAP.
# Docker fullstack test/it tests still Skip unless MANIFESTER_*_ENDPOINT are set.
# The import check for deterministic packages is the detcheck tool:
# go run ./ops/tools/detcheck <packages>
# Toolchain + commit hook: `mise install` (pins in mise.toml; postinstall runs
# make install-hooks). Without mise: `make install-hooks` alone.

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

# Everything except internal/verify/sim and test/it (those have their own targets).
UNIT_PKGS = $(shell go list ./... | grep -vE '/verify/sim(/|$$)|/test/it$$')

export CRAP_THRESHOLD

# Proto plugins (pin versions; local buf, no BSR). Override via env if needed.
# buf itself comes from mise (`mise install`); plugins still go-install.
BUF            ?= buf
PROTOC_GEN_GO  ?= protoc-gen-go
PROTOC_GEN_GRPC ?= protoc-gen-go-grpc
PROTO_GO_VER   ?= v1.35.2
PROTO_GRPC_VER ?= v1.5.1

.PHONY: build compile test sim it-hermetic test-all test-race \
	ascii-check lint fmt-check vet crap crap-baseline crap-strict install-hooks proto proto-tools

build: proto fmt-check ascii-check vet lint compile test sim it-hermetic crap

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

# No non-ASCII characters in code files (ops/scripts/ascii-check.sh).
ascii-check:
	@ops/scripts/ascii-check.sh

compile:
	go build ./...

$(COVER_DIR)/.gitignore:
	@mkdir -p $(COVER_DIR) && echo '*' > $@

# Unit packages only (not verify/sim, not test/it).
test: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_UNIT) $(UNIT_PKGS)

# Deterministic simulation packages.
sim: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_SIM) ./internal/verify/sim/...

# Hermetic integration tests (in-process memstore). Fullstack Docker tests
# in the same package Skip unless MANIFESTER_DYNAMO_ENDPOINT + MANIFESTER_S3_ENDPOINT are set.
it-hermetic: $(COVER_DIR)/.gitignore
	go test -coverprofile=$(COVER_IT) ./test/it/

# All tests without lint/compile/crap.
test-all: test sim it-hermetic

# Race detector: too slow for `build` (>10 min on this tree), run on demand.
# internal/verify/sim/kafka alone needs ~16m under -race on a typical laptop; Go's default
# 10m package timeout cuts it mid-campaign. Keep this above measured wall time.
test-race:
	go test -race -timeout 30m ./...

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

# Deliberate, reviewable: rewrite ops/crap-baseline.json from the current tree.
crap-baseline:
	@test -f $(COVER_UNIT) || $(MAKE) test
	@test -f $(COVER_SIM) || $(MAKE) sim
	@test -f $(COVER_IT) || $(MAKE) it-hermetic
	go run ./cmd/crapscore -profile $(COVER_UNIT) \
		-merge-profile $(COVER_SIM),$(COVER_IT)$(if $(CRAP_MERGE),$(comma)$(CRAP_MERGE)) \
		-update-baseline
