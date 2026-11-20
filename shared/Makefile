# Build for shared (leaf code with no product types: AWS metrics, HTTP connection
# settings, run info). `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, modernize, tests.
# Toolchain: `mise install` at the repo root (pins in the root mise.toml). The commit hook is
# installed by the root Makefile (make install-hooks).

.PHONY: build test it

build: fmt-check ascii-check vet lint modernize-check test

test:
	go test -race ./...

# Podman-backed smoke tests for testinfra helpers (DynamoDB Local, PostgreSQL, SeaweedFS).
# Invoked from controlplane make it so root make it runs them once. Never skips.
it:
	@podman info >/dev/null 2>&1 || { echo "make it: Podman is not running (or the Podman socket is not reachable). Start Podman and run again; the integration tests are never skipped." >&2; exit 1; }
	go test -tags integration -count=1 -timeout 15m -v ./testinfra

include ../make/common.mk
