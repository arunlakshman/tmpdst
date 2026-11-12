# Build for shared (leaf code with no product types: AWS metrics, HTTP connection
# settings, run info). `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, modernize, tests.
# Toolchain: `mise install` at the repo root (pins in the root mise.toml). The commit hook is
# installed by the root Makefile (make install-hooks).

.PHONY: build test

build: fmt-check ascii-check vet lint modernize-check test

test:
	go test -race ./...

include ../make/common.mk
