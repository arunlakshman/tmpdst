# Build for dst. `make` / `make build` is the full gate:
#   fmt, ascii, vet, lint, modernize, tests, determinism.
# Toolchain: `mise install` at the repo root (pins in the root mise.toml). The commit hook is
# installed by the root Makefile (make install-hooks).

.PHONY: build test fmt-check vet lint modernize-check modernize ascii-check determinism

build: fmt-check ascii-check vet lint modernize-check test determinism

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

modernize-check:
	go fix -diff ./...

modernize:
	go fix ./...

ascii-check:
	@ops/scripts/ascii-check.sh

test:
	go test -race ./...

# Determinism gate for the single-goroutine packages (rng, sched, clock, ...).
determinism:
	bash ops/scripts/ci-determinism.sh
