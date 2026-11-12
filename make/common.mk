# Build rules shared by every module Makefile (dst, controlplane, broker).
# A module Makefile includes this file at its end:  include ../make/common.mk
# Every rule runs inside the module folder (make -C <module>).

.PHONY: fmt-check vet lint modernize-check modernize ascii-check

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

# Rules are in the .golangci.yml of the module. Any finding fails the build.
lint:
	golangci-lint run ./...

# Built-in go fix modernizers (range-int, waitgroup.Go, errors.AsType, etc.).
# Diff-only: fails the gate if the tree is not already modernized; does not rewrite.
modernize-check:
	go fix -diff ./...

# Apply the same modernizers locally (review the diff before committing).
modernize:
	go fix ./...

# No non-ASCII characters in the code files of the module (../ops/scripts/ascii-check.sh).
ascii-check:
	@../ops/scripts/ascii-check.sh .
