# Root build of the monorepo. It only calls the Makefile of each module, in this order:
#   dst, controlplane, broker  (a module may import only the modules before it).
# `make build` is the full gate. It ends by building the two binaries into bin/.
# Toolchain and commit hook: `mise install` (pins in mise.toml; postinstall runs make install-hooks).
# The commit hook (ops/githooks/pre-commit) runs `make build`.
# Bypass once: SKIP_BUILD_HOOK=1 git commit ...

.PHONY: build generate test install-hooks

build:
	$(MAKE) -C dst build
	$(MAKE) -C controlplane build
	$(MAKE) -C broker build
	mkdir -p bin
	cd controlplane && go build -o ../bin/mdscontrolplane ./cmd/mdscontrolplane
	cd broker && go build -o ../bin/mdsbroker ./cmd/mdsbroker

# dst has no generated files. The broker target that generates is called proto.
generate:
	$(MAKE) -C controlplane generate
	$(MAKE) -C broker proto

test:
	$(MAKE) -C dst test
	$(MAKE) -C controlplane test
	$(MAKE) -C broker test

# Symlink the hook into .git/hooks so every commit runs `make build`.
# Does not change git config.
install-hooks:
	@mkdir -p .git/hooks
	@ln -sfn ../../ops/githooks/pre-commit .git/hooks/pre-commit
	@chmod +x ops/githooks/pre-commit
	@echo "installed .git/hooks/pre-commit -> ops/githooks/pre-commit"
