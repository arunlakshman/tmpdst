# Root build of the monorepo. It only calls the Makefile of each module, in this order:
#   dst, shared, controlplane, broker  (a module may import only the modules before it).
# `make build` is the full gate. It ends by building the two binaries into bin/.
# Toolchain and commit hook: `mise install` (pins in mise.toml; postinstall runs make install-hooks).
# Rules shared by the modules (fmt-check, vet, lint, modernize, ascii-check) are in make/common.mk.
# The commit hook (ops/githooks/pre-commit) runs `make build`.
# Bypass once: SKIP_BUILD_HOOK=1 git commit ...

.PHONY: build generate test sim sim-campaign determinism install-hooks

build:
	$(MAKE) -C dst build
	$(MAKE) -C shared build
	$(MAKE) -C controlplane build
	$(MAKE) -C broker build
	mkdir -p bin
	cd controlplane && go build -o ../bin/mdscontrolplane ./cmd/mdscontrolplane
	cd broker && go build -o ../bin/mdsbroker ./cmd/mdsbroker

# Only the control plane generates Go (the proto lives in controlplane/api). The broker imports it.
generate:
	$(MAKE) -C controlplane generate

test:
	$(MAKE) -C dst test
	$(MAKE) -C shared test
	$(MAKE) -C controlplane test
	$(MAKE) -C broker test

# Targets a module does not have are left out on purpose (the list below names each module).
# dst has no sim and no sim-campaign. broker has no determinism target: it runs its script.
sim:
	$(MAKE) -C controlplane sim
	$(MAKE) -C broker sim

sim-campaign:
	$(MAKE) -C controlplane sim-campaign
	$(MAKE) -C broker sim-campaign

determinism:
	$(MAKE) -C dst determinism
	$(MAKE) -C controlplane determinism
	cd broker && bash ops/scripts/ci-determinism.sh

# Symlink the hook into .git/hooks so every commit runs `make build`.
# Does not change git config.
install-hooks:
	@mkdir -p .git/hooks
	@ln -sfn ../../ops/githooks/pre-commit .git/hooks/pre-commit
	@chmod +x ops/githooks/pre-commit
	@echo "installed .git/hooks/pre-commit -> ops/githooks/pre-commit"
