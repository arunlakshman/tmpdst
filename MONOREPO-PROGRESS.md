# Monorepo progress

T0.1 | done | - | baselines in build/baseline (all green) | needed kern.ipc.somaxconn=1024 on this Mac
T0.2 | done | fe260a1 | root make build green; goldens, cpsim300, kafka300, test counts equal baseline | Dockerfile of controlplane edited, untested (no Docker); go.work has a genproto replace; mise.toml created early (T0.3 must remove module copies)
T0.4 | done | 596d83b | dst/cmd/determinismcheck; verdicts same as old (dst, controlplane green) | -allow flag now required
T0.7 | done | 94e5ae4 (merge b47dc25) | Partition spec renamed Broker; speccheck+conformance tests green | commit signed Haiku
T0.3 | done | 5d62e0c (merge da9da30) | make/common.mk, one mise.toml, one ascii-check | three .golangci.yml kept separate (different panic-exclusion paths; merging would loosen rules)
Merged gate after T0.4+T0.7+T0.3 | green | da9da30 | build exit 0; cpsim300, kafka300, golden equal baseline |
T0.5 | done | 4919ee6 | shared/ with awsmetrics, httpconnections, runinfo; root make build green | no ldflags existed (only a comment); Dockerfile COPY shared untested
T0.6 | done | e789990 | one proto in controlplane/api; protos were equal; root make build + broker e2e green | architecture_test.go import rules adjusted (outside Touch, forced by move); broker go.mod edited by hand, check with GOWORK=off before release
T0.8 | done | 3e2b302 | check-module-deps.sh first in make build + guard test; build exit 0 | depguard rules added in dst/shared/controlplane, config accepted but a real hit not proven; shell guard is the enforced check
T0.9 | done | 942dfa8 | root README with Roadmap, module READMEs, doc paths, Podman wording | many old design docs still name old paths (listed as stale in the T0.9 report)
T0.10 | done with 1 gap | (this commit) | see below | make it NOT PASSED

P0 stage gate (2026-11-12):
- build exit 0; cpsim300, kafka300 and golden pins equal baseline
- controlplane make mutants 16/16 killed (after fixing ops/tools/mutate: it copied the module to a temp folder and broke the replace ../dst path; siblings are now linked)
- broker injected-bug and conformance tests green
- race: controlplane make test-race RACE_RUNS=3 ok; dst make test ok
- determinism: dst ok, controlplane ok, broker red (known, ISSUES.md)
- test count (go test ./... -list . | wc -l): dst 102, shared 21, controlplane 310, broker 480 = 913; baseline 938. Lower because duplicate tests were deleted (one copy of awsmetrics, httpconnections, runinfo; one determinismcheck test set)
- make it: NOT PASSED. Podman machine is up, but the dynamodb-local container did not start in 300s ("create container: context deadline exceeded"). Other packages ok. Likely Podman setup (2 GiB VM, image pull, testcontainers) - not checked further.

## Deferred

- `it` gate (cd controlplane && make it): NOT RUN (Docker daemon not reachable; docker is podman).

## User decisions

- 2026-11-12: "phase 1" means P0 only. Stop at the P0 gate and wait for the user.
- 2026-11-12: user raised kern.ipc.somaxconn to 1024 (httpconnections test needs it).
- controlplane Dockerfile (root build context) untested: NOT RUN (no Docker). Also .dockerignore no longer applies.
- Docs still name old repo paths (T0.9). broker/build/ci/determinism.workflow.yml is an unused leftover.
- Broker determinism gate (bash broker/ops/scripts/ci-determinism.sh) is RED and was red BEFORE the move too (a056a4b): internal/core/operation_runner.go imports context, not on the allow list. Not in `make build`. Out of scope (decision 5). Ask the user: add context to the allow list or change the code.
- Standalone `make -C broker build` needs controlplane generated *.pb.go: run root `make generate` first.
- Check each go.mod builds with GOWORK=off before release.
- 2026-11-12: user decision: the monorepo uses Podman, not Docker. 'it' gate stays NOT RUN until the podman machine is up (ask before starting it).
- 2026-11-12: user: broker determinism gate is NOT to be fixed now; recorded in ISSUES.md. User said to start the podman machine.
- 2026-11-12: user asked for a pull request for P0 to the tmpdst repo at the end (explicit OK to push branch mono/p0 only; no tags). User asked: never commit generated code.
