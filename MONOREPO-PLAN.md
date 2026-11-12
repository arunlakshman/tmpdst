# Monorepo plan: one repo for dst, shared, controlplane and broker

Audience: one orchestrator agent, and the subagents it hands tasks to.
Written: 2026-11-12. P0 runs this week. P1 runs next week.

A subagent reads sections 2, 4, 5 and its own task card. It does not need the rest.
The orchestrator reads the whole file.

Words used in this file:

- **module**: one Go module, with its own `go.mod`.
- **spec**: a small, fault-free program that shows the correct behavior of a service.
  Other names for it in the code: reference model, model.
- **subject**: the real or simulated code that we compare with a spec.
- **kernel**: the code that runs a simulation: one goroutine, one seeded random stream,
  one event queue on virtual time.
- **golden hash**: a fixed trace hash for a fixed seed. If the code still does the same
  thing, the hash does not move.
- **RE-PIN**: a task that is allowed to change golden hashes, in one commit of its own.
- **sealed sum type**: a Go interface with an unexported marker method, for example
  `type Action interface{ isAction() }`. Only the package that defines it can add cases.

---

## 1. Context: what we have today

Three git repos, each with its own `go.mod`:

| Repo (local path) | Module path today | Commit to copy | What it is |
|---|---|---|---|
| `/Users/arun.lakshman/tmp/dst` | `github.com/manifesterio/dst` | `bc02a62` | The DST library: `sim`, `eng`, `rng`, `sched`, `clock`, `trace`, `assert`, `fault`, `campaign`, `mutant`, `model`, `golden`, `check`, `realclock`, `syncclock`. About 2700 lines. |
| `/Users/arun.lakshman/tmp/mds-controlplane` | `github.com/manifesterio/mds-controlplane` | `31c86a3` | The replicated control plane. Uses dst (`sim`, `sched`, `rng`, `fault`, `assert`, `campaign`). Has its own simulator (`internal/verify/simulator`, about 4400 lines with `cmd/cpsim`) and its own spec (`internal/spec`) checked by `test/conformance` on rapid. |
| `/Users/arun.lakshman/tmp/tmpdst` | `github.com/manifesterio/mds` | `a056a4b` | The broker. It does NOT use dst. It has its own kernel (`internal/verify/simulator`, about 4400 lines), its own spec framework (`internal/verify/speccheck`, about 4100 lines) and its own copy of the control plane proto (`api/controlplane`). |

Known duplicates and differences (checked on 2026-11-12):

| Thing | Broker (tmpdst) | Control plane | dst | Notes |
|---|---|---|---|---|
| Machine contract | `internal/core` | `internal/runtime` | `sim`, `eng` | core and runtime are near copies. `clock.go`, `env.go`, `fnv.go`, `operation_runner.go` differ by a few lines. `action.go`, `event.go`, `controlplane.go` differ a lot. core has `fact.go`, `inbox.go`, `kafka.go`, `machine.go`, `policy.go` that runtime does not have. |
| Kernel | own (`internal/verify/simulator`: lanes, CountedRand, phases, timeline, cost model, tag tables) | dst `sim` plus its own env, wake-token queues | `sim` | Three kernels. |
| Spec framework | `speccheck/checker` (own generator, shrinker, comparator, observation, runsettings, injectedbug) | `test/conformance` on rapid, `ops/mutants.txt` + `ops/tools/mutate` | `model` (lockstep on rapid), `mutant`, `check`, `golden` | Three frameworks. |
| Control plane spec | `speccheck/controlplane.go` + `speccheck/groups.go` (651 lines, in broker `core` types) | `internal/spec` (in `contract`/`runtime` types) | - | Two copies of one spec, in different types. |
| Object store spec | `speccheck/objectstore.go` (in `core.IOCall` types) | - | - | |
| Partition spec | `speccheck/partition.go` | - | - | The user decided: the broker is the surface, so this becomes the Broker spec. |
| Control plane proto | `api/controlplane` (copy) | `internal/controlplane/api/controlplane` (has `options` annotations for the `mdswire` generator, plus `log/v1`) | - | Differ in `go_package` and option lines. Field level must be checked. |
| Leaf code | `internal/system/{awsmetrics,httpconnections,runinfo,fakeclock,realclock,s3store}` | same names | `realclock`, `syncclock` | `awsmetrics` identical. `httpconnections` 4 lines differ, `runinfo` 8 lines (only the ldflags path). `fakeclock`, `realclock`, `s3store` depend on core/runtime types, so they cannot move before the types move (P1). |
| determinismcheck | `ops/tools/determinismcheck` | same | same | Three diverged copies with different `allow.txt`. |
| Build | Makefile 155 lines, pre-commit hook runs `make build` | 132 lines | 38 lines | fmt-check, vet, lint, modernize, ascii-check, install-hooks are copied three times. |
| Dead code | `core` still has `LogAppend`, `LogRead`, `LogGetEnd`, `LogClaimEpoch`, `LogDeleteOld` | - | - | Left from before the control plane moved out. Used only in `core` and in the tag tables of the kernel. Removing them may move golden hashes, so it is a P1 RE-PIN item. |

Other facts that matter:

- Broker golden hashes: `internal/verify/simulator/kafka/golden_test.go` and `kafka/testdata/golden_hashes.txt`.
- Control plane golden hashes: `internal/verify/simulator/golden_test.go`. Last known green: hashes
  `aaf01c86 4032d6fb f1e59c13 0609269d 951ef86c`, 27 paths, mutants 16/16.
- The broker `make build` runs `e2e-kafka-cli`. That script finds the control plane in a sibling
  folder `../mds-controlplane` (override `MDS_CONTROLPLANE_DIR`). It needs Java and minio (mise).
- Generated files (`*.pb.go`, the control plane `*_gen.go`) are not in git. A new checkout or
  worktree must run the generate step first.
- Each repo has a pre-commit hook: `.git/hooks/pre-commit` is a symlink to
  `ops/scripts/githooks/pre-commit`, which runs `make build`. It is slow (about 8 minutes for the
  broker alone). Bypass only with `SKIP_BUILD_HOOK=1`, and only if the user says so.
- The determinism gate (`ops/scripts/ci-determinism.sh`) also checks that every third-party module
  used by a deterministic package has an audit record in `docs/dep-audits/<module>@<version>.md`.
- The control plane `go.mod` requires dst by the public pseudo-version
  `v0.0.0-20261106132358-bc02a6279445`, with no `replace`.
- Untracked files that are NOT part of any repo: tmpdst `.idea/` and `deploy/aws/`; mds-controlplane
  `docs/architecture-map.html` and `docs/story-*.html`. Do not commit them, do not delete them.

---

## 2. Decisions (the user made these; do not reopen them)

1. **One git repo.** It is the tmpdst repo (its remote is the `mds` repo). Four folders at the root,
   each its own Go module:

       dst/            github.com/manifesterio/mds/dst
       shared/         github.com/manifesterio/mds/shared
       controlplane/   github.com/manifesterio/mds/controlplane
       broker/         github.com/manifesterio/mds/broker

2. **Dependency order:** `dst <- shared <- controlplane <- broker`. A module may import only the
   modules to its left. `dst` has no product words (no Kafka, no S3, no topic, no broker).
3. **No git history is kept** for the copied repos. They come in as one commit whose message names
   the source repo and commit of each. The old repos stay as they are. Do not change, archive or
   push them.
4. **Root Makefile only calls the module Makefiles**, in the order dst, shared, controlplane,
   broker. The output is two binaries: one control plane binary, one broker binary.
5. **Scope is code organization, readability and maintenance. Nothing else.** No new features, no
   new coverage, no new fault kinds, no new checks. A gap stays a gap. A limitation stays a
   limitation.
6. **Specs:** one spec per service, owned by that service, in a package other modules can import
   (not under `internal/`). The S3 / object store spec and other shared outside-world specs go to
   `shared/`. The control plane spec lives only in `controlplane/`. The broker spec (today
   `Partition`) lives in `broker/`.
7. **Roadmap only, not now:** a network model, a disk / crash-state model, a linearizability
   checker. They get one line each in the root README Roadmap section. The existing log-replay
   oracle of the control plane is kept as it is.
8. **Type safety is a top goal.** The user wants the compiler to catch bugs, not the tests. Go
   generics complexity is fine. No `any` in product code paths where a type can say it. Sealed sum
   types with exhaustive switches checked by the `gochecksumtype` linter.
9. **Never push.** No push, no remote tags, no remote changes.

---

## 3. Orchestrator guide

### 3.1 Your job

You run the plan. You do not write product code yourself, except small merge fixes.

1. Read this whole file. Create `MONOREPO-PROGRESS.md` at the repo root if it is missing.
2. Find every task whose `Needs` are done and that is not started.
3. Start it with the Agent tool. Use the prompt template in section 6, and the `Model` and
   `Effort` of the card.
4. When a subagent reports, check the report (3.4). Merge its work.
5. Run the gate named on the card on the merged result. Write the result in the progress file.
6. At the end of a phase (or a P1 stage), run the stage gate before you start the next one.
7. Stop and ask the user when 3.5 says so. Otherwise keep going.
8. Tell the user in one short line when each task is done, and in a short summary when each
   stage is done: what changed, gate results, anything deferred.

The work is resumable. The progress file and `git log` say what is done. If your context is lost,
read both and go on from there.

### 3.2 Branches and worktrees

- P0 works on the local branch `mono/p0`, made from `toys3` in `/Users/arun.lakshman/tmp/tmpdst`.
- P1 works on `mono/p1`, made from `mono/p0` after the P0 gate is green.
- T0.1 and T0.2 run in the main checkout (they set up the repo). After T0.2, every task runs with
  `isolation: "worktree"` on top of the current work branch.
- Merge one task at a time. Run the card gate after each merge.
- Generated files are not in git. A new worktree must run `make generate` at the root first
  (after T0.3; before that, the module `make proto` / `make generate`).
- `build/` is not in git and not shared between worktrees. Baselines live in the main checkout at
  `/Users/arun.lakshman/tmp/tmpdst/build/baseline/`. Give subagents this absolute path.
- The pre-commit hook runs the full root `make build` on every commit. That is slow (expect 15
  minutes or more). Ask subagents to run the gates first and commit once at the end, not after
  every small step. Do not edit files in a worktree while its commit hook runs.

### 3.3 Running tasks at the same time

- Tasks with the same `Group` letter may run at the same time. Their `Touch` lists do not overlap.
- Tasks marked `Serial` run alone.
- This is a laptop with little compute. At most 3 subagents at a time, and at most 2 of them
  running a full `make build` at the same time.
- If two running tasks turn out to need the same file, let the first finish and restart the second
  on the merged result.

### 3.4 Check every report before you merge

1. The report has real gate output (the last lines), not a summary of it.
2. `git diff --stat` touches only the `Touch` list of the card. Anything else needs a reason.
3. Golden hashes did not move, unless the card says `RE-PIN`.
4. Every planted bug is still caught (control plane `make mutants`, broker injected-bug tests),
   where the card names it.
5. A deleted test is either named on the card, or the same bug is caught by a named test.
6. Nothing was reported as passing that did not run (see rule 4.6).
7. No new feature, check or fault kind slipped in (decision 5). Deleting code is the goal.

If a report fails a check, send it back to the same subagent with SendMessage. After two failed
tries, stop and ask the user.

### 3.5 Stop and ask the user if

- a golden hash moves in a task that is not `RE-PIN`, and the cause is not clear in 15 minutes;
- a `RE-PIN` task changes the set of assertion names, a scenario passes fewer seeds, or a planted
  bug is no longer caught;
- the two control plane specs, or the two proto files, disagree on a rule or a field (T0.6, T1.8);
- the two `s3store` copies behave differently (T1.6);
- a task needs a change in `controlplane/.../replica` or `controlplane/.../system/live` beyond the
  signature changes the card names;
- the dst v2 design (T1.1) is ready: the user approves it before any code is written;
- something would need a push, a tag, a remote change, or `SKIP_BUILD_HOOK`;
- a stage gate is red and the fix is not a small merge fix.

### 3.6 The progress file

`MONOREPO-PROGRESS.md` at the repo root has one line per task:

    T0.2 | done | <commit> | root make build green | note
    T1.4 | done | <commit> | make it NOT RUN (no Docker) | run it before release

Add a section `Deferred` for things not run or not done, and a section `User decisions` for
answers the user gave during the work. Commit the file with each merge.

---

## 4. Rules for everyone

1. **Decisions in section 2 are fixed.** Do not reopen them.
2. **Behavior stays the same.** Every task is a move, a rename, a merge of copies, or a deletion.
   If a change would make the system do something new, or check something new, do not do it.
   Write it in your report as an idea.
3. **Golden hashes stay**, except in `RE-PIN` tasks. A `RE-PIN` task updates the pins in a commit
   of its own, and the commit message says why the hash moved.
4. **The specs stay independent.** A spec shares types with the code, never logic. A spec never
   imports the package it is the spec of. The Kafka oracle of the broker simulation never imports
   any spec package (this keeps check C4 honest).
5. **No Docker in `make build`.** Tests that need containers use the `integration` build tag and run
   with `make it` in `controlplane/`.
6. **Never say a gate passed if it did not run.** If Docker, Java or minio is missing, the report
   says `<gate> NOT RUN (<reason>)`, and the orchestrator records it under `Deferred`.
7. **Never push.** See decision 9.
8. **Plain simple English** in names, comments, docs and test messages. The maintainers read
   English as a second language. Short direct sentences, no idioms, no cryptic abbreviations.
   Define a jargon word once. ASCII only (the `ascii-check` gate fails on anything else).
9. **One concern per commit**, but few commits (the hook is slow). Commit messages end with the
   Co-Authored-By line from the session.
10. **New tools go in `mise.toml`** in the same change. New third-party imports in a deterministic
    package need an audit record (`audit-go-dependency` skill).
11. **Test files move with their code.** The broker risk gate counts a package's coverage only from
    its own tests.
12. **Do not touch** the untracked files listed in section 1, or the three old repos.

---

## 5. Gates and baselines

| Name | Command (from the repo root unless said) | Notes |
|---|---|---|
| `build` | `make build` | Root build: runs dst, shared, controlplane, broker in order. Before T0.2 there is no root build; use each repo's own `make build`. |
| `golden` | part of `make build` (broker `kafka` golden test, control plane simulator golden test) | Must not move except in `RE-PIN` tasks. |
| `campaign` | `cd controlplane && go run ./cmd/cpsim -seeds 2000 -scenario all -report` and `cd broker && go run ./cmd/mdssimulator -scenario kafka -seeds 2000` | Stage gates. After T1.9 both run through the one campaign tool. |
| `mutants` | `cd controlplane && make mutants` and `cd broker && go test ./internal/verify/speccheck/... ./internal/broker -run 'Injected|Conformance' -count=1` | Every planted bug must be caught. After T1.12 the broker command changes; the card says to what. |
| `race` | `cd controlplane && make test-race RACE_RUNS=3` and `cd dst && make test` (dst tests already run with `-race`) | Stage gates. |
| `determinism` | each module's determinism gate (`make determinism` or `ops/scripts/ci-determinism.sh`) | Part of `make build` in dst and controlplane. Broker: `bash ops/scripts/ci-determinism.sh`. |
| `it` | `cd controlplane && make it` | Needs Docker. Never skips; if Docker is off, report NOT RUN. |
| `e2e` | part of broker `make build` (`e2e-kafka-cli`) | Needs Java and minio from mise. |

Stage gate = `build`, `campaign`, `mutants`, `race`, `determinism`, and `it` if Docker is on.

Baselines: `/Users/arun.lakshman/tmp/tmpdst/build/baseline/` (made by T0.1). Every RE-PIN task
compares against them.

---

## 6. Subagent prompt template

    You are doing task {ID} of MONOREPO-PLAN.md (at the repo root): {title}.
    Read sections 2, 4 and 5 of that file, and the card for {ID}. Then read the code you will touch.
    You are in your own git worktree on top of {branch}. Run `make generate` at the repo root first,
    because generated files are not in git.
    Touch only: {Touch}. If you need more, say why in your report.
    Behavior must not change (rule 4.2). Golden hashes must not move{unless RE-PIN}.
    Run the gates first, then commit locally, in as few commits as make sense. Do not push.
    The commit hook runs the full build and is slow; do not edit files while it runs.
    Run this gate before you report: {Gate}. Baselines: /Users/arun.lakshman/tmp/tmpdst/build/baseline/.
    Report:
      - what changed and what you deleted, with lines added and removed (git diff --stat);
      - the last 15 lines of each gate you ran, and the golden result;
      - the planted-bug result, if the card names it;
      - every item of the card's Definition of done, each marked DONE or NOT DONE with a reason;
      - anything you skipped and why, and anything you found that the plan got wrong.
    If a check is red, say so. Do not hide it. Never say a gate passed if it did not run.

Model choice: `haiku` for mechanical work (moves, renames, path fixes, docs), `sonnet` by default,
`opus` for design, for the dst v2 kernel, and for code where a mistake is hard to see.
Run the orchestrator itself on `opus`.

---

## 7. Target shape

### 7.1 At the end of P0

    /                         (repo root, no go.mod)
      go.work                 use ./dst ./shared ./controlplane ./broker
      Makefile                calls the module Makefiles, in order
      make/common.mk          fmt-check, vet, lint, modernize-check, modernize, ascii-check
      mise.toml               one copy, union of the tools of the three repos
      ops/githooks/pre-commit runs the root make build
      ops/scripts/ascii-check.sh
      README.md               what each module is, the dependency picture, Roadmap
      MONOREPO-PLAN.md, MONOREPO-PROGRESS.md
      bin/                    (not in git) mdscontrolplane, mdsbroker
      dst/                    the old dst repo; determinismcheck now at dst/cmd/determinismcheck
      shared/                 awsmetrics, httpconnections, runinfo
      controlplane/           the old mds-controlplane repo; proto now public at controlplane/api
      broker/                 the old tmpdst repo; uses controlplane/api, no proto copy;
                              Partition spec renamed to Broker spec

### 7.2 At the end of P1

    dst/
      kernel/        typed world: nodes, services, addresses, timers, lanes, phases,
                     incarnations, crash / restart / freeze, one rng, one queue, trace hash
      live/          the production run loop for the same machines (moved from controlplane)
      check/         Spec[Op, Res], Lockstep (on rapid), SelfCheck, log Replay (kept),
                     planted bugs, assertion catalog
      campaign/      seed runs, until-stable, report (text and JSON), golden pins
      rng/ sched/ clock/ trace/ assert/ fault/ realclock/ syncclock/   (kept)
      cmd/determinismcheck/
    shared/
      objectstore/   contract types (put, get, delete, list) as a sealed sum, the spec,
                     the S3 adapter (one copy)
      awsmetrics/ httpconnections/ runinfo/ clock adapters
    controlplane/
      api/           proto (public)
      contract/      plain value types of the control plane calls (public)
      spec/          THE control plane spec (public): topics, segments, groups, rebalance
      client/        the gRPC client the broker uses (was broker internal/cpclient)
      internal/...   everything else as today
      internal/verify/simulator   on dst v2
    broker/
      spec/          the Broker spec (was Partition)
      internal/verify/simulator   on dst v2; no own kernel; no hand-written fakes if T1.13 lands
      internal/...   no own copy of the control plane spec, proto, or client

Deleted at the end of P1: the broker kernel, `broker/internal/verify/speccheck/checker`
(generator, shrinker, comparator, observation, runsettings), the broker copy of the control plane
spec, dst `sim`, `eng` and `model` (merged into `kernel` and `check`), the duplicated parts of
broker `core` and control plane `runtime`, the copies of fakeclock, realclock and s3store, the
dead `Log*` actions in broker `core`.

---

## 8. Phase P0 (this week): mechanical moves, no behavior change

Every P0 task keeps every golden hash and every seed result exactly the same.

**T0.1 Freeze the baselines.**
Needs: none. Serial. Model: haiku. Effort: lower. Touch: `build/baseline/` in tmpdst only.

- Steps: in each of the three repos, as they are today, run:
  - dst: `make build`.
  - mds-controlplane: `make build`, `go run ./cmd/cpsim -seeds 300 -scenario all -report -json`,
    `make mutants`.
  - tmpdst: `make build`, `go run ./cmd/mdssimulator -scenario kafka -seeds 300`, and the
    injected-bug and conformance tests (section 5, `mutants`).
  - Save each output to `/Users/arun.lakshman/tmp/tmpdst/build/baseline/<repo>-<what>.txt`.
  - Copy the golden pin files and golden test files into `build/baseline/golden/`.
  - Write `build/baseline/README.txt`: the date, the three commits, whether Docker, Java and minio
    are present, and the test count of each repo (`go test ./... -list . | wc -l` is enough).
- Definition of done: every file above exists, every command is green, the README has the three
  commits. If any repo is not green today, stop and ask the user (the baseline must be green).
- Gate: the commands above.

**T0.2 Make the monorepo (the copy commit).**
Needs: T0.1. Serial. Model: sonnet. Effort: default. Touch: the whole repo, in the main checkout.

- Steps:
  1. `git switch -c mono/p0` from `toys3` in `/Users/arun.lakshman/tmp/tmpdst`.
  2. Move every tracked file into `broker/` with `git mv` (use `git ls-files` so untracked files
     like `.idea/` and `deploy/aws/` are not touched). `MONOREPO-PLAN.md` and
     `MONOREPO-PROGRESS.md` stay at the root.
  3. Copy only the tracked files of the other repos at the named commits:
     `git -C /Users/arun.lakshman/tmp/mds-controlplane archive 31c86a3 | tar -x -C controlplane`
     and the same for dst at `bc02a62` into `dst/`. (Untracked files are left out this way.)
  4. Remove per-repo things that make no sense in a sub-folder: each module's `.github/` (if any),
     and the per-repo `install-hooks` targets (the root installs the hook, step 8).
  5. Rename the module paths to `github.com/manifesterio/mds/{dst,controlplane,broker}` and fix
     every import (`gofmt -l` must be clean after). Also fix the `-X` ldflags paths of `runinfo`,
     `go_package` options in the `.proto` files, `buf` configs, and paths in the
     `ci-determinism.sh` scripts.
  6. Add `go.work` (`use ./dst ./controlplane ./broker`). In each `go.mod`, require the in-repo
     modules it needs with `v0.0.0` and add a `replace` to the relative path, so a module also
     builds alone and `go mod tidy` works. The control plane drops the dst pseudo-version.
  7. In each `ci-determinism.sh`, treat modules under `github.com/manifesterio/mds/` as first-party:
     they need no audit record. Delete the now-unused record
     `controlplane/docs/dep-audits/github.com/manifesterio/dst@v0.0.0-20261106132358-bc02a6279445.md`
     (or wherever it is; find it by name).
  8. A minimal root `Makefile` with `build`, `generate`, `test`, `install-hooks` that call
     `$(MAKE) -C dst ...`, `-C controlplane`, `-C broker` in that order. `build` ends by building
     `bin/mdscontrolplane` and `bin/mdsbroker`. Add `bin/` and `build/` to the root `.gitignore`.
     Move the hook to `ops/githooks/pre-commit` at the root (runs root `make build`, keeps the
     `SKIP_BUILD_HOOK` switch), and run `make install-hooks` so `.git/hooks/pre-commit` points to it.
  9. Point the broker e2e script at `../controlplane` by default (keep `MDS_CONTROLPLANE_DIR`).
  10. One commit. Message: "Copy dst, mds-controlplane and the broker into one repo", with a body
      that lists each source repo, its path and its commit (section 1 table), and says the module
      paths were renamed and nothing else changed.
- Must not: change any logic, any test, any golden pin, any doc text other than paths.
- Definition of done: root `make build` green; every golden hash equals the baseline; cpsim 300
  seeds and mdssimulator 300 seeds give the same lines as the baseline; test counts equal the
  baseline; `git status` shows only the untracked files from section 1; the hook runs the root build.
- Gate: `build`, `golden`, and the two 300-seed campaigns compared with the baseline.

**T0.3 One copy of the build rules and tools config.**
Needs: T0.2. Group A. Model: sonnet. Effort: lower.
Touch: root `Makefile`, `make/`, `mise.toml`, `.golangci.yml` files, `ops/scripts/`, the three
module Makefiles, module `ops/scripts/ascii-check.sh` and `ops/scripts/githooks/`.

- Steps:
  1. Write `make/common.mk` with `fmt-check`, `vet`, `lint`, `modernize-check`, `modernize`,
     `ascii-check`. Each module Makefile does `include ../make/common.mk` and deletes its own copy.
     Module-specific targets (proto, sim, sim-campaign, risk, mutants, it, fuzz, e2e) stay.
  2. One `ops/scripts/ascii-check.sh` at the root. Delete the three copies.
  3. One `mise.toml` at the root: the union of the three (go, golangci-lint, buf, java, minio).
     Same versions. Delete the module copies. The postinstall hook runs root `make install-hooks`.
  4. `.golangci.yml`: diff the three. If they are the same except paths, keep one at the root
     (golangci-lint finds a config in a parent folder). If they differ, keep one root file only if
     the merged rules give zero new findings in every module; otherwise keep the module files and
     write the differences in the report. Never weaken a rule to make a module pass.
  5. Root `make` targets: `build`, `generate`, `test`, `sim`, `sim-campaign`, `determinism`,
     `install-hooks`, each calling the module targets in order. A module that lacks a target is
     skipped by name, not by error-ignoring.
- Must not: drop any step of any module's `build` target. List every module `build` step before
  and after in the report; the lists must be equal.
- Definition of done: the three `build` step lists are unchanged; root `make build` green; no copy
  of the common targets is left in a module Makefile; one `mise.toml`, one ascii-check script.
- Gate: `build`, `golden`.

**T0.4 One determinismcheck tool.**
Needs: T0.2. Group A. Model: sonnet. Effort: default.
Touch: `dst/ops/tools/determinismcheck` (moves to `dst/cmd/determinismcheck`), the copies in
`controlplane/ops/tools/` and `broker/ops/tools/`, the three `ci-determinism.sh`, the
`audit-go-dependency` skill text if it names the old path.

- Steps:
  1. Diff the three copies of the tool code and its tests. Build one tool in
     `dst/cmd/determinismcheck` that has every check of all three (the union). The allow list is not
     built in: each module passes its own file with `-allow` (move each module's `allow.txt` next to
     its `ci-determinism.sh`, for example `controlplane/ops/determinism-allow.txt`).
  2. Each `ci-determinism.sh` runs `go run github.com/manifesterio/mds/dst/cmd/determinismcheck`.
  3. Delete the two other copies and their tests. Keep the testdata in dst.
- Must not: change any verdict. Before you delete, run all three old tools on their old package
  lists and save the output; after, run the new tool; the verdicts must be the same.
- Definition of done: one tool; each module's determinism gate green with the same verdicts; the
  union of the old tests passes on the new tool.
- Gate: `determinism` in all modules, `build`.

**T0.5 Create `shared/` with the leaf code that has no product types.**
Needs: T0.2. Group B (may run with T0.3 and T0.4 only if Touch does not meet; it touches
`go.work`, so start it after T0.3 merges if in doubt). Model: haiku. Effort: lower.
Touch: new `shared/`, `go.work`, `controlplane/internal/system/{awsmetrics,httpconnections,runinfo}`,
`broker/internal/system/{awsmetrics,httpconnections,runinfo}`, their importers, ldflags in Makefiles
and Dockerfiles.

- Steps:
  1. New module `shared/` (`github.com/manifesterio/mds/shared`) with its own `go.mod`, `Makefile`
     (includes `make/common.mk`, `build: fmt-check ascii-check vet lint modernize-check test`) and
     `README.md`. Add it to `go.work`, and to the root Makefile between dst and controlplane.
  2. Move `awsmetrics` (identical in both), `httpconnections` and `runinfo` to `shared/`.
     For the few lines that differ, read both and keep the behavior both callers need. If they
     truly need different behavior, stop and report.
  3. Fix the `-X ...runinfo.GitCommit=` ldflags paths in every Makefile, script and Dockerfile.
  4. `controlplane` and `broker` `go.mod`: require and replace `shared`.
- Must not: move `fakeclock`, `realclock` or `s3store` (they need core/runtime types; that is T1.6).
- Definition of done: one copy of each of the three packages; `go build` of both binaries prints the
  git commit (`-version` flag or startup log, whichever exists today); root `make build` green.
- Gate: `build`, `golden`.

**T0.6 One control plane proto (public).**
Needs: T0.2. Group C (start after T0.5 merges; both touch module `go.mod` files). Model: sonnet.
Effort: default.
Touch: `controlplane/internal/controlplane/api/` (moves to `controlplane/api/`), its buf config,
`controlplane` imports of it, the `protoc-gen-mdswire` plugin paths, `controlplane` and `broker`
`ci-determinism.sh` edge paths, `broker/api/`, `broker/internal/cpclient`, `broker/go.mod`, root
`Makefile` (generate order).

- Steps:
  1. First, compare the two `controlplane.proto` files (and `shared/v1`) with the option
     annotations (`(controlplane.options.v1.*)`, the `options.proto` import) and `go_package`
     removed. The rest must be equal: same messages, field names, numbers and types. If any field
     differs, STOP and show the diff to the orchestrator, who asks the user.
  2. Move `controlplane/internal/controlplane/api` to `controlplane/api` so other modules can import
     it. Fix the buf config, `go_package` options, the generator plugin, imports and the determinism
     edge path.
  3. Delete `broker/api/controlplane` and its buf config. `broker/internal/cpclient` imports
     `github.com/manifesterio/mds/controlplane/api/controlplane/v1` (and `shared/v1`).
     `broker/go.mod` requires and replaces `controlplane` (and through it `dst`, `shared`).
  4. Generation: the broker no longer generates control plane Go. Root `make generate` runs the
     control plane generate before the broker build.
- Must not: move `cpclient` (that is T1.7), change any message, change wire bytes.
- Definition of done: one copy of the proto; the broker builds against the control plane's
  generated package; e2e Kafka CLI green (it talks to the real control plane over gRPC, so it
  proves the wire matches); golden hashes unchanged.
- Gate: `build`, `golden`, `e2e`, `it` if Docker is on.

**T0.7 Rename the Partition spec to the Broker spec.**
Needs: T0.2. Group A. Model: haiku. Effort: lower.
Touch: `broker/internal/verify/speccheck/partition*.go` and its importers, `speccheck/README.md`,
`speccheck/doc.go`, `checker/suites/partition.go`, `checker/README.md`, broker docs that name it.

- Steps: rename the type `Partition` to `Broker` and the files `partition.go` to `broker.go`,
  `partition_test.go`, `partition_model_test.go` and `suites/partition.go` the same way. Update the
  doc comments: the broker is the surface we check; a partition is one part of its state, not a
  service. Keep the package where it is (it moves to `broker/spec` in T1.9).
- Must not: change any rule of the spec or any test logic.
- Definition of done: no exported name or file named `Partition` / `partition` is left for this
  spec (a field or variable that really means one partition may keep the word); tests green.
- Gate: `build`, `golden`, `mutants` (broker part).

**T0.8 Dependency guard.**
Needs: T0.5, T0.6. Group D. Model: haiku. Effort: lower.
Touch: new `ops/scripts/check-module-deps.sh`, root `Makefile`, module lint configs.

- Steps:
  1. A script that reads each `go.mod` and fails if a module requires a module to its right
     (dst requires none of shared/controlplane/broker; shared requires only dst; controlplane
     requires only dst and shared). Run it first in root `make build`.
  2. A depguard rule in each module's lint config with the same rule, so the editor shows it too.
  3. One test case for the script: a temp `go.mod` with a wrong require must make it fail.
- Definition of done: the script runs in `make build`; a wrong require fails it with a plain
  English message that names both modules.
- Gate: `build`.

**T0.9 READMEs, roadmap and doc paths.**
Needs: T0.3 to T0.8. Serial. Model: sonnet. Effort: lower.
Touch: root `README.md`, each module `README.md`, docs that name old repo paths or sibling folders.

- Steps:
  1. Root README: what the repo is, the four modules in one line each, the dependency picture
     (`dst <- shared <- controlplane <- broker`), how to build (`mise install`, `make build`), where
     the two binaries are, and a `Roadmap` section with exactly these lines: network model; disk and
     crash-state model; linearizability checker. One line each, marked "not started".
  2. Each module README: fix paths and commands for the new layout. Remove text that says the
     modules are separate repos or that one fetches dst from GitHub.
  3. Grep all docs for `mds-controlplane`, `../mds-controlplane`, `manifesterio/dst`,
     `manifesterio/mds/internal`, `tmpdst`. Fix paths. Do not rewrite old design docs; a path fix
     only. List any doc that is clearly stale in the report; do not delete it (ask the user).
- Definition of done: the greps above return only historical mentions that you listed in the
  report; the root README has the Roadmap section.
- Gate: `build` (ascii-check).

**T0.10 P0 stage gate.**
Needs: all T0. Serial. Model: haiku (runner) and the orchestrator checks. Effort: lower.

- Run the stage gate (section 5). Compare `campaign` at 300 seeds with the T0.1 baseline line by
  line, and the golden pins file by file.
- Definition of done: every gate green or NOT RUN with a reason in `Deferred`; goldens equal;
  campaign lines equal; test count equal or higher (higher only from T0.8). Then tell the user P0 is
  done, with the summary, and wait for the user before starting P1.

---

## 9. Phase P1 (next week): typed dst v2, one toolkit, both simulations on it

P1 is large and hard. It is split into four stages. Each stage ends green. If the week runs out,
stop at the end of a stage, never in the middle.

- Stage P1a: dst v2 (design, kernel, check, campaign). No product code changes.
- Stage P1b: types and ownership (machine contract, shared object store, control plane public
  surface, one control plane spec, broker spec package).
- Stage P1c: both simulations move to dst v2 (RE-PIN).
- Stage P1d: spec-check toolkit swap, optional Faulty(spec), deletions, docs.

The guiding rule for dst v2: **it holds what the existing kernels already do, nothing more.** Every
feature of the broker kernel and of dst `sim` that some scenario uses must have a place in v2. No
new fault kinds, no network model, no disk model.

### Stage P1a: dst v2

**T1.1 Design dst v2 (design only, no code).**
Needs: P0 done and user go-ahead. Serial. Model: opus. Effort: higher.
Touch: new `dst/docs/v2-design.md` only.

- Context to read first: `dst/sim`, `dst/eng`, `dst/model`, `dst/check`, `dst/campaign`;
  the broker kernel (`broker/internal/verify/simulator/*.go`: `kernel.go`, `engine.go`, `roles.go`,
  `run.go`, `runner.go`, `scheduler.go`, `schedimpl.go`, `rand.go`, `randimpl.go`, `inboximpl.go`,
  `cost.go`, `tags.go`, `timeline.go`, `view.go`, `assert.go`); `broker/internal/core`;
  `controlplane/internal/runtime`; `controlplane/internal/system/live`; the control plane simulator
  (`controlplane/internal/verify/simulator`, especially `node.go`, `groups.go` wake tokens,
  `services.go`, `env.go`); `controlplane/cmd/cpsim` (`campaign.go`, `report.go`).
- The design must answer, with Go signatures:
  1. **Machine contract.** Keep sans-IO: a machine's `Step` takes one typed input and returns typed
     outputs as plain data, so the production run loop (`live`) can run the same machine with real
     IO. Starting proposal (refine it, or explain why not):

         type Machine[In, Out any] interface {
             Step(now Time, in In) []Out
             StateHash() uint64
         }
         // In and Out are sealed sum types owned by the product.

  2. **Typed routing.** The world must not hold `any` in product paths, and no `ErrNotMine` chain.
     Starting proposal: the product gives a typed router per machine,
     `Route[Out any] func(r *Router, from Addr, out Out)`, whose body is an exhaustive type switch
     (checked by `gochecksumtype`). The router calls typed kernel functions:

         func Send[In any](r *Router, to Addr[In], msg In)
         func SetTimer[In any](r *Router, self Addr[In], id TimerID, after Duration, msg In)
         func Call[Req, Resp, In any](r *Router, svc ServiceAddr[Req, Resp], req Req,
             reply func(Result[Resp]) In, caller Addr[In])

     `Addr[In]` values are made only by the kernel at spawn time, so sending a wrong message type to
     a node does not compile. Go methods cannot have type parameters, so these are package funcs.
     Type erasure (`any`) is allowed only inside the kernel, in one file, and the design names it.
  3. **Services** (today: broker FakeService engines, dst Engine): typed
     `Service[Req, Resp]` with state hash, issue-time fault draws, and "crash of a caller" behavior
     (effects already applied stand; replies to a dead incarnation are dropped).
  4. **Ordering:** one queue ordered by (time, seeded tie, sequence), plus FIFO lanes per key (the
     broker kernel has lanes; the control plane simulator fakes them with wake tokens). Lane keys
     are typed values, not strings, if that is possible without `any`.
  5. **Incarnations, crash, restart, exit, freeze, attrition, cost model, phases** (Faults,
     Recovering, Recover / Settle), `OnPhase`, `Quiescent`. Map each to v2.
  6. **Randomness:** one SplitMix64 stream with call counting and `Derive` (broker CountedRand).
  7. **Trace:** the trace hash, the tag tables (broker `tags.go`), the timeline of facts. Say how
     a typed message gets its tag without a big hand table if possible (for example a `Tag()`
     method on the sealed sum).
  8. **Assertions and buggify sites as a catalog:** declared as package-level values
     (`var leaderElected = assert.Sometimes("...")`), so a campaign can list "never reached" and
     "never fired" without the hand list `expectedReachable` in `cmd/cpsim/campaign.go`.
  9. **The spec toolkit API** (`dst/check`): `Spec[Op, Res]`, `Lockstep` on rapid with reboot and
     dirty reboot, `SelfCheck` (spec against a naive model), log `Replay` (kept), planted bugs.
  10. **Campaign API** (`dst/campaign`): scenario registry, seed ranges, workers, until-stable,
      replay one seed, report text and JSON, golden pins, determinism canary (run a seed twice).
  11. **A mapping table**: one row for every exported thing in the broker kernel, in dst `sim`/`eng`,
      and in the control plane simulator host code, with its v2 place, or "deleted because ...".
      Nothing may be missing from the table.
  12. **A migration order** and which golden hashes will move in which task.
- Must not: add a feature from the Roadmap; change product code.
- Definition of done: the design file exists; the mapping table is complete (the orchestrator
  checks this by listing exported names with `go doc -all` in each kernel package); the user has
  read and approved it. STOP and ask the user after this task.
- Gate: none (review by the user).

**T1.2 Build the dst v2 kernel.**
Needs: T1.1 approved. Serial. Model: opus. Effort: higher.
Touch: new packages under `dst/` as named in the design (for example `dst/kernel`), `dst/examples`,
`dst/.golangci.yml` (enable `gochecksumtype`), `dst/README.md`.

- Steps:
  1. Write the kernel as designed. Keep old `dst/sim` and `dst/eng` in place for now; both products
     still use them or their own kernel until P1c.
  2. Port the kernel tests: the behavior tested by `broker/internal/verify/simulator/kernel_test.go`,
     `sched_test.go`, `rand_test.go`, `tags_test.go` and `dst/sim` tests must have a matching v2
     test. Keep a table in the report: old test -> new test.
  3. Port `dst/examples/kv` and `dst/examples/kvchaos` to v2 (new copies; the old ones go in T1.14).
  4. Enable `gochecksumtype` in the lint config of every module (it is in golangci-lint; record it
     in nothing else, mise already pins golangci-lint).
  5. Determinism gate covers the new packages.
- Must not: import anything product-specific; use `any` outside the one kernel file the design
  names; add features not in the design.
- Definition of done: every row of the T1.1 mapping table that belongs to the kernel exists; ported
  tests green with `-race`; both examples run the same seed twice to the same hash; `gochecksumtype`
  is on and green; `go doc` of each new package reads well in simple English.
- Gate: `cd dst && make build`, root `build`.

**T1.3 Build `dst/check` (one spec toolkit).**
Needs: T1.1 approved. Group E (may run with T1.2; separate packages). Model: opus. Effort: default.
Touch: `dst/check/` (extend), `dst/model` (merge into check; keep the old package until T1.14),
`dst/mutant`.

- Steps:
  1. `Spec[Op, Res]` interface: `Apply(Op) Res` and `StateHash() uint64`.
  2. `Lockstep`: draws an op list with rapid, runs each op on the spec and on the subject, compares
     results and, where both have one, the state hash. Has reboot and dirty-reboot ops (the control
     plane `test/conformance` `Target` has `Reboot`, `DirtyReboot(keep)`, `Checkpoint`). Shrinking is
     rapid's. Ops are generated simplest first (`dst/model` `SimplestFirst`).
  3. `SelfCheck`: compares a spec with a naive model on random ops (today:
     `controlplane_model_test.go`, `partition_model_test.go` in the broker).
  4. Keep `Replay`, `NoHole`, `Holders`, `Prefixes`, `Monotonic`, `Converged` as they are (the
     control plane log-replay oracle uses them).
  5. Planted bugs: one way to name a bug, run a test with it, and require it to be caught within a
     seed budget (`mutant.Require`, `RequireClean`). The control plane text-replacement mutants
     (`ops/mutants.txt`, `ops/tools/mutate`) stay as they are; they are a different tool for real
     code. The broker `injectedbug` wrappers will use this API in T1.12.
  6. A replay message on failure: the seed, the shrunk op list as Go code, and the command to run it
     again (today the broker `runsettings` and the control plane README both do this).
- Definition of done: unit tests for each part, including a test that a planted bug in a tiny
  example spec is caught and shrunk to at most 3 ops; doc comments in simple English.
- Gate: `cd dst && make build`.

**T1.4 Build `dst/campaign` v2 (one campaign tool and report).**
Needs: T1.1 approved. Group E. Model: sonnet. Effort: higher.
Touch: `dst/campaign/`, new `dst/cmd` helper package if the design says so.

- Steps:
  1. Move the report logic of `controlplane/cmd/cpsim/report.go` into `dst/campaign`: counts, failed
     runs with replay commands, every assertion by name and kind with checks and passes, never
     reached, buggify sites with asked / on / calls / fired, never fired, the coverage curve, JSON.
  2. Until-stable mode (`-until-stable`, `-stable-after`, `-max-seeds`) as in cpsim.
  3. The broker runner features: run every Nth seed twice to prove determinism
     (`CheckDeterminism`), and the environment variables `MDS_SEEDS`, `MDS_SEED`, `MDS_CAMPAIGN`.
     Keep both the flags and the variables; one place parses them.
  4. A scenario registry: a product registers named scenarios; one helper builds a `main` with the
     standard flags, so each product's `cmd/...sim` is a few lines.
  5. "Never reached" and "never fired" come from the declared catalog (T1.1 point 8), not from a
     hand list.
- Must not: change what the report says about a run. The text format may change only where the two
  old formats differ; keep the cpsim format where they differ, and list the differences.
- Definition of done: tests for the report (port `cmd/cpsim/report_test.go`), for until-stable, and
  for the determinism re-run; a tiny example scenario in `dst/examples` runs through it.
- Gate: `cd dst && make build`.

**T1.5 Stage P1a gate.** Needs: T1.2, T1.3, T1.4. Serial. Model: haiku.
Run the stage gate. Product goldens must not move (no product code changed). Tell the user.

### Stage P1b: types and ownership

**T1.6 One machine contract; base types from dst.**
Needs: T1.5. Serial. Model: opus. Effort: higher.
Touch: `broker/internal/core`, `controlplane/internal/runtime`, `dst` (base types if the design put
them there), and every importer (mostly import and type-name changes).

- Steps:
  1. The product-free parts of core and runtime (time types `NodeTime`, `RealTime`, `Duration`; the
     `Clock` interface; `fnv` hashing; `RequestID`, `NodeInstance`, boot numbers; `Environment`;
     the `Machine` contract) come from dst v2, as the design says. Delete them from core and runtime.
  2. What stays in core and runtime is only each product's own messages, as sealed sum types.
  3. Delete the dead `Log*` actions from broker `core` and from the broker tag tables. This may move
     broker golden hashes: if it does, it is a RE-PIN in a commit of its own (only for this reason).
  4. `controlplane/.../replica` and `controlplane/internal/system/live` may change only by the
     type and signature changes this task forces. List every such line in the report.
- Must not: change any rule of any machine.
- Definition of done: no type is defined twice across core, runtime and dst; goldens unchanged
  (except a documented Log* RE-PIN); both 300-seed campaigns green.
- Gate: `build`, `golden`, `campaign` (300 seeds), `mutants`.

**T1.7 `shared/objectstore`: contract, spec and one S3 adapter.**
Needs: T1.6. Group F. Model: sonnet. Effort: higher.
Touch: new `shared/objectstore/`, `shared/clock` (or as the design names it), broker
`internal/core` object store actions, `broker/internal/verify/speccheck/objectstore*.go`, both
`internal/system/{s3store,fakeclock,realclock}`, their importers.

- Steps:
  1. The object store calls (put, get a byte range, delete, list) and their results become one
     sealed sum type in `shared/objectstore`. The broker `core` actions use or wrap them (the design
     says which; a broker action may hold an `objectstore.Call`).
  2. Move the ObjectStore spec to `shared/objectstore/spec.go` (with its tests and its sorted-slice
     self-check).
  3. Merge the two `s3store` copies into one `shared/objectstore/s3`. First diff them (about 70
     lines differ in `store.go` and `classify.go`). If a difference is only types, merge. If it is
     behavior (error classes, retries, paging), STOP and report; the user decides. Keep every test of
     both copies (`dial_test`, `fakes3_test`, `faults_integration_test`, `main_integration_test`,
     `conform_test`).
  4. `fakeclock` and `realclock`: both products' versions wrap or copy dst `syncclock`/`realclock`.
     Keep one adapter (in `shared`, or none at all if the dst types now fit directly).
- Definition of done: one object store spec, one S3 adapter, one clock adapter; check C3 (S3 adapter
  against the spec) still runs (it needs `MDS_S3_ENDPOINT`; NOT RUN is allowed with the reason);
  goldens unchanged.
- Gate: `build`, `golden`, `it` if Docker is on.

**T1.8 Control plane public surface: contract, spec, client.**
Needs: T1.6. Group F. Model: sonnet. Effort: higher.
Touch: `controlplane/internal/controlplane/contract` (to `controlplane/contract`),
`controlplane/internal/spec` (to `controlplane/spec`), `broker/internal/cpclient` (to
`controlplane/client`), broker `core` control plane call types, their importers, the determinism
gate lists.

- Steps:
  1. Move `contract` and `spec` out of `internal/` so the broker can import them. Their own imports
     must then also be public or from dst; if `spec` needs an internal package, move only what it
     needs (types), never logic.
  2. Move `cpclient` to `controlplane/client`. Its API speaks `controlplane/contract` types.
  3. Broker `core` control plane call and reply types: replace the broker's copies with
     `controlplane/contract` types (wrapped in a broker action if the sealed sum needs it). Delete
     the broker's copies (`core/controlplane.go`, about 340 lines, differs a lot from runtime's).
- Must not: change the wire format or any client behavior (timeouts, retries, error mapping).
- Definition of done: the broker has no type that copies a control plane contract type; the e2e
  test and the broker conformance tests are green; goldens unchanged.
- Gate: `build`, `golden`, `e2e`, `mutants`.

**T1.9 One control plane spec; the broker spec package.**
Needs: T1.7, T1.8. Serial. Model: opus. Effort: higher.
Touch: `broker/internal/verify/speccheck/{controlplane,groups,snapshot,hash}*.go`,
`speccheck/specsystem`, `checker/suites/controlplane*.go`, the broker spec (to `broker/spec`), C1 and
C4 tests, `controlplane/spec`.

- Steps:
  1. FIRST, write down every rule of the broker copy (`speccheck/controlplane.go`, `groups.go`) next
     to the matching rule of `controlplane/spec` (`spec.go`, `groups.go`). The doc comments list the
     rules, so this is a comment-to-comment compare, then a code check. Put the table in the report.
     If any rule differs, STOP and ask the user which one is right. Do not choose.
  2. Delete the broker copy. `specsystem` (production code on top of the specs), the C1 broker
     conformance test, and the C4 Kafka-oracle test use `controlplane/spec` through
     `controlplane/contract` types.
  3. Move the Broker spec (renamed in T0.7) to `broker/spec`, with its tests.
  4. The Kafka oracle (`broker/internal/verify/simulator/kafka`) must still import no spec package.
     Add a depguard rule for it.
- Definition of done: one control plane spec in the repo; the broker spec in `broker/spec`; checks
  C1 and C4 green, with every injected bug still caught; goldens unchanged.
- Gate: `build`, `golden`, `mutants`.

**T1.10 Stage P1b gate.** Needs: T1.6 to T1.9. Serial. Model: haiku.
Stage gate with 2000-seed campaigns. Tell the user.

### Stage P1c: both simulations on dst v2 (RE-PIN)

Acceptance rule for every RE-PIN task in this stage:

- the same scenarios exist, with the same names;
- the same seed ranges pass (2000 seeds per scenario, and 300 seeds compared with the baseline);
- the set of assertion names is the same as before, and every one that had checks before has checks
  now (list the counts before and after side by side);
- every buggify site that fired before fires now;
- every planted bug is still caught;
- the determinism canary passes (a seed run twice gives the same hash);
- the new golden pins are written in one commit of their own, whose message says "RE-PIN: kernel
  moved to dst v2" and lists the old and new hashes.

**T1.11 Move the control plane simulation to dst v2.** RE-PIN.
Needs: T1.10. Serial. Model: opus. Effort: higher.
Touch: `controlplane/internal/verify/simulator`, `controlplane/cmd/cpsim`,
`controlplane/internal/controlplane/{replica,domain/rebalance,server}` (only the signature changes
forced by the new machine contract), `controlplane/internal/system/live` (moves to `dst/live` if the
design says so; same behavior).

- Steps:
  1. Replicas and rebalance machines become v2 machines. The host code in `node.go` and `groups.go`
     uses typed addresses and typed routers.
  2. The wake-token queues in `groups.go` become kernel FIFO lanes.
  3. `cmd/cpsim` becomes a thin main over `dst/campaign`. `expectedReachable` is deleted; the catalog
     gives the same list (prove it: print both before deleting and compare).
  4. The log-replay oracle (`oracle.go`) is kept as it is; only its types change.
  5. `live` runs the same machines in production. The control plane binary, `make it`, and the
     kind/helm deploy (if the user asks to run it) must behave the same.
- Definition of done: the acceptance rule above; `make it` green (or NOT RUN with reason); the
  control plane no longer imports dst `sim` or `eng`.
- Gate: stage gate.

**T1.12 Move the broker simulation to dst v2.** RE-PIN.
Needs: T1.10. Serial (do not run with T1.11; both are large). Model: opus. Effort: higher.
Touch: `broker/internal/verify/simulator/**`, `broker/cmd/mdssimulator`, `broker/internal/core`
(machine side), `broker/internal/broker` (only forced signature changes),
`broker/internal/system/production` (the production run loop: it should become `dst/live` use if the
design says so).

This is the biggest task. The orchestrator may split it into these sub-tasks, run one after the
other, each green before the next:

- T1.12a: the fake services (`fakeservices/fakeobjectstore`, `fakecontrolplane`, `fakelogservice`,
  `fakenetwork`, `internal/fakecompletions`) become v2 services. Same fault draws in the same order.
- T1.12b: the broker machine, the Kafka workload, producers, consumers and invariants
  (`kafka/*.go`) run on v2. The Kafka oracle keeps its independence.
- T1.12c: `cmd/mdssimulator` becomes a thin main over `dst/campaign`. `echo` scenario ported.
- T1.12d: delete the old broker kernel (`kernel.go`, `engine.go`, `roles.go`, `run.go`,
  `runner.go`, scheduler, rand, inbox, cost, tags, timeline, view, assert files and their tests,
  `internal/campaign`, `internal/session`), once every kernel test has a v2 twin (T1.2 table).

- Definition of done: the acceptance rule above; the broker has no kernel of its own; the e2e test
  green; `production` runs the broker the same way as before (the e2e test proves the main path).
- Gate: stage gate.

**T1.13 Stage P1c gate.** Needs: T1.11, T1.12. Serial. Model: haiku.
Stage gate with 2000-seed campaigns for every scenario, the side-by-side assertion counts, and
`make test-race RACE_RUNS=3`. Tell the user, with the old and new golden hashes.

### Stage P1d: toolkit swap, optional Faulty(spec), cleanup

**T1.14 Broker spec checks on `dst/check`.**
Needs: T1.13. Serial. Model: sonnet. Effort: higher.
Touch: `broker/internal/verify/speccheck/checker/**`, the C1, C2, C3 tests
(`broker/internal/broker/conformance_test.go`, `harness_test.go`,
`fakeservices/conformance_test.go`, `shared/objectstore/s3/conform_test.go`), the injected bugs.

- Steps:
  1. C1, C2 and C3 run through `dst/check.Lockstep`. The generator profiles become rapid generators
     with the same weights. Every interesting case the old coverage tests counted
     (`TestControlPlaneCoverage` and friends) is still counted, and still must be non-zero.
  2. The injected bugs (`checker/injectedbug`) use the `dst/check` planted-bug API. Every one is
     still caught.
  3. Delete `checker/comparator.go`, `generator/`, `observation/`, `runsettings/`, `rand.go` and
     their tests, once their behavior has a home. List every deleted test and the test that now
     catches its bug.
  4. Update the `mutants` gate command in section 5 of this file.
- Must not: lower any count, weaken any check, or drop any injected bug.
- Definition of done: the list in step 3 is complete; every injected bug caught; coverage counts
  non-zero; the `speccheck/checker` tree is gone (or only a small product adapter is left, named in
  the report).
- Gate: `build`, `mutants`.

**T1.15 (Stretch) Faulty(spec) replaces the hand-written fakes.** RE-PIN.
Needs: T1.14. Serial. Model: opus. Effort: higher. Do it only if the week has time. It slips first.
Touch: `dst/check` or `dst/kernel` (the `Faulty` wrapper), `broker/internal/verify/simulator/fakeservices`.

- Steps: write `Faulty(spec, faults)` that turns a fault-free spec into a v2 service with exactly
  today's fault kinds of each fake (error with nothing applied, timeout with nothing applied,
  applied but reply lost, slow reply, outage window; whatever each fake has today, and nothing
  else). Replace `fakeobjectstore` and `fakecontrolplane` with it. Check C2 (fakes against specs)
  becomes unnecessary; delete it and say so.
- Must not: add a fault kind. Before you start, list every fault kind of each fake with its file
  and line; after, show where each one lives in `Faulty`.
- Definition of done: the P1c acceptance rule; the two fakes are gone.
- Gate: stage gate.

**T1.16 Delete the old dst packages and finish the docs.**
Needs: T1.14 (and T1.15 if it ran). Serial. Model: sonnet. Effort: default.
Touch: `dst/sim`, `dst/eng`, `dst/model`, old examples, every README, `docs/` paths,
`dst/docs/lessons.md`, the "Stay out" section of `dst/README.md`.

- Steps:
  1. Delete `dst/sim`, `dst/eng`, `dst/model` and the old examples, once nothing imports them.
  2. Update `dst/README.md`: the "Stay out" list now allows generic spec machinery (`check`), but
     still no product words and no product specs.
  3. Update module READMEs, `controlplane/docs/simulation.md`, `broker/internal/verify/speccheck/README.md`
     (or its new place) for the new names and commands.
  4. Write `dst/docs/v2.md`: a short guide in simple English with one small example for each part.
- Definition of done: nothing imports the deleted packages; greps for `sim.Process`, `eng.Engine`,
  `ErrNotMine`, `speccheck/checker` return nothing outside history notes.
- Gate: `build`.

**T1.17 P1 final gate.** Needs: all T1. Serial. Model: haiku runner, orchestrator checks.
Stage gate, plus `cd controlplane && make test-race RACE_RUNS=5`, 5000-seed campaigns with
`-report`, `make it` with Docker on, and a fresh clone `make build`. Record line counts before
(baseline) and after, per module, in the progress file. Tell the user.

---

## 10. Roadmap (out of scope for P0 and P1)

These go into the root README Roadmap section as one line each. Do not build them now.

- Network model (links, partitions as a kernel feature).
- Disk and crash-state model (what a crash leaves on disk).
- Linearizability checker (WGL style, using a spec as the sequential model).

## 11. Traps found while writing this plan

- `go mod tidy` ignores `go.work`. Without a `replace` per in-repo module it tries to download
  `github.com/manifesterio/mds/dst` and fails. Keep the `replace` lines.
- The dependency audit step of `ci-determinism.sh` prints `module@version` for every non-main
  module. In-repo modules have no real version; skip `github.com/manifesterio/mds/*` there.
- The control plane `api` is under `internal/`. Go does not let another module import an
  `internal/` package. T0.6 moves it out.
- A sealed sum type cannot get a new case from another package (the marker method is unexported).
  Moving `ObjectPut` and friends out of broker `core` needs a wrapper case or a new sum in the new
  package. T1.1 must decide this.
- Go methods cannot have type parameters. Typed `Send`, `Call` and `SetTimer` must be package
  functions.
- The broker kernel tag tables (`tags.go`) feed the trace hash. Any change to them moves golden
  hashes. That is why the dead `Log*` removal is a RE-PIN.
- The broker `make build` e2e step looks for the control plane in a sibling folder. T0.2 fixes it.
- After T0.2 moves the hook script, the old `.git/hooks/pre-commit` symlink points to nothing. Run
  root `make install-hooks` in the same task.
