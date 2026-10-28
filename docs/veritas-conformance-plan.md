# Plan: spec-based (veritas) conformance, redesigned as if built today

Status: PROPOSAL for review. Nothing in this plan has been executed.
Scope: `internal/verify/veritas/**` and every test that runs a veritas check.
Delivery: nine work packages run by subagents, integrated and verified by one
agent, landed as **one commit** on branch `toys3` (see section 9).

---

## 1. Starting point (what I found, so the plan is honest)

The lockstep machinery already exists. This plan is a redesign-and-consolidate,
not a greenfield build.

Already good (keep):
- `veritastest.Check[S, Op]`: generic runner with seeded generation, a `Step`
  that compares impl vs spec, an `Invariant` after every step, deterministic
  shrinking (drop chunks, `Shrink`, `ShrinkAll`) and a pasteable Go-literal
  report. Panics count as failures.
- Three small specs (`ObjectStore`, `ControlPlane`, `Partition`) with rules
  written in the doc comments and an incremental `StateHash`.
- Checks C1-C4 wired (decision 21), planted bugs (called mutants in the code) for the broker, planted-bug tests
  that prove the shrinker works for the object store and control plane.

Gaps observed (each is a work item below). Items marked (verify) were read
only partly and Phase 0 must confirm them before anyone acts on them:

| # | Gap | Evidence |
|---|-----|----------|
| G1 | Generators are copy-pasted. Object-store ops are generated separately in `sim/engines/conformance_test.go`, `sys/s3store/conform_test.go` and (verify) `veritas/objectstore_test.go`. Control-plane ops are generated in `sim/engines`, `controlplane/cptest/conform_test.go`, `veritas/controlplane_test.go`, `applier/applier_test.go`. (`machine/lockstep_test.go` uses 6 fixed commands and has no generator: W0 checked.) The "same key, same shape, replayed" trick (`len(shape)*7+...*131+variant`) is duplicated. | grep + reads |
| G2 | Results are compared as strings: `fmt.Sprintf("%+v", completion)`, `%v %q`. nil vs empty slices are papered over by comments ("%+v prints nil and empty slices alike"). A real difference can hide, and a failure message is hard to read. | `engines` `render`, `s3store` `step`, `cptest` `show` |
| G3 | Seed control is not wired. `Options.Seed`, `Options.Stop` (wall-clock campaigns) and `Options.Replay` exist, but every caller passes `Seeds: N, Seed: -1` and no `Replay`, so a failure prints no replay command and replaying one seed means editing the test. | grep: no flag/env use outside `MANIFESTER_S3_ENDPOINT` |
| G4 | Planted-bug coverage is uneven. The broker has `Bug*` mutants and a test that they are caught. The machine has 4 (`logmutants_test.go`); the broker has 9 `Bug` values but its conformance test catches only `BugWrongOffset` and `BugServeCorrupt`. The applier has none of its own (W0 corrected this). C2 (`sim/engines`), C3 (`s3store`, `cptest`) and the `machine` lockstep have none (grep for Bug/Mutant finds nothing), so nothing proves those suites can go red. | grep |
| G5 | C2 runs with faults off only. Nothing checks that, under injected faults, a completion is either the spec's answer or an allowed fault, with the right `Outcome` (`MaybeApplied`). | `engines/conformance_test.go` header |
| G6 | `TestConformanceLogEngine` is `t.Skip`; its C2 moved to `machine` lockstep. The skip is a dangling promise. | same file |
| G7 | Three SplitMix64 copies (`veritastest.Rand`, `sim/randimpl.go`, `broker/harness_test.go`). `sim` and `veritastest` each have a reference-vector test; the broker copy has none, and no test ties the copies together. | grep |
| G8 | Naming that confuses readers: `TB`, `Check`, "lockstep" (see section 4). | your feedback |
| G9 | Stale docs: `veritas/doc.go` still says C3 targets "the control-plane HTTP client" (it is gRPC now); `MemKV` says "production store is boltkv"; the ControlPlane comment says CAS-on-base retry is how real stores work. | reads |
| G10 | Specs are validated unevenly. ObjectStore has a sorted-slice model; ControlPlane has invariants but no independent model; Partition has a 43-line test. Who checks the answer key? | reads |
| G11 | Generator quality is only measured in `cptest` (`TestGeneratorCoverage`). Elsewhere nobody knows if a generator ever produces a replay, a conflict, an invalid batch or a `MaxBytes` cut. | grep |

---

## 2. If I were building this from scratch: the design

Principles, in priority order:
1. **One spec per outside dependency, tiny and boring** (exists).
2. **Observation, not strings.** Every subject returns a normalized
   `Observation` (error code, outcome, payload as plain comparable values).
   Comparison is `==`/`reflect.DeepEqual` on that type, rendered only for
   failure messages.
3. **One generator per spec**, parameterized by a named *profile*, shared by all
   layers. A generator is judged by a *coverage test* that lists which
   interesting cases it reaches.
4. **One suite per spec, many subjects.** `ObjectStoreSuite(t, newSubject)`,
   `ControlPlaneSuite(t, newSubject)`. Spec, sim engine, S3 adapter, applier
   (mem and Pebble), memstore, gRPC client, machine are all just factories.
5. **Every suite proves it can fail**: a list of planted bugs, all must be
   caught. A suite with no planted-bug test is treated as untested.
6. **Reproducible by construction**: a failure prints seed, shrunk literal and
   the exact command to replay it. Seeds, replay and campaign length come from
   env vars in one place.
7. **Judge independence** (C4): the Kafka oracle never imports veritas. Keep.
8. **Determinism rule**: `veritas` and `veritastest` stay free of `testing`,
   `flag`, `time`, `os`, goroutines. Anything touching env/flags lives in a
   separate edge package.

Layers:

```
L0  veritas/            specs: ObjectStore, ControlPlane, Partition  (exists)
L1  veritastest/        runner: SpecComparator, Reporter, Rand, shrink (exists, renamed)
L2  veritastest/generator/ shared op generators + profiles + coverage stats   (NEW)
L3  veritastest/observation/ Observation types + normalizers per spec           (NEW)
L4  veritastest/suites/ ObjectStoreSuite, ControlPlaneSuite, PartitionSuite(NEW)
L5  veritastest/plantedbug/ wrapper subjects that add known bugs            (NEW)
L6  veritastest/runcontrol/ env/flag -> Options (seeds, replay, campaign)       (NEW)
    subjects live next to the code they test: engines, s3store, applier,
    memstore, machine, broker (C1 keeps its own Kafka-shaped suite)
```

Dependency direction: subjects -> suites -> (generator, observation, plantedbug, runcontrol) ->
veritastest -> veritas. `veritas`/`veritastest`/`generator`/`observation`/`suites` obey the
determinism rule; `runcontrol` is edge code (may use `os`, `flag`, `time`, `testing`).

---

## 3. Delta table: ideal vs current

| Topic | Current | Target |
|-------|---------|--------|
| Compare | string render per site | `Observation` equality, one renderer |
| Generators | ~8 copies | `generator.ObjectOps`, `generator.ControlPlaneOps` with profiles |
| Suites | rig/pair/state per site | `suites.*` take `func() Subject` |
| Planted bugs | broker + applier only | every suite has a planted-bug list and a catch test |
| Seed control | hard-coded | `MANIFESTER_SEEDS`, `MANIFESTER_SEED`, `MANIFESTER_CAMPAIGN` |
| Failure output | seed + literal | + exact replay command |
| Faults | C2 faults-off only | design doc now, implementation as stretch (section 8) |
| Spec validation | uneven | each spec has an independent model or property set |
| Names | `TB`, `Check` | `Reporter`, `SpecComparator` (see section 4) |

---

## 4. Naming decisions (veto any of these)

| Now | Proposed | Reason |
|-----|----------|--------|
| `veritastest.TB` | `Reporter` | it only receives `Helper/Errorf/Logf`; "TB" is cryptic |
| `veritastest.Check[S,Op]` | `SpecComparator[S,Op]` | says what it does; "Check" is vague |
| `Replay` / "lockstep" in comments | "compared step by step against the spec" | plain words; keep the word "lockstep" once in the package doc with a one-line definition |
| `Step` field | keep | it is the verb of the loop |
| package names | keep `veritas`, `veritastest` | renaming import paths is churn without payoff |
| `Failure`, `Options`, `Run`, `Rand` | keep | fine |

Mechanical renames use `gopls rename` / `gofmt -r`, repo-wide, in work
package W1 only, before anything else moves.

### 4.1 Language rules (the readers are South Indian engineers)

The maintainers read English as a second language. They are strong at it, but
they read plain, simple English and do not use native-speaker idioms. All new
names, comments, test messages and docs follow these rules. Every subagent gets
them in its brief.

1. **Short, direct sentences.** Subject, verb, object. One idea per sentence.
   Active voice ("the broker sends the batch"), not passive chains.
2. **No idioms or slang.** Do not write "happy path", "bail out", "blow up",
   "on the fly", "belt and braces", "in the wild", "ballpark", "gotcha".
   Write "normal case", "return early", "fail", "while running".
3. **No clever jargon when a plain word works.** In new code:
   - "mutant" -> **planted bug**; "kill a mutant" -> **catch a planted bug**
     (package `plantedbug`, not `mutant`). Existing `Bug*` names in the broker
     stay as they are.
   - "oracle" -> keep the word (the repo already uses it) but define it once
     in each package doc: "a checker we trust to say if a result is correct".
   - "lockstep" -> "compare step by step". "shrink" -> keep, and define once:
     "make the failing case smaller".
   - "flaky" -> "sometimes fails".
4. **Names say what the thing is.** Prefer `expectedResult` / `actualResult`
   over `want` / `got`; `specResult` / `subjectResult` over `a` / `b`.
   No abbreviations that need a dictionary (`obs`, `acts`, `gen`): use
   `observation`, `actions`, `generator` for exported names and package names
   where a full word is not awkward. (So: `veritastest/observation`,
   `veritastest/generator`, `veritastest/plantedbug`, `veritastest/runcontrol`
   instead of `obs`, `gen`, `mutant`, `vtrun`.)
5. **Comments explain WHY, in 1-3 plain sentences.** Start with what the code is
   for, then the one rule that is easy to miss. Do not restate the code.
   Good: "Replay returns the first answer again. A broker may retry after a
   timeout, so the same key must never get new offsets."
6. **Error and test messages are full sentences with the numbers in them:**
   "seed 17, step 4: the subject returned offset 3, the spec returned offset 2."
   Not "mismatch at 4".
7. **Spell out time and size words:** `retryDelay`, `maxBytes`; avoid `ttl`,
   `qps`, `ctx` in comments (the Go name `ctx` in code is fine).
8. **No emoji, no humour, no metaphors** in code or docs. Plain ASCII only (the
   gate also requires this).
9. Each package doc ends with a short "Terms" list when it uses a word from
   rule 3.

Review check (part of W10): grep the diff for the banned idioms in rule 2 and
for the old names in rule 3; a reviewer reads five random new comments aloud
and confirms each is one plain sentence per idea.

---

## 5. Work packages (subagent briefs)

Rules for every subagent:
- Follow section 4.1 (language rules) in every name, comment and message.
- Do NOT commit, do not run `git add`, do not stage `deploy/` or `docs/learn/`.
- Own only the files listed. If you need a change outside them, stop and report.
- ASCII only in `*.go`/`*.sh`/`*.yml` (the gate fails on any non-ASCII byte).
- No new third-party dependencies. Stdlib `testing` only.
- Follow `docs/refactoring_rules.md` vocabulary in your report (name the
  refactorings used).
- Finish by running `go build ./... && go vet` on your packages and the tests
  you own; report results verbatim, including failures.

### W0 - Baseline and inventory (read-only, 1 agent, runs first)
- Run every conformance/lockstep test; record pass/fail and wall time per test.
- Build the **planted-bug baseline**: for each suite, list what planted bugs (mutants) exist today.
- Confirm or refute gaps G1, G5-G7, G11 with exact `file:line`.
- Count distinct generators and the seed counts per site.
- Output: `scratchpad/w0-baseline.md` (not committed). Everything else is judged
  against it.

### W1 - Harness core: renames + options (1 agent, before W2-W8)
Files: `veritastest/lockstep.go` (+ rename file to `comparator.go`),
`veritastest/doc.go`, `veritastest/veritastest_test.go`, repo-wide rename sites.
- `TB` -> `Reporter`, `Check` -> `SpecComparator`, `fakeTB` -> `fakeReporter`.
- Add `Failure.ReplayCommand` plumbing: `Options.Replay` becomes required in
  `Run` docs (nil allowed but warned in doc) and `Report` prints it.
- Doc comment defines "lockstep" in plain words once.
- Acceptance: `go build ./...` and `go test ./internal/verify/veritas/...` green;
  zero remaining occurrences of the old names (`grep`).
- **Barrier**: W2-W8 start only after W1 lands in the working tree.

### W2 - `veritastest/runcontrol` (edge package) (1 agent, parallel)
Files: new `veritastest/runcontrol/runcontrol.go`, `runcontrol_test.go`.
- `runcontrol.Options(t, defaultSeeds int, replayCmd func(seed uint64) string)`
  reads `MANIFESTER_SEEDS` (count), `MANIFESTER_SEED` (single replay),
  `MANIFESTER_CAMPAIGN` (duration, uses `time`) and returns
  `veritastest.Options`, with the replay command filled in
  (`MANIFESTER_SEED=<n> go test <pkg> -run <TestName> -v`).
- Tests: env parsing, bad values fail loudly, default behaviour unchanged.
- Acceptance: with no env set, behaviour identical to today's `Seeds: N, Seed: -1`.

### W3 - Observation types (`veritastest/observation`) (1 agent, parallel)
Files: new `observation/observation.go`, `observation/observation_test.go`.
- `ObjResult{Code core.ErrCode, Outcome core.Outcome, Data []byte, Keys []string}`
  and `CPResult{Code, Outcome, Assigned []core.BatchRange, Ranges []core.BatchRange,
  End core.Offset, Topics []core.TopicInfo}` with a `Normalize` that makes
  nil == empty, and `String()` for messages.
- Converters: from `core.Completion` (spec and sim engines), from `sys` adapter
  returns (`error` -> `ErrCode` via the existing classifiers), from gRPC replies.
- Acceptance: table tests cover nil/empty, every ErrCode, every Outcome; no
  `%+v` left in the package.

### W4 - Shared generators (`veritastest/generator`) (1 agent, parallel)
Files: new `generator/object.go`, `generator/controlplane.go`, `generator/profile.go`, `generator/*_test.go`.
- `ObjectOps(profile)` and `ControlPlaneOps(profile)` returning
  `func(*veritastest.Rand, int) Op` for one shared op sum type per spec.
- Profiles (named, documented): `Default`, `SmallKeys` (heavy collisions),
  `ReplayHeavy` (same key/same shape, same key/different shape, fresh key/same
  shape), `InvalidHeavy` (empty key, negative partition, zero records,
  out-of-range reads), `ManyBatches` (40+ batches per commit), `Lookup cuts`
  (MaxBytes boundaries, From inside long runs).
- Coverage test per generator: over N seeds, assert each interesting case is
  reached at least M times (replay accepted, replay conflict, invalid commit,
  lookup MaxBytes cut, describe unknown topic, get out of range, delete
  missing, list empty prefix...). Print a table on `-v`.
- Acceptance: the coverage table is generated and committed as a test, and every
  case hits its minimum. Old per-site generators still exist until W6-W8 migrate.
- Note: shared generators change *which* sequences each seed produces. That is
  accepted; the safety net is coverage tests (this package) plus caught planted bugs
  (W5), not seed-for-seed equality.

### W5 - Suites and planted bugs (`veritastest/suites`, `veritastest/plantedbug`) (1 agent, after W2-W4)
Files: new `suites/object.go`, `suites/controlplane.go`, `suites/partition.go`,
`plantedbug/object.go`, `plantedbug/controlplane.go`, and their tests.
- Subject interfaces (small): `ObjectSubject.Do(op) observation.ObjectResult` plus
  `StateHash() (uint64, bool)` (ok=false when a subject has no spec hash, e.g.
  the real S3), `ControlPlaneSubject` likewise, `Reset/Close` via factory.
- `suites.ObjectStore(t, newSubject, opts)` builds a `SpecComparator`: spec vs
  subject, compare `Observation`s after every op, compare state hash when
  available, run invariants.
- Suites run the shared generator profiles as subtests so a failure names the
  profile.
- Planted bugs (each is a wrapper `Subject` around a correct one, so it works on
  every layer):
  - ObjectStore: Put aliases the caller's slice; Get range end off by one;
    Get out-of-range returns OK; List unsorted; List treats prefix as substring;
    Delete missing returns OK; Put empty key accepted.
  - ControlPlane: base off by one; replay assigns fresh offsets; replay with a
    different request not rejected; partial apply on an invalid batch; lookup
    `>=` vs `>` at MaxBytes; lookup skips the run that contains `From`;
    describe unsorted; topic partition count shrinks.
  - Catch test: `suites.RequireCaught(t, plantedBugs, suiteRunner)` fails if any
    planted bug is not caught within the seed budget. Print the catch table.
- Partition: a suite for `veritas.Partition` against a naive slice model
  (G10), since C1 uses it only indirectly.
- Acceptance: all planted bugs caught on the spec-vs-spec layer in under the seed
  budget; catch table printed.

### W6 - Migrate C2 (sim engines) and G6 (1 agent, after W5)
Files: `verify/sim/engines/conformance_test.go`, `engines/internal/enginetest`
(only if needed).
- Replace `rig`, `render`, `genObj`, `genCP`, `genBatches` with a Subject over
  the engine (issue op, drain queue, read completion, `SpecStateHash`).
- Resolve `TestConformanceLogEngine`: delete the skip and point the doc to
  `machine` lockstep, or turn it into a real forwarding test. No dangling skip.
- Add the catch test (engine wrapped by planted bugs).
- Acceptance: same engines covered, profiles run as subtests, planted bugs caught.

### W7 - Migrate C3 and the control-plane stores (1 agent, after W5)
Files: `sys/s3store/conform_test.go`, `controlplane/cptest/*_test.go`,
`controlplane/memstore/*_test.go`, `controlplane/applier/applier_test.go`
(lockstep part only), `controlplane/machine/lockstep_test.go`.
- Each becomes a Subject factory feeding `suites.*`: s3store (fake S3 hermetic
  always; real endpoint when `MANIFESTER_S3_ENDPOINT` set), memstore,
  memstore over gRPC, applier on MemKV, applier on Pebble, machine fold.
- Keep the scenario tests (`TestReplayMemstore`, `TestManyBatchesMemstore`,
  `TestRoundTrip`) as named regressions, but build them from shrunk literals
  produced by the suites.
- Add the catch test for at least memstore and applier.
- Acceptance: hermetic tests green with no env; env-gated tests still skip
  cleanly when endpoints are absent and are listed in the final report.
- Risk: Pebble runs are slower; keep their seed counts (15) and make them
  overridable by `MANIFESTER_SEEDS`.

### W8 - C1 broker and C4 oracle touch-up (1 agent, after W2)
Files: `broker/conformance_test.go`, `broker/harness_test.go`,
`verify/sim/kafka/c4_test.go` (options only).
- Switch to `runcontrol.Options` so seeds/replay work. Replace the broker harness's
  private SplitMix copy with `veritastest.Rand` (G7).
- Do NOT make C4 import veritas (independence rule). Add a comment-level
  guard: a test that fails if `oracle.go` imports `internal/verify/veritas`.
- Add cross-check test: `veritastest.Rand` stream equals `sim` counted stream
  for the same seed (lives in a test that may import both).
- Acceptance: C1 planted bugs (`BugWrongOffset`, `BugServeCorrupt`) still caught;
  guard test fails if someone adds the forbidden import (verify by temporarily
  adding it).

### W9 - Spec validation and docs (1 agent, parallel from the start)
Files: `veritas/doc.go`, `veritas/*_test.go`, `veritastest/doc.go`, stale
comments in `applier/memkv.go`, new `veritastest/README.md`.
- Fix G9 (HTTP client -> gRPC; boltkv; "28 faults" claim checked against
  `c4_faults_test.go` or removed).
- G10: add an independent naive model for ControlPlane (maps and loops, no
  prefix-hash cache) and compare spec vs model with the same runner; keep
  `fullStateHash` vs `StateHash` test.
- README: "How to add a new spec + suite in 30 minutes" (the from-scratch
  recipe: write spec, write generator profile + coverage test, write suite,
  write planted bugs, wire runcontrol) and a table of C1-C4 with real/sim labels.
- Acceptance: doc claims match code (agent lists each claim with file:line).

---

## 6. Order and parallelism

```
W0 baseline -----------------------------------------------+
W9 docs/spec validation (parallel throughout) -------------+|
W1 renames (barrier) --> W2 runcontrol --+                      ||
                         W3 observation +--> W5 suites+planted bugs +--> W6 C2
                         W4 gen  ---+                      +--> W7 C3/stores
                                    W8 C1/C4 (after W2) ---+
                                                            --> W10 integrate + gate + commit
```

Agents W2, W3, W4, W9 and W8 touch disjoint files and can run at the same time.
W6 and W7 touch disjoint trees and can run at the same time after W5.

---

## 7. W10 - Integration, verification, single commit (one agent, or me)

1. Re-run W0's measurements. Produce a before/after table: tests, seeds, wall
   time, generator count, planted bugs caught.
2. Full gate: `make build` (proto, fmt-check, ascii-check, vet, lint, compile,
   test, sim, it-hermetic, crap) and `ops/scripts/ci-determinism.sh`.
   Needs `mise install` (buf, golangci-lint). If the toolchain is missing, say
   so and stop; do not claim the gate passed.
3. Flake check: run every conformance test 20 times, `-count=20`, plus
   `-race` on `veritastest/...` and `suites/...`.
4. Planted-bug check: the catch table must show every planted bug caught.
5. Replay check: introduce a temporary bug, confirm the failure prints a
   command, run that command, confirm it reproduces, remove the bug.
6. Determinism of the suites: run a suite twice with the same seed and compare
   the report output byte for byte.
7. Stage explicit paths only (not `git add -A`; `deploy/` and `docs/learn/`
   are unrelated untracked directories). One commit, message in the repo's
   style (`refactor(verify): ...`), pre-commit hook runs `make build` itself.
   Review with `/code-review` before pushing.

---

## 8. Stretch (design now, build in a follow-up commit)

**Fault-aware C2.** With faults on, an engine may answer `ErrTimeout`,
`ErrUnavailable`, `ErrIO`, `ErrConnClosed`, with `Outcome` `NotApplied` or
`MaybeApplied`. Rule to check: if the engine says `Applied` the observation
must equal the spec's; if `NotApplied` the engine state hash must equal the
spec state before the op; if `MaybeApplied` the engine state hash must equal
either the spec state before or after the op, and the spec then follows
whichever branch the engine proves it took (on the next op that observes it).
This is a nondeterministic-spec comparator and deserves its own design review,
so it is not in the single commit.

Also deferred: linearizability checks for concurrent callers of the real
adapters (`vsys` serializes with a mutex today).

---

## 9. Definition of done

- [ ] Zero uses of `TB`, `Check` (the type), `fakeTB`; `grep` proves it.
- [ ] No `%+v`/`%v %q` string comparison in any conformance `Step`.
- [ ] One generator per spec; old copies deleted; coverage test passes.
- [ ] Every suite has a planted-bug catch test; catch table attached to the PR text.
- [ ] `MANIFESTER_SEED=n go test <pkg> -run <Test>` replays; failure output
      prints that exact command.
- [ ] C4 independence guard test exists and was seen failing once.
- [ ] No dangling `t.Skip` for conformance (the env-gated real S3/Dynamo ones
      are documented and skip with a clear message).
- [ ] Docs match code; README recipe exists.
- [ ] `make build` and `ci-determinism.sh` pass; wall time of the conformance
      tests no more than +20 percent over the W0 baseline.
- [ ] One commit, explicit paths, nothing from `deploy/` or `docs/learn/`.

## 10. Risks and how the plan handles them

| Risk | Handling |
|------|----------|
| The refactor silently weakens coverage | planted-bug catch table before/after; generator coverage tests; W0 baseline |
| Shared generators change what each seed exercises | accepted explicitly; seed numbers in old regression literals are replaced by shrunk literals |
| CRAP gate (threshold 20) fails on new non-test helpers | W1-W5 keep functions small; `make crap` run per package before integration |
| Determinism gate rejects `runcontrol` | it is a separate edge package outside the deterministic list; confirm with `ci-determinism.sh` |
| Real S3 / DynamoDB paths cannot run locally | stated in the final report as not run; hermetic fakes cover the logic |
| Renames collide with parallel edits | W1 is a barrier and lands first |
| Pebble suites slow the gate | seed counts stay as today; `MANIFESTER_SEEDS` override |
| Subagent edits outside its files | each brief lists owned files; W10 diffs the file list against ownership |

## 11. Decisions I made so you can veto (defaults in bold)

1. Rename `TB` -> **`Reporter`**, `Check` -> **`SpecComparator`** (alternative:
   keep `Check`, rename only `TB`).
2. Fault-aware C2 is **a follow-up**, not in this commit.
3. Package names and import paths **unchanged**.
4. New third-party test libraries: **none** (stdlib `testing` only).
5. `docs/learn/*` and `deploy/` **excluded** from the commit.
6. Seed-for-seed output of existing tests **not preserved**; planted bugs and
   coverage are the guard instead.
