# CP DSM and event-loop plan

Move the log control plane onto the broker's `core.Machine` shape so one `Step` runs under the sim kernel and a control-plane-owned run loop. Brokers keep speaking `CPCall` whose payloads are generated gRPC API types. The wire protocol is gRPC plus protobuf (hard cut off JSON HTTP). The shared Dynamo log becomes an engine. Veritas stays the independent oracle but shares the generated schema types. The `internal/controlplane/` tree is an autonomous extractable unit. PR ids run CP-DSM-1 through CP-DSM-8.

## How to read this

One box is one unit of work. Every box names the evidence that checks it. A nested box is a sub-step of the box above it. Check a box only when its evidence exists, a file, a log line, a screenshot, a test run, or a SHA. The body is a how-to. The appendices explain and record.

The program runs `pstack/skills/poteto-mode/playbooks/autopilot-stack.md`. The root owns the stack topology. Owners stop at STACK-READY. The operator lands the chain bottom-up. PR ids CP-DSM-1 through CP-DSM-8 are the operator's landing items.

Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

## Program checklist

### Arm the program

- [ ] State the protocol and this plan to the operator, then stop. Start execution only on the operator's explicit go.
- [ ] Read these from trunk at program start. Re-read them at every tick.
  - [ ] `git show origin/main:pstack/skills/poteto-mode/playbooks/autopilot-stack.md`
  - [ ] `git show origin/main:pstack/skills/poteto-mode/playbooks/autopilot-full.md`
  - [ ] `git show origin/main:pstack/skills/swarm/SKILL.md`
  - [ ] `git show origin/main:pstack/skills/control-cli/SKILL.md`
  - [ ] `git show origin/main:pstack/skills/poteto-mode/playbooks/opening-a-pr.md`
  - [ ] `git show origin/main:pstack/skills/how/SKILL.md`
  - [ ] `git show origin/main:pstack/skills/interrogate/SKILL.md`
  - [ ] `git show origin/main:pstack/skills/show-me-your-work/SKILL.md`
  - [ ] `git show origin/main:docs/cp-dsm-eventloop-plan.md`
- [ ] On the operator's go, arm the audit tick as `/loop 1h` with the tick prompt below. Never leave the cadence to memory.
- [ ] Use this tick prompt, verbatim. "Re-read the execution playbook from trunk. Audit the operation against it and fix drift in this tick. Probe every active lane and judge progress by side effects only. Stand down a stuck lane and dispatch its replacement now. Then post a short status message to the operator in chat only when the audit found a tracked change that no earlier status message reported, such as a PR opened, a code-ready head, a round launched or closed, a verdict, a merge, a stuck agent and the action taken, a blocker added or cleared, or a decision only the operator can make. Name every such change and nothing else. Do not repeat a table, the merged list, or an unchanged blocker. If the audit found none, end the turn with no reply text. Either way, log this tick's row in your decision trail. The row names the items reported, or none."
- [ ] On the operator's hold or stand-down, send every owner a zero-writes order at once.

### Spawn owners

- [ ] Spawn one owner per PR with the full lifecycle the execution playbook names.
- [ ] Follow this dependency graph. Start dependent work only after its parent merges, or base it on the parent branch when the execution playbook stacks.
  - [ ] CP-DSM-1 first from `main`.
  - [ ] CP-DSM-2 and CP-DSM-3 after CP-DSM-1. Independent. Both branch from CP-DSM-1 (or stack CP-DSM-3 on CP-DSM-2 if the forge prefers a single line).
  - [ ] CP-DSM-4 after both CP-DSM-2 and CP-DSM-3.
  - [ ] CP-DSM-5 after CP-DSM-4.
  - [ ] CP-DSM-6 after CP-DSM-5.
  - [ ] CP-DSM-7 after CP-DSM-6.
  - [ ] CP-DSM-8 after CP-DSM-7.
- [ ] Hold the file boundaries. Each PR's **Files.** section is the only allowed touch set for that owner.
- [ ] Hold the control-plane autonomy rule on every PR. Packages under `internal/controlplane/` (except explicitly named monorepo adapters) must not import `manifester/broker`, `manifester/sim`, `manifester/sys`, `manifester/cmd`, `manifester/veritas`, `manifester/kwire`, or `manifester/format`. Allowed deps are stdlib, `manifester/core` (future shared module), gRPC/protobuf runtime libs, and other `manifester/controlplane/...` packages including `internal/controlplane/api/v1`. Brokers, sim, veritas, sys, and cmd may import `internal/controlplane/api/v1`. They must not be imported by `internal/controlplane/statemachine`. Prove each PR with `go list` import checks in that PR's live lanes.
- [ ] Hold the gRPC contract rules from Appendix E on every PR that touches the wire or `CPCall` payloads.
- [ ] Hold the review gate. CP-DSM-7 and CP-DSM-8 change manicp or kafka-log operator-visible behavior. They wait for the operator's review in chat with screenshots and a video before merge.

### PR mechanics, for every PR

- [ ] Resolve the forge once. Default to `gh`; if `command -v origin` succeeds and Origin can resolve the repository, use `origin pr` for every PR operation. Record any fallback to `gh`. Never require `gt`.
- [ ] Open the PR ready, never draft, per **Opening a PR**. Use the run's built-in PR tool when it has one, else `origin pr create --status open --base <base-branch>` or `gh pr create --base <base-branch>` according to the resolved forge. A stack child targets its parent branch.
- [ ] Run the repo's lint and typecheck once before the PR-facing push. Push with hooks on.
- [ ] Run `/deslop` before each commit and `/no-comments` before review.
- [ ] Triage every Bugbot and security-reviewer comment per `../references/bugbot-triage.md`.
- [ ] Rebase onto current trunk before the code-ready report and babysit. Keep that merge base in fix rounds. Rebase again only at merge prep, on a `git merge-tree` conflict with trunk, or on a CI failure that comes from a change on trunk.

### Verdict and merge, for every PR

- [ ] A full swarm runs once, at the code-ready head. Another full swarm runs only for new behavior, a redesign, or a large new diff. A rebase, a merge-conflict resolution, or a minor fix gets a single verifier at the new head. Run each full swarm per `pstack/skills/swarm/SKILL.md`. One gates lane. The ten live lanes from the PR's **Verify, live** block. The perf lane from its **Verify, perf** block. Two or more audit lanes, each with its own focus, that read the diff and the receipts and distrust the PR body. The root audits the receipts in the merge-ready report before the verdict.
- [ ] Clean only when every lane is `PASS`. Findings go back to the owner, including a defect that a lane filed as a note. A new head gets a fresh verdict, except for results that stay valid under the patch-id rule in `playbooks/shipping.md`.
- [ ] Root appends each clean PR to the base-branch stack. The operator lands bottom-up. Patch-id rule from `playbooks/shipping.md` applies after any rebase onto the parent.

### Boot recipe, for every live lane

Each live lane runs on its own cloud VM at the PR head. Drive through `control-cli` from `cursor-team-kit`.

- [ ] `git fetch origin <head-branch> && git checkout <head SHA>`.
- [ ] Install Go toolchain matching `go.mod`. No long-running UI server. Ready when `go env GOROOT` prints.
- [ ] Deliver input only through `control-cli` commands that run the lane's named `go test` or `go run ./cmd/manisim ...` line. Read-only diagnostics are `go test -c` exit codes and captured stdout/stderr files under `/tmp/swarm-<pr-id>/worker-<n>/`.
- [ ] Save every screenshot to `/tmp/swarm-<pr-id>/worker-<n>/<slug>.png` and return the paths with the report. Capture the terminal pane that shows the pass line or failure.

## Record decisions, rename, and gRPC API scaffold (CP-DSM-1)

**Depends on.** None.

**Files.**

- [ ] Edit `docs/decisions.md`.
- [ ] Edit `docs/sim-log-engine.md`.
- [ ] Edit `docs/lessons.md` only if a cross-link is required.
- [ ] Edit `docs/log-cp-service.md` title paths toward `internal/controlplane/`.
- [ ] Rename tree `cp/` → `internal/controlplane/` and fix all Go imports. Binary name stays `manicp`.
- [ ] Create `internal/controlplane/api/v1/*.proto` (`package controlplane.v1`).
- [ ] Create local-only `internal/controlplane/api/buf.yaml` / `buf.gen.yaml` and a `make proto` target. No Buf Schema Registry.
- [ ] Generate Go on build via `make proto` (mise-pinned `buf`); gitignore `internal/controlplane/api/v1/*.pb.go`. Do not commit generated files.
- [ ] Edit `internal/core/action.go` for `Log*` IOActions and for `CPCall`/`CPDone` to carry `internal/controlplane/api/v1` messages (replace hand-written `CPOp` as the wire contract).
- [ ] Edit `internal/core/event.go` and `internal/core/inbox.go` (`ClassOf`) for new bodies.
- [ ] Create `internal/core/logio_test.go` and API round-trip tests.
- [ ] Edit `refactoring_backlog.md` Next-up line to point at this program.

**Build.**

- [ ] Add decision rows for DSM machine package, `internal/controlplane/` autonomy, and gRPC-as-protocol (supersede HTTP wording in decisions 3 and 23 for new work). Keep Pebble and the gRPC adapter outside `Step`.
- [ ] Define unary RPCs CommitSegment, Lookup, DescribeTopics, CreateTopic, plus GetStatus. Register `grpc.health.v1`.
- [ ] Define typed `ErrorDetail` with `ErrCode` and `Outcome`. Non-OK RPCs use gRPC status plus that detail. Success returns the response message only.
- [ ] Add `LogAppend`, `LogRead`, `LogCheckTail`, `LogTrim` IOActions and matching `*Done` completions with `Outcome`.
- [ ] Extend `ClassOf` for every new `EventBody`.
- [ ] `make proto` is reproducible on a clean machine with local buf + pinned plugins.
- [ ] Compile-only for the new machine. No manicp gRPC cutover yet in this PR unless needed for green builds after the rename.

**You see.**

- [ ] `make proto` and `go test ./core ./controlplane/api/... -count=1` print `ok`. Imports use `manifester/controlplane/...`. No remaining production import of `manifester/cp/...`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Proto generate + `ClassOf` + rename compile. Run `make proto` and `go test ./core ./controlplane/... -count=1` (packages that still exist after rename).

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go test ./core ./broker -count=1` at trunk and head. Save `core-broker-regress.png`. Pass when both print `ok`.
- [ ] Lane 2. Run `make proto` then `go test ./controlplane/api/... -count=1`. Save `proto-api.png`. Pass when `ok`.
- [ ] Lane 3. Run `go test ./veritas -count=1`. Save `veritas.png`. Pass when `ok` (may still use adapters if full type cut lands in CP-DSM-7).
- [ ] Lane 4. Run `go test ./kwire -count=1`. Save `kwire.png`. Pass when `ok`.
- [ ] Lane 5. Run `go test ./format -count=1`. Save `format.png`. Pass when `ok`.
- [ ] Lane 6. Run `ops/scripts/ci-determinism.sh` or the repo's detcheck make target. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 7. Run `go test ./sim/engines/cpengine -count=1`. Save `cpengine.png`. Pass when `ok`.
- [ ] Lane 8. Run `go test ./sim/engines/logengine -count=1`. Save `logengine.png`. Pass when `ok`.
- [ ] Lane 9. Diff `docs/decisions.md` and confirm DSM, autonomy, and gRPC decision rows are present. Save `decision-row.png`. Pass when rows name `internal/controlplane/`, gRPC hard-cut intent, and veritas apply-code independence.
- [ ] Lane 10. Confirm no `manifester/cp/` imports remain in non-test production packages (`go list` / rg). Save `rename-imports.png`. Pass when the scan is clean.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. `go test ./core -count=1` wall time at trunk and head, plus `make proto` wall time at head.
- [ ] Probe. Interleave trunk then head three times each with `/usr/bin/time -p` for `go test ./core`. Time `make proto` three times on head.
- [ ] Baseline. Record the trunk `go test ./core` real seconds first. Trunk lacks `make proto`.
- [ ] Rule. Head `go test ./core` under trunk times 1.5 plus 2 s. `make proto` under 60 s absolute on the lane VM.

**Review gate.** None. CP-DSM-1 is not review-gated.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current trunk after the verdict, patch-id unchanged.
- [ ] Root appends CP-DSM-1 to the stack. Operator lands it.

## Scaffold the CP machine package (CP-DSM-2)

**Depends on.** CP-DSM-1.

**Files.**

- [ ] Create `internal/controlplane/statemachine/` (preferred name under the extractable `internal/controlplane/` root; do not place it under `internal/verify/sim/` or `internal/broker/`).
- [ ] Create `Step`, `StateHash`, `New` Factory mirroring the broker's `core.MachineFactory` shape.
- [ ] Create boot handling for `Started` and `Stop` only.
- [ ] Edit `ops/scripts/ci-determinism.sh` and `ops/tools/determinismcheck` allow lists so `internal/controlplane/statemachine` is scanned.
- [ ] Create package tests for hash stability on no-op completions.
- [ ] Create an import-boundary test that fails if `internal/controlplane/statemachine` imports a forbidden monorepo package.

**Build.**

- [ ] Machine ignores unknown completions. `StateHash` is stable across a no-op Step.
- [ ] No Dynamo, HTTP, Pebble, goroutine, broker, sim, sys, cmd, veritas, kwire, or format imports in the package.
- [ ] Public API is enough for a future external module consumer to construct the Factory and drive `Step` with `core` plus `internal/controlplane/api/v1` types only.

**You see.**

- [ ] `go test ./controlplane/machine -count=1` prints `ok`, and detcheck lists the package as clean.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Hash stability and Started/Stop tests in `internal/controlplane/statemachine`. Run `go test ./controlplane/machine -count=1` and the detcheck script.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go test ./broker ./core -count=1` at trunk and head. Save `regress.png`. Pass when both `ok`.
- [ ] Lane 2. Run `go test ./controlplane/machine -count=1`. Save `machine.png`. Pass when `ok`.
- [ ] Lane 3. Run detcheck. Save `detcheck.png`. Pass when exit 0 and the new package is included.
- [ ] Lane 4. Run `go test ./controlplane/logcp -count=1 -short` if present, else `go test ./controlplane/logcp -count=1 -run TestNew`. Save `logcp-untouched.png`. Pass when `ok`.
- [ ] Lane 5. Run `go test ./controlplane/logsm -count=1 -run TestX2 -timeout 5m`. Save `logsm-x2.png`. Pass when `ok`.
- [ ] Lane 6. Run `go test ./sim/engines/logengine -count=1`. Save `logengine.png`. Pass when `ok`.
- [ ] Lane 7. Confirm `internal/controlplane/statemachine` imports stay inside the autonomy allow-list. Save `imports.png`. Pass when `go list -f '{{join .Imports "\n"}}' ./controlplane/machine` shows no `manifester/broker`, `manifester/sim`, `manifester/sys`, `manifester/cmd`, `manifester/veritas`, `manifester/kwire`, `manifester/format`, `pebble`, `net/http`, or Dynamo SDK paths.
- [ ] Lane 8. Run `go test ./sys -count=1`. Save `sys.png`. Pass when `ok`.
- [ ] Lane 9. Run `go vet ./controlplane/machine`. Save `vet.png`. Pass when silent.
- [ ] Lane 10. Run `gofmt -l controlplane/machine`. Save `gofmt.png`. Pass when empty.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. `go test ./controlplane/machine -count=1` real seconds, and trunk has no package so also record absolute budget for empty Step loop of 10k Started no-ops in a microbench if added.
- [ ] Probe. Head microbench `BenchmarkStepNoop` if present, else package test time three times.
- [ ] Baseline. Trunk has no feature. Record that. Absolute budget is 2 s for the package tests.
- [ ] Rule. Head package tests finish under 2 s real on the lane VM.

**Review gate.** None. CP-DSM-2 is not review-gated.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends CP-DSM-3's parent tip. Operator lands after CP-DSM-1.

## Add the log-store engine (CP-DSM-3)

**Depends on.** CP-DSM-1. Independent of CP-DSM-2.

**Files.**

- [ ] Create `internal/verify/sim/engines/logstore/` (faulting Dynamo log engine for `LogAppend`/`LogRead`/`LogCheckTail`/`LogTrim`).
- [ ] Edit `internal/verify/sim/engine.go` registration sites or kafka config only as needed to construct it behind a flag.
- [ ] Edit `internal/controlplane/statemachine` to emit log IOActions on a timer-driven catch-up sketch that does not yet apply commands.
- [ ] Create engine unit tests for if-absent put, lost answer as `MaybeApplied`, and ordered landings.

**Build.**

- [ ] Logstore engine draws faults at issue time and completes every live ReqID.
- [ ] Machine can Append/Read/CheckTail through actions only.

**You see.**

- [ ] `go test ./sim/engines/logstore ./controlplane/machine -count=1` prints `ok`, including a seed that shows a lost append completion.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Engine fault and machine IOAction tests. Run `go test ./sim/engines/logstore ./controlplane/machine -count=1`.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go test ./sim/engines/logengine -count=1` at trunk and head. Save `logengine-regress.png`. Pass when both `ok`.
- [ ] Lane 2. Run `go test ./sim/engines/logstore -count=1`. Save `logstore.png`. Pass when `ok`.
- [ ] Lane 3. Run a focused lost-append test. Save `lost-append.png`. Pass when the completion is `ErrTimeout` with `MaybeApplied` or the engine's documented equivalent.
- [ ] Lane 4. Run `go test ./controlplane/machine -count=1`. Save `machine.png`. Pass when `ok`.
- [ ] Lane 5. Run detcheck. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 6. Run `go test ./sim/engines/cpengine -count=1`. Save `cpengine.png`. Pass when `ok`.
- [ ] Lane 7. Run `go test ./broker -count=1`. Save `broker.png`. Pass when `ok`.
- [ ] Lane 8. Confirm kafka default scenario still builds. Save `manisim-list.png`. Pass when `go run ./cmd/manisim -h` exits 0.
- [ ] Lane 9. Run `go test ./sim -count=1 -short` if available. Save `sim-short.png`. Pass when `ok` or skip is documented.
- [ ] Lane 10. Run `gofmt -l sim/engines/logstore controlplane/machine`. Save `gofmt.png`. Pass when empty.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. `go test ./sim/engines/logstore -count=1` real seconds at head. Trunk lacks the package, so also absolute budget for 1k append issue/land cycles in a bench if added.
- [ ] Probe. Head package tests three times.
- [ ] Baseline. Trunk lacks the feature. Record that.
- [ ] Rule. Head package tests under 5 s real. Bench if present stays under 500 ns/op per land on the lane VM, or the test-time rule alone if no bench yet.

**Review gate.** None. CP-DSM-3 is not review-gated.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends. Operator lands in stack order.

## Port apply and hole-fill into Step (CP-DSM-4)

**Depends on.** CP-DSM-2 and CP-DSM-3.

**Files.**

- [ ] Edit `internal/controlplane/statemachine` apply path (rules currently in `internal/controlplane/stateupdater` pure subset and `logsm`/`logcp` replica observe/apply).
- [ ] Edit or wrap `internal/controlplane/stateupdater` so the pure fold is callable without Pebble.
- [ ] Create lockstep tests machine-vs-veritas for commit sequences.
- [ ] Do not delete `logcp` loops yet.

**Build.**

- [ ] Slot-ordered apply inside `Step`. Hole fill emits no-op appends as actions.
- [ ] `StateHash` covers applied digest comparable to applier digest.

**You see.**

- [ ] Lockstep test prints equal answers and hashes for a fixed seed command list.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Lockstep and hole-fill tests. Run `go test ./controlplane/machine ./controlplane/applier -count=1 -timeout 10m`.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go test ./controlplane/logsm -count=1 -run X2 -timeout 10m` at trunk and head. Save `logsm-x2-regress.png`. Pass when both `ok`.
- [ ] Lane 2. Run machine-vs-veritas lockstep. Save `lockstep.png`. Pass when digests match.
- [ ] Lane 3. Run hole-fill unit. Save `holefill.png`. Pass when `ok`.
- [ ] Lane 4. Run `go test ./controlplane/machine -count=1`. Save `machine.png`. Pass when `ok`.
- [ ] Lane 5. Run `go test ./veritas -count=1`. Save `veritas.png`. Pass when `ok`.
- [ ] Lane 6. Run `go test ./controlplane/logcp -count=1 -run TestService -timeout 10m`. Save `logcp-service.png`. Pass when `ok`.
- [ ] Lane 7. Run detcheck. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 8. Run `go test ./sim/engines/logengine -count=1`. Save `logengine.png`. Pass when `ok`.
- [ ] Lane 9. Confirm decision 21 still holds (no import of veritas from controlplane/machine). Save `no-veritas-import.png`. Pass when `go list` shows none.
- [ ] Lane 10. Run `gofmt -l controlplane/machine controlplane/applier`. Save `gofmt.png`. Pass when empty.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Lockstep wall time for the same fixed seed list at head. Trunk lacks the machine lockstep, so also absolute budget.
- [ ] Probe. Head lockstep three times.
- [ ] Baseline. Trunk lacks the feature. Record that.
- [ ] Rule. Head lockstep under 30 s real for the default seed list on the lane VM.

**Review gate.** None. CP-DSM-4 is not review-gated.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends. Operator lands in stack order.

## Port commit queue and K in-flight (CP-DSM-5)

**Depends on.** CP-DSM-4.

**Files.**

- [ ] Edit `internal/controlplane/statemachine` commit queue, group packing, K in-flight append state.
- [ ] Create tests for lost append retry same entry later slot, and sticky single appender behavior at machine level.
- [ ] Keep `logcp` production path unchanged.

**Build.**

- [ ] Client commit events enqueue. Flusher loops become in-flight ReqID slots capped at K.
- [ ] Answers wait until the slot applies.

**You see.**

- [ ] A unit seed shows K concurrent `LogAppend` actions and answers only after apply.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Commit queue and K tests. Run `go test ./controlplane/machine -count=1 -timeout 10m`.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go test ./controlplane/logcp -count=1 -run Sim -timeout 15m` at trunk and head when the test exists. Save `logcp-sim-regress.png`. Pass when both `ok`.
- [ ] Lane 2. Run K in-flight test. Save `k-inflight.png`. Pass when at most K appends are live.
- [ ] Lane 3. Run lost-slot retry test. Save `lost-slot.png`. Pass when the same entry lands in a later slot once.
- [ ] Lane 4. Run group packing test. Save `group.png`. Pass when caps match `MaxCmdsPerSlot`.
- [ ] Lane 5. Run `go test ./controlplane/machine -count=1`. Save `machine.png`. Pass when `ok`.
- [ ] Lane 6. Run lockstep again. Save `lockstep.png`. Pass when digests match.
- [ ] Lane 7. Run detcheck. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 8. Run `go test ./sim/engines/logstore -count=1`. Save `logstore.png`. Pass when `ok`.
- [ ] Lane 9. Run `go test ./broker -count=1`. Save `broker.png`. Pass when `ok`.
- [ ] Lane 10. Run `gofmt -l controlplane/machine`. Save `gofmt.png`. Pass when empty.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Machine-only commit throughput for a fixed 1k commit seed at head. Absolute budget because trunk lacks the path.
- [ ] Probe. Head test three times.
- [ ] Baseline. Trunk lacks the feature. Record that.
- [ ] Rule. 1k commits finish under 10 s real on the lane VM.

**Review gate.** None. CP-DSM-5 is not review-gated.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends. Operator lands in stack order.

## Port role, checkpoint, and trim (CP-DSM-6)

**Depends on.** CP-DSM-5.

**Files.**

- [ ] Edit `internal/controlplane/statemachine` role timers, takeover/failback, checkpoint snapshot publish via `ObjPut`, trim via `LogTrim`.
- [ ] Create tests for standby refuse, takeover after silence, restore on `Started`.
- [ ] Still no manicp cutover.

**Build.**

- [ ] Advisory roles. Slot put remains the fence.
- [ ] Catch-up before Ready on `Started`.

**You see.**

- [ ] A crash/restart unit shows restore then Ready, and standby commit returns refused without append.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Role and checkpoint tests. Run `go test ./controlplane/machine -count=1 -timeout 15m`.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go test ./controlplane/logcp -count=1 -run Checkpoint -timeout 10m` at trunk and head. Save `ckpt-regress.png`. Pass when both `ok`.
- [ ] Lane 2. Run takeover unit. Save `takeover.png`. Pass when standby becomes active after silence.
- [ ] Lane 3. Run failback unit. Save `failback.png`. Pass when preferred rank resumes.
- [ ] Lane 4. Run restore-on-Started unit. Save `restore.png`. Pass when Ready follows catch-up.
- [ ] Lane 5. Run standby refuse unit. Save `standby-refuse.png`. Pass when no LogAppend is emitted.
- [ ] Lane 6. Run trim tombstone unit. Save `trim.png`. Pass when trim floor cannot be overwritten.
- [ ] Lane 7. Run full `go test ./controlplane/machine -count=1`. Save `machine.png`. Pass when `ok`.
- [ ] Lane 8. Run detcheck. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 9. Run `go test ./sim/engines/logengine -count=1`. Save `logengine.png`. Pass when `ok`.
- [ ] Lane 10. Run `gofmt -l controlplane/machine`. Save `gofmt.png`. Pass when empty.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Restore-plus-catch-up time for a fixed checkpoint fixture at head. Absolute budget.
- [ ] Probe. Head restore test three times.
- [ ] Baseline. Trunk lacks the feature. Record that.
- [ ] Rule. Restore fixture under 5 s real on the lane VM.

**Review gate.** None. CP-DSM-6 is not review-gated.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends. Operator lands in stack order.

## Cut over production manicp to gRPC (CP-DSM-7)

**Depends on.** CP-DSM-6.

**Files.**

- [ ] Create `internal/controlplane/runloop` as the production event loop twin of `sys.Run`, owned by the extractable `internal/controlplane/` tree.
- [ ] Create `internal/controlplane/executor` for Dynamo and S3 action execution. Keep AWS SDK imports out of `internal/controlplane/statemachine`.
- [ ] Create the gRPC server adapter that maps unary RPCs to machine events and maps status+`ErrorDetail` on the way out.
- [ ] Create the broker-side / executor gRPC client that turns `CPCall` into unary RPCs (plaintext). Preferred address dialing. Standby failover list.
- [ ] Edit `cmd/manicp` as a thin main only. Flags, listen addr, `internal/controlplane/runloop` wiring. No protocol logic in cmd.
- [ ] Delete JSON HTTP `/v1/*` handlers. Hard cut. All tests updated in this PR so the suite stays green.
- [ ] Move `veritas` (and remaining oracles) onto `internal/controlplane/api/v1` message types. Keep apply logic unshared.
- [ ] Do not add `internal/controlplane/statemachine` imports of `manifester/sys`. Inject metrics/runinfo via interfaces if needed.

**Build.**

- [ ] `manicp` serves gRPC only. `grpc.health.v1` plus `GetStatus`.
- [ ] Broker prod executor speaks gRPC. Sim engines still fake `CPCall` with the same generated payloads.
- [ ] Deadline/cancel without a success response maps to `ErrTimeout` + `MaybeApplied`.
- [ ] X2-style and package tests are green on the machine+gRPC path.
- [ ] A consumer with `internal/controlplane/` + `core` can link machine, run, api, and exec without `broker` or `sim`.

**You see.**

- [ ] `go test ./controlplane/... ./cmd/manicp ./broker ./veritas ./sim/... -count=1` (scoped as CI does) is green, and a grpcurl or Go client CommitSegment against local manicp returns assigned ranges after Ready.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Full control-plane and client tests on gRPC. Run `go test ./controlplane/... ./cmd/manicp ./broker ./veritas -count=1 -timeout 20m`.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run the repo's default `go test` targets that formerly hit JSON CP at trunk, and the gRPC equivalents at head. Save `manicp-regress.png`. Pass when both sides are green for their protocol.
- [ ] Lane 2. Boot manicp, check `grpc.health.v1` Serving after Ready. Save `grpc-health.png`. Pass when Serving.
- [ ] Lane 3. Unary CommitSegment against fakedb. Save `commit-grpc.png`. Pass when assigned ranges return under status OK.
- [ ] Lane 4. Lookup after ack. Save `lookup-grpc.png`. Pass when ranges match.
- [ ] Lane 5. Standby CommitSegment returns non-OK status with typed `ErrorDetail` (unavailable / refused). Save `standby-status.png`. Pass when detail includes role/blocked info consistent with GetStatus.
- [ ] Lane 6. Client cancel mid-commit yields `MaybeApplied` classification in the executor helper test. Save `maybe-applied.png`. Pass when the helper asserts Outcome.
- [ ] Lane 7. Confirm JSON `/v1/` routes are gone (404 or connection refused on old HTTP). Save `http-gone.png`. Pass when no JSON CP handler remains.
- [ ] Lane 8. Run metrics export test if still present. Save `metrics.png`. Pass when required series exist or the test is updated and green.
- [ ] Lane 9. Run detcheck. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 10. Run autonomy `go list` on `./internal/controlplane/statemachine ./controlplane/run ./controlplane/exec` and `gofmt -l cmd/manicp controlplane`. Save `autonomy-gofmt.png`. Pass when forbidden imports are absent and gofmt is clean.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Single-node fakedb CommitSegment p50 for 1k commits at trunk (legacy JSON if still on trunk) and head (gRPC machine).
- [ ] Probe. Interleave trunk and head harness three times each.
- [ ] Baseline. Record trunk p50 first. If trunk already renamed, use the last HTTP baseline recorded in the decision trail.
- [ ] Rule. Head p50 under trunk times 1.3 when protocols are comparable, else absolute CommitSegment p99 under 50 ms on the lane VM fakedb.

**Review gate.** The operator reviews before merge.

- [ ] Copy lane 2 and lane 3 screenshots into `docs/media/cp-dsm-7-review-health.png` and `docs/media/cp-dsm-7-review-commit.png`.
- [ ] Record a 30 to 60 second video of manicp boot plus one gRPC commit on a lane VM. Save it as `docs/media/cp-dsm-7-review.mp4`.
- [ ] Post the screenshots and the video in chat. Stop at merge-ready. Wait for the operator's click.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends. Operator lands after review click.

## Cut over kafka-log and delete the forks (CP-DSM-8)

**Depends on.** CP-DSM-7.

**Files.**

- [ ] Edit `internal/verify/sim/kafka` config so `kafka-log` runs CP replica processes plus logstore engine.
- [ ] Delete or gut `internal/verify/sim/engines/logengine` replica/state once mutants and C2 move to the machine.
- [x] Delete `logcp` goroutine loops and `logcp.Env` scheduler path when unused (retired; SoT is `internal/controlplane/statemachine` + `exec`).
- [ ] Edit `docs/sim-log-engine.md`, `docs/log-cp-service.md`, `docs/decisions.md`, `refactoring_backlog.md`.
- [ ] Re-home X2 coverage as kernel seeds where practical.

**Build.**

- [ ] One CP implementation in sim and prod. Veritas remains oracle only.
- [ ] Mutants `BugWriterOffsets`, `BugTrimDelete`, `BugPerReplicaWindow`, `BugHoleSkip` still caught.

**You see.**

- [ ] `go run ./cmd/manisim -scenario kafka-log -seeds 300` finishes with 0 failing, and docs describe the machine topology.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Mutant tests and machine package tests. Run `go test ./sim/kafka -count=1 -run Log -timeout 30m` and `go test ./controlplane/machine -count=1`.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten lanes on `grok-4.7-high-fast` at the PR head, per the boot recipe.

- [ ] Lane 1. Regression lane against trunk. Run `go run ./cmd/manisim -scenario kafka -seeds 30` at trunk and head. Save `kafka-scenario-regress.png`. Pass when both report 0 failing (veritas CP path must stay identical).
- [ ] Lane 2. Run `go run ./cmd/manisim -scenario kafka-log -seeds 300`. Save `kafka-log-300.png`. Pass when 0 failing.
- [ ] Lane 3. Run determinism canary for kafka-log (every Kth seed twice). Save `kafka-log-det.png`. Pass when TraceHash matches on the canary seeds.
- [ ] Lane 4. Run mutant suite. Save `mutants.png`. Pass when each named mutant is caught.
- [ ] Lane 5. Run C2-style conformance against the machine path. Save `c2.png`. Pass when `ok`.
- [ ] Lane 6. Run manicp machine path tests. Save `manicp.png`. Pass when `ok`.
- [ ] Lane 7. Confirm `logengine` replica/state is gone or is a thin shim documented as deleted. Save `logengine-gone.png`. Pass when the package no longer owns replica apply.
- [ ] Lane 8. Run detcheck. Save `detcheck.png`. Pass when exit 0.
- [ ] Lane 9. Run `go test ./it -count=1 -timeout 30m` if emulator available, else record skip with reason in the lane report and pass the substituted `go test ./controlplane/logcp ./cmd/manicp -count=1 -timeout 20m`. Save `it-or-sub.png`. Pass when `ok` or documented skip plus substitute `ok`.
- [ ] Lane 10. Run `gofmt -l sim cp cmd docs`. Save `gofmt.png`. Pass when touched Go files are clean.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. `manisim -scenario kafka-log -seeds 30` wall time at trunk (old logengine) and head (machines). Steps count must remain comparable.
- [ ] Probe. Interleave trunk and head three times each.
- [ ] Baseline. Record trunk wall time and steps first.
- [ ] Rule. Head wall time under trunk times 1.5, and steps within 20 percent of trunk for the same seed set. If topology change makes steps unlike, drop the ratio and require head 30 seeds under 120 s absolute with 0 failing.

**Review gate.** The operator reviews before merge.

- [ ] Copy lane 2 and lane 3 screenshots into `docs/media/cp-dsm-8-review-300.png` and `docs/media/cp-dsm-8-review-det.png`.
- [ ] Record a 30 to 60 second video of kafka-log seeds starting and the summary line. Save it as `docs/media/cp-dsm-8-review.mp4`.
- [ ] Post the screenshots and the video in chat. Stop at merge-ready. Wait for the operator's click.

**Merge.**

- [ ] Root's clean verdict at the exact head SHA.
- [ ] Bugbot triage done.
- [ ] Rebased onto current parent after the verdict, patch-id unchanged.
- [ ] Root appends. Operator lands after review click.

## Close the program

- [ ] Every box above is checked with its evidence.
- [ ] Reply to the operator with the report the execution playbook names.

## Appendix A. Prototype evidence

Open question. What end-state matches "CP as DSM and event loop" in this repo?

Prototype path. `.scratch/cp-dsm-proto/DECISION.md`.

Variants compared.

- A. CP replica is `core.Machine`, log is engine. Chosen.
- B. Keep logengine for swarm, share helpers only. Rejected. Misses the broker twin goal.
- C. Single-thread logcp outside detcheck. Rejected. Still not kernel DST.

Operator amendment after plan draft. `internal/controlplane/` must stay an autonomous extractable package for a future move out of the monorepo. Machine, run loop, and Dynamo/S3 executors live under `internal/controlplane/`. Sim and cmd adapt inward. Shared dependency is `core` only.

SHA of the decision file is whatever is in the working tree at plan write time. No runtime seed was executed in the prototype. Unproven items remain catch-up timing parity and kafka-log 300-seed green without hand re-baseline. Those are gated in CP-DSM-6 and CP-DSM-8 live lanes.

How explorers that informed the prototype.

- [Explore sim kernel DST](277eb11f-97d7-484c-b846-33f2e2157e67)
- [Explore broker DSM pattern](74d3a42a-da8f-42be-96a8-d19c80768be2)
- [Explore CP vs event loop](3bd44e61-81a0-48a0-9936-9bce0e7d5d5d)
- [Explore sim-log CP docs](471c64d6-a38f-473d-ba53-7aee2f6714ef)

## Appendix B. Alternatives rejected

Keep X2 cooperative scheduler as the long-term DST. Rejected by lesson 0. Go cannot replace `select` or the goroutine scheduler, so sans-I/O `Step` is the shared boundary.

Fold CP into the broker process. Rejected by decisions 3 and 23. CP stays a separate HTTP service.

Share apply code with veritas. Rejected by decision 21. Lockstep would only test plumbing.

Leave `logengine.state` forever as a third production-shaped copy after the machine cutover. Rejected once the real machine answers brokers. Veritas remains the oracle. A thin faulting logstore engine stays.

Put the CP run loop in `sys` next to the broker. Rejected for extractability. A future `cp` module would still need monorepo `sys`.

Let `internal/controlplane/statemachine` import `sim` helpers for tests. Rejected. Tests that need the kernel live in `internal/verify/sim/...` and import `cp`, not the reverse.

## Appendix C. Risks

CP-DSM-1. Rename + proto hard cut leaves a stray `manifester/cp` import or breaks ClassOf. Owner watches rename scan and `make proto`.

CP-DSM-1. Buf plugins drift across machines. Owner pins plugin versions in `internal/controlplane/api/buf.gen.yaml` / Go tool versions and checks `make proto` on a clean VM.

CP-DSM-2. Autonomy leaks through a convenience import from `sys` or `sim`. Owner watches the import-boundary test every PR.

CP-DSM-7. Hard cut misses a JSON client in `test/integrationtest/` or scripts. Owner greps `/v1/` and old HTTP helpers before code-ready.

CP-DSM-3. Fault coins drawn at land time instead of issue time break lesson 14. Owner watches issue-time tests.

CP-DSM-4. Pulling Pebble into the machine fails detcheck and autonomy. Owner watches import lane.

CP-DSM-7. HTTP adapter deadlocks waiting for Step. Owner watches healthz and commit lanes under `-race`.

CP-DSM-7. `internal/controlplane/controlplaneclient` today imports `manifester/sys`. Leaving that edge blocks a clean module split. Owner either injects interfaces in this PR or files a follow-up box before program close.

CP-DSM-8. kafka-log hash and step-count churn. Owner expects hand review of the scenario summary, not silent golden edits. Attrition now kills CP nodes, which changes coverage. Owner watches mutant catch seeds.

Flaky `TestMetricsAreExported` under race (`docs/flaky-tests.md`) can poison CP-DSM-7. Owner treats it as known flake and reruns before filing a machine bug.

## Appendix D. Links and reading list

Read before editing.

- `docs/lessons.md` lesson 0
- `docs/sim-log-engine.md`
- `docs/log-cp-service.md`
- `docs/decisions.md` rows 1, 9, 11, 14-23, 48-54, and the new CP-DSM plus autonomy decisions from CP-DSM-1
- `internal/core/machine.go`, `internal/system/run.go` (broker reference only), `internal/broker/broker.go`, `internal/verify/sim/kernel.go`
- `.scratch/cp-dsm-proto/DECISION.md`
- Autonomy allow-list in this plan's Spawn owners section

Green-gate commands (from exploration). Prefer these exact lines in PR boxes when they fit.

- Broker DST. `go test ./sim/kafka -count=1` and `go run ./cmd/manisim -scenario kafka -seeds 300`
- Logengine. `go test ./sim/engines/logengine -count=1` and `go test ./sim/engines -run TestConformanceLogEngine -count=1`
- Logcp X2. `go test ./controlplane/logcp -run 'TestSimSeeds|TestSimIsDeterministic' -count=1` and `LOGCP_SEEDS=20000 go test ./controlplane/logcp -run TestSimSeeds -v`
- Logsm lockstep. `go test ./controlplane/logsm -run X2Lockstep -count=1` and `LOGSM_SEEDS=100000 LOGSM_PEBBLE_SEEDS=2000 go test ./controlplane/logsm -run X2Lockstep -v -timeout 40m`
- Kafka-log. `go run ./cmd/manisim -scenario kafka-log -seeds 300`

Control surface. This repo has no in-tree `control-cli` package. Live lanes use the `control-cli` skill from `cursor-team-kit` to drive `go test` and `cmd/manisim` / `cmd/manicp` on the lane VM.

How synthesis. [How explainer CP DSM](987d55ce-7b14-4d23-a542-476071d5bf32) still named `sys.Run` as the prod twin. Superseded by the autonomy amendment. Prod twin is `internal/controlplane/runloop`.

File-pointer exploration. [CP DSM file pointers](4692b50a-d28d-4f57-8774-249d11ac7efe). PR-boundary table. [CP DSM PR boundaries](a65ea4cf-785e-4112-bb8e-7c267efa8318). Stack order in this plan wins where the table still put prod under `sys`.

Route CP-DSM-1 and CP-DSM-8 through `pstack/skills/how/SKILL.md` before coding if the owner did not write the plan.

Route CP-DSM-7 and CP-DSM-8 through `pstack/skills/interrogate/SKILL.md` before the code-ready report.

Decision trail per `pstack/skills/show-me-your-work/SKILL.md` for the program root.

## Appendix E. gRPC grill decisions (operator)

Settled in the design grill. Bind every wire-facing PR to these rules.

- Vocabulary stays (`CommitSegment`, `Lookup`, `DescribeTopics`, `CreateTopic`). The protocol is the gRPC API. Hand-written `core.CPOp` is no longer the wire contract.
- `.proto` is the source of truth. Go is generated. Local-only **buf** (Apache-2.0). No Buf Schema Registry.
- Layout is `internal/controlplane/` and `internal/controlplane/api/v1` with `package controlplane.v1`. Binary name stays `manicp` this wave.
- Errors are gRPC status plus typed `ErrorDetail` carrying `ErrCode` and `Outcome`. No OK response with an err field.
- RPCs are unary for the four ops. Also `GetStatus`. Also standard `grpc.health.v1`.
- Brokers dial the preferred control-plane address directly. Standby list for failover. No mesh sticky LB on commits.
- Hard cut from JSON HTTP. All tests green in the same stack. Prototype, not prod.
- Machine bridge keeps `CPCall` / `CPDone`. Payloads are generated `internal/controlplane/api/v1` messages. Executor performs gRPC. Sim fakes the same payloads.
- `veritas` uses the same generated messages. Apply logic stays unshared (decision 21).
- Plaintext everywhere now. TLS later.
- Client deadline or cancel without a prior success response maps to `ErrTimeout` + `MaybeApplied`.

Rejected in the grill.

- Protobuf types only at a JSON-era edge while keeping hand-written `CPOp` as protocol.
- Dual HTTP+gRPC production window.
- Always-OK responses with an err field.
- gRPC LB / mesh sticky for the appender.
- I/O inside `Step`.
- BSR-hosted modules as a build requirement.
