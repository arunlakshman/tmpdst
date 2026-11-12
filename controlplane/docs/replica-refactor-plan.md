# Replica refactor plan

Audience: one orchestrator agent in a new session, and the subagents it starts.
You have none of the earlier conversation. This file is all the context you need.

A subagent reads sections 3 and 5 and its own card. It does not need the rest.

## 1. Why this work exists

`internal/controlplane/replica` is one node's copy of the shared log. Every
metadata change goes through it: writing to the log, reading it, failover,
checkpoints and restore. A mistake here can lose data or stop the service.

The greenfield refactor (`docs/greenfield-refactor-plan.md`, finished, all gates
green) froze this package on purpose (its rule 4). So the rest of the code got
clean, and the riskiest package did not.

The user wants, in this order:

1. **Safety (P0):** prove that the tests catch mistakes in `replica`, and fix two
   tight retry loops.
2. **Readability and maintainability (P0.5):** make the package easy to read and
   safe to change.

Not a goal: fewer lines at any cost, new features, performance work.

### How the package works (read this first)

- The writer takes queued commands, packs them into one entry, and writes it to the
  next slot with a conditional put. One write is in flight at a time.
- Every node reads the log in slot order and applies each command. Offsets are
  decided at apply time.
- The writer answers a client only after it has applied the entry. An acknowledged
  write is therefore always in the log.
- Failover: after `TakeoverAfter` of silence, a standby claims a new epoch with a
  compare-and-swap, then writes a fence entry. A writer that sees a newer epoch
  steps down.
- Restart: the writer uploads a full snapshot to the object store and trims the log.
  A restarting node reads the newest snapshot, then the log after it.

Two writers can overlap without harm, because the log has one order and every node
decides outcomes from it. Do not weaken this.

### What is wrong today (checked against the code at `cd33d2b`)

| Problem | Where |
|---|---|
| Only 1 of 16 mutants targets `replica` (the simulator also has 2 fake-log bugs) | `ops/mutants.txt` |
| A failed append retries at once, with no delay, even on a permanent error | `commit.go:231-239` |
| A bad checkpoint CRC is read again at once, forever | `checkpoint.go:207-209` |
| Startup polls the replica loop with no delay | `server/node.go:209-234` |
| False coverage: "replayed key answered from the window" fires on every apply (139,097 times in 500 runs; 17 without this line) | `commit.go:386` |
| Unreachable path | `commit.go:409` |
| Dead code with no callers: `Promote`, `Demote`, `SeedTopic`, `seeds`, `reseedTopics` | `role.go:39-46`, `replica.go:140-152` |
| Two nodes with one name never fence each other | `role.go`, `observeWriter` |
| A heartbeat that really landed looks lost on retry, so an extra no-op is written | `groupIDMatches` |
| `replica` knows about one domain command | `commit.go:70` |
| About 30 fields in one struct, and about 10 overlapping flags | `replica.go:17-88` |
| Simulator-only faults live in production code | `role.go:210-242`, `config.go:79-82` |
| Hand-written binary layers around the protobuf entries | `envelope.go`, `commit.go:286-349` |
| Stale names and comments | see R3.2 and R3.3 |
| The snapshot is built on the replica loop, in memory, and its cost is not measured | `checkpoint.go:128` |

The simulator runs on virtual time. CPU work costs nothing there and request rates
do not register, so it cannot see the retry loops or a slow snapshot. Each fix for
those needs its own direct test.

## 2. Orchestrator guide

### 2.1 Your job

You run the plan. You do not write code, except merge fixes.

1. Read this file. Then do R0.1 yourself.
2. Find every card whose `Needs` are done. Start it with the Agent tool, using the
   template in section 5 and the card's `Model` and `Effort`.
3. Check the report (2.3), merge it, run the gate on the merged tree, and record the
   result in `docs/replica-refactor-progress.md`. Commit that file with the merge.
4. At the end of a phase, run the phase gate before you start the next phase.
5. Stop and ask the user when 2.4 says so. Otherwise keep going.

The work is resumable: the progress file and git say what is done.

### 2.2 Branches and merging

- Work on a local branch `refactor/replica`, made from `refactor/greenfield`.
- Every task uses `isolation: "worktree"`, on top of the current `refactor/replica`.
- Merge by cherry-pick, one task at a time. Run the gate after each merge. A task
  branch stays checked out in its worktree, so it cannot be rebased.
- Run tasks at once only if they share a `Group` letter. At most 3 at a time.
- `Serial` tasks run alone.

### 2.3 Check every report before you merge

1. It has real gate output (the last lines), not a summary.
2. `git diff --stat` touches only the `Touch` list. Anything else needs a reason.
3. Golden hashes did not move, unless the card says `RE-PIN`.
4. Every mutant is still killed, if the card's gate names mutants.
5. Nothing is reported as passing that did not run.

A failed check goes back to the same agent with SendMessage. After 2 failed tries,
see the escalation rule in 2.5.

### 2.4 Stop and ask the user if

- a golden hash moves in a task not marked `RE-PIN`, and the cause is not clear in
  15 minutes;
- a new mutant survives and shows a **real bug**: report it, do not fix it inside the
  mutant task;
- the R4.5 design note is ready (a planned stop);
- a task needs a change in `internal/system/live`, or any change in `dst`;
- something needs a push, a tag or a change on a remote;
- the R1.4 benchmark crosses its limit (see the card);
- an escalated task still fails on `opus`.

### 2.5 Keep the cost down

The user asked for cost-effective models. These rules are part of the plan.

| Model | Use it for |
|---|---|
| `haiku` | Mechanical work with an exact list: deletes, renames, comments |
| `sonnet` | The default: tests, mutants, small behaviour changes, moving fields |
| `opus` | Only R4.5 and R4.6, where a mistake is hard to see. Nothing else starts here |

- **Orchestrator:** run on `sonnet`. The cards are detailed, and it mostly merges and
  runs gates. Use `opus` only if the user says so.
- **Escalation:** a task that fails 2 times moves up one tier (`haiku` to `sonnet`,
  `sonnet` to `opus`), as a new agent that gets the failure report. Write this in the
  progress file.
- **Effort:** pass the card's `Effort` to the Agent tool. `lower` for mechanical
  cards, and for checks that verify themselves.
- **Fixes:** use SendMessage to the same agent. It keeps its context, so a fix costs
  less than a new start.
- **Do not fork.** A fork always runs on your model and carries your whole context.
- **Narrow gates in agents, full gates by you.** An agent runs only its card's gate.
  Use `make mutants MUTANT=name` for one mutant. Only you run the race, campaign and
  `make it` gates, and only at the end of a phase.
- **Short reports:** the last 15 lines of each gate and a few lines of notes. No file
  dumps, and no retelling of the card.

### 2.6 Progress file

`docs/replica-refactor-progress.md`, one line per task:

    R1.1 | done | <commit> | make mutants 22/22, golden unchanged | note

Add a section for deferred items and an audit log, newest last.

## 3. Rules for everyone

1. **Plan rule 4 of the greenfield plan is lifted for `replica` only, for this plan.**
   `internal/system/live` is still not changed. `dst` is not changed.
2. **The earlier decisions stand.** Commands in the log, events in memory. `internal/spec`
   shares types with the code, never logic. Nothing is deployed, so log bytes and
   checkpoint format may change, with no migration code. Do not change the rebalance
   protocol or the Kafka differences (`docs/kafka-differences.md`).
3. **No push.** No push, no tags, no remote changes, no force-push.
4. **One concern per commit.** Commits end with the Co-Authored-By line from the
   session reminder. Plain simple English in names, comments and docs. ASCII only.
5. **Do not add** layers, interfaces or options that no card asks for.
6. **`replica` stays deterministic:** no map order, clock, goroutines or new imports.
   `make determinism` checks this. `replica` may import only `contract`, `metadb`,
   `metadb/engine` and `runtime` (see `architecture_test.go`). Protobuf stays behind
   `metadb`.
7. **A card that does not say `RE-PIN` must not move a golden hash.**
8. **`RE-PIN` rules:**
   - Put the new pins in a commit of their own, and say why in the message.
   - Prove the cause: switch the new behaviour off for a moment, and show the old
     hashes come back. Do not commit the switch.
   - `internal/verify/simulator/golden_test.go` has two tables, `goldenHashes` and
     `goldenWithoutGroups`. A change in `replica` can move both, so check both.
   - Record the new 300-seed cpsim hashes in the progress file.
9. **Go code:** use the `use-modern-go` skill when writing or changing Go.
10. **Docker rule:** `make it` must really run. If Docker is off, report `make it NOT
    RUN`. Never say it passed.

### Traps in this code

- Generated files are not in git. A new worktree must run `make generate` first.
- `inFlightAppend.cmds` and `waiters[id]` share one backing array. `retargetCommit`
  relies on this. Do not copy one without the other.
- `refuseClients` walks `inFlightAppends`, then `queue`, then `waiters`, and skips ids
  it has seen. Clients must get exactly one answer.
- A heartbeat is an in-flight append with `cmds: nil`.
- `flushCommits` starts a new entry whenever `inFlightAppends` is empty. Anything that
  takes an entry out of the map while it still must be retried breaks the one-in-flight
  rule and the slot order.
- `nextReq` never resets. Stale answers are dropped because ids never repeat. Keep it so.
- Timers carry a generation, and a stale one is ignored. `onCheckpointTimer` does not
  re-arm when it starts a restore; a later step does.
- `StateHash` feeds the golden hash (`simulator/sim.go:264`). Adding, removing or
  reordering a fold in it moves the hash. Field moves that keep the fold order do not.
- `EntryCommands` and `EntryWriter` are used by the simulator.
- `make build` runs `gofmt -l .`, which also scans `.claude/worktrees`. It can fail in
  the main checkout while an agent has unformatted files. Run it there only when no
  agent is mid-edit.
- `make lint` fails with "parallel golangci-lint is running" if two run at once. Retry.
- `make lint` does not use the `integration` tag. Tagged code needs
  `golangci-lint run --build-tags integration ./...` by hand.
- Mutant runs fill the Go build cache. Watch the disk.
- Leave the untracked `docs/*.html` files and the worktrees under `/private/tmp/claude-502`
  alone. They are not part of this work.

## 4. Gates and baseline

| Name | Command | When |
|---|---|---|
| `build` | `make build` | after every merge |
| `golden` | the golden tests in `internal/verify/simulator`, part of `make build` | after every merge |
| `mutants` | `make mutants` (about 2 minutes) | where a card says, and in each phase gate |
| `race` | `make test-race RACE_RUNS=3` | phase gate |
| `campaign` | `go run ./cmd/cpsim -seeds 2000 -scenario all -report` | phase gate |
| `it` | `make it` (Docker on) | phase gate |
| `integration vet` | `go vet -tags integration ./...` | after any change in a signature |

Phase gate = `build`, `mutants`, `race`, `campaign`, `it`.

Baseline at `cd33d2b`. R0.1 confirms it.

- Golden: `basic/17` b8160450304fcc49, `chaos/42` fe0e8c6763239d00.
- Without groups: `basic/17` 08bfa86385d8e8ae, `chaos/42` 45d210abda0e2bed.
- 300 seeds: aaf01c86 4032d6fb f1e59c13 0609269d 951ef86c, 27 paths.
- 5000 seeds: 1040e70a fdec5cb0 ab7fbfaf fb406383 b39861c5.
- Mutants: 16 of 16 killed, 113 s.
- `replica` statement coverage: unit 66.9%, conformance 62.3%, simulator 87.7%, all
  three 91.7%. To measure:
  `go test -count=1 -coverpkg=github.com/manifesterio/mds-controlplane/internal/controlplane/replica -coverprofile=OUT PKG`
  for each of `./internal/controlplane/replica`, `./test/conformance` and
  `./internal/verify/simulator`.

## 5. Subagent prompt template

    You are doing task {ID} of docs/replica-refactor-plan.md: {title}.
    Read sections 3 and 5 of that file and the card for {ID}. Then read only the code
    you will touch. You are in your own git worktree on top of refactor/replica. Run
    `make generate` first, because generated files are not in git.
    Touch only: {Touch}. If you need more, say why in your report.
    Commit locally, one concern per commit. Do not push.
    Run this gate before you report: {Gate}. Baseline: section 4 of the plan.
    Report in under 25 lines: what changed, lines added and removed, the last 15 lines
    of each gate, the golden result, the mutant result, anything you skipped and why,
    and anything the plan got wrong. If a check is red, say so. Do not hide it.

## 6. Tasks

Line numbers are from `cd33d2b` and will drift. Find the code by its text.

### P0 Setup

**R0.1 Set up.** Serial. Done by the orchestrator, no subagent. Make the branch
`refactor/replica` (this file is already in its history). Create the progress file. Run `make build`
and `make mutants`, and compare them with section 4. Tell the user in one line if
Docker is off, because every phase gate needs it. Gate: `build`, `mutants`.

### P1 Safety net (no production changes)

How to write a mutant: see the top of `ops/mutants.txt`. One exact text replacement,
and the narrowest command that kills it. Use a unit test if one fits, else
`go test ./internal/verify/simulator -run ^TestScenariosPass$ -count=1`. A mutant that
needs a chance seed is not good enough: run it 3 times, unseeded.

For a survivor, in order:
1. Write a small test that kills it. Put the test in the card's test file.
2. If the mutant cannot change any result, drop it, and write why in the report.
3. If it shows a real bug, keep the code as it is, leave the mutant out, and report it.

Add each mutant at the end of the file. When tasks of one group both add mutants, the
orchestrator keeps both sides of the merge.

**R1.1 Mutants for the write role.** Needs: R0.1. Group A. Model: sonnet. Touch:
`ops/mutants.txt`, `replica/role_test.go`. Candidates:
- `fence-ignored`: `observeWriter` never sets `fencedFlag`.
- `claim-while-behind`: `canClaim` drops `m.applied >= m.goalTail`.
- `claim-with-broken-reads`: `claimDue` drops the `lastPullOK` check.
- `standby-commits`: `canCommit` returns `m.ready` without `m.active`.
- `active-before-fence`: `onClaimDone` sets `active` before its fence entry applies.
- `stepdown-drops-clients`: `stepDown` does not call `refuseClients`.
Done when: each one is killed, or dropped or reported with a reason.
Gate: `make mutants MUTANT=<each>`, `go test ./internal/controlplane/replica`.

**R1.2 Mutants for the commit path.** Needs: R0.1. Group A. Model: sonnet. Touch:
`ops/mutants.txt`, `replica/commit_test.go`. Candidates:
- `retry-at-new-slot`: `retrySameSlot` uses `alloc`, which leaves a hole.
- `lost-slot-counts-as-won`: `lost` is always false.
- `retarget-dropped`: `retargetCommit` returns true without changing the waiter.
- `answers-misordered`: `deliver` gives the first answer to every command.
- `queue-limit-ignored`: `Offer` skips the `queueMax` check. This path has no test
  today, so write one.
- `below-floor-is-commit`: `isBelowFloor` always returns false.
Done when: as R1.1. Gate: as R1.1.

**R1.3 Mutants for checkpoint and restore.** Needs: R0.1. Group A. Model: sonnet.
Touch: `ops/mutants.txt`, `replica/checkpoint_test.go`, `replica/trimmed_test.go`.
Candidates:
- `trim-after-failed-upload`: `onObjPutDone` trims even if the put failed.
- `restore-oldest`: `onObjListDone` reads `d.Keys[0]`.
- `list-failure-means-empty`: a failed list is treated as no checkpoint.
- `catch-up-through-trim`: `noCheckpointFound` drops the `needsCheckpoint` guard.
- `ready-before-restore`: `maybeBecomeReady` drops its guard on `needsCheckpoint`.
- `trim-too-far`: `emitTrim` trims past the checkpoint (`checkpointSlot + keep`).
- `checkpoint-behind-tail`: `maybeCheckpoint` drops the guard. This one may have no
  effect, because every checkpoint has its own key. If so, drop the mutant and report
  that the comment on the guard is stale, so R3.2 can fix it.
Done when: as R1.1. Gate: as R1.1.

**R1.4 Measure the snapshot.** Needs: R0.1. Group A. Model: sonnet. Effort: lower.
Touch: `internal/controlplane/metadb/bench_test.go` (it exists; reuse what it has). Benchmark
`Snapshot` and `Restore` on the Pebble engine with 10k, 100k and 1M keys of a realistic
size (`engine/pebble`, topics and segments). Report the time, bytes and memory. Change
no production code. **Limit:** if `Snapshot` takes more than 1 s at any size the user
could reach, stop and ask, because a stalled loop stops heartbeats and the default
takeover is 5 s. Gate: `go test -run '^$' -bench Snapshot ./internal/controlplane/metadb`.

**Phase gate P1.** Expect 30 or more mutants, golden unchanged.

### P2 Fix the risky behaviour

**R2.1 Backoff on a failed append.** Needs: P1. Group B. Model: sonnet. Effort: higher.
RE-PIN. Touch: `replica/commit.go`, `replica/replica.go`, `replica/commit_test.go`,
`replica/config.go`, `simulator/golden_test.go`, `ops/mutants.txt`.
Today a failed append retries at once (`onCommitAppendDone`, `retrySameSlot`). Change:
- Wait on a new timer with a generation before the retry. Start at 10 ms, double each
  time, cap at 1 s, and reset after a successful append. No random jitter.
- **Keep the entry in `inFlightAppends` while it waits.** Otherwise `flushCommits`
  starts a new entry and breaks the slot order, and `refuseClients` misses it. Use a new
  field or a reserved id, your choice, and drop the timer on `stepDown` and `clearLocal`.
- A permanent error (`ErrInvalid`) also uses the backoff. It does not crash.
- `heartbeatDue` must stay false while a retry waits.
Tests: failed appends emit no `LogAppend` before the timer fires, the delay doubles and
caps, a stale timer does nothing, and stepping down answers the waiting clients once.
Add a mutant `append-retry-no-wait`. Gate: `build`, `make mutants`.

**R2.2 Backoff on a bad checkpoint.** Needs: R2.1. Serial. Model: sonnet. RE-PIN (the
`Corrupt` fault reaches this path). Touch: `replica/checkpoint.go`,
`replica/checkpoint_test.go`, `simulator/golden_test.go`, `ops/mutants.txt`.
In `onObjGetDone`, a bad CRC sets `checkpointRetry` and calls `armCheckpointRetry`,
instead of reading again at once. The next try lists again, so a newer checkpoint heals
it. Test: a permanently bad object emits no IO until the timer, and a newer key is used
once it appears. Add a mutant. Do not add a fallback to an older checkpoint (section 7).
Gate: `build`, `make mutants`.

**R2.3 Startup wait.** Needs: P1. Group B. Model: sonnet. Effort: lower. Touch:
`server/node.go`, `server/node_test.go`. `waitReady` polls with no delay. Wait 10 ms
between polls, using `n.config.Clock`, and keep all the current exit paths. Test: a node
that is not ready gets only a bounded number of polls. Golden must not move (the
simulator does not use `server`). Gate: `build`, `go test -race ./internal/controlplane/server`.

**R2.4 Fence same-name writers.** Needs: P1. Group B. Model: sonnet. Effort: lower.
Touch: `replica/role.go`, `replica/role_test.go`, `ops/mutants.txt`. In `observeWriter`,
check `epoch > m.epoch` before the early return for the node's own name. A node's own
entries never carry a higher epoch than the one it holds, so a normal run is unchanged.
Test: a writer that reads an entry with its own name and a higher epoch steps down. Add
a mutant. Golden must not move. Gate: `build`, `make mutants MUTANT=<name>`.

**Phase gate P2.**

### P3 Delete, rename, remove leaks

**R3.1 Delete dead code.** Needs: P2. Serial. Model: haiku. Effort: lower. Touch:
`replica/role.go`, `replica/replica.go`, `replica/commit.go`. Delete:
- `Promote` and `Demote` (keep `dropRole`).
- `SeedTopic`, `topicSeed`, the `seeds` field, `reseedTopics` and its call.
- The `Reachable` line in `afterApply` (`commit.go:386`). Keep the one in `Offer`.
- The unreachable `if` and `Reachable` inside `deliver` (`commit.go:408-410`).
Search before each delete, and stop if a caller exists. Golden must not move. If it does,
report it and do not re-pin. Gate: `build`.

**R3.2 Fix stale comments.** Needs: R3.1. Group C. Model: haiku. Effort: lower. Touch:
comments only in `replica/*.go`, `server/brokers_handler.go`, `server/doc.go`,
`metadb/doc.go`, `executor/doc.go`, and `docs/lint-baseline.md`. Fix:
- `decision 8`, `decision 48`, `decision 55`, `decision 56`, and every `CP-DSM-N`:
  remove the reference, and say the reason in plain words if it is not clear.
- `role.go:15`: remove the link to the missing `docs/writer-fencing-design.md`.
- `commit.go:16`: "kinds 0..2" is wrong.
- `role.go`: "old fakelogservice" is gone.
- `server/brokers_handler.go`: `api/v1` and `system/production/lease.go` are gone.
- `server/doc.go`: "Offer/fold reads on the run-loop goroutine".
- `metadb/doc.go`: "NewMem ... without Pebble", "replica Step".
- `replica.go`: the `StateHash` comment says "every field". It does not. Say what it holds.
- `checkpoint.go`: the guard comment, if R1.3 found it stale.
- `docs/lint-baseline.md`: "tasks 7, 9, 23, 25" means nothing now.
No code changes. Gate: `build`.

**R3.3 One set of names.** Needs: R3.1. Group C. Model: haiku. Touch: `replica/*.go`,
`cmd/cpsim/campaign.go`, `cmd/cpsim/*_test.go`, `docs/glossary.md`, and panic and error
strings in `metadb/*.go`, `domain/topic/*.go`, `domain/group/*.go`. Renames:
- The assertion prefix `logcp-sim:` becomes `replica:`, in code and in `expectedReachable`.
- Words in comments and names: `fold` becomes `state` or `apply`, `DSM` and "machine"
  become `replica`, "advisory role" becomes `write role`. The glossary is the source.
- `fencedFlag` becomes `fenced`. `openMetadata` becomes `openDB`. `meta` becomes `db`.
- Error prefixes: `metadata:` becomes `metadb:`, `topics:` becomes `topic:`, `groups:`
  becomes `group:`.
Do not rename the receiver `m`, or exported names used outside `replica`. Check that no
test matches an old error text. Golden must not move: if assertion names turn out to
feed the hash, stop and report. Gate: `build`, `integration vet`.

**R3.4 Move simulator faults out of production.** Needs: R3.2, R3.3. Serial. Model:
sonnet. RE-PIN. Touch: `replica/role.go`, `replica/commit.go`, `replica/config.go`,
`replica/replica.go`, `simulator/scenario.go`, `simulator/golden_test.go`,
`cmd/cpsim/campaign.go`, `docs/simulation.md`.
Delete `StallProb`, `CrashProb`, `StallDur`, `DownDur`, `maybeReplicaFault`, `softCrash`,
`betweenDur`, and the fields `crashed`, `stallUntil`, `downUntil`. The scenarios already
have real faults (`CrashEvery`, `PauseEvery`), so raise those to keep the pressure where
`SoftFault` was in use (`crash-restart`, `chaos`). `afterCrashRestart` stays, because
`hitTrimmed` uses it; rename it `restartFromCheckpoint`. Move the two lost coverage paths
(`replica crashed`, `replica stalled`) to the simulator, or remove them from
`expectedReachable` with a reason. Check that `make mutants` still kills everything, and
that the report has no never-reached path. Gate: `build`, `make mutants`, campaign 2000.

**R3.5 General idempotency key.** Needs: R3.3. Group D. Model: sonnet. Touch:
`replica/commit.go`, `replica/commit_test.go`, `metadb/entry.go`, `metadb/topics.go`.
`Offer` and `retargetCommit` ask whether a command is a `CommitSegment`. Give `metadb`
two general calls: `Entry.RetryKey() (string, bool)` and `DB.KnownAnswer(Entry)`.
`replica` uses only those, and has no `CommitSegment` left in it. Behaviour is the same.
Golden must not move. Gate: `build`, `make mutants`.

**Phase gate P3.**

### P4 Reshape

**R4.1 One protobuf log entry.** Needs: P3. Serial. Model: sonnet. Effort: higher. RE-PIN
allowed. Touch: `api/controlplane/log/v1/log.proto`, `metadb/entry.go`,
`metadb/entry_codec.go`, `metadb/fuzz_test.go`, `replica/commit.go`, `replica/envelope.go`,
`replica/replay.go`, `replica/entrysize_test.go`, `replica/export_test.go`, `Makefile`
(fuzz list), `ops/tools/protoc-gen-mdswire` only if needed.
Add a log message `Slot { epoch, writer, id, repeated Entry commands }`, where a heartbeat
or fence has no commands. Replace the `0xC8` and `0xC5` layers and the single-entry path.
Encode and decode live in `metadb`, because `replica` may not import protobuf. Keep the
T4.4 rules (`docs/dep-audits/google.golang.org/protobuf@v1.35.2.md`): deterministic
marshal, no maps or extensions, reject unknown fields. Keep the public shape of
`EntryCommands` and `EntryWriter`. Recheck that the size limit holds, using
`TestDefaultLimitKeepsEntriesUnderTheDynamoDBItemLimit`. Add a fuzz target. Compare ids
for every entry, so the heartbeat false "lost" is gone. Add a test for that, and a mutant.
A truncated or corrupt entry still ends the replica with a Crash. Delete `envelope.go`,
`encodeGroupParts`, `decodeGroup`, `takeUvarint`. Gate: `build`, `make mutants`, `make fuzz`.

**R4.2 Split the struct: checkpoints.** Needs: R4.1. Serial. Model: sonnet. Touch:
`replica/replica.go`, `replica/checkpoint.go`, `replica/role.go`, tests only if needed.
Move `checkpointSlot`, `checkpointAt`, `checkpointPending`, `checkpointData`,
`checkpointRequest`, `checkpointStage`, `trimThrough`, `checkpointGen`, `restoreKey`,
`needsCheckpoint` and `checkpointRetry` into a small struct `checkpoints`, as a field of
`Replica`. Move a method onto the new struct only if it touches nothing else. No change
in behaviour, and the fold order in `StateHash` stays the same. Golden must not move.
Gate: `build`, `make mutants`.

**R4.3 Split the struct: commit queue.** Needs: R4.2. Serial. Model: sonnet. Touch: as
R4.2, with `replica/commit.go`, `replica/reads.go`. Move `queue`, `inFlightAppends`,
`waiters`, `answers`, the backoff fields of R2.1, `seq`, `high`. Rules and gate as R4.2.

**R4.4 Split the struct: catch-up and write role.** Needs: R4.3. Serial. Model: sonnet.
Touch: as R4.2. Make `catchUp` (`applied`, `goalTail`, `buf`, `pending`, `pendingKind`,
`timerGen`, `tailChecked`, `readWaiters`, `tailsStarted`, `tailRound`) and `writeRole`
(`epoch`, `active`, `claimReq`, `claimExpect`, `fenced`, `stepDowns`, `maxEpochSeen`,
`roleGen`, `lastPullOK`, `lastOtherAt`, `lastOwnAt`). `Replica` should end with about 12
fields. Rules and gate as R4.2.

**R4.5 Design note for explicit states. STOP.** Needs: R4.4. Serial. Model: opus. Touch:
a new `docs/replica-states.md`, under 120 lines. Read `domain/rebalance/machine.go`, which
is the model to follow. List every flag left (`started`, `stopping`, `exited`, `booting`,
`ready`, `active`, and the checkpoint flags). Build the table of which combinations can
really happen, with a test or a code path as evidence. Propose one state type (for example
Starting, Restoring, CatchingUp, Standby, Claiming, Writer, Stopped), with allowed moves
and which input each state takes. Say what would be deleted, how the hash stays the same,
and how the mutants cover it. **The orchestrator stops here and shows the user the note.**
The plan may also end here (section 8). Gate: none.

**R4.6 Explicit states.** Needs: R4.5 and the user's OK. Serial. Model: opus. Effort:
higher. Touch: `replica/*.go`, tests. Do what the note says. The flags go away. `Step`
dispatches on the state, as `rebalance` does. No change in behaviour, so golden should not
move. If it must, stop and ask. Gate: `build`, `make mutants`, `race`, campaign 2000.

**R4.7 Complete the state hash.** Needs: R4.6, or R4.4 if the plan ended at the stop. Serial.
Model: sonnet. Effort: lower. RE-PIN. Touch: `replica/replica.go`,
`simulator/golden_test.go`. Add the behaviour fields that `StateHash` leaves out, such as
the buffer size and the time fields. Keep its comment true. Gate: `build`, `mutants`.

**Phase gate P4.**

### P5 Cover the rest

**R5.1 Unit tests for failure paths.** Needs: P4. Group E. Model: sonnet. Effort: lower.
Touch: `replica/*_test.go`. Add table tests for what no suite reaches:
- A corrupt or truncated log entry: Crash, and no state change.
- `Snapshot` and `Restore` failure, and a store that cannot reopen: Crash.
- A trimmed log with no checkpoint: retries, and does not catch up from 0.
Find the current gaps by running the coverage commands of section 4. Gate:
`go test ./internal/controlplane/replica`, and report coverage for all three suites.

**R5.2 Buggify sites in `replica`.** Needs: P4. Group E. Model: sonnet. RE-PIN. Touch:
`replica/`, `simulator/`, `golden_test.go`, `docs/simulation.md`. Follow
`domain/rebalance/sites.go`: faults come through `Config.Faults`, nil in production. A site
only does what a slow host or a replayed message could do. Add: a late timer, a stale timer,
an append answer that comes twice, a claim answer that comes late. All must fire in a
campaign. Gate: `build`, `make mutants`, campaign 2000, with none in "never fired".

### P6 Finish

**R6.1 Docs.** Needs: P5. Serial. Model: haiku. Touch: `replica/doc.go`, `docs/glossary.md`,
`internal/controlplane/doc.go`, `docs/simulation.md`. Make them true. `doc.go` should be
one page that a new reader can follow, including the states if R4.6 ran. Gate: `build`.

**R6.2 Final acceptance.** Done by the orchestrator. Check section 8 and write the numbers
in the progress file. Do not merge to `main`. Tell the user what is left.

## 7. Open decisions (use the default; do not stop)

| Question | Default | Who decides later |
|---|---|---|
| Keep an older checkpoint as a fallback? | No. Only the backoff in R2.2 | User, at the end |
| Permanent append errors? | Backoff forever (R2.1) | User, at the end |
| Move the snapshot off the loop? | No, unless R1.4 crosses its limit | User, after R1.4 |

## 8. Done means

Stop point: **if time runs short, end after R4.4.** P1 to P3 and the split are worth having
alone. R4.5 and R4.6 are the riskiest, and need the user's OK anyway. If the plan ends there,
still run R4.7, R5.1 and R6.1.

- Fresh clone: `make build` green with Docker off. `make it` green with Docker on.
- `make test-race RACE_RUNS=5` green. `go run ./cmd/cpsim -seeds 5000 -scenario all -report`
  clean, with nothing in never reached or never fired.
- `make mutants` kills all of them, with at least 15 that target `replica`.
- Coverage of `replica` is at least the 91.7% baseline, measured the same way.
- No `Promote`, `Demote`, `SeedTopic`, `StallProb`, `CrashProb`, `CP-DSM`, `logcp-sim`,
  `0xC5`, `0xC8`, or `CommitSegment` left in non-test `replica` code.
- `Replica` has about 12 fields. With R4.6, one state in place of the flags.
- `internal/system/live` has no diff against `refactor/greenfield`.
- Nothing pushed.

Report honestly: lines of production and test code against the baseline (the goal is
clarity, so growth in tests is fine), what was skipped, and which RE-PINs happened and why.

## 9. Not in this plan

- The two integration tests in `docs/integration-test-gaps.md`. Do them as separate work.
- TLS and auth on the gRPC server, and the kind and Helm deploy test.
- Changes to the rebalance protocol or to `dst`.
- More than one append in flight, or other throughput work.
