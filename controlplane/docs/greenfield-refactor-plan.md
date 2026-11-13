# Greenfield refactor plan: mds-controlplane

Audience: one orchestrator agent, and the subagents it hands tasks to.
This file replaces `internal/DDD-REFACTOR-BRIEF.md` (task T0.5 deletes the brief).

A subagent reads section 2, section 4 and its own task card. It does not need the rest.

## 1. Orchestrator guide

### 1.1 Your job

You run the plan. You do not write the code, except for merge fixes.

1. Read this whole file. Create `docs/refactor-progress.md` if it is missing.
2. Find every task whose `Needs` are done and is not started.
3. Start them with the Agent tool. Use the template in section 4, and the `Model` and `Effort` of the card.
4. When a subagent reports, check the report (1.4). Merge its work into the work branch.
5. Run the gate named on the card on the merged result. Write the result in the progress file.
6. At the end of a phase, run the phase gate (section 3) before you start the next phase.
7. Stop and ask the user when a rule in 1.5 says so. Otherwise keep going.

The work is resumable: the progress file and git say what is done. If your context is lost, read
both and go on from there.

### 1.2 Branches and worktrees

- Work on a local branch `refactor/greenfield`, made from `main`. Never commit on `main`.
- Every task runs with `isolation: "worktree"`, on top of the current `refactor/greenfield`.
- Merge one task at a time, and run the gate after each merge.
- Generated files are not in git. A new worktree has no `*.pb.go`, so the subagent must run
  `make proto` first (after T0.4: `make generate`).
- `build/` is not in git and is not shared. Keep baselines in the main checkout, and give subagents
  the absolute path.

### 1.3 Running tasks at the same time

- Tasks with the same `Group` letter may run at the same time. Their `Touch` lists do not overlap.
- Tasks marked `Serial` run alone. Nothing else runs while they run.
- Phases P3 and P4 are mostly serial, because they rename things that every package imports.
- At most 4 subagents at a time. If two running tasks turn out to need the same file, let the first
  finish and restart the second on the merged result.

### 1.4 Check every report before you merge

1. The report has real gate output (the last lines), not a summary.
2. `git diff --stat` touches only the `Touch` list of the card. Anything else needs a reason.
3. The golden hashes did not move, unless the card says `RE-PIN`.
4. Every mutant is still killed, if the card names `make mutants`.
5. A test that was deleted is either named on the card, or its bugs are caught by a named test.
6. Nothing was reported as passing that did not run. See the Docker rule in section 2.

If a report fails a check, send it back to the same subagent with SendMessage. After two failed
tries, stop and ask the user.

### 1.5 Stop and ask the user if

- a golden hash moves in a task not marked `RE-PIN`, and the cause is not clear in 15 minutes;
- a mutant survives and the fix is not an obvious harness change;
- a task needs real changes in `replica` or `internal/system/live` (section 2, rule 4);
- a task would change `dst` in a way this plan does not name;
- the known flake (section 3) fails twice in a row;
- the rebalance model and the rebalance machine disagree, and it is not clear which one is right;
- something would need a push, a tag, or a change on a remote.

### 1.6 The progress file

`docs/refactor-progress.md` has one line per task:

    T1.2 | done | <commit> | make build green | note
    T2.2 | done | <commit> | make it NOT RUN (no Docker) | run it before release

Add a section for deferred items. Commit the file with each merge.

## 2. Rules for everyone

Decisions the user has made. Do not reopen them.

1. **Greenfield.** Nothing is deployed. Log bytes, stored keys, checkpoint format and `api/v1` may all
   change. No compatibility code, no migration code.
2. **Commands in the log, events in memory.** The log holds commands. The outcome (such as offsets) is
   decided when the command is applied, on every replica. Events are made at apply time and are never
   stored. A writer that decides outcomes before it appends works from stale state: that is the
   planted bug `writer-offsets`.
3. **The model stays independent.** `internal/spec` shares types with the code, never logic. Nothing in
   `internal/spec` imports `domain`, `metadb` or `engine`. Never test the code against itself.
4. **`replica` and `internal/system/live` are out of scope.** Allowed in `replica`: renames of imports
   and types forced by P3 and P4, and the removal in T1.6. Nothing else. `live` is not changed at all.
5. **No Docker in `make build`.** Tests that need containers use the `integration` build tag and run
   with `make it`. `make it` fails when Docker is missing. It never skips. If Docker is off, the report
   says `make it NOT RUN`, and the progress file records it as deferred. Never say it passed.
6. **Never push.** No push, no remote tags, no remote changes, no force-push. Keep the module paths
   `github.com/manifesterio/dst` and `github.com/manifesterio/mds-controlplane`, and the temporary
   `replace` in `go.mod`.
7. **`dst`:** only the change in T1.10 is agreed. `dst` is at `~/tmp/dst`, branch `dst-process`. The
   simulation report stays in this repo.

Working rules:

- One concern per commit. Commits end with the Co-Authored-By line from the session.
- Plain simple English in names, comments and docs (`docs/simple-english-names.md`). ASCII only.
- Test files sit next to the code they test, in the same package. The only exceptions are
  `test/conformance` and `test/integration`.
- Do not add new layers, interfaces or options that no task asks for. Deleting code is the goal.
- New third-party imports in a gated package need the `audit-go-dependency` skill.

## 3. Gates

| Name | Command | When |
|---|---|---|
| `build` | `make build` | after every merge |
| `golden` | the golden test in `internal/verify/simulator`, part of `make build` | every merge; may move only in `RE-PIN` tasks |
| `mutants` | `make mutants` (exists after T1.1) | end of each phase from P1, and where a card says so |
| `race` | `make test-race RACE_RUNS=3` | end of each phase |
| `campaign` | `go run ./cmd/cpsim -seeds 2000 -scenario all` | end of each phase |
| `it` | `make it` (exists after T2.1) | where a card says so, and at the end of each phase from P2 |

Phase gate = `build`, `mutants`, `race`, `campaign`, and `it` if Docker is on.

Golden hashes may move only in tasks marked `RE-PIN`: T1.10, T4.4 and T6.5. Update the pins in
`internal/verify/simulator/golden_test.go` in a commit of its own, and say why in the message.

Known flake: `TestConformReplica` timed out once on the 3 s call limit under `-race`. If it is the only
failure, run it once more alone and report it as the known flake. A second failure is real. There is
also one unexplained failure under `-race` with heavy parallel load. Report it if you see it again.

## 4. Subagent prompt template

    You are doing task {ID} of docs/greenfield-refactor-plan.md: {title}.
    Read sections 2 and 3 of that file, and the card for {ID}. Then read the code you will touch.
    You are in your own git worktree on top of refactor/greenfield. Run `make proto` (after T0.4:
    `make generate`) first, because generated files are not in git.
    Touch only: {Touch}. If you need more, say why in your report.
    Commit locally, one concern per commit. Do not push.
    Run this gate before you report: {Gate}. Baselines: {absolute path}.
    Report: what changed, what you deleted, the number of lines added and removed, the last 15 lines
    of each gate, the golden result, the mutant result, anything you skipped and why, anything you
    found that the plan got wrong. If a check is red, say so. Do not hide it.

Model choice: `haiku` for mechanical work, `sonnet` by default, `opus` for design and for code where a
mistake is hard to see. Run the orchestrator itself on `opus`.

## 5. Target shape

    internal/controlplane/
      api/                 .proto files only (generated Go is not in git)
      contract/            generated plain value types, plus a few hand-written helpers
      wire/                generated converters
      domain/
        topic/             decide.go events.go tables.go codec.go schema.go
        group/             the same files
        rebalance/         the group protocol as a pure state machine
        broker/            broker leases
      metadb/              the database: runs commands on schemas, log entry format, checkpoint, state hash
      metadb/engine/       the storage engine interface (was kv)
      metadb/engine/memory/ and metadb/engine/pebble/
      replica/             unchanged, except rule 4
      executor/            unchanged, except its tests
      server/              topics_handler.go groups_handler.go brokers_handler.go status_handler.go node.go
    internal/spec/         the independent model, now also with the rebalance model
    internal/testinfra/    containers for tests (integration tag only)
    test/conformance/      the model conformance tests
    test/integration/      end-to-end tests on the real binary (integration tag only)

Deleted in the end: `controlplanetest` and everything in it, `groups`, `groups/storage`, `metadata`,
`module`, `kv`, `topics`, `membership`.

The mental model: `engine` is like InnoDB, and `metadb` is like the MySQL server on top of it. A schema
(`topic`, `group`) owns its tables. One difference: durability is in the replicated log, not in the
engine. The engine can be thrown away and rebuilt from the log and a checkpoint.

The pattern in `domain/topic` and `domain/group`:

    Decide(view, command) -> (events, error code)    pure rules, no writes
    Apply(txn, event)                                writes the event to the tables

There are no aggregates, repositories or per-command services. The log already puts every command in
one order, so aggregates are not needed for concurrency. A `CommitSegment` spans many topics in one step,
so a `Topic` aggregate would be the wrong boundary.

## 6. Tasks

Card fields: `Needs` (tasks that must be done first), `Group` (same letter may run at once) or `Serial`,
`Model`, `Effort`, `Touch` (where the work is), `Gate`.

### Phase P0: baseline and easy cleanup

**T0.1 Freeze the baseline.** Needs: none. Serial. Model: haiku. Effort: lower. Touch: `build/` only.
Record `go run ./cmd/cpsim -scenario all -seeds 300 -first 1` and a full `make build` in the main
checkout's `build/`. Note whether Docker is running. Done when: both files exist and the gate is green.
If `main` is not green, stop and ask. Gate: `build`.

**T0.2 Delete `api/v1/proto_test.go`.** Needs: T0.1. Group A. Model: haiku. Effort: lower.
Touch: that file. It only tests the protobuf library. Gate: `build`.

**T0.3 Rename the server files.** Needs: T0.1. Group A. Model: haiku. Effort: lower.
Touch: `server/`. `topics.go`, `groups.go`, `brokers.go` and `status.go` become `*_handler.go`, and their
tests follow. `node.go` stays. Fix the comments that name these files. Gate: `build`.

**T0.4 Stop committing generated files.** Needs: T0.1. Group A. Model: sonnet. Touch: `Makefile`,
`.gitignore`, `deploy/docker/`, `.github/workflows/`, `ops/scripts/`, the three `*_gen.go` files.
Remove `contract/wire_types_gen.go`, `groups/wire_types_gen.go` and `wire/wire_gen.go` from git and ignore
them. Add `make generate` (proto, then wiregen). Remove `wire-check`. Make `make build`, the Dockerfile
and the CI job run generation first, because they cannot compile without it. CI stays a determinism
check for now: it cannot do more until the `dst` `replace` is gone.
Done when: a clean clone plus `make build` is green. Check this with a fresh `git clone` into a temp
folder. Gate: `build`.

**T0.5 Delete the old brief.** Needs: T0.1. Group A. Model: haiku. Effort: lower. Touch:
`internal/DDD-REFACTOR-BRIEF.md`, `docs/test-cuts.md`. Delete the brief. Add a one-line note at the top
of `test-cuts.md` that this plan replaces its later steps. Gate: `build`.

### Phase P1: the new safety net

Everything after this phase depends on it, so it comes first. The old suite stays until T1.5 proves the
new one catches the same bugs.

**T1.1 Mutants without flags in production.** Needs: P0. Serial. Model: sonnet. Effort: higher.
Touch: `ops/tools/mutate/` (new), `ops/mutants.txt` or similar (new), `Makefile`.
A small tool. A mutant is one exact text replacement: a file, the old text, the new text. For each mutant
the tool copies the tree to a temp folder, applies it, and runs a named test command. The mutant is
killed when that command fails. It fails loudly if the old text is not found exactly once, so a mutant
cannot rot without notice. Add `make mutants`. Do not add it to `make build`.
First mutants, all in real code: segment replay gets new offsets, `FindSegments` stop rule off by one,
`DescribeTopics` not sorted, `writer-offsets`, `per-replica-window`. Add at least 3 for group state:
generation check dropped, assign to an unknown member allowed, offset commit without the member check.
The other bugs stay as test doubles: `forgetful` (crash loses answered writes), `staleReads` (no data
after a failure), and the simulator's `log-overwrite` and `claim-always-wins`.
Done when: every mutant is killed by the current suite. Gate: `build`, `mutants`.

**T1.2 New conformance harness.** Needs: T1.1. Serial. Model: opus. Effort: higher.
Touch: `test/conformance/` (new), `internal/spec` only if it finds a real model bug.
Follow Bornholt et al., SOSP 2021, sections 4 and 5. Keep it small: target about 500 lines in all.

    type Target struct {
        Exec        func(ctx, op) (Done, error)
        Reboot      func()          // nil: this target has no such operation
        DirtyReboot func(keep int)
        Checkpoint  func()
        Inject      func(kind int)
    }
    func Run(t, newTarget func(*rapid.T) Target)

- A nil function means the operation is never drawn. This replaces the six flags in `options` and the
  two optional interfaces. A target with `Inject` draws only operations that are safe to send twice.
- `ops.go` is the list of operations, ordered from simple to complex, because the shrinker prefers the
  first ones. Each has a short comment. The name "alphabet" goes away.
- Background operations do nothing in the model. After a restart the harness compares the whole state
  and replays every earlier commit.
- Keep bias small, and only where the existing evidence shows it is needed (small name sets, replay and
  conflict draws, the current generation, the `MaxBytes` edges).
- Keep the "has failed" rule: after an injected failure an error is allowed, a wrong answer never is.
- `Run` counts answers by operation and code, and fails if an expected answer is never seen, as
  `requireReached` does today.
First target: `metadata` on the memory and Pebble engines, with a local slot counter private to this test
package. Gate: `build`.

**T1.3 Replica target: restart, crash, failures.** Needs: T1.2. Group B. Model: opus. Touch:
`test/conformance/replica_test.go` and helpers. Port `disk`, `process`, `nodeSystem` from `crash_test.go`
in fewer lines. Variants: with and without log trim, with and without failures. Gate: `build`.

**T1.4 gRPC target.** Needs: T1.2. Group B. Model: sonnet. Touch: `test/conformance/grpc_test.go`. The real
client, server and replica. Call the generated gRPC client directly, with a small test helper. Do not
import `controlplanetest.Client`. Gate: `build`.

**T1.5 Prove it, then delete the old suite.** Needs: T1.3, T1.4. Serial. Model: sonnet. Effort: higher.
Touch: `Makefile`, `ops/mutants*`, `controlplanetest/*_test.go`, `controlplanetest/memorystore`,
`internal/spec/README.md`.
Point every mutant and test double at the new suite. Only when all are killed, delete `conform_test.go`,
`alphabet_test.go`, `crash_test.go`, `bugs_test.go`, `roundtrip_test.go` and `memorystore`. Move
`TestRoundTrip`'s useful checks to the gRPC target or drop them if covered. `TestConformModelTwice` and
`TestConformMemorystore` go without a replacement: both compare the model with itself. Update `spec-long`
and the README. Done when: `make mutants` kills everything with the old suite gone, and the time to run
the new suite is no longer than the old. Gate: phase-level `mutants`, `build`.

**T1.6 Remove planted bugs from production.** Needs: T1.5. Serial. Model: sonnet. Touch: `replica/config.go`,
`replica/commit.go`, `replica/replica.go`, `replica/role.go`, `metadata/entry.go`, `metadata/topics.go`,
`topics/commit.go`, `verify/simulator/scenario.go`, `verify/simulator/services.go`, `simulator_test.go`.
Delete `InjectedBug`, `WriterBases`, `CommitWithBases`, `commitWithBases`, `assignAt`, `projectBases` and
`proj`. `writer-offsets` becomes a text mutant run by the simulator. This is the exception in rule 4.
Golden must not move, because normal runs never took these paths. Gate: `build`, `mutants`.

**T1.7 Move helpers out of `controlplanetest`.** Needs: T1.5. Group C. Model: sonnet. Touch:
`controlplanetest/{groupfix,kvtest,storecall,metadatastore}`, and the tests that use them in `groups`,
`groups/storage`, `metadata`, `topics`.
Delete these four packages. Inline small fixtures in each test package. Tests that need a store for the
coordinator use `spec.New()` directly as the mock (the paper does this, and it keeps the model up to
date). Unit tests in other packages must not import `controlplanetest` after this.
Done when: `grep` shows only `fakedynamodb`, `client.go` and `grpcserver.go` left there. Gate: `build`.

**T1.8 Fuzz the decoders.** Needs: T1.5. Group C. Model: sonnet. Touch: `*_test.go` next to the decoders in
`metadata`, `topics`, `groups/storage`. Add native Go fuzz tests: `Decode` and each record decoder must
return an error or a value on any bytes. The repo has none today. Note the panics that are on purpose
(fail-stop on corrupt state), and keep them. Add a short `make fuzz`, not in `make build`. Gate: `build`.

**T1.9 Simulation report.** Needs: T1.5. Group C. Model: sonnet. Touch: `cmd/cpsim/`,
`internal/verify/simulator/` (only `sim.go` and `run.go` if needed), `docs/simulation.md`.
Ideas from moonpool's `SimulationReport`. Build a `Report` in this repo from `dst/campaign`: seeds run,
passed and failed; failed seeds with their replay command, listed first; every always and sometimes
assertion with checks and passes; assertions never reached; a coverage curve (new assertions seen against
seed number) and the seed where it stopped growing. Add `-until-stable` (keep running until no new
assertion appears for N seeds, up to a limit), and print a plain warning if the limit was hit first.
Replace the hand-kept `expectedReachable` check only if the replacement catches a path that stops being
reached. Add `-json` output. Do not copy multiverse exploration or edge coverage. Golden must not move.
Gate: `build`, `campaign`.

**T1.10 Buggify in `dst`, and first use.** Needs: T1.9. Serial. Model: sonnet. Effort: higher. RE-PIN.
Touch: `~/tmp/dst/fault/`, `~/tmp/dst/campaign/`, `docs/dep-audits/github.com/manifesterio/dst@v0.3.0.md`,
`internal/verify/simulator/`, `cmd/cpsim/`.
`dst/fault.Injector` already has the buggify mechanism: `Maybe` and `MaybeP` switch a site on for the run,
then fire by chance. Today no product code calls them (only `Fault`, in `replica/role.go`). So add what
is missing, and no more: per-site counts in the injector (was on, times fired), carried to the campaign
summary and shown in the report, and a site that never fired across a campaign is listed. Then add sites in the
simulator's log and object store engines (slow answer, short read page) and later in `rebalance` (T6.2).
Sites in `replica` are out of scope. Commit `dst` locally on `dst-process`, do not push, and update the
audit record. Gate: `build`, `campaign`, phase gate for P1.

### Phase P2: real services through testcontainers

**T2.1 Test infrastructure.** Needs: P1. Serial. Model: sonnet. Touch: `internal/testinfra/` (new), `go.mod`,
`go.sum`, `Makefile`. All files have the `integration` build tag.
`testinfra.DynamoDB(t)`, `Postgres(t)` and `S3(t)`, each with one shared container per test package and a
fresh table, schema or bucket per test. Pin image versions (`amazon/dynamodb-local`, the Postgres image
the Makefile pinned, SeaweedFS for S3). Add `make it`: `go test -tags integration ./...`, which fails when Docker
is off. Check that `go list -deps ./cmd/mdscontrolplane` does not list testcontainers.
Gate: `build`, `it` (needs Docker; if off, see rule 5).

**T2.2 DynamoDB.** Needs: T2.1. Group D. Model: sonnet. Touch: `executor/dynamolog/*_test.go`.
Run `dynamolog_test`, `table_test` and `logtest.Run` on DynamoDB Local under the tag. The tests with
`db.Fail` use a small wrapper around `dynamolog.Client` (`recDB` already shows the pattern). Gate: `build`, `it`.

**T2.3 Postgres.** Needs: T2.1. Group D. Model: sonnet. Touch: `executor/sqllog/*_test.go`,
`ops/scripts/postgres-test.sh`, `Makefile`. Use `testinfra.Postgres`. Delete the script, `it-postgres` and
the `MDS_TEST_POSTGRES_DSN` skips in this package. Gate: `build`, `it`.

**T2.4 S3.** Needs: T2.1. Group D. Model: sonnet. Touch: `internal/system/s3store/*_test.go`.
Delete `fakes3_test.go`. Run the tests on SeaweedFS under the tag. Inject failures with a wrapper on the
HTTP client given to the S3 SDK, not with a fake server. `classify_test` needs no server and stays in the
normal build. Gate: `build`, `it`.

**T2.5 End-to-end tests on the real binary.** Needs: T2.2, T2.3, T2.4. Serial. Model: opus.
Touch: `test/integration/` (new), `cmd/mdscontrolplane/logstore_test.go`, `postgres_failover_test.go`,
`logstore.go`, `controlplanetest/`, `Makefile`.
Build the real binary once, and run it with `-dynamo-endpoint`, `-s3-endpoint`, `-s3-path-style`,
`-create-tables` and the Postgres flags. Move three-node takeover, restart from checkpoint, Postgres
failover and store-and-lookup here. A test-only client with failover goes in a `_test.go` file here.
Flag and startup-error tests that need no service stay in `cmd/`, as table tests. Remove the `deps` seam if
nothing needs it any more. Delete what is left of `controlplanetest`, including `fakedynamodb` (1,211
lines), `client.go` and `grpcserver.go`. `test/conformance` keeps its own small gRPC helper.
Done when: `controlplanetest` is gone. Gate: phase gate for P2.

### Phase P3: names

No behaviour change. The golden hashes must not move. Mechanical work, but on files everywhere, so serial.
Use `gopls rename` or the like, one rename per commit.

**T3.1 `kv` becomes `metadb/engine`.** Needs: P2. Serial. Model: haiku. Touch: all importers.
`kv.Store` becomes `engine.Engine`, `Transaction` becomes `engine.Txn`. Split so the interface package has
no `sync`: `engine` (interface and record codec), `engine/memory`, `engine/pebble`. Add `engine` to the
determinism gate. Gate: `build`, `determinism`.

**T3.2 `metadata` becomes `metadb`.** Needs: T3.1. Serial. Model: haiku. Touch: all importers.
`metadata.Store` becomes `metadb.DB`. Keep the methods `replica` calls. Update the determinism script,
`architecture_test.go` and doc comments. Gate: `build`.

**T3.3 Prune `metadb`.** Needs: T3.2. Serial. Model: sonnet. Touch: `metadb/`, their tests.
`replica` calls about a dozen methods. Many in `topics.go` (`Commit`, `Lookup`, `Describe`, `CreateTopicIf`
as public, `Window`, `Assigned`, `OffsetFetch`, `DescribeGroup`, `ListGroups`) and `CommitBatch` look unused
in production, though a comment says the replica uses `CommitBatch`. Verify each with a search. Delete what
only tests call and rewrite those tests through `Write` and `Read`. Keep tests whose rule would lose its
only check, per the mutants. Also fix stale comments (`Veritas`, `cp/dynamo`, `slice 4a`). Gate: `build`, `mutants`.

### Phase P4: one source for types

Protobuf is the source, and the core still uses plain structs. Generated messages import `unsafe` and
`reflect`, and the core uses these types as map keys, so they cannot be the core types.

**T4.1 Split the proto files.** Needs: P3. Serial. Model: sonnet. Touch: `api/`, `Makefile`,
`ops/scripts/ci-determinism.sh`, importers of `cpv1`, `ops/tools/wiregen`.
Three packages: shared messages (no service), `controlplane` (the service), `log` (log entries, imports
shared only). So code that handles log entries does not pull in `grpc`. Drop the `Log` name prefix. Add the
new packages to the audit edge list in the script. Gate: `build`, `determinism`.

**T4.2 `wiregen` becomes a `protoc` plugin.** Needs: T4.1. Serial. Model: opus. Touch: `ops/tools/`,
`api/buf.gen.yaml`, `api/`.
Today the docs of `CommitSegment` and others sit in Go strings inside the generator, along with a table of
renames. Move doc comments into the `.proto`, and renames into custom options. The plugin writes the plain
structs and converters from that. The list of types in `main.go` goes away. Done when: the generated Go has
the same types and the same docs, and `ops/tools/wiregen` is gone or much smaller. Gate: `build`.

**T4.3 Clean up the API.** Needs: T4.2. Serial. Model: sonnet. Touch: `api/`, `server/`, `contract/`, `spec`
only for renames, tests. `api/v1` may break. Rename RPC `Lookup` to `FindSegments` and `GroupOffsetCommit`
style names to match the domain words. Change `GroupDescription` so the generator can map it, and delete
the hand-written converters at the ends of the server files. Move them all to `wire`, which then is the
only place that converts. Done when: `server/` holds no field-by-field conversion. Gate: `build`, `mutants`.

**T4.4 Log entries in protobuf.** Needs: T4.3. Serial. Model: opus. Effort: higher. RE-PIN. Touch:
`metadb/entry.go`, `metadb/*_test.go`, `topics/codec.go`, `groups/storage/codec.go`, `module/module.go`,
`docs/dep-audits/`, `ops/tools/determinismcheck/`.
Log entries use the `log` proto package, in one codec in `metadb`. Marshal with `Deterministic: true`. Delete
the hand-written entry codecs and `TestGoldenEntryBytes`, or replace it with a smaller check. Keep the
`Encode`, `Decode` and `ApplyBytes` shape that `replica` calls. **State records keep their hand-written
codecs**: the state hash reads their bytes, and protobuf bytes are not promised stable between versions.
Protobuf now runs inside the deterministic path, so audit it with `audit-go-dependency`. If the audit finds a
real problem, stop and ask. Do not use it blindly.
Done when: the fuzz tests from T1.8 are moved, the audit record exists and hashes are re-pinned once.
Gate: phase gate for P4.

### Phase P5: domain contexts

The state bytes do not change here, so golden must not move.

**T5.1 Schema, and the topic context.** Needs: P4. Serial. Model: opus. Effort: higher. Touch:
`internal/controlplane/domain/topic/` (new), `topics/` (deleted), `metadb/`, `module/` (deleted), `doc.go`,
`architecture_test.go`, `ops/scripts/ci-determinism.sh`, `ops/tools/determinismcheck/allow.txt`.
This task sets the pattern that T5.2 copies. Replace `module.Module` (7 methods with repeated type switches)
with `metadb.Schema`: a table of commands and reads, one row per operation (log kind, decide or read
function), so adding an operation is one row. Write `domain/topic`: `decide.go` (rules, pure), `events.go`
(`SegmentCommitted`, `TopicCreated`, `PartitionsGrown`), `tables.go` (keys and reads and writes), `codec.go`
(state records), `schema.go`. Add `domain/topic` to the determinism gate; `checksum` needs `crypto/sha256`, which
is not on the allow list, so add it as standard library with a short reason. Keep `KnownCommit` on `metadb` for `replica`.
Done when: `decide.go` writes nothing, a test can call `Decide` with a plain view, and mutants are moved and
killed. Gate: `build`, `mutants`, `determinism`.

**T5.2 The group context.** Needs: T5.1. Group E. Model: sonnet. Effort: higher. Touch:
`domain/group/` (new), `groups/storage/` (deleted), `metadb/`.
The same five files. Events: `GroupInstalled`, `AssignmentsSet`, `MembersRemoved`, `OffsetsCommitted`. The topic
dependency stays a function given at wiring. `groups/` (the coordinator) stays until P6 and keeps working.
Gate: `build`, `mutants`, `determinism`.

**T5.3 The broker context.** Needs: T5.1. Group E. Model: sonnet. Touch: `domain/broker/` (new),
`membership/` (deleted), `server/brokers_handler.go`, `server/node.go` (only the import and type).
Move `membership`. Its state is not in the log, so it is not a schema. Broker calls stay on the inject path:
changing that would need `live` or `replica`. Gate: `build`, phase gate for P5.

### Phase P6: rebalance

The riskiest work, and the reason for most of the rest. Today the coordinator has mutexes, goroutines,
timers and parked channels, and it holds a lock across log writes. Neither the simulator nor the model
covers it.

The design, which T6.1 turns into a short note:

- `domain/rebalance` is a pure `machine.Machine` with no locks with no locks, goroutines, channels or clock.
- Input values: `Join`, `Sync`, `Heartbeat`, `Leave`, `OffsetCommit`, `OffsetFetch`, `Describe`, `List`,
  `CallAbandoned` (the RPC context ended), `TimerFired`, `RoleChanged`, `CommandDone`, `ReadDone`.
- Output values: `SubmitCommand`, `SubmitRead`, `Reply`, `sim.SetTimer`, `sim.CancelTimer`.
- A group waiting on a log write queues its later inputs in order. That is what the group mutex does now.
- A write that may have changed the log, and failed, ends the term as `settle` does today: parked calls get
  `ErrUnavailable`, and state is read again.
- Time comes from `cx.Now()` and `cx.Wall()`, member ids from `cx.Rand()`.
- Group phases are data that makes bad states hard to write, not a bag of optional fields.

Hosting, so that `replica` and `live` do not change:

- In production, `server.Node` runs a second `live.Run` with its own inbox. `live.Config` already takes any process.
- `SubmitCommand` becomes `ControlPlaneDeliver` to the replica inbox. `SubmitRead` becomes `LocalRead`. The
  replica's reply comes back through `Node.reply`, which delivers it to the rebalance inbox.
- `RoleChanged` is sent from the replica loop's `AfterStep` hook in `node.go` when active-and-ready changes.
  This replaces the 100 ms `watchGroups` poll.
- In the simulator, each node gets a second dst process that crashes and restarts together with the replica.

**T6.1 Note and model.** Needs: P5. Serial. Model: opus. Effort: higher. Touch: `docs/rebalance.md` (new),
`internal/spec/rebalance.go` (new), `internal/spec/rebalance_test.go` (new).
Write down the input and output tables, and each rule that today's `coordinator_test.go` and `rules_test.go`
check. Write a small, separate model of the group protocol, from those rules and the classic Kafka protocol.
It runs one input at a time and has no fault handling. Target: under 300 lines. Do not copy structure from
`group.go`. Show the note to the user before going on. It is the last cheap point to change the design.
Gate: `build`.

**T6.2 The machine.** Needs: T6.1. Serial. Model: opus. Effort: higher. Touch: `domain/rebalance/` (new).
Build the machine beside the old coordinator. Port every case of `coordinator_test.go`, `rules_test.go` and
`waitjoin_test.go` as table tests on inputs and outputs, so they need no sleeps. Add `dst` buggify sites where valid but rare
paths exist. Add it to the determinism gate. Done when: all ported cases pass. Gate: `build`, `determinism`.

**T6.3 Conformance and mutants.** Needs: T6.2. Serial. Model: opus. Touch: `test/conformance/rebalance_test.go`,
`ops/mutants*`. A target for machine against model, with a simple fake log. Operations include timers firing
and failed writes. Add mutants: generation not raised, follower answered before the assignment is logged, expired
member not removed, parked call never answered, state kept after losing the role. Add invariants: every call
gets an answer, the generation never goes down, the leader is a member. Done when: all mutants are killed.
Gate: `build`, `mutants`.

**T6.4 Host it in the node.** Needs: T6.3. Serial. Model: opus. Effort: higher. Touch: `server/`, `test/conformance/grpc_test.go`.
Wire it as above, and move the gRPC group handlers to it. The gRPC conformance target now also covers the group calls. Delete `watchGroups`, `nodeGroups`
and `groupRole`. `server` must not import `groups` after this. Gate: `build`, `mutants`, `race`, `it`.

**T6.5 Run it in the simulator.** Needs: T6.4. Serial. Model: opus. Effort: higher. RE-PIN. Touch:
`internal/verify/simulator/`, `cmd/cpsim/`, `docs/simulation.md`.
Add the rebalance process to each node, and a workload of consumers that join, sync, heartbeat, commit
offsets, leave and go silent. The oracle checks invariants across takeover: no two leaders of one generation
are answered as current, acked offset commits survive, a member that stays silent is removed. Add rebalance paths to the coverage
report. Add at least 2 planted bugs that only show under faults. Gate: phase gate for P6 with `-seeds 5000`.

**T6.6 Delete the old coordinator.** Needs: T6.5. Serial. Model: haiku. Effort: lower. Touch: `groups/`, what imports it.
Delete `groups/` entirely, and generate the call types into `domain/rebalance`. Gate: `build`, `mutants`.

### Phase P7: finish

**T7.1 Docs and rules.** Needs: P6. Group F. Model: sonnet. Touch: `internal/controlplane/doc.go`, `docs/glossary.md` (new),
`internal/spec/README.md`, `docs/simulation.md`, `README.md`, `deploy/README.md`, stale `docs/cp-dsm-*` (ask before deleting).
Rewrite the package map, including the engine and database mental model. Write one line per term in the glossary.
Remove the map comment at the top of the server group file. Gate: `build`.

**T7.2 Import rules.** Needs: P6. Group F. Model: sonnet. Touch: `architecture_test.go`,
`replica/import_boundary_test.go`, `ops/scripts/ci-determinism.sh`. Rules: `domain/*` import only `contract`,
`runtime`, `engine` and each other as wired; `metadb` imports `domain/topic` and `domain/group`, and not
`domain/rebalance`; `internal/spec` imports none of `domain`, `metadb`, `engine`; nothing outside tests imports
`testinfra` or `spec`. Gate: `build`.

**T7.3 Final acceptance.** Needs: T7.1, T7.2. Serial. Model: sonnet. Touch: none, except fixes. Run the
acceptance list below and report it to the user with real output.

## 7. Final acceptance

- `make build` green on a fresh clone, with Docker off.
- `make it` green, with Docker on. This must really run. Docker deploy (kind, helm) waits until `dst` is
  published, and is reported as not tested.
- `make test-race RACE_RUNS=5` green. `go run ./cmd/cpsim -seeds 5000 -scenario all` clean.
- `make mutants` kills every mutant. No planted-bug code in non-test packages.
- No generated file in git. No `controlplanetest`, `fakedynamodb`, `fakeS3`, `memorystore`, `groupfix`.
- `domain/*` are in the determinism gate. `domain/rebalance` has no `sync`, `time`, goroutine or channel.
- The simulator reaches the rebalance paths, and the report shows them.
- `replica` diff against `main` is only renames and T1.6. `internal/system/live` has no diff.
- Test code is smaller than the baseline, with more paths checked. Report both numbers.

## 8. Not in this plan

- Moving `replica`'s remaining inject paths (brokers, status). That needs changes in `replica`.
- Multiverse exploration and edge coverage in the simulator.
- Invariants that read domain events as trace facts. It is a good next step once events exist, but needs a
  way out of `replica`.
- Publishing `dst`, removing the `replace`, full CI, and the Docker and kind deploy test. They wait for the move
  to manifesterio, which the user does.

## 9. Sources

- Bornholt et al., "Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3",
  SOSP 2021: https://jamesbornholt.com/papers/shardstore-sosp21.pdf
- moonpool simulation docs: https://pierrez.github.io/moonpool/llms.html#simulationreport
