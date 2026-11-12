# Test cuts: plan and evidence

Note: docs/greenfield-refactor-plan.md replaces this file's later steps.

Note: most packages named below are gone. `controlplanetest` (and `fakedynamodb`, `memorystore`,
`metadatastore` under it) was replaced by `test/conformance` and the mutants of `ops/mutants.txt`;
`kv`, `metadata`, `topics` and `membership` became `metadb/engine`, `metadb`, `domain/topic` and
`domain/group`.

Note: the `groups` package named below was deleted in T6.6. Its tests live on in `domain/rebalance`
(same test names). The paths below are history.

Goal: cut the test code from about 12,000 lines (plus about 1,400 of test support) without losing the
ability to catch bugs. This file is the plan (step 0). Nothing is deleted yet.

## 1. Baseline (step 0)

Commit `c6c3320`. Test lines: 12,052. Test support: 1,421 (fakedynamodb 680, logtest 251,
controlplanetest 313, five small helpers 177).

Bugs the suite must catch today. All are found:
- 5 rapid planted bugs (controlplanetest/bugs_test.go): ReplayGetsNewOffsets, MaxBytesOffByOne,
  TopicsNotSorted, CrashLosesAnsweredWrites, NoDataAfterFailure.
- 3 simulator planted bugs (simulator_test.go, TestPlantedBugsAreFound): writer-offsets,
  log-overwrite, claim-always-wins.
- Crash, failure-injection and conformance tests (TestCrashReplica, TestFailureInjectionReplica,
  TestConformReplica, TestConformGRPC): pass.

Race check: `make test-race RACE_RUNS=5` gave 4 of 5 runs green. TestConformReplica timed out once on
the 3-second call limit (run 2, 1 of 5 runs) and passed 72 runs afterwards (6 processes in parallel,
under -race). Treated as one unexplained flake, not investigated further, by decision. Evidence:
`build/race-evidence/` (rapid failure file) and `build/test-race-step0/run-2.txt`. Note: `build/` is not
in git.

Coverage per package (own tests only, `go test -coverprofile`; files in `build/`):

| package | statements |
|---|---|
| cmd/cpsim | 23.7% |
| cmd/mdscontrolplane | 83.0% |
| internal/controlplane/api/v1 | 3.9% |
| internal/controlplane/controlplanemetrics | 94.1% |
| internal/controlplane/controlplanetest | 77.5% |
| internal/controlplane/controlplanetest/fakedynamodb | 97.2% |
| internal/controlplane/controlplanetest/memorystore | 100.0% |
| internal/controlplane/controlplanetest/metadatastore | 100.0% |
| internal/controlplane/executor | 88.6% |
| internal/controlplane/executor/dynamolog | 88.3% |
| internal/controlplane/executor/logtest | 75.9% |
| internal/controlplane/executor/sqllog | 8.1% |
| internal/controlplane/groups | 86.1% |
| internal/controlplane/groups/storage | 98.2% |
| internal/controlplane/metadb/engine | 100.0% |
| internal/controlplane/metadb/engine/pebble | 85.2% |
| internal/controlplane/domain/broker | 100.0% |
| internal/controlplane/metadb | 79.2% |
| internal/controlplane/replica | 85.5% |
| internal/controlplane/server | 73.5% |
| internal/controlplane/topics | 92.8% |
| internal/controlplane/wire | 10.5% |
| internal/runtime | 77.6% |
| internal/spec | 96.5% |
| internal/system/awsmetrics | 86.9% |
| internal/system/fakeclock | 100.0% |
| internal/system/httpconnections | 100.0% |
| internal/system/live | 86.3% |
| internal/system/realclock | 100.0% |
| internal/system/runinfo | 95.0% |
| internal/system/s3store | 96.7% |
| internal/verify/simulator | 89.6% |
| ops/tools/determinismcheck | 91.4% |

`sqllog` shows 8% because its tests skip without a Postgres DSN (MDS_TEST_POSTGRES_DSN). Coverage cannot
protect that package here, so its tests are not cut except exact duplicates.

## 2. What would be lost if a group of tests were deleted

Method: remove a whole group of test files, run ALL tests with `-coverpkg=./internal/...,./cmd/...`
(so the rapid tests and the simulator count for every package), and list the functions that were
covered before and are not covered after. Combined baseline: 79.4% of statements, 954 of 1,104
functions covered.

| group removed | lines | statements | functions that become uncovered |
|---|---|---|---|
| G1 all replica/*_test.go | 2,224 | -0.4 | 7: String, TakeAnswers, InFlightCommits, QueueDepth, encodeGroup, trip, Booting |
| G2 groups, groups/storage, metadata, topics, engine unit tests | 2,600 | -7.0 | 64: the group coordinator and group state machine (join, sync, settle, timers), metadata CommitBatch, topics Trim, Window, Assigned, engine codec Fail |
| G3 server/*_test.go | 715 | -6.4 | 96: all group RPC handlers, BrokerHeartbeat, status, 36 generated wire converters |
| G4 cmd/mdscontrolplane tests | 963 | -3.3 | 33: main, run, serve, openNode, flags, state open and wipe |
| G5 architecture_test.go | 392 | 0.0 | 0 (it checks imports, not behaviour) |
| G6 runinfo and awsmetrics tests | 530 | -0.5 | 3: isTimeout, uploadToS3, HealthCheckHandler |
| G7 executor exec/fence tests and dynamolog_test | 555 | -1.2 | 4: dynamolog raiseFloor, writerOf, TrimThrough, deleteBatch |

What this says:
- The rapid tests and the simulator already cover the replica almost completely. The replica example
  tests (G1) are the big saving. Four of the seven functions (TakeAnswers, InFlightCommits,
  QueueDepth, Booting) have no production caller: they exist only for these tests. They are deleted
  together with their tests (deadcode first).
- The group coordinator (join, sync, heartbeat, rebalance) is covered ONLY by groups/coordinator_test and
  groups/rules_test. The rapid alphabet covers group storage operations, not the coordinator. So those
  tests stay, with cuts only by making them table-driven.
- The server handlers, the group RPCs and the 36 generated converters are covered only by the server
  tests. They stay.
- runinfo and awsmetrics ARE used by the binary (main.go calls them), so the code stays and so do the
  tests; only duplicates go.

## 3. Plan table

Lines are current. "After" is an estimate and will be checked with the proof steps. A file is only cut
after its mutation check (section 4).

| file | lines | action | after | reason |
|---|---|---|---|---|
| replica/group_lockstep_test | 285 | delete | 0 | random lockstep against the model; the rapid tests do the same and cover more (needs mutation check) |
| replica/controlplane_deliver_test | 161 | delete | 0 | the simulator sends every operation through this path |
| replica/role_test | 422 | trim to takeover timing, fencing, lost and landed claim, restoring node | 150 | the simulator leader-fencing scenario covers the rest |
| replica/commit_test | 228 | trim to maybe-applied retry, retarget, one append in flight | 80 | rapid commit and crash cover the rest |
| replica/checkpoint_test | 226 | trim to bad CRC, restore miss, failed list retried, vanished checkpoint | 100 | publish and trim are covered by the rapid trim variant |
| replica/replica_test | 248 | trim to state hash tests and catch-up | 90 | stop and start are covered by every other test |
| replica/entrysize_test | 118 | keep size edges, drop the repeated case | 70 | encodeGroup is only covered here |
| replica/openstore_test, trimmed_test, lockstep_test, local_read_test, reads_test | 442 | keep | 442 | trip and trimmed-log edges are only covered here; lockstep is the cheap model check |
| architecture_test | 392 | replace by a short `go list -deps` test | 60 | keep the rules that still matter; no behaviour is covered |
| groups/coordinator_test, rules_test | 760 | table-driven, same cases | 500 | only place that covers the coordinator |
| groups/storage rules_test, codec_test | 405 | trim to one example per rule | 150 | rapid covers group storage operations; no storage function is lost in G2 |
| metadata tests (5 files, bench) | 621 | keep golden bytes, snapshot CRC and damage, corrupt entries; drop repeats | 330 | CommitBatch is only covered here |
| topics tests (3 files) | 364 | keep Trim, Window, Assigned, codec damage | 220 | those three functions are only covered here |
| engine/engine_test | 165 | trim | 90 | codec Fail is only covered here |
| server tests (4 files) | 715 | keep handlers; trim wire round trips | 595 | only place for the group RPCs and brokers |
| cmd/logstore_test | 575 | table-driven flags and startup errors; keep three-node takeover and restart | 350 | one shared log suite (see backends) |
| cmd/main_test | 158 | table-driven | 100 | same cases, less code |
| executor/exec_test, fence_test | 284 | trim cases already in logtest conformance | 180 | keep Classify and conflict semantics |
| dynamolog_test, table_test | 366 | keep backend cases (floor, trim, conditional put); move shared cases to logtest | 326 | raiseFloor, TrimThrough, deleteBatch only covered here |
| sqllog_test | 219 | keep | 219 | coverage cannot prove it; exact duplicates only |
| s3store tests (3 files) | 476 | trim duplicates only | 430 | checkpoints stay in S3 |
| runinfo_test, awsmetrics_test | 530 | table-driven | 430 | the code is used by the binary |
| live_test | 322 | keep, trim duplicates | 290 | the production runner |
| fakedynamodb tests | 531 | keep, trim duplicates | 480 | the fake needs its own tests |
| controlplanetest (conform, alphabet, crash, bugs, small) | 1,142 | keep | 1,142 | the property tests and planted bugs |
| spec_test | 280 | keep, exact duplicates only | 260 | the model itself |
| others (domain/broker, httpconnections, clocks, wire, runtime, metrics, ...) | about 650 | keep | about 650 | small, each covers its own code |

Test support (1,421 lines): keep fakedynamodb (680) and logtest (251, grows a little as shared cases
move in). Merge groupfix, kvtest, storecall, metadatastore and memorystore (177 lines) into one small
helper file (about 100). controlplanetest helpers (313) stay: it is the property-test harness.

## 4. Is the 6,000 target reachable?

Adding up the table gives about 8,600 test lines (a saving of about 3,400) and about 1,350 lines of
support. Reaching 6,000 would mean cutting tests that are the only coverage of the group coordinator,
the server handlers and generated converters, and the backends. The rule says a smaller saving is better
than a lost guarantee, so the plan stops at about 8,600 unless you say otherwise. Support cannot reach
700 because fakedynamodb alone is 680; without fakedynamodb and logtest it is about 530.

## 5. Proof after every cut

1. `make build` green.
2. The 5 rapid and 3 simulator planted bugs are still found.
3. Function coverage compared with `build/cover-before-func.txt` (own tests) and the combined numbers
   above. No covered function becomes uncovered unless the function is deleted. A drop of more than 2
   points in a package needs a written reason here.
4. Mutation spot check for each deleted file: 2 or 3 small mutations in a scratch worktree (never
   committed), with the remaining test that fails.
5. `make test-race` once at the very end (not between cuts, by decision).

Stop and ask if: a golden hash moves; a planted bug is not found; a package drops more than 2 points
without a reason; production code other than unused code must change.
