# Chapter 9. Verification, veritas specs, conformance, and quality gates

`manifester` runs operations at the same time and adds faults during the runs. The spec in `veritas/` states the result that the outside world must see for each operation. Tests compare the implementation with the spec. Conformance tests, deterministic simulation, and integration tests run the comparison. A determinism gate and a CRAP gate add static checks. Read `veritas/doc.go` first. `veritas/doc.go` states the contract for most of this chapter.

## 9.1 Four parts check the implementation against the spec

The design has four parts:

- Executable specs in `veritas/` state the required behavior. The specs are sequential and fault-free. The specs hold plain data. A reader can check each spec line by line (`veritas/doc.go`, decision 21).
- Lockstep conformance in `veritas/veritastest/` compares two runs. The test runs the same random sequence against the implementation and the spec. The test compares every result. The test checks invariants after every step. The test shrinks a failure to a replayable example.
- Mutants are broken variants that each check must reject. A test that passes correct code and broken code proves nothing.
- Static gates check the source and the test coverage. `tools/detcheck` and `scripts/ci-determinism.sh` keep deterministic packages deterministic. `cmd/crapscore` reports complex functions that have low test coverage.

The layers repeat checks with different methods. One layer uses a sequential spec. Another layer uses Kafka wire requests. Another layer uses a simulated faulty world. Another layer uses a real client. Each layer can detect a bug that another layer missed.

## 9.2 The veritas specs state the required behavior

Package `veritas` holds three specs. `ObjectStore` and `ControlPlane` answer `core.IOAction` with `core.Completion` through `Exec`. All three specs have a `StateHash`. No code iterates a map to decide behaviour. Where code iterates a map, the code sorts the keys first (`sortedKeys` in `veritas/hash.go`, with its `//detcheck:ok` reason).

### `Partition` states the rules for one Kafka partition (`veritas/partition.go`)

`Partition` is an append-only log of `[][]byte`:

- Offsets are dense from 0. The offset of a record is the index of the record.
- `Append(batch)` returns the offset of the first record in the batch. The offset is the prior high watermark. An empty batch changes nothing.
- The log commits everything the log appends. `HighWatermark()` is `core.Offset(len(recs))`.
- `Read(from, max)` treats `max <= 0` as a request for all records. `from < 0` or `from > HighWatermark()` gives `ok == false` (`OFFSET_OUT_OF_RANGE`). `from == HighWatermark()` is valid and empty.
- `StateHash()` folds the count first. `StateHash()` then folds every record in order.

The spec has no concurrency, no input or output, and no failure mode. When the broker disagrees with `Partition`, the broker code must show why the spec is wrong.

### `ObjectStore` states the rules for stored objects (`veritas/objectstore.go`)

`ObjectStore` is a key to bytes map with sorted keys alongside. `Exec` handles `ObjPut`, `ObjGet`, `ObjDelete`, and `ObjList`:

- `Put` rejects an empty key with `ErrInvalid` and changes nothing. For any other key, `Put` stores a copy and replaces any object at that key. `Put` returns `OK` and `Applied`.
- `Get` returns `ErrNotFound` for an absent key. `Get` reads `[Off, Off+Len)`. `Len <= 0` reads to the end. `Off < 0`, `Off > size`, or `Off+Len > size` gives `ErrInvalid`. The request is malformed, and the key is not missing. `Off == size` with `Len <= 0` gives empty data.
- `Delete` returns `ErrNotFound` and `NotApplied` for an absent key. For a present key, `Delete` removes the object and returns `OK` and `Applied`.
- `List` returns prefix keys in sorted order. `List` returns `nil` if no key matches. `List` never fails.

The spec has no faults. The spec outcomes are `Applied` or `NotApplied`, and the outcomes are never `MaybeApplied`. Production code must resolve an uncertain write before a test can compare the write. This requirement is intentional.

### `ControlPlane` states the rules for committed segments (`veritas/controlplane.go`)

`ControlPlane` models the control plane as a broker sees the control plane (`core.CPOp`). `ControlPlane` stores committed segments by key. `ControlPlane` stores per-partition runs (`core.BatchRange`). `ControlPlane` stores topic partition counts.

The following rules state how `ControlPlane` records and reads the runs:

- The log of a partition is a set of runs `[Base, Base+Count)`. The end of the log is the `Base+Count` of the newest run. The end is 0 when the log is empty. No separate counter can drift from the runs.
- `CommitSegment` is idempotent by key. The same key with the same batches returns the original ranges again, with `OK` and `Applied`. The same key with different batches gives `ErrConflict`.
- An invalid commit records nothing, and another commit can reuse the key. Invalid commits have an empty key, empty batches, `Records <= 0`, an empty topic, or a negative partition. Each case gives `ErrInvalid`.
- A valid commit assigns each batch a `Base`. The `Base` is the current end of the partition for the batch. The commit assigns bases in request order. The commit grows the end after each batch. Runs stay contiguous per partition. The commit covers all partitions in one step, and the commit is all or nothing.
- A commit creates a topic, and a topic grows to cover the partition. Real stores guard this step with compare-and-set on the run base and retry. The sequential spec does not need that guard.
- `Lookup` returns `ErrInvalid` for `From < 0`. For any other `From`, `Lookup` returns runs whose last offset is `>= From`, in order. `Lookup` also returns the end offset. `MaxBytes > 0` stops after the batch that reaches `MaxBytes`. `Lookup` returns at least one batch if any batch matches. An unknown partition returns an empty result with end 0.
- `DescribeTopics` returns topics in sorted order. An empty request means all topics. `DescribeTopics` omits unknown names. `CreateTopic` never shrinks a topic. `CreateTopic` rejects an empty name and `partitions <= 0`.

`veritas/snapshot.go` adds two read-only functions. `Ends()` returns the partitions that have a run and the ends of the partitions, in topic then partition order. `SegmentKeys()` returns the segment keys in sorted order. Tests use both functions for checkpoints. Neither function changes state.

### `StateHash` detects state drift (`veritas/hash.go`)

`StateHash` uses FNV-1a over a canonical, length-prefixed byte stream. Distinct states cannot collide by concatenation. `ObjectStore` and `ControlPlane` cache folded prefix states. A change at sorted index `i` truncates the cache to `i+1`. A put near the end costs little. `veritas/hashinc_test.go` (`TestIncrementalStateHashMatchesFull`) checks the incremental hash against a fold from scratch. The test verifies the optimisation against the simple version. Conformance compares full completions first. The hash detects drift that no read has exposed.

### The specs serve as fakes in `veritas/vsys/`

`veritas/vsys/vsys.go` adapts the specs to the production interfaces in `sys/`. `vsys.ObjectStore` implements `sys.ObjectStore`. `vsys.ControlPlane` implements `sys.ControlPlane`. The adapters return spec error codes as `*vsys.Error` for a `sys.Classifier`. `vsys` is not deterministic, unlike `veritas`. `vsys` uses `context` and a serialising `sync.Mutex`. `scripts/ci-determinism.sh` includes `./veritas` and `./veritas/veritastest`, but the script excludes `veritas/vsys`. A production `Executor` can run against the specs with no network. Compile-time assertions check that the adapters satisfy the `sys` interfaces.

## 9.3 `veritastest` runs the implementation and the spec in lockstep

`veritas/veritastest/doc.go` describes the method. The test runs random sequences against the implementation and the spec. The test compares the results. The test checks invariants after every step. The test shrinks a failing sequence and prints the sequence as a Go literal. A developer can paste the Go literal into a test. The core type is `Check[S, Op]` in `veritas/veritastest/lockstep.go`:

| Field | Role |
|---|---|
| `New` | Fresh implementation paired with a fresh spec, both empty |
| `Gen` | Uses the supplied `*Rand` for the i-th operation |
| `Step` | Apply one op to both. An error marks any difference |
| `Invariant` | Optional, after every `Step`. A panic counts as failure |
| `Shrink` and `ShrinkAll` | Simpler versions of one op and the whole sequence |
| `Format` | Render an op as a Go expression for the report |
| `Ops`, `MaxShrinkRuns` | Sequence length and shrink budget (0 = 5000) |

Determinism comes from the code structure. `veritastest.Rand` (`veritas/veritastest/rand.go`) is SplitMix64 with Lemire's unbiased `Intn`. `veritastest.Rand` imports `math/bits` and nothing else. `Options` maps test flags to seeds. `Seeds` means seeds 1..N. `Seed >= 0` means a single seed. `Stop` means a wall-clock campaign. `Replay` renders the replay command. Shrinking drops chunks first, in halves, quarters, then singles. Shrinking then tries the first failing `Shrink` candidate for each op. Shrinking then tries `ShrinkAll`. Shrinking tries candidates in fixed order until no candidate makes progress. Any failure counts. A shrunk sequence may expose a different bug, and that bug still counts as a failure. A `Failure` report prints the seed and the sequence as a `[]Op{...}` literal. A developer can paste that literal into a regression test. A developer can paste that literal into a regression test for the bug. `TestObjectStoreCheckShrinksPlantedBug` (`veritas/objectstore_test.go`) is an example. A store ignores `Off` when `Len <= 0`. The test shrinks the failure to one put and one get.

## 9.4 C1, C2, C3, and C4 compare four pairs

`veritas/doc.go` states the four checks:

| Check | Under test | Against | Lives in |
|---|---|---|---|
| C1 | Real broker, no faults | One `veritas.Partition` per (topic, partition) | `broker/conformance_test.go` |
| C2 | Each sim engine, faults off | Its spec, same completions, same `StateHash` per op | `sim/engines/conformance_test.go` |
| C3 | `sys` adapters (S3, CP HTTP client) | These specs | With the adapters. The only check that executes `sys` |
| C4 | Kafka oracle | `veritas.Partition` on its own | `sim/kafka/c4_test.go` |

C1 uses `TestConformanceBroker`. The test drives the real broker state machine. The test uses a fault-free test setup over `veritas.ObjectStore` and `ControlPlane`. The test builds requests with `kwire`. C1 compares every ack, fetch, and `ListOffsets` response with the spec. For produce, C1 compares the error and the base offset with the high watermark from the spec. The broker refuses idempotent entries (decision 41). For fetch, C1 compares the watermark, contiguity, and per-record values with `Partition.Read`. C1 checks the record CRC with `kwire.ParseBatch`. For `ListOffsets`, the latest offset equals the watermark, and the earliest offset is 0. The invariant requires no violations from the test setup and no stray responses. `corruptGets=true` flips a byte in every third `ObjGet`. Fetch must detect the corruption, and fetch must not serve the corrupted bytes. `TestConformanceBrokerSmallSegments` reduces `MaxSegmentBytes`, `MaxSegmentBatches`, and `MaxSegmentPartitions`. Each seal rule fires in short runs. The sequences have 80 ops. The mix is about 60 percent produce, 25 percent fetch, and 15 percent list. The test runs 100 seeds.

C2 sets fault probabilities to zero. Each engine must return the same completions as the spec for the engine. The cached hash of each engine must track `StateHash` after every operation. `TestConformanceObjStore` compares `sim/engines/objstore` with `veritas.ObjectStore`. `TestConformanceCPEngine` compares `cpengine` with `veritas.ControlPlane`. Both tests run 200 seeds at 120 ops each. `TestConformanceLogEngine` compares `logengine` with `ControlPlane` on completions. The tick chain for `logengine` never drains, so each op runs 100 ms of virtual time. The test has no spec hash to compare. C2 runs with faults off. C2 proves that an engine implements the spec when nothing goes wrong. The invariant layer of the simulator checks correctness with faults on. C2 does not check that case.

C3 is the one check that runs `sys` code. C3 tests the S3 and CP-HTTP adapters against the specs. The tests live with the adapters. The fakes need no Docker. The fakes are `cp/dynamo/fakedb`, a fake S3 for `sys/s3store`, and `httptest` for `cp/cpclient` (see `docs/crap-gate.md`). The tests run under `go test ./...`. A change to an adapter must keep the C3 test for the adapter passing.

C4 keeps two implementations separate. The oracle in `sim/kafka` never imports `veritas` (decision 21). The external test package may import `veritas`, because `detcheck` skips `_test.go` files. `TestC4OracleDoesNotImportVeritas` enforces the split. The test parses non-test sources and fails on any `veritas` import. `TestC4AcceptsCorrectHistories` runs 200 seeds. The test covers incremental checks and one-shot checks. The test requires the log that the oracle reconstructs to equal `veritas.Partition` with no differences. `TestC4RejectsEveryFault` requires the test to report each fault in `sim/kafka/c4_faults_test.go` over enough applicable seeds. The faults include duplicated records, dropped acked ranges, and off-by-one acks. `veritas/doc.go` summarises the set as 28 faults and fenced acks.

## 9.5 Each test layer detects a different class of bug

The table lists each layer, the code that the layer runs, and the bugs that the layer detects:

| Layer | What runs | Detects |
|---|---|---|
| Unit (`go test ./veritas/ ./kwire/ ./broker/`) | Spec rules, codecs, single components | Wrong rule, offset, CRC |
| Lockstep (C1, C2) | Random sequences against the spec | Divergence between two implementations of one contract |
| Simulation (`go test ./sim/...`) | Faulty worlds, invariants, C4 oracle | Lost, duplicated, and reordered records under faults |
| Integration, hermetic (`go test ./it/`) | Real brokers + real `kgo`, in-process backends | Wiring, wire protocol, client-visible behaviour |
| Integration, full-stack (`scripts/it-up.sh`) | Same flows on DynamoDB and S3 in Docker | Backend behaviour no fake reproduces |
| Static gates | `detcheck`, audits, CRAP | Nondeterminism, unreviewed deps, risky untested code |

The specs have unit tests of their own. `veritas/partition_test.go`, `objectstore_test.go`, and `controlplane_test.go` test each rule. `TestObjectStoreVsSortedSlice` checks the store against a separate sorted-slice model. `TestControlPlaneInvariants` checks contiguous offsets. `TestControlPlaneInvariants` checks that the end equals the committed counts. `TestControlPlaneInvariants` checks CAS by base. `TestControlPlaneInvariants` checks idempotent commit. A spec could share a wrong rule with the implementation. The spec tests reduce that risk, and C4 independence reduces that risk further.

## 9.6 Integration tests run real brokers and the real client (`it/`)

Package `it` (`it/doc.go`) drives real brokers with the real `franz-go` client. Package `it` uses `sys/prod` wiring, as in `cmd/manibroker`. Package `it` imports `kgo`, and no deterministic package imports `kgo`.

Hermetic tests live in `it/hermetic_test.go` and `it/stack_test.go`. The hermetic tests use loopback and nothing else. The CP uses `memstore` over `httptest`. The store is an in-memory store from `vsys.NewObjectStore`. Brokers come from `prod.Start` on `127.0.0.1:0`. The tests run under `go test ./...`. The tests cover round trips (`TestProduceConsumeRoundTrip`). The tests cover metadata and API versions. The tests cover InitProducerId, which the broker does not offer (decision 41). The tests cover unknown-topic produce and `ListOffsets`. The tests cover `TestKillBrokerMidProduce`, a graceful-stop flush, and the default 200 ms batch-window rule.

Full-stack tests live in `it/fullstack_test.go`. The full-stack tests run the same shared flows, `runRoundTrip` and `runKill`, over `cp/dynamo` and `sys/s3store`. The tests skip unless both endpoints are set. `scripts/it-up.sh` starts `deploy/docker-compose.yml`. The script runs ExtendDB on `127.0.0.1:18080` and SeaweedFS S3 on `127.0.0.1:8333`, on loopback and nothing else. The script prints `MANIFESTER_DYNAMO_ENDPOINT` and `MANIFESTER_S3_ENDPOINT`. `scripts/it-down.sh` stops the stack with `docker compose down -v`.

## 9.7 The determinism gate rejects nondeterministic code and unaudited dependencies

`tools/detcheck` (`tools/detcheck/detcheck.go`) is a `go/analysis` checker. `detcheck` reports imports that are not on the exact-path allow-list in `tools/detcheck/allow.txt`. The tool embeds that allow-list. `detcheck` reports `go` statements, channel types, channel sends, and channel receives. `detcheck` reports `select`. `detcheck` reports range over a map and reflect map iteration. `detcheck` reports package-level variables, because mutable globals share state between sim runs. Error sentinels and blank assertions are exceptions to that rule. Suppression needs a reason. Write `//detcheck:ok <reason>` on the same line or the previous line. A bare `//detcheck:ok` is a finding in itself. The checker does not check `_test.go` files, and C4 relies on that exemption. The allow-list uses exact paths. Adding a line requires an audit. A line may use `/...` for a whole audited subtree and for nothing else. The allow-list omits `time`, `math/rand`, `crypto/rand`, `os`, `net`, `sync`, `runtime`, `unsafe`, `reflect`, `maps`, `hash/maphash`, and `context`. The header of `allow.txt` lists the full set.

`scripts/ci-determinism.sh` runs the gate in three steps:

1. `detcheck` runs over `./core/... ./sim/... ./format/... ./kwire ./broker/... ./veritas ./veritas/veritastest`. `detcheck` also runs over every `toys3` package except `prod` and `cmd`. The script derives that list from `go list`, so a new pure package cannot pass without a check.
2. Audit records. Every third-party module that those packages depend on needs a record at `docs/dep-audits/<module>@<version>.md`. The version in the file name must match the version in `go.mod`. A version bump requires a new audit.
3. Determinism canary. Each package that implements `TestDeterminismCanary` (`sim/`, `toys3/sim/`) runs every seed twice. The two runs must match. The script reports a package without a canary as skipped.

To audit a dependency, use `scripts/audit-dep.sh` and `.claude/skills/audit-go-dependency/SKILL.md`. The template is in `docs/dep-audits/README.md`. Run the script in a temporary module. The script leaves the repo untouched. The script reports imports, transitive packages, `detcheck` findings, and hints. Review each finding for effects across runs. Check what grep cannot check. Check aliasing through three-index slices and `unsafe`. Check hidden timestamp or random defaults. Check the required Go version. Prove the result with a test. Encode and decode twice, in two processes. The outputs must match byte for byte and under `DeepEqual`. Then decide ACCEPTED, ACCEPTED WITH RULES, or REJECTED. If you accept the dependency, write the record with findings that name a file and a line. Add the exact paths to `allow.txt`. Run `scripts/ci-determinism.sh`. Production code in `sys/` needs no audit. Do not weaken `detcheck` to pass CI. Do not suppress a finding without a reason. Do not accept a dependency because "it's just a codec."

## 9.8 The CRAP gate rejects complex functions that lack coverage (`cmd/crapscore`)

`make build` is the full gate. `make build` runs `fmt-check`, `vet`, `compile`, and `test`. `make build` runs `crap` last (`Makefile`). CRAP (Savoia & Evans) scores change risk (`docs/crap-gate.md`, decision 40).

\[
\mathrm{CRAP}(m) = \mathrm{comp}(m)^2 \cdot (1 - \mathrm{cov}(m))^3 + \mathrm{comp}(m)
\]

At 100% coverage, CRAP equals complexity. At 0% coverage, CRAP is `comp² + comp`. Past a complexity of about 30, no function can score below the default threshold of 30. Complexity comes from `cmd/crapscore/analyze.go`. The count is `1 + if + for + range + non-default case + non-default select clause + && + ||`. The count includes nested function literals. Coverage comes from `build/cover.out`, which `make test` writes. A function in a file absent from the profile counts as 0%. A function with no profiled statements counts as 100%. The tool uses the standard library and nothing else (decision I).

The gate uses a baseline. `crap-baseline.json` records each function over the threshold and the CRAP of the function. The file is empty at present, so no function scores over 30. The gate fails when a new function scores over the threshold. The gate fails when a baselined function rises by more than the 0.5 tolerance. An improvement never fails the gate. `make crap-strict` ignores the baseline. `make crap-baseline` rewrites the baseline. Review the diff after any rewrite. A lower entry does not fail the gate. A new entry records accepted debt, and the change needs a reason for that debt. The threshold is 30, and the project did not set the threshold to 6. At full coverage, CRAP equals complexity. A threshold of 6 would have blocked 275 of 1248 functions when the project added the gate. A threshold that low could lead authors to split functions to lower the score. You can run `make build CRAP_THRESHOLD=15` to test a lower threshold. Skipped Docker tests mean the gate measures coverage from plain `go test ./...`. That result is stable anywhere. Backend packages use tests that need no Docker, so that coverage is available on any machine. You can merge integration coverage with `make crap CRAP_MERGE=build/it-cover.out`.

## 9.9 Each check must fail on a mutant

A check that has never failed is a hypothesis. The repo ships broken code to test each check:

- `broker.Config.Bug` lives in `broker/config.go`. `broker.Config.Bug` is test-only, and zero means the real broker. `BugEarlyAck` acks after PUT and before `CommitSegment`. `BugWrongOffset` adds 1 to the fetch base. `BugServeCorrupt` skips the fetch CRC. Other mutants change segment caps, timers, retry keys, and produce CRC checks.
- `TestConformanceBrokerCatchesMutants` is the C1 mutant test. The test fails if `BugWrongOffset` survives 20 seeds. The test fails if `BugServeCorrupt` survives 20 seeds under corrupt GETs.
- Direct tests live in `broker/broker_test.go`. `TestWrongOffsetMutant`, `TestEarlyAckMutant`, and `TestCorruptGetRetried` assert that a mutant misbehaves. The tests assert that the real broker does not misbehave. A pass rules out a mutant that never broke the code.
- `brokenStore` is a planted bug in `veritas/objectstore_test.go`. `nonIdempotent` is a planted bug in `veritas/controlplane_test.go`. `nonIdempotent` uses a fresh key for each commit, so a replay commits twice. Each planted bug must be found, and each failure must shrink to two ops.
- Each fault in `sim/kafka/c4_faults_test.go` must be rejected. `TestC4RejectsEveryFault` checks that rule.

The simulator records the first seed that detects each mutant. See `sim/kafka/kafka_test.go` and `sim/kafka/logmutants_test.go`. For new work, add a check and the mutant for the check in the same change.

## 9.10 Each bug has a test that detects the bug first

The table lists each bug, the first layer that the project expects to detect the bug, and the test for the bug:

| Bug | First layer expected to detect it | Test |
|---|---|---|
| Fetch batch at base offset +1 | C1 against `Partition` | `TestConformanceBrokerCatchesMutants`, `TestWrongOffsetMutant` |
| Corrupted object bytes served | C1 `corruptGets=true`, fetch CRC | `TestConformanceBroker`, `TestCorruptGetRetried` |
| Ack before `CommitSegment` lands | Broker unit test, CP held | `TestEarlyAckMutant` |
| Commit replay gets fresh offsets | `ControlPlane` lockstep | `TestControlPlaneCheckShrinksPlantedBug` |
| Range get ignores `Off` | `ObjectStore` lockstep | `TestObjectStoreCheckShrinksPlantedBug` |
| Engine completion or state drifts, faults off | C2 + `StateHash` per op | `TestConformanceObjStore`, `TestConformanceCPEngine`, and `TestConformanceLogEngine` |
| Incremental hash ≠ full fold | Spec unit test | `TestIncrementalStateHashMatchesFull` |
| Oracle accepts duplicate, dropped, or mis-offset records | C4 fault injection | `TestC4RejectsEveryFault` |
| Oracle rejects a correct history | C4 acceptance, 200 seeds | `TestC4AcceptsCorrectHistories` |
| Oracle imports `veritas` | Source-parsing guard | `TestC4OracleDoesNotImportVeritas` |
| Nondeterministic construct or unlisted import | Static analysis | `detcheck` through `scripts/ci-determinism.sh` |
| Dependency bumped without re-audit | Audit-record check | `scripts/ci-determinism.sh` step 2 |
| Same seed, two runs differ | Determinism canary | `TestDeterminismCanary` |
| Acked records lost when a broker dies | Hermetic integration | `TestKillBrokerMidProduce` |
| Real backend differs from fakes | Full-stack integration | `TestFullStackRoundTrip` and `KillBroker` |
| Complex function loses coverage | CRAP baseline check | `make crap` |

## 9.11 Commands

| Command | What it does |
|---|---|
| `make build` | Full gate, gofmt, vet, build, test with coverprofile, CRAP last |
| `make test` | `go test -coverprofile=build/cover.out ./...` |
| `go test ./veritas/ ./veritas/veritastest/` | Spec units, planted-bug shrink tests, test code self-tests |
| `go test ./broker/ -run TestConformanceBroker -count=1` | C1 (+ small-segments and mutant variants) |
| `go test ./sim/engines/ -run TestConformance -count=1` | C2, objstore, cpengine, logengine vs specs |
| `go test ./sim/kafka/ -run C4 -count=1` | C4, accepts correct, rejects every fault |
| `go test ./it/ -run TestProduceConsumeRoundTrip -count=1` | Hermetic integration, no Docker |
| `eval "$(scripts/it-up.sh)" && go test ./it/ -run TestFullStack -count=1; scripts/it-down.sh` | Full-stack integration |
| `./scripts/ci-determinism.sh` | detcheck + audit records + canary |
| `scripts/audit-dep.sh <module>@<version>` | Start a dependency audit (repo untouched) |
| `make crap` / `make crap-strict` / `make crap-baseline` | CRAP gate / no-baseline / rewrite baseline (review the diff) |

The determinism gate is separate from `make build`. See the `Makefile` header. You can add `./scripts/ci-determinism.sh` before `crap` in the `build` recipe. You can run the gate before you push changes that touch `core/`, `sim/`, or a dependency.

## Key takeaways

- `veritas/` holds three sequential specs with no faults. Each spec has a `StateHash`. A reader can argue about each spec line by line.
- Conformance runs the implementation and the spec in lockstep. Conformance compares every completion, and C2 compares every hash. Conformance shrinks a failure to a Go literal that a developer can paste into a test.
- C1 covers the broker. C2 covers the sim engines. C3 covers the `sys` adapters. C4 covers the oracle. A source check keeps C4 from importing `veritas`.
- A check without a mutant is unproven. Add the check and the mutant in the same change.
- Determinism and change risk have gates. The gates are `detcheck`, the audit records, the canary, and the CRAP baseline.

## Checkpoint

<details>
<summary>1. Why must the C4 oracle not import <code>veritas</code>, and what enforces it?</summary>

Shared code could make a wrong history look correct to both sides. Independence makes C4 a check on the oracle. Without independence, C4 would repeat the same code and agree with the same code. `TestC4OracleDoesNotImportVeritas` (`sim/kafka/c4_test.go`) parses non-test sources. The test fails on any `veritas` import. The external test package may import `veritas`, because `detcheck` skips `_test.go` files.

</details>

<details>
<summary>2. A fetch returns batches at offsets 5 then 7. Which check names that, and how?</summary>

C1 in `broker/conformance_test.go` detects the gap. Fetched batches must be contiguous. Each base must equal the next expected offset. The test compares values against `Partition.Read`, and the test compares the watermark against the spec. The gap fails even if no client-visible value differed.

</details>

<details>
<summary>3. A <code>sim/</code> dependency bump fails <code>ci-determinism.sh</code> step 2. What do you do, and never do?</summary>

Run a new audit. Run `scripts/audit-dep.sh <module>@<new-version>`. Follow `.claude/skills/audit-go-dependency/SKILL.md`. Write `docs/dep-audits/<module>@<new-version>.md` with findings that name a file and a line. Run the gate again. Do not weaken `detcheck`. Do not delete an allow-list check. Do not suppress a finding without `//detcheck:ok <reason>`.

</details>

<details>
<summary>4. <code>make crap</code> says a baselined function "got worse". Meaning and options?</summary>

The CRAP of that function rose by more than 0.5 above `crap-baseline.json`. The function has more branches, less coverage, or both. The preferred fixes are tests or a split of the function. Run `make crap-baseline` if you accept the debt, and give a reason for the diff in the PR.

</details>

<details>
<summary>5. Why does C2 run with faults off, and what covers faults-on?</summary>

C2 makes one claim. With faults at zero, completions and `StateHash` match the spec after every operation. Readers can interpret faulty runs after readers know that fault-free runs match. Simulator invariants cover behaviour with faults on. C4 covers faulty histories. Mutants record the first seed that detects each fault.

</details>

## Try it

1. Run `go test ./veritas/ -run 'TestObjectStoreCheckShrinksPlantedBug|TestControlPlaneCheckShrinksPlantedBug' -v -count=1`. The shrunk sequences have two ops.
2. Run `go test ./broker/ -run TestConformanceBroker -count=1`. Run `go test ./sim/engines/ -run TestConformance -count=1`. Run `go test ./broker/ -run TestConformanceBrokerCatchesMutants -count=1`.
3. Run `./scripts/ci-determinism.sh`. Find a `//detcheck:ok` (start at `veritas/hash.go`) and judge whether its reason suffices.
4. Run `make crap-strict`. For the worst function, compute the coverage it needs to reach CRAP 30. Compute the complexity cut as the other option. Which fix would you choose for a PR?
5. Run `eval "$(scripts/it-up.sh)"`. Run `go test ./it/ -run TestFullStackRoundTrip -count=1`. Run `scripts/it-down.sh`. Compare what the full-stack run proves with what the hermetic round trip proves.

## Further reading

- `veritas/doc.go`. `veritas/doc.go` states the four checks.
- `docs/crap-gate.md`. `docs/crap-gate.md` covers CRAP sources, the threshold rationale, and the baseline.
- `docs/dep-audits/README.md`. `docs/dep-audits/README.md` gives the audit template. `tools/detcheck/allow.txt` gives the allow-list.
- `.claude/skills/audit-go-dependency/SKILL.md`. `.claude/skills/audit-go-dependency/SKILL.md` gives the full audit workflow.
- `docs/decisions.md`. `docs/decisions.md` records decisions 21 (specs), 40 (CRAP), 41 (no idempotent producers), and I (audits).
- `docs/lessons.md` lesson 12. `docs/lessons.md` lesson 12 lists the determinism rules that `detcheck` enforces.
- Earlier chapters in `docs/onboarding/`. Earlier chapters cover the broker, the CP, and the simulator that these checks test.
