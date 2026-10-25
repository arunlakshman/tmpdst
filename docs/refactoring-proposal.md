# Refactoring proposal (general and DDD), second edition

*Note: the DynamoDB-transaction control plane (`cp/dynamo`) named in this proposal was removed (decision 54); the log-based CP (`cp/logcp`, `cp/logsm`) is the only one, so references to `cp/dynamo` are historical.*

Status: proposal only, nothing applied. It supersedes the open items of `ddd-refactoring-proposal.md`
(deferred by decision 34). Measured on branch `toys3` at commit `ebea3be` plus uncommitted edits.

**Caveat on moving ground.** Another agent is editing `cp/dynamo`, `broker/config.go`, `broker/segment.go`,
`sim/engines/cpengine`, `veritas` and docs for CP range compaction. Numbers for those areas are mid-change.
Every item touching them is marked **[after compaction]** and must wait for that work to merge.

"Hash-neutral" means no trace hash and no `StateHash` output changes. Frozen: decisions 7, 10, 11, 12
(fact names), 21 (spec independence), 22, 28, 34; detcheck bans goroutines, channels, map iteration,
package vars and `time` in `core/` and `sim/`.

## 1. Re-measurement of the earlier proposal

| Earlier item | Then | Now | Verdict |
|---|---|---|---|
| Broker size | about 1,740 lines | 1,949 non-test lines in 9 files (`segment.go` 430, `ops.go` 310, `broker.go` 314); 2,021 test lines | Worse (+12%), all of it in the segment pipeline |
| 7. `ErrFenced` split | proposed | Done as decision 34: `BrokerFenced()` / `ProducerFenced()`, 6 call sites, 19 mentions of `ErrFenced` remain | Done |
| 8. Incarnation fencing | open question | Decided: CP fences by epoch only | Done |
| 2. Kafka-error translation | proposed | Still open. Kafka codes `err*` are chosen in 16 places in `broker/` (`committed()` has a 4-way switch, `isolate()` picks `errInvalidPIDMapping`, `exhaustCode` in ops) | Valid, small |
| 6. Six FNV hashers | 6 | Still 6 `hasher` types (`broker`, `veritas`, `sim/echo`, `sim/kafka`, `sim` (struct form), `netsim`) and **4 copies of `fnvU64`** (`broker/hash.go:172`, `sim/hash.go:73`, `sim/kafka/hash.go:48`, `netsim.go:675`), each with its own `fnvu64_test.go` (4 test files). 43 hits of the FNV constants | Unchanged |
| 6. Three retry skeletons | 3 | 5: `broker.retry` (segments), `broker.retryOp` (ops), `sim/echo.backoff`, `dynamo.commit` loop (`maxAttempts = 20`), `dynamo/store.go:178` (batch-get backoff). Two doubling formulas: `Broker.backoff` and `sim/echo.backoff` | Slightly worse |
| 5. Fact name constants | string literals | Worse. 18 `emit("...")` sites (15 distinct names) and 36 literal uses of the three best-known names alone, spread over 18 files. Consumers now include `sys/prod/metrics.go`, `sys/prod/seglog.go`, `sim/kafka/segtiming.go`, `segcaps.go`, `invariants.go`, `oracle.go`. Field keys (`put_ns`, `wait_ns`, `commit_ns`, `total_ns`, `inflight`, `buffered`, ...) are literals too; 24 time-field literals in just three consumers | Worse and now operationally risky: a typo silently zeroes a Prometheus series |
| 5. Fact field readers | n/a | 3 independent copies of "find field k in a fact": `prod.field/fi/fs` (`seglog.go:43`), `sim/kafka.fieldOf` (`oracle.go:149`), and an open-coded loop in `prod.Metrics.Observe` (`metrics.go:105,116`) | New duplication |
| 1. Language drift | glossary proposed | No `docs/glossary.md` exists. `release` still means "answer with error" (`segment.go`), `pbatch`/`op`/`segment`/`BatchRange` unchanged. `BatchRange` is in 51 places and `Base == -1` is the rejection test in 3 | Valid, not started |
| 3. Value objects | proposed | Not started. `core.SegmentRef` 23 uses, `SegmentKey` 32 uses (all `string`), `core.TopicPartition{` literal built in 55 places | Valid, big blast radius |
| 4. Broker aggregates | proposed | `segment.go` now holds 3 types and 20 functions: open buffer, sealed segment state machine (5 states), commit scheduler (`pump`), isolation, retry, ack/release, trip. `bufBytes` is touched in 7 places across 3 files; `slot.pending` in 5 places across 4 files | Worse and more urgent (performance work lands here) |

New growth since the earlier proposal:

- **Observers.** `sys/prod/metrics.go` (155 lines), `seglog.go` (132), `cp/cpobs` (346), `sys/awsobs` (244),
  `sys/runinfo` (4 files, about 470). Prometheus names are literals at the construction site
  (`metricsNS`/`"manicp"` plus a `Name:` string; 17 `Name:` literals in the observers and 19 more in `sys/`
  and `cp/`). The same names are re-typed in Python and JSON: `deploy/aws/observability/gen/gen_dashboards.py`
  (28 mentions), `bench/summarize.py` (15), `docs/observability.md` (46), dashboards JSON. There is no test
  tying them together.
- **Command wiring.** `cmd/manibroker/main.go` is 206 lines; `main()` is 160 lines, the longest function in the
  repo. `cmd/manicp/main.go` `main()` is 86 lines. Copy-pasted between the two: `-metrics-addr` flag plus
  listen/`http.Serve(runinfo.Mux(...))` goroutine, `-log-level`, `-run-id`, `-manifest-out` (identical 190-char
  help strings), `runinfo.New`/`NewLogger`/`slog.SetDefault`/"starting" log with 6 identical attributes,
  manifest write and log block (`writeManifest` in the broker, inlined in the CP), `fatal()`. About 45 lines
  in each binary. `manibench` and `manisim` have their own `main` plus `flag` parsing.
- **Tuning flags.** Six flags (`-segment-bytes`, `-segment-window`, `-max-segment-*`, `-max-buffered`,
  `-resume-buffered`) exist in four spellings: flag definition, `tuning` struct, `apply`, the "tuning" log
  line, and the manifest map (`main.go` lines 174-186 repeat all of them twice, with different key names:
  `segment_window` in the log, `segment_window_ns` in the manifest). Defaults live in `broker.DefaultConfig`
  and `withDefaults` (two places, `config.go:95` and `:130`).
- **Test helpers.** Duplicated names in `_test.go` files: `step` x3, `setup` x3, `run` x3, `put` x3,
  `newHarness` x3, `commit`, `fact`, `counter`, `report`, `seeds`, `firstCatch`, `ioOfAction` x2 each.
  `broker/harness_test.go` (406 lines), `sim/engines/internal/fakeio` and `cp/cptest` each carry their own
  version of "drive a machine, collect actions". Test code is 2,021 lines in `broker/` against 1,949 of source.
- **Mutants.** 9 `Bug*` modes, 9 `b.cfg.Bug` checks inside production flow control (`addBatch` has three).
  Acceptable (test-only, decision 25) but each new performance feature will want its own mutant.
- **Benchmark finding** (`aws-benchmark-results-r1.md`). Ceiling about 12 segments/s and about 115-130 MB/s
  per broker. At 32 clients the queue behind earlier commits is about 1,000 ms against 65 ms of commit, so ack
  latency is 1.2 s: the serial commit stage (`pump()` commits only `b.segs[0]`) is the bottleneck, not DynamoDB
  (commit 30-60 ms, zero throttles). Every performance item below goes through `segment.go`.

Still good (do not touch): `core` as published language, `veritas` as independent spec, `cp/wire.go` as
anti-corruption layer, `format/` and `kwire/` as clean modules, facts as the single observation channel.

## 2. Quick wins (all S, hash-neutral, parallelizable)

**Q1. `docs/glossary.md` and doc comments.** Carry over the earlier glossary (Segment, SegmentKey,
ObjectRange, RecordBatch/AdmittedBatch/PendingBatch, Assignment/Verdict, Broker epoch vs Producer epoch,
Trip, Ack, Request part, Slot) and add the new terms: *seal*, *flush window*, *commit queue*, *wait / put /
commit time*, *backpressure*, *run*, *cell*. Files: new `docs/glossary.md`; one-line comment edits.
Proof: `go build`, `go vet`. Risk: none.

**Q2. Unexported broker renames.** `release` to `reject`, `pbatch` to `admitted`, `op` to `part` (or
`request part`), `sealBatches` (function) vs `sealBatches` (const `sealBatches = "batches"`) collision: the
function and the reason constant have the same identifier root; rename the constant to `sealByBatches`.
Files: `broker/*.go` only (conflicts with the compaction agent on `segment.go`/`config.go`: **[after
compaction]**, or do `ops.go`, `produce.go`, `conn.go`, `fetch.go` first). Proof: golden hashes, `StateHash`
unchanged because no private field order changes.

**Q3. Fact name and field constants in `core`.** New `core/factnames.go`:
`const FactSegmentSealed = "segment_sealed"` and the key constants per fact (`KeyPutNS = "put_ns"`...),
including a doc comment per fact listing fields in order and a comment "frozen, decision 12". Replace
literals in `broker/{broker,segment}.go`, `sys/prod/{metrics,seglog}.go`, `sim/kafka/{segtiming,segcaps,
invariants,oracle}.go`, `sim/echo`. Add `core.FactField(f, key)` (+ `FactI64`, `FactStr`) to kill the three
field-lookup copies (pure, no maps, detcheck-safe). Add one test in `core` pinning the list of names (a
sorted slice literal) and one in `broker` that runs a scenario and asserts every `emit` name is in the list.
Hash: neutral, constants are the same strings. Proof: golden hashes, mutants, plus the new pin test. Effort S
(mechanical; about 100 edits). Conflict risk: `broker/segment.go` **[after compaction]**; consumers can go first.

**Q4. Metric name constants and a contract test.** One file per binary listing names and labels:
`sys/prod/metricnames.go`, `cp/cpobs/names.go` (exported consts). Add a test that scrapes a registered
`Metrics` and checks every const is present; add `scripts/check-metric-names.sh` (grep the const set against
`gen_dashboards.py`, `summarize.py`, `docs/observability.md`). Hash: none (outside the determinism closure).
Effort S. Value: dashboards and the bench summarizer break silently today.

**Q5. `cmd/internal/wiring` (shared observer and process wiring).** New package, outside the deterministic
closure, holding what the two mains copy: `Flags` struct (`MetricsAddr`, `LogLevel`, `RunID`,
`ManifestOut`) with `Register(fs)`, `Setup(service, brokerID) (log, info)`, `ServeMetrics(addr, handler)`,
`WriteManifest(...)`, `Fatal`. Mains shrink about 45 lines each; `main()` in `manibroker` drops from 160 to
under 90 together with Q6. Hash: none. Proof: `cmd/manibroker/tuning_test.go`, a flag-help golden test
(`-h` output unchanged except nothing), `it/` hermetic tests, the runbook smoke start. Conflicts: touches
`cmd/` only; the uncommitted `cmd/manibroker` edits (tuning flags) should be committed first.

**Q6. One effective-config struct.** `tuning` already exists; make it produce `Describe() map[string]any`
(one key set, one unit convention) used by both the "tuning" log line and the manifest; delete the two
hand-written maps. Keep `broker.DefaultConfig` as the single default source (remove duplicate defaults in
`withDefaults` by having it merge from `DefaultConfig`). Files: `cmd/manibroker/{main,tuning}.go`,
`broker/config.go` **[after compaction]** for the defaults half. Risk: the manifest key names are read by
`summarize.py`/`collect-results`; keep the existing keys (add `_ns` suffix rule to the test).

**Q7. Shared `core.Hasher`.** One FNV-1a type in `core` (`type Hasher uint64`, methods `Byte`, `U64`, `Str`,
`Bytes`, `Bool`) replacing the 6 hashers and 4 `fnvU64` copies and 4 `fnvu64_test.go`. This is allowed:
`core` is already imported by all, no maps/time. Must produce bit-identical sums: keep the offset basis and
prime, and keep `sim`'s struct-form `hasher` as a thin wrapper only if its byte order differs (check
`sim/hash.go` first; if it differs, leave it, document why, and do the other five). Hash: **neutral by
construction, proven by the golden hashes**; this is the one quick win where a mistake changes every hash, so
the golden test is the whole test. Effort S-M, sequential per package but parallelizable across packages
after `core/hasher.go` lands.

## 3. Structural refactors

### S1. Extract the segment commit pipeline from `Broker` (M, the enabler for performance work)

Today `segment.go` mixes four things behind one `Broker`: (a) the open-buffer rules (`addBatch`, caps),
(b) the sealed segment state machine (put, backoff, put-done, commit, commit-backoff), (c) commit
scheduling (`pump` = "only the head commits"), (d) answering (`committed`, `isolate`, `ack`, `release`,
`failSegment`, `trip`). The benchmark says (c) is the policy that must change, so make it a seam.

Plan, same package, same state, files only (decision 22, and `StateHash` reads private fields):

1. `broker/open.go`: `openSegment`, `addBatch`, `seal`, seal reasons, `commitItems`, `maxItemGrowth`
   (move, no edit). Hash-neutral by construction.
2. `broker/pipeline.go`: `segment`, `segState`, `put`, `onPutDone`, `retry`, `segTimer`, `remove`,
   `endSegment`, plus a `commitSched` type, a plain struct (no interface values holding funcs, no maps):

   ```go
   // commitSched decides which uploaded segments may have a CommitSegment in flight.
   type commitSched struct{ mode commitMode } // commitSerial (today), commitDisjoint, commitGroup
   func (c commitSched) ready(segs []*segment) []*segment
   ```

   `pump()` becomes `for _, s := range b.sched.ready(b.segs) { acts = append(acts, b.commit(s)...) }`.
   Serial mode returns `segs[0]` iff `segPutDone`, identical behaviour. A `switch` on `mode` (not a func
   field, detcheck-safe).
3. `broker/answer.go`: `committed`, `isolate`, `failSegment`, `ack`, `release`, `trip`, with the
   Kafka-code translation from S2.
4. `Broker` keeps `segs []*segment` and `open`; `bufBytes` accounting becomes two methods
   (`admit(pb)` / `retire(s)`) so the only writes to `bufBytes` are in one file (today 7 places, 3 files).

Hash: neutral (moves plus one extracted method; field order in `hash.go` untouched). Proof: golden hashes in
`sim/kafka` and `sim/echo`, all 9 mutants caught at the same first seed, `TestDeterminismCanary`,
`broker/conformance_test.go`, `flush_test.go`, `timing_test.go`. Risk: low-medium; do it as three commits
(1, 2, 3), each green. **[after compaction]** since the other agent is editing `segment.go`.

### S2. Kafka error translation in one place (S-M)

`broker/kafkaerr.go`: `type kcode int16` with the current `err*` constants moved from `config.go:164`;
`func verdictCode(r core.BatchRange) kcode` (the `committed()` switch: OK, OutOfOrder, ProducerFenced,
else UnknownServer); `func failCode(cause failCause, pb *admitted) kcode` (replaces `isolate()`'s inline
`errInvalidPIDMapping` and `exhaustCode`). `pb.part` and `op.lpart/fpart` keep holding `*kmsg` structs for
now; a later `answer` handle can cut that leak but is not required. Values stay `int16` on the wire.
Hash: neutral (codes are the same numbers; confirm `produce_acked`/`segment_failed` `code` field is still
`I64(code)`). Proof: broker unit tests, golden hashes. Can precede S1 for `ops.go`/`produce.go` call sites.

### S3. Pluggable commit scheduling (L, performance work, not pure refactor)

Built on S1. Three modes behind `commitSched`:

- **Parallel disjoint-partition commits.** Two sealed segments may be in `segCommit` at once if their
  partition sets are disjoint (offset order only matters per partition). Needs `ready()` to see the
  partitions of segments already in flight, and `onCommit` to retire out of order (`remove(s)` already
  searches, `endSegment` calls `pump`). Invariant to keep: per partition, commits in seal order, and ack only
  after commit.
- **Group commit.** Merge the batch lists of consecutive uploaded segments into one `CommitSegment`; needs a
  CP contract change (multiple segment keys per transaction), so it collides with the 100-item budget
  (decision 35) and with range compaction. Defer until compaction is merged.
- **Serial** stays as the default and the reference for lockstep.

Hash: **not neutral** for the non-serial modes (new schedule changes traces); serial mode must stay
bit-identical, so land the interface first (S1) and the new modes behind a `Config` field defaulting to
serial. Add a mutant `BugCommitOutOfOrder` (commits overlapping partitions concurrently) that the oracle must
catch. Needs new invariants in `sim/kafka/invariants.go`: per-partition offsets monotone in seal order.

### S4. Value objects in `core` (M-L, deferred)

`core.SegmentKey` (named string), `BatchIdentity{Producer, PEpoch, BaseSeq}`, `OffsetRange{Base, Count}`,
and a `Verdict` replacing `Err/Dup/Base==-1` in `BatchRange`. Blast radius is every package (51 uses of
`BatchRange`, 32 of `SegmentKey`, 23 of `SegmentRef`, 55 `TopicPartition{` literals). Hash: neutral only if
the tag/hash encoders keep field-by-field order (verify). Do `SegmentKey` alone first (smallest, 32 uses; the
`format.SegmentKey()` function becomes `format.NewSegmentKey`). Everything else waits until compaction and the
performance work have landed, because both rewrite `BatchRange` consumers. Keep `veritas` and `dynamo` types
independent (decision 21): they convert at the boundary, they do not share the value-object logic.

### S5. Retry/backoff helper (S-M)

One pure `core.Backoff{Base, Max}` with `Delay(n)`, used by `Broker.backoff` and `sim/echo.backoff` (same
doubling, same cap; verify identical for n=1..MaxAttempts before swapping). Leave `dynamo` loops on their own
(real time, jitter, `ctx` sleep: they are edge code and decision 21 keeps them separate); at most share a
tiny `sleep` in `cp/dynamo`. A shared "retry skeleton" across broker segment vs op is NOT proposed: the two
state machines differ (timer gen, conn-closed exit) and merging them is exactly the risk in section 5.
Hash: neutral if the delays are equal.

### S6. Observer wiring and test harness consolidation (M)

- `sys/prod`: split `metrics.go` into a table of collectors built from the Q4 constants, and make
  `SegmentLog` and `Metrics` consume facts through one `FactSink` fan-out (`main.go` already hand-composes
  `prev`/`segLog`; move the compose into `prod.NodeConfig` as `Facts []func(core.Fact)`).
- Tests: one `internal/machinetest` (driver: `Step`, `Collect`, `Fact(name)`, `Counter`) replacing the
  duplicated `step/setup/run/put/newHarness` helpers in `broker`, `sim/echo`, `sim/kafka`, `cp/cptest`.
  Do not share it with `veritas` (decision 21: spec tests stay independent).

## 4. Execution order, parallelism, proofs

| # | Item | Effort | Risk | Hash | Proof | Conflicts |
|---|---|---|---|---|---|---|
| Q1 | glossary | S | none | neutral | build | none |
| Q3 | fact constants + field readers | S | low | neutral | golden, mutants, pin test | `segment.go` (broker side after compaction) |
| Q4 | metric constants + contract test | S | low | neutral | new scrape test | `sys/prod`, `cp/cpobs` |
| Q5/Q6 | cmd wiring, config describe | S-M | low | neutral | tuning tests, `it/`, `-h` golden | `cmd/`, defaults half in `config.go` |
| Q7 | `core.Hasher` | S-M | medium | neutral, proven by golden | golden hashes (the only check) | all hash.go files, 6 packages |
| Q2 | broker renames | S | low | neutral | golden | `segment.go`, `config.go` |
| S2 | `kafkaerr.go` | S-M | low | neutral | broker tests, golden | `segment.go` |
| S1 | pipeline extraction | M | low-med | neutral | golden, 9 mutants same seed, canary, conformance | `segment.go` |
| S5 | `core.Backoff` | S-M | low | neutral | golden + delay table test | `broker.go`, `sim/echo` |
| S6 | observer fan-out, test harness | M | low | neutral | existing tests | `sys/prod`, tests |
| S3 | commit scheduling modes | L | high | serial neutral, others rebaseline | new mutant + invariants, lockstep | `segment.go`, CP contract |
| S4 | value objects | L | medium | neutral if verified | golden | every package |

Parallel waves (separate subagents, disjoint files):

- **Wave A (now, safe during compaction):** Q1 (docs only, new file), Q4 (`sys/prod`, `cp/cpobs`; the `cpobs`
  edits are outside what compaction touches), Q5 (`cmd/`), consumer half of Q3 (`sys/prod/seglog.go`,
  `sim/kafka/*`). These four touch disjoint directories.
- **Wave B (after compaction merges):** Q3 broker half + Q2 + S2, one agent, one commit each (all in
  `broker/`). In parallel with it, Q7 `core.Hasher` in the other packages except `broker` (`veritas`, `sim*`),
  then `broker/hash.go` last.
- **Wave C:** S1 (one agent, three commits), in parallel with S5 and S6 (different files).
- **Wave D:** S3, then S4.

Standard validation for every step: `go vet`, `go test ./...`, `scripts/ci-determinism.sh` (detcheck),
golden trace hashes unchanged, each mutant caught at the same first seed, `TestDeterminismCanary`, lockstep
(veritas vs memstore and DynamoDB), and `it/` hermetic tests for anything under `cmd/` or `sys/`.

## 5. What NOT to do

From the earlier list, still binding: no repositories over broker state, no I/O-doing domain services, no
event sourcing or bus, no func/map handler dispatch (detcheck), no shared idempotence-rules package
(decision 21), no oracle importing `veritas` logic, no renaming of facts, `ErrCode` strings or enum order
(decision 12), no broker sub-package split (decision 22; `StateHash` reads every private field and
`pbatch` / `slot` / `conn` form a pointer cycle), no generic entity base type, no Kafka numbers in `core`.

New:

- **Do not put Prometheus or `slog` into `broker/`.** Decision 36: metrics are facts counted at the edge.
  Q3/Q4 make the facts typed; they must not make the broker aware of metrics.
- **Do not make `cmd/internal/wiring` import `broker` internals or the sim.** It sits above `sys/prod`.
- **Do not unify `broker` segment retry and `ops` retry** into one generic state machine; share only the delay
  function (S5).
- **Do not share the commit decision code between `veritas` and `cp/dynamo`** to "dedupe" `decide.go`; the
  lockstep check relies on two implementations.
- **Do not add an interface with function fields for commit scheduling.** Use a struct with a mode enum and a
  `switch`.
- **Do not generalize `core.Hasher` into `hash/fnv` or `maphash`.** The sums are golden; `maphash` is seeded
  per process.
- **Do not change the flag names or manifest keys** read by the runbook and `summarize.py` while doing Q5/Q6.
- **Do not edit `cp/dynamo`, `veritas` or `sim/engines/cpengine` until range compaction merges.**

## 5b. Go idiom check (Effective Go)

Applied to the items above; the proposal follows these, and reviewers should hold the implementations to them.

- **Names without stutter.** `core.Hasher` with `Byte/U64/Str`, not `HasherWriteByte`; `core.Backoff.Delay`, not
  `GetDelay`; accessors without `Get` (existing `BrokerFenced()` is right). New constructor `format.NewSegmentKey`
  follows `ring.New`-style naming inside its package. Fact constants are `core.FactSegmentSealed` (the
  `core.` prefix already says "core", so avoid `core.CoreFact...`).
- **Package names:** `internal/machinetest` and `cmd/internal/wiring` are lowercase single words; no
  underscores, no `util`/`common`.
- **Small types, not big interfaces.** Commit scheduling is a struct plus mode enum (S3), consistent with
  detcheck. A one-method interface named `-er` is fine in `sys`, never in `core` state. Add compile-time
  checks `var _ core.Machine = (*Broker)(nil)` where missing.
- **`iota` enums** for `commitMode`, `kcode` groups and seal reasons (today seal reasons are strings that feed
  the `reason` fact; keep the strings, they are hash inputs, but derive them from one table).
- **Errors:** return `error` last, keep the success path left-aligned, prefix messages with the origin
  (`"tuning: -segment-bytes: ..."`); the cmd wiring package returns errors and lets `main` call `Fatal`.
  `panic` stays only for impossible states via `env.Unreachable`.
- **Doc comments** sit directly above each top-level declaration and start with the name (Q1 and Q3 add the
  per-fact field-order comments this way).

## 6. Sequencing against the performance work

1. **Now (while range compaction is in flight):** wave A only. None of it touches `cp/dynamo`, `veritas`,
   `sim/` engines or `broker/{segment,config}.go`.
2. **Range compaction merges** (reduces commit items; changes the cost model in `commitItems`, which S1
   moves verbatim, so S1 must follow it, not precede it).
3. **Wave B then S1**, then commit pipelining. Commit pipelining/parallel disjoint commits is S3 and needs S1's
   `commitSched`; doing it without S1 means adding the fourth responsibility to a 430-line file.
4. **Compression** (batch bytes in the segment; touches `format/` and `openSegment.bytes`, `bufBytes`
   accounting): after S1, because the bytes accounting is centralized by `admit`/`retire`. Hash will change.
5. **Group commit** (CP contract change) after parallel disjoint commits is measured: it needs the new
   multi-segment CP op, so it is a CP change first (`veritas` spec, then `dynamo`), and conflicts with
   compaction's `cp/dynamo` work.
6. **Multi-broker Metadata** (decision 29 leaderless): independent of the pipeline. Touches `broker/metadata.go`
   and `ops.go`; do S2 first so the new error paths use `kcode`. S4 (`SegmentKey`, `Verdict`) last, in a quiet
   period.

## 7. If you only do three things

1. **Q3 + Q4: typed fact names/keys and metric names with contract tests.** Highest value over risk now:
   the observers, dashboards and the bench summarizer all depend on strings that nothing checks.
2. **S1 (after compaction): extract the commit pipeline with a `commitSched` seam.** The benchmark's ceiling
   is the serial commit stage; this is the one refactor that directly enables the fix, and it is hash-neutral.
3. **Q5 + Q6: shared cmd wiring and one effective-config description.** Removes about 90 duplicated lines,
   shrinks the 160-line `main()` and makes the next tuning flag a one-place change.

Runners-up: Q7 `core.Hasher` (high duplication, but one-shot risk to every golden hash) and S2.
