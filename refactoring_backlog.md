# Refactoring backlog

Prioritized Fowler (and selective DDD-adjacent) work for this repo.
Pick items **one wave at a time**. Do not start P3 god-object splits until P1 value objects and relevant P2 shared kernels have landed for that area.

**Status legend:** ~~struck~~ = done · open = not started

---

## Sequencing rules

1. **Safety first** — dead code / unused flags before behavior reshapes.
2. **Types before moves** — value objects / shared primitives before Extract Class / Move Function that touch those fields.
3. **Share before split** — dedupe forks (logsm↔logcp, echo↔kafka) before carving god types further apart.
4. **Extract Class last within a package** — Split Phase / Extract Function on hot paths first so classes have clean seams.
5. **One vertical slice per PR** — one type, one subsystem, or one shared helper — not “refactor broker.”
6. **Green gate (mandatory)** — after **every backlog line item** and before **every commit**, run the relevant tests and confirm the build is green. Do not strike an item or commit until tests pass. Prefer package-scoped `go test` for the touched packages first; widen if the change crosses packages. Sim hash / lockstep / cptest packages remain the hard gate for deterministic/core changes — don’t cross packages until the type/helper is stable.

Suggested waves: **P0 → P1 → P2 → P3 → P4**.

| Wave | Theme | Why first |
|------|--------|-----------|
| **P0** | Dead code, unused flags, tiny cleanups | Zero behavior risk; shrinks noise |
| **P1** | Value objects + shared micro-helpers | Types stabilize every later move |
| **P2** | Cross-copy dedupe (logsm/logcp, echo/kafka, codecs, cmd boot) | Stops paying twice |
| **P3** | Split Phase / Extract Function on hot paths | Makes Extract Class safe |
| **P4** | Extract Class / polymorphism / ACL finish | Structure after seams exist |

**Next up:** P1 `TopicPartition` (P0 cleared).

---

## P0 — Just delete / clarify

- [x] ~~Unused `openSegment.commitItems` — **Remove Dead Code** — `broker/segment.go` (note: `cpengine.commitItems` is live; do not remove that)~~
- [x] ~~Unused `asErr` — **Remove Dead Code** — `cp/logcp/service.go`~~
- [x] ~~Unused `prep` HistogramVec — **Remove Dead Code** — `cp/cpobs/cpobs.go`~~
- [x] ~~`query(..., consistent bool)` always `true` — **Remove Flag Argument** — `cp/logcp/dynlog.go`~~
- [x] ~~`parseBatch(..., verifyCRC bool)` — **Remove Flag Argument** — `kwire/batch.go` (prefer `parseBatch` / `parseBatchUnverified` or keep public wrappers only)~~
- [x] ~~`dispatch(..., resolved bool)` — **Remove Flag Argument** — `broker/ops.go` → `dispatch` vs `dispatchAfterRefresh` (or equivalent)~~
- [x] ~~`retry(s, from segState)` — **Remove Flag Argument** — `broker/segment.go` → keep only `retryPut` / `retryCommit`~~
- [x] ~~`lost(..., abort bool)` — **Remove Flag Argument** — `sim/kafka/workload.go` + `sim/echo/workload.go` → `lostAbort` / `lostPeerClosed` (or equivalent)~~
- [x] ~~`hash(cached bool)` — **Remove Flag Argument** — `sim/engines/netsim/netsim.go` → explicit cached vs full methods~~
- [x] ~~`pull` / `SyncTo` `force bool` — **Remove Flag Argument** — `cp/logsm/replica.go` → `pullNow` / `pullPaced` or always-force first read~~
- [x] ~~`attempt("won"|"lost"|"error")` — **Remove Flag Argument** / typed result — `cp/logcp/committer.go` + `cp/logcp/metrics.go`~~
- [x] ~~Redundant `cur = max(cur, n)` after `if n > cur` — **Inline Variable** — `cp/applier/applier.go`~~

---

## P1 — Value objects + tiny shared helpers

### How to land each VO

1. Add type in the **owning package** (`core` or `kwire` for TP/code; `cp` or shared internal for Slot).
2. Convert **one consumer package** end-to-end (e.g. broker only), keep adapters at wire. **Run tests; green before continuing.**
3. Convert sim + veritas + cp call sites. **Run tests after each package (or each commit).**
4. Delete leftover primitive aliases / helper overloads. **Run tests again.**
5. No behavior change — tests/hashes unchanged. **Strike the backlog line only after green.**

### Explicitly out of P1

Bounded-context merge, Extract Class on Broker/Service/kernel, Dynamo ACL — those are P2–P4.

### Items

- [ ] Topic/partition pair (`TopicPartition` / `TP`) — **Replace Primitive with Object** — broker, kwire, sim/kafka, wire
- [ ] Kafka `err*` `int16` → `KafkaCode` — **Replace Primitive with Object** — `broker/config.go` + call sites
- [ ] Slot / HW / offset IDs — **Replace Primitive with Object** — logcp/logsm replica, dynlog
- [ ] CRC table once — **Replace Temp with Query** — `format/format.go` (+ kwire if shared)
- [ ] `fieldOf` / fact getters — **Move Function** / **Extract Function** — echo, kafka oracle, `sys/prod/seglog`, metrics
- [ ] `sample` / TwoSpeed — **Pull Up Method** / **Parameterize Function** — fakeio, logengine, echo/kafka config
- [ ] FNV helpers if duplicated — **Move Function** — broker hash, netsim, etc.
- [ ] Resp type-assert boilerplate — **Extract Function** / **Parameterize Function** — logsm store, applier store, memstore, logcp

**VO landing order:** (1) `TopicPartition` → (2) `KafkaCode` → (3) Slot/position IDs → (4) optional key wrappers at Dynamo/S3 edge.

---

## P2 — Stop paying twice (shared kernel / template)

- [ ] DynLog Append/Read/query/CheckTail — **Substitute Algorithm** / shared dynloglet — `cp/logcp/dynlog.go` ↔ `cp/logsm/dynlog.go`
- [ ] Replica apply / hole-fill / buf — **Extract Class** → shared core — `logcp/replica.go` ↔ `logsm/replica.go`
- [ ] Commit append/retry — **Combine Functions into Class** — `logcp/committer.go` ↔ `logsm/committer.go`
- [ ] Binary reader/cursor — **Extract Class** — `logcp/format`, `logsm/codec`, `applier/keys`
- [ ] Wire Batch/Range map pairs — **Combine Functions into Transform** / **Extract Function** — `cp/wire.go`
- [ ] Echo ↔ kafka client FSM — **Extract Superclass** / **Form Template Method** — `sim/echo/workload` ↔ `sim/kafka/workload`
- [ ] Echo ↔ kafka campaign presets — **Parameterize Function** / **Form Template Method** — `*/config.go`, invariants
- [ ] Boundary faults plan — **Form Template Method** — `fakeio` ↔ `logengine`
- [ ] `serveMetrics` + `writeManifest` — **Extract Function** / **Move Function** — `cmd/manibroker` ↔ `cmd/manicp`
- [ ] Service boot skeleton — **Form Template Method** — same cmds
- [ ] ApiVersions / Supported cache — **Substitute Algorithm** — `kwire/supported.go`
- [ ] Encode/Decode framing — **Extract Function** / **Introduce Parameter Object** — `kwire/codec.go`

**P2 rule:** pick **one** fork at a time (logsm/logcp dynlog+replica **or** cmd boot **or** shared binary reader) — not all three.

---

## P3 — Hot-path clarity (Split Phase / Extract Function)

Do per package **after** P1 types for that package.

### broker

- [ ] **Split Phase:** `flow`, `addBatch`, produce/fetch skeleton vs admit, `onFetchGet`, `drain`
- [ ] **Extract Function:** backoff, find-by-id, mirror response shape, `checkBatch` groups, metadata topic builder, ApiVersions single path, StateHash folds
- [ ] **Decompose Conditional:** timers, ListOffsets timestamps, fetch corrupt path (**Guard Clauses**)
- [ ] **Separate Query from Modifier:** `req`/`id`, `finishPart`
- [ ] **Encapsulate Collection:** op/conn find-drop
- [ ] **Consolidate Conditional Expression:** `ack`/`release` → `answerPart`

### kwire / format

- [ ] **Split Phase:** `parseBatch`, `format.Decode` / Encode steps
- [ ] **Extract Function:** BuildBatch encode loop, index entry pair

### cp/logcp

- [ ] **Split Phase:** `Start`, `catchUp`, `applyBatch`, `Checkpoint`, `TrimBelow`, fresh join/wait
- [ ] **Extract Function** / **Parameterize Function:** `withFresh`, wait-for-finish, pruneObjects, roleTick arms
- [ ] **Introduce Parameter Object:** `applyCmd` site, CKPT attrs → `ckptPointer`
- [ ] **Move Function/Field:** `trimmedPast`, Dynamo attrs out of checkpoint, role clocks off replica
- [ ] **Encapsulate Collection:** buf/waiters
- [ ] **Separate Query from Modifier:** `holeTick`
- [ ] **Rename Field:** `adv`/`kick`
- [ ] **Extract Function:** metrics registration by domain; batchwrite retry body
- [ ] **Parameterize Function:** config `setI`/`setI64`/… defaults

### cp/logsm

- [ ] **Extract Function:** SyncTo loop, Marshal checkpoint, Do commit loop
- [ ] **Decompose Conditional:** Read CheckTail retry
- [ ] **Encapsulate Collection:** buf/watch/done
- [ ] **Separate Query from Modifier:** `holeDue`
- [ ] **Split Phase** / **Extract Function:** codec encode table
- [ ] **Introduce Assertion:** checkpoint Machine is veritas
- [ ] Unify `decodeSlot` with logcp

### cp/applier

- [ ] **Extract Function** / **Introduce Parameter Object:** `assign` ends/runs/order
- [ ] **Decompose Conditional:** `commitTx`
- [ ] **Replace Temp with Query:** digest path
- [ ] **Split Phase:** SnapshotAt / Restore
- [ ] **Move Function:** Trim/Window/StateHash/Assigned off snapshot file
- [ ] **Extract Function:** pebble `newTxn`, digest build

### cp/cpclient / cpobs / fakedb

- [ ] **Split Phase** / **Extract Function:** client do/doAt/decode; Middleware observe→log
- [ ] **Introduce Parameter Object:** Faults; optional endpoint struct
- [ ] **Change Function Declaration:** `errOf func() string`
- [ ] **Extract Function:** fakedb preamble; Put/Update apply; `parseBetween`
- [ ] **Decompose Conditional** / **Guard Clauses:** expr SET, BatchWriteItem

### sim

- [ ] **Split Phase** / **Extract Function:** kernel setup/boot/pump/loop entry; netsim send/dial/flush; logengine defaults; fakeio Execute; kafka newBatches/onProduce/onFetch/AtEnd; echo onGet/onCommit/next; runner seed→fold
- [ ] **Decompose Conditional:** attrition, crash, produce classify, reply
- [ ] **Consolidate Duplicate Conditional Fragments:** callEngine, segcaps Observe/AtEnd, config knobs
- [ ] **Replace Loop with Pipeline:** serveReads, TP assembly, response index by TP
- [ ] **Extract Function:** producer/consumer shared TP request helper; codes.go predicates
- [ ] **Move Function:** CPTruth view; engineCtx file; unify landBody/rstBody

### sys

- [ ] **Extract Function** / **Split Phase:** net Shutdown/accept; executor timeout template; metrics NewMetrics + Observe map; seglog attrs; awsobs finalize; s3store Get; run waitForWork; manifest enrich→put
- [ ] **Move Function:** conn loops; fact accessors; S3 writer overlap
- [ ] **Decompose Conditional** / **Guard Clauses:** enqueue, getFailed, classify ladder
- [ ] **Consolidate Duplicate Conditional Fragments:** inbox Control/Data Pop
- [ ] **Extract Function:** node `wireBundle`

### veritas

- [ ] **Split Phase** / **Extract Function:** execCommit; Get range; lockstep named shrink passes
- [ ] **Form Template Method:** prefix StateHash with ControlPlane

### cmd

- [ ] **Split Phase:** manibroker/manicp serve; openLog phases; manibench run; crapscore analyse
- [ ] **Extract Function:** openS3Objects, wireFacts, applySegment/BufferLimits, shouldFinalCheckpoint, Evaluate classify/improved, ParseTree helpers, coverageFor, openClients+cleanup, formatFailure, fail(), ReportView
- [ ] **Introduce Parameter Object:** brokerFlags groups, worker, BaselinePolicy, logFlags nesting
- [ ] **Preserve Whole Object:** runtimeSnapshot for manifest
- [ ] **Slide Statements:** validate log-level before “starting”
- [ ] **Decompose Conditional:** tuning apply, parseArgs, improved branches
- [ ] **Combine Functions into Class:** pebbleState lifecycle
- [ ] **Replace Conditional with Polymorphism** (light): scenario registry, bufferPolicy auto/fixed, store-kind

### tools / api

- [ ] **Extract Function** / **Parameterize Function:** json2yaml `renderChild`; detcheck optional checker list; hoist loadAllow

---

## P4 — Structure (Extract Class / polymorphism / ACL)

After P1–P3 seams exist.

- [ ] Broker collaborators — **Extract Class** — conn table, segment pipeline, op table
- [ ] op by kind — **Replace Type Code with Subclasses** / **Replace Conditional with Polymorphism** — `broker/ops.go`
- [ ] segState machine — **Replace Conditional with Polymorphism** — `broker/segment.go`
- [ ] logcp.Service parts — **Extract Class** — queue/committer, fresher, checkpoint, role, dyn attr helpers
- [ ] Shared replica/dynlog types — **Extract Class** (finish P2) — after shared kernel exists
- [ ] `sim.run` / `logengine.Engine` / `netsim.Engine` / `kafka.Workload` — **Extract Class** — lifecycle, dispatcher, replicaSet, clientSession, linkTable
- [ ] cstate/opState/mutants — **Replace Type Code** / **Polymorphism** — kafka + echo
- [ ] executor Obj* switch — **Replace Conditional with Polymorphism** — `sys/prod/executor.go`
- [ ] Dynamo/S3/wire ACL — **Encapsulate Record** / **Extract Class** / **Hide Delegate** — ckptPointer, logical keys, store adapters (DDD edge)
- [ ] Bounded context formalize — shared kernel or hard ACL — logsm vs logcp; echo vs kafka (DDD #1; only after P2 dedupe choice)

---

## Suggested calendar

1. **Now:** clear P0. *(done — tests green on touched packages)*
2. **Next:** P1 `TopicPartition` → `KafkaCode` → Slot IDs (one PR each; **test + green per item/commit**).
3. **Then:** P2 pick one fork (**test + green per item/commit**).
4. **Then:** P3 Split Phase/Extract Function only where P1 types exist (**test + green per item/commit**).
5. **Last:** P4 Extract Class / polymorphism / ACL / formal bounded contexts (**test + green per item/commit**).

---

## Cross-cutting reminders (from earlier review)

Highest leverage themes (mapped into waves above):

1. Share or formally fork **logsm ↔ logcp** (P2 / P4)
2. **Extract Class** on god types after seams (P4)
3. **Remove Dead Code** / flags (P0)
4. Shared **cmd boot** + split **openLog** (P2 / P3)
5. Echo ↔ Kafka **client FSM / campaign** (P2)
6. Shared **binary reader** (P2)
