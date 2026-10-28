# Refactoring backlog

Prioritized Fowler (and selective DDD-adjacent) work for this repo.
Pick items **one wave at a time**. Do not start P3 god-object splits until P1 value objects and relevant P2 shared kernels have landed for that area.

**Status legend:** ~~struck~~ = done or obsolete · open = not started

**Ground truth (toys3 tip):** CP-DSM-1..8 landed. `controlplane/logcp` and `controlplane/logsm` are deleted (`5d8be1d`). Prod and sim share `controlplane/machine` + gRPC (`controlplane/api/v1`, `apiconv`). Dynamo log I/O is `controlplane/exec` (+ sim `logstore`). Path prefix `cp/` in older notes means `controlplane/`.

---

## Sequencing rules

1. **Safety first** — dead code / unused flags before behavior reshapes.
2. **Types before moves** — value objects / shared primitives before Extract Class / Move Function that touch those fields.
3. **Share before split** — dedupe remaining forks (echo↔kafka, cmd boot) before carving god types further apart.
4. **Extract Class last within a package** — Split Phase / Extract Function on hot paths first so classes have clean seams.
5. **One vertical slice per PR** — one type, one subsystem, or one shared helper — not "refactor broker."
6. **Green gate (mandatory)** — after **every backlog line item** and before **every commit**, run the relevant tests and confirm the build is green. Do not strike an item or commit until tests pass. Prefer package-scoped `go test` for the touched packages first; widen if the change crosses packages. Sim hash / lockstep / cptest packages remain the hard gate for deterministic/core changes — don't cross packages until the type/helper is stable.

Suggested waves: **P0 → P1 → P2 → P3 → P4**.

| Wave | Theme | Why first |
|------|--------|-----------|
| **P0** | Dead code, unused flags, tiny cleanups | Zero behavior risk; shrinks noise |
| **P1** | Value objects + shared micro-helpers | Types stabilize every later move |
| **P2** | Cross-copy dedupe (echo/kafka, cmd boot, codecs) | Stops paying twice |
| **P3** | Split Phase / Extract Function on hot paths | Makes Extract Class safe |
| **P4** | Extract Class / polymorphism / ACL finish | Structure after seams exist |

**Next up:** P3 `controlplane/machine` leftovers (`roleTick`, `maybeHoleFill`), or attrition/stall follow-up (`docs/cp-dsm-attrition-stall-followup.md`).

---

## P0 — Just delete / clarify

- [x] ~~Unused `openSegment.commitItems` — **Remove Dead Code** — `broker/segment.go` (note: `cpengine.commitItems` is live; do not remove that)~~
- [x] ~~Unused `asErr` — **Remove Dead Code** — was `cp/logcp/service.go` (package deleted)~~
- [x] ~~Unused `prep` HistogramVec — **Remove Dead Code** — `controlplane/cpobs`~~
- [x] ~~`query(..., consistent bool)` always `true` — **Remove Flag Argument** — was `logcp/dynlog.go` (now single `exec/dynlog.go`)~~
- [x] ~~`parseBatch(..., verifyCRC bool)` — **Remove Flag Argument** — `kwire/batch.go` (prefer `parseBatch` / `parseBatchUnverified` or keep public wrappers only)~~
- [x] ~~`dispatch(..., resolved bool)` — **Remove Flag Argument** — `broker/ops.go` → `dispatch` vs `dispatchAfterRefresh` (or equivalent)~~
- [x] ~~`retry(s, from segState)` — **Remove Flag Argument** — `broker/segment.go` → keep only `retryPut` / `retryCommit`~~
- [x] ~~`lost(..., abort bool)` — **Remove Flag Argument** — `sim/kafka/workload.go` + `sim/echo/workload.go` → `lostAbort` / `lostPeerClosed` (or equivalent)~~
- [x] ~~`hash(cached bool)` — **Remove Flag Argument** — `sim/engines/netsim/netsim.go` → explicit cached vs full methods~~
- [x] ~~`pull` / `SyncTo` `force bool` — **Remove Flag Argument** — was `logsm/replica.go` (deleted with twin)~~
- [x] ~~`attempt("won"|"lost"|"error")` — **Remove Flag Argument** / typed result — was `logcp/committer.go` (deleted with twin)~~
- [x] ~~Redundant `cur = max(cur, n)` after `if n > cur` — **Inline Variable** — `controlplane/applier/applier.go`~~

---

## P1 — Value objects + tiny shared helpers

### How to land each VO

1. Add type in the **owning package** (`core` or `kwire` for TP/code; `controlplane` or shared internal for Slot).
2. Convert **one consumer package** end-to-end (e.g. broker only), keep adapters at wire. **Run tests; green before continuing.**
3. Convert sim + veritas + controlplane call sites. **Run tests after each package (or each commit).**
4. Delete leftover primitive aliases / helper overloads. **Run tests again.**
5. No behavior change — tests/hashes unchanged. **Strike the backlog line only after green.**

### Explicitly out of P1

Bounded-context merge, Extract Class on Broker/Service/kernel — those are P2–P4.

### Items

- [x] ~~Topic/partition pair (`TopicPartition` / `TP`) — **Replace Primitive with Object** — broker, kwire, sim/kafka, wire~~
- [x] ~~Kafka `err*` `int16` → `KafkaCode` — **Replace Primitive with Object** — `broker/config.go` + call sites~~
- [x] ~~Slot / HW / offset IDs — **Replace Primitive with Object** — `core.LogSlot` is already the parameter on `controlplane/exec.Log` (`Append`/`Read`/`CheckTail`/`TrimThrough`)~~
- [x] ~~CRC table once — **Replace Temp with Query** — `format/format.go` (+ kwire if shared)~~
- [x] ~~`fieldOf` / fact getters — **Move Function** / **Extract Function** — echo, kafka oracle, `sys/prod/seglog`, metrics~~ (`core.Fact.Field`)
- [x] ~~`sample` / TwoSpeed — **Pull Up Method** / **Parameterize Function** — fakeio, logengine, echo/kafka config~~ (use `TwoSpeed.Sample`; echo/kafka `slow()` ctor dupes left)
- [x] ~~FNV helpers if duplicated — **Move Function** — broker hash, netsim, etc.~~ (`core.FnvU64`; echo/veritas naive hashers left)
- [x] ~~Resp type-assert boilerplate — **Extract Function** / **Parameterize Function** — stores / applier / memstore~~ (`core.RespAs`)

**VO landing order:** (1) `TopicPartition` → (2) `KafkaCode` → (3) Slot/position IDs → (4) optional key wrappers at Dynamo/S3 edge.

---

## P2 — Stop paying twice (shared kernel / template)

### Closed by CP-DSM (do not reopen as logcp↔logsm)

- [x] ~~DynLog Append/Read/query/CheckTail fork — **obsolete**: twin deleted; sole Dynamo loglet is `controlplane/exec/dynlog.go`~~
- [x] ~~Replica apply / hole-fill / buf fork — **obsolete**: twin deleted; live path is `controlplane/machine` (+ sim `logstore` for durable slots / CPTruth)~~
- [x] ~~Commit append/retry fork — **obsolete**: twin deleted; live path is `controlplane/machine/commit.go`~~
- [x] ~~Binary reader/cursor across logcp/format + logsm/codec — **obsolete as a cross-package fork** (twins deleted). Leftover same-package sticky cursors (`applier/keys.go` `reader`, `applier/entry.go` `decoder`, `machine/commit.go` `takeUvarint`) are optional P3 polish, not a P2 share~~
- [x] ~~Wire Batch/Range map pairs — **done/obsolete**: JSON `controlplane/wire.go` deleted (CP-DSM-7); gRPC uses `controlplane/apiconv`~~

### Landed

- [x] ~~Echo ↔ kafka client FSM — **Extract Superclass** / **Form Template Method** — `sim/internal/session` (`Loop` hooks; protocol + StateHash stay in scenarios; TraceHash canaries green)~~
- [x] ~~Echo ↔ kafka campaign presets — **Parameterize Function** / **Form Template Method** — `sim/internal/campaign` (Swarm `afterKnobs` preserves kafka PartialIOMax draw order; TraceHash canaries matched)~~
- [x] ~~Bridge boundary faults — **Form Template Method** — `fakeio.Params.Draw` shared by fakeio Execute and logengine (coin order preserved; TraceHash stable)~~
- [x] ~~`serveMetrics` + `writeManifest` — **Extract Function** / **Move Function** — `sys/runinfo.ServeMetrics` + `Publish` (manicp metrics gains Read/Write/Idle timeouts matching broker)~~
- [x] ~~Service boot skeleton — **Form Template Method** — same cmds — **not worth it**: after logger setup the two `serve` bodies share no steps; a template needs hooks without deleting branches~~
- [x] ~~ApiVersions / Supported cache — **Substitute Algorithm** — `kwire/supported.go` (`supported` table + `supportedByKey`)~~
- [x] ~~Encode/Decode framing — **Extract Function** — `kwire/codec.go` (`appendFlexibleTag`, `readBody`)~~

**P2:** closed.

---

## P3 — Hot-path clarity (Split Phase / Extract Function)

Do per package **after** P1 types for that package.

### broker

- [x] ~~**Consolidate Conditional Expression:** `ack`/`release` → `answerPart`~~
- [x] ~~**Split Phase:** `flow` → `updatePressure`/`syncReads`; flatten `addBatch` / `onFetchGet` (+ `intact`); left `drain` alone (already flat)~~
- [ ] **Extract Function:** backoff, find-by-id, mirror response shape, `checkBatch` groups, metadata topic builder, ApiVersions single path, StateHash folds
- [ ] **Decompose Conditional:** timers, ListOffsets timestamps, fetch corrupt path (**Guard Clauses**)
- [ ] **Separate Query from Modifier:** `req`/`id`, `finishPart`
- [ ] **Encapsulate Collection:** op/conn find-drop
- [ ] **Split Phase:** produce/fetch skeleton vs admit (remaining)

### kwire / format

- [ ] **Split Phase:** `parseBatch`, `format.Decode` / Encode steps
- [ ] **Extract Function:** BuildBatch encode loop, index entry pair

### controlplane/machine

Old `logcp` / `logsm` P3 names (`Start`, `catchUp`, `applyBatch`, `Checkpoint`, `TrimBelow`, `withFresh`, `SyncTo`, `holeTick`, `holeDue`, `decodeSlot`, `adv`/`kick`, `setI`/`setI64`, batchwrite retry) are **gone**. DSM already split catch-up / commit / checkpoint / role across files. Do **not** retarget the deleted logsm list onto `Machine`. Remaining smells:

- [ ] **Extract Function** / **Decompose Conditional:** `roleTick` arms — `controlplane/machine/role.go`
- [ ] **Move Field:** role clocks (`lastOwnAt` / `lastOtherAt`) off the replica struct if a role collaborator earns its place
- [ ] **Encapsulate Collection:** `buf` / waiters maps on `Machine`
- [ ] **Separate Query from Modifier:** `maybeHoleFill` (still decides and emits)
- [ ] **Extract Function:** `cpobs.New` metrics registration by domain (live registrar; HTTP `Middleware` is test-only husk)

### controlplane/applier

- [ ] **Extract Function** / **Introduce Parameter Object:** `assign` / `assignAt` ends/runs/order
- [ ] **Decompose Conditional:** `commitTx`
- [ ] **Replace Temp with Query:** digest path (`applier/keys.go`)
- [ ] **Split Phase:** SnapshotAt / Restore
- [ ] **Move Function:** Trim/Window/StateHash/Assigned off `snapshot.go`
- [ ] **Extract Function:** pebblekv `View` / `Update` bodies (no `newTxn`); digest build

### controlplane/cpclient / fakedb / exec

- [x] ~~cpclient HTTP `decode` / Faults / endpoint struct / `errOf` — **obsolete** (gRPC + `apiconv` / `mapRPCErr`; client holds an address list)~~
- [x] ~~cpobs Middleware observe→log as prod path — **obsolete** (manicp serves gRPC; Middleware only in cpobs tests; live hook is `APIOptions`)~~
- [ ] **Split Phase** / **Extract Function:** cpclient `do` / `doAt`
- [ ] **Extract Function:** fakedb preamble; Put/Update apply (`ops.go` / `expr.go` `applyUpdate` / `applyClause`); `parseKeyCond` (was `parseBetween`)
- [ ] **Decompose Conditional** / **Guard Clauses:** expr SET, BatchWriteItem
- [ ] **Extract Function** / **Split Phase:** `exec/dynlog` Append/Read/CheckTail clarity (single copy; no twin merge)

### sim

- [ ] **Split Phase** / **Extract Function:** kernel setup/boot/pump/loop entry; netsim send/dial/flush; logengine defaults + `plan`; fakeio Execute; kafka newBatches/onProduce/onFetch/AtEnd; echo onGet/onCommit/next; runner seed→fold
- [ ] **Decompose Conditional:** attrition, crash, produce classify, reply
- [ ] **Consolidate Duplicate Conditional Fragments:** callEngine, segcaps Observe/AtEnd, config knobs
- [ ] **Replace Loop with Pipeline:** serveReads, TP assembly, response index by TP
- [ ] **Extract Function:** producer/consumer shared TP request helper; codes.go predicates
- [ ] **Move Function:** CPTruth view helpers; engineCtx file; unify `landBody`/`rstBody` (fakeio vs netsim)

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
- [ ] **Slide Statements:** validate log-level before "starting"
- [ ] **Decompose Conditional:** tuning apply, parseArgs, improved branches
- [ ] **Combine Functions into Class:** pebbleState lifecycle
- [ ] **Replace Conditional with Polymorphism** (light): scenario registry, bufferPolicy auto/fixed, store-kind

### tools

- [ ] **Parameterize Function:** detcheck optional checker list (`checkFile` always runs imports/globals/node switch; no toggle). `loadAllow` / json2yaml / `api/openapi` share is gone — do not reopen.

---

## P4 — Structure (Extract Class / polymorphism / ACL)

After P1–P3 seams exist.

- [ ] Broker collaborators — **Extract Class** — conn table, segment pipeline, op table
- [ ] op by kind — **Replace Type Code with Subclasses** / **Replace Conditional with Polymorphism** — `broker/ops.go`
- [ ] segState machine — **Replace Conditional with Polymorphism** — `broker/segment.go`
- [ ] `controlplane/machine.Machine` — **Extract Class** (optional; file seams already exist as `commit.go` / catch-up in `machine.go` / `checkpoint.go` / `role.go`) — only if a collaborator earns a type boundary
- [x] ~~`logcp.Service` / shared replica↔dynlog finish-P2 — **obsolete** (no `Service`; no twin)~~
- [ ] `sim.run` / `logengine.Engine` / `netsim.Engine` / `kafka.Workload` — **Extract Class** — lifecycle, dispatcher, replicaSet, clientSession, linkTable
- [ ] cstate/opState/mutants — **Replace Type Code** / **Polymorphism** — kafka + echo
- [ ] executor Obj* switch — **Replace Conditional with Polymorphism** — `sys/prod/executor.go`
- [ ] Dynamo/S3/gRPC ACL — **Encapsulate Record** / **Hide Delegate** — edges are `exec/dynlog` items, `exec/objects`, `apiconv`, `applier/keys` (`ckptPointer` and JSON wire are gone)
- [ ] Bounded context formalize — hard ACL between `controlplane/machine` (deterministic Step) and `controlplane/exec` / AWS I/O; echo vs kafka only after P2 dedupe choice

---

## Suggested calendar

1. **Now:** clear P0. *(done — tests green on touched packages)*
2. **P1 VO helpers:** done.
3. **CP-DSM program:** done (stack landed; twin + JSON/OpenAPI husk removed).
4. **P2 shared kernels:** done (kwire, runinfo, campaign, session, codec framing, boundary Draw).
5. **Next:** P3 Split Phase/Extract Function (**test + green per item/commit**), or attrition/stall follow-up in parallel.
6. **Last:** P4 Extract Class / polymorphism / ACL / formal bounded contexts (**test + green per item/commit**).

---

## Cross-cutting reminders (from earlier review)

Highest leverage themes still live (mapped into waves above):

1. **Extract Class** on god types after seams (P4), especially broker + `controlplane/machine`
2. ~~Shared serve/manifest helpers~~ — **done** (`sys/runinfo.ServeMetrics` / `Publish`)
3. ~~Echo ↔ Kafka client FSM / campaign~~ — **done** (`sim/internal/session`, `sim/internal/campaign`)
4. ~~kwire framing + boundary fault Draw~~ — **done** (`appendFlexibleTag`/`readBody`, `fakeio.Params.Draw`)
5. ~~Share or formally fork logsm ↔ logcp~~ — **done by deletion**; one machine + exec remains
6. ~~Shared binary reader across log twins~~ — **gone with the twins**
7. ~~Remove Dead Code / flags (P0)~~ — **done**
