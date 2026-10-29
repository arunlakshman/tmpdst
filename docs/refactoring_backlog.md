# Refactoring backlog

Prioritized Fowler (and selective DDD-adjacent) work for this repo.
Pick items **one wave at a time**. Do not start P3 god-object splits until P1 value objects and relevant P2 shared kernels have landed for that area.

**Status legend:** ~~struck~~ = done or obsolete · open = not started

**Ground truth (toys3 tip):** CP-DSM-1..8 landed. `internal/controlplane/logcp` and `internal/controlplane/logsm` are deleted (`5d8be1d`). Prod and sim share `internal/controlplane/statemachine` + gRPC (`internal/controlplane/api/v1`, `apiconv`). Dynamo log I/O is `internal/controlplane/executor` (+ sim `logstore`). Path prefix `cp/` in older notes means `internal/controlplane/`.

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

**Next up:** P4 Extract Class / polymorphism / ACL. Attrition and stall follow-up stays in `docs/cp-dsm-attrition-stall-followup.md` and is not part of this wave.

---

## P0 — Just delete / clarify

- [x] ~~Unused `openSegment.commitItems` — **Remove Dead Code** — `internal/broker/segment.go` (note: `cpengine.commitItems` is live; do not remove that)~~
- [x] ~~Unused `asErr` — **Remove Dead Code** — was `cp/logcp/service.go` (package deleted)~~
- [x] ~~Unused `prep` HistogramVec — **Remove Dead Code** — `internal/controlplane/controlplanemetrics`~~
- [x] ~~`query(..., consistent bool)` always `true` — **Remove Flag Argument** — was `logcp/dynlog.go` (now single `exec/dynlog.go`)~~
- [x] ~~`parseBatch(..., verifyCRC bool)` — **Remove Flag Argument** — `internal/wire/kafkawire/batch.go` (prefer `parseBatch` / `parseBatchUnverified` or keep public wrappers only)~~
- [x] ~~`dispatch(..., resolved bool)` — **Remove Flag Argument** — `internal/broker/ops.go` → `dispatch` vs `dispatchAfterRefresh` (or equivalent)~~
- [x] ~~`retry(s, from segState)` — **Remove Flag Argument** — `internal/broker/segment.go` → keep only `retryPut` / `retryCommit`~~
- [x] ~~`lost(..., abort bool)` — **Remove Flag Argument** — `internal/verify/simulator/kafka/workload.go` + `internal/verify/simulator/echo/workload.go` → `lostAbort` / `lostPeerClosed` (or equivalent)~~
- [x] ~~`hash(cached bool)` — **Remove Flag Argument** — `internal/verify/simulator/fakeservices/fakenetwork/netsim.go` → explicit cached vs full methods~~
- [x] ~~`pull` / `SyncTo` `force bool` — **Remove Flag Argument** — was `logsm/replica.go` (deleted with twin)~~
- [x] ~~`attempt("won"|"lost"|"error")` — **Remove Flag Argument** / typed result — was `logcp/committer.go` (deleted with twin)~~
- [x] ~~Redundant `cur = max(cur, n)` after `if n > cur` — **Inline Variable** — `internal/controlplane/stateupdater/applier.go`~~

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
- [x] ~~Kafka `err*` `int16` → `KafkaCode` — **Replace Primitive with Object** — `internal/broker/config.go` + call sites~~
- [x] ~~Slot / HW / offset IDs — **Replace Primitive with Object** — `core.LogPosition` is already the parameter on `internal/controlplane/executor.Log` (`Append`/`Read`/`CheckTail`/`TrimThrough`)~~
- [x] ~~CRC table once — **Replace Temp with Query** — `internal/wire/segmentformat/format.go` (+ kwire if shared)~~
- [x] ~~`fieldOf` / fact getters — **Move Function** / **Extract Function** — echo, kafka oracle, `internal/system/production/seglog`, metrics~~ (`core.Fact.Field`)
- [x] ~~`sample` / TwoSpeed — **Pull Up Method** / **Parameterize Function** — fakeio, logengine, echo/kafka config~~ (use `TwoSpeed.Sample`; echo/kafka `slow()` ctor dupes left)
- [x] ~~FNV helpers if duplicated — **Move Function** — broker hash, netsim, etc.~~ (`core.Fnv64Hash`; echo/veritas naive hashers left)
- [x] ~~Resp type-assert boilerplate — **Extract Function** / **Parameterize Function** — stores / applier / memstore~~ (`core.ResponseAs`)

**VO landing order:** (1) `TopicPartition` → (2) `KafkaCode` → (3) Slot/position IDs → (4) optional key wrappers at Dynamo/S3 edge.

---

## P2 — Stop paying twice (shared kernel / template)

### Closed by CP-DSM (do not reopen as logcp↔logsm)

- [x] ~~DynLog Append/Read/query/CheckTail fork — **obsolete**: twin deleted; sole Dynamo loglet is `internal/controlplane/executor/dynlog.go`~~
- [x] ~~Replica apply / hole-fill / buf fork — **obsolete**: twin deleted; live path is `internal/controlplane/statemachine` (+ sim `logstore` for durable slots / CPTruth)~~
- [x] ~~Commit append/retry fork — **obsolete**: twin deleted; live path is `internal/controlplane/statemachine/commit.go`~~
- [x] ~~Binary reader/cursor across logcp/format + logsm/codec — **obsolete as a cross-package fork** (twins deleted). Leftover same-package sticky cursors (`applier/keys.go` `reader`, `applier/entry.go` `decoder`, `machine/commit.go` `takeUvarint`) are optional P3 polish, not a P2 share~~
- [x] ~~Wire Batch/Range map pairs — **done/obsolete**: JSON `internal/controlplane/wire.go` deleted (CP-DSM-7); gRPC uses `internal/controlplane/apiconvert`~~

### Landed

- [x] ~~Echo ↔ kafka client FSM — **Extract Superclass** / **Form Template Method** — `internal/verify/simulator/internal/session` (`Loop` hooks; protocol + StateHash stay in scenarios; TraceHash canaries green)~~
- [x] ~~Echo ↔ kafka campaign presets — **Parameterize Function** / **Form Template Method** — `internal/verify/simulator/internal/campaign` (Swarm `afterKnobs` preserves kafka PartialIOMax draw order; TraceHash canaries matched)~~
- [x] ~~Bridge boundary faults — **Form Template Method** — `fakeio.Params.Draw` shared by fakeio Execute and logengine (coin order preserved; TraceHash stable)~~
- [x] ~~`serveMetrics` + `writeManifest` — **Extract Function** / **Move Function** — `internal/system/runinfo.ServeMetrics` + `Publish` (manicp metrics gains Read/Write/Idle timeouts matching broker)~~
- [x] ~~Service boot skeleton — **Form Template Method** — same cmds — **not worth it**: after logger setup the two `serve` bodies share no steps; a template needs hooks without deleting branches~~
- [x] ~~ApiVersions / Supported cache — **Substitute Algorithm** — `internal/wire/kafkawire/supported.go` (`supported` table + `supportedByKey`)~~
- [x] ~~Encode/Decode framing — **Extract Function** — `internal/wire/kafkawire/codec.go` (`appendFlexibleTag`, `readBody`)~~

**P2:** closed.

---

## P3 — Hot-path clarity (Split Phase / Extract Function)

Do per package **after** P1 types for that package.

### broker

- [x] ~~**Consolidate Conditional Expression:** `ack`/`release` → `answerPart`~~
- [x] ~~**Split Phase:** `flow` → `updatePressure`/`syncReads`; flatten `addBatch` / `onFetchGet` (+ `intact`); left `drain` alone (already flat)~~
- [x] ~~**Extract Function:** backoff was already `Broker.backoff`. Find-by-id is `segByReq`, `segByTimer`, and `opByTimer`. Response skeletons are `produceSkeleton`, `fillFetchSkeleton`, and `listOffsetsSkeleton`. `checkBatch` splits parse from policy. `metaTopic` builds one topic. ApiVersions is `apiVersionsReply`. StateHash folds are `hashScalars`, `topics`, and `openSeg`.~~
- [x] ~~**Decompose Conditional:** the flush timer is `onFlushTimer`. ListOffsets timestamps are `listOne`. A corrupt fetch GET is `retryCorrupt`. An accepted batch is `acceptFetch`.~~
- [x] ~~**Separate Query from Modifier:** `finishPart` writes the last part through `readySlot`. `req` and `id` stay one-line allocators. Every caller wants the new id, and a query with no caller adds a layer.~~
- [x] ~~**Encapsulate Collection:** conn and op slices go through find, add, and drop (`addConn`, `addOp`, and the existing find and drop). A conn table type is P4 Extract Class.~~
- [x] ~~**Split Phase:** produce, fetch, and list offsets build the response skeleton, then admit.~~

### kwire / format

- [x] ~~**Split Phase:** `parseBatch` was already `checkBatchShape`, the CRC check, and `decodeBatch`. `Decode` was already `decodeHeader`, `decodeIndex`, and `attachPayloads`. `Encode` measures, then `writeObject`.~~
- [x] ~~**Extract Function:** `encodeRecords` is the BuildBatch loop. `readIndexEntry` is the read pair of `putEntry`.~~

### controlplane/machine

Old `logcp` / `logsm` P3 names (`Start`, `catchUp`, `applyBatch`, `Checkpoint`, `TrimBelow`, `withFresh`, `SyncTo`, `holeTick`, `holeDue`, `decodeSlot`, `adv`/`kick`, `setI`/`setI64`, batchwrite retry) are **gone**. DSM already split catch-up / commit / checkpoint / role across files. Do **not** retarget the deleted logsm list onto `Machine`. Remaining smells:

- [x] ~~**Extract Function** / **Decompose Conditional:** `roleTick` asks `failbackDue`, `takeoverDue`, and `heartbeatDue`, then `setActive` or `emitHeartbeat`.~~
- [x] ~~**Move Field:** role clocks stay on `Machine` — **not worth it**: `lastOwnAt` and `lastOtherAt` have one writer (`observeWriter`) and one reader (`takeoverDue`, `heartbeatDue`). A role struct would sit between those two lines.~~
- [x] ~~**Encapsulate Collection:** `clearLocal` wipes `buf`, `waiters`, and the other in-flight maps that `softCrash` and `hitTrimmed` both reset. A map type does not earn a boundary.~~
- [x] ~~**Separate Query from Modifier:** `holeFillPlan` chooses wait, fill, arm, or the hole-skip mutant. `maybeHoleFill` applies that choice.~~
- [x] ~~**Extract Function:** `cpobs.NewWithLogger` builds HTTP collectors in `registerHTTP` and Dynamo collectors in `registerDynamo`.~~

### controlplane/applier

- [x] ~~**Extract Function** / **Introduce Parameter Object:** `assign` and `assignAt` share `assignPlan`.~~
- [x] ~~**Decompose Conditional:** `commitTx` and the writer-base path share `storeCommit`.~~
- [x] ~~**Replace Temp with Query:** `digestBytes` is the digest input.~~
- [x] ~~**Split Phase:** `SnapshotAt` writes through `writeSnap`. `Restore` was already `restoreChunk` and `readEntry`.~~
- [x] ~~**Move Function:** `Trim`, `Window`, `StateHash`, and `Assigned` live in `query.go`.~~
- [x] ~~**Extract Function:** pebble `View` and `Update` stay separate — **not worth it**: each body is the snapshot versus the indexed batch, and a shared constructor is the `newTxn` this line forbids.~~

### controlplane/cpclient / fakedb / exec

- [x] ~~cpclient HTTP `decode` / Faults / endpoint struct / `errOf` — **obsolete** (gRPC + `apiconv` / `mapRPCErr`; client holds an address list)~~
- [x] ~~cpobs Middleware observe→log as prod path — **obsolete** (manicp serves gRPC; Middleware only in cpobs tests; live hook is `APIOptions`)~~
- [x] ~~**Split Phase** / **Extract Function:** `do` times the call. `failover` walks addresses. `doAt` is one attempt.~~
- [x] ~~**Extract Function:** `rejectCond` is the shared condition check. `clauseValue` reads one SET or ADD clause. `parseKeyRange` is the BETWEEN half of `parseKeyCond`. `writeCount` and `applyWrite` are the BatchWriteItem steps.~~
- [x] ~~**Decompose Conditional** / **Guard Clauses:** SET versus ADD is `clauseValue`, then the verb. BatchWriteItem holds the last request, otherwise `applyWrite`.~~
- [x] ~~**Extract Function** / **Split Phase:** `DynLog.between` builds the shared key condition. Read, CheckTail, and TrimThrough set their own flags on it.~~

### sim

- [x] ~~**Split Phase** / **Extract Function:** `setup` is `wireEngines`, `addInvariants`, `addNodes`, `bootNodes`, and `scheduleCampaign`. netsim `send`, `dial`, and `flush`, logengine `plan`, fakeio `Execute`, kafka `newBatches`, `onProduce`, `onFetch`, and `AtEnd`, and echo `onGet`, `onCommit`, and `next` are already one phase each.~~
- [x] ~~**Decompose Conditional:** attrition, crash, produce classify, and reply — **not worth it**: `attrition` is already guard clauses, and `onProduce` is the classification. Moving that switch one hop does not delete a branch.~~
- [x] ~~**Consolidate Duplicate Conditional Fragments:** callEngine, segcaps Observe/AtEnd, config knobs — **not worth it**: campaign presets already share the knobs (P2). The remaining copies are protocol-specific.~~
- [x] ~~**Replace Loop with Pipeline:** serveReads, TP assembly, response index by TP — **not worth it**: the loops are the read. A pipeline type would add a layer with one caller.~~
- [x] ~~**Extract Function:** producer and consumer TP request helper, codes.go predicates — **not worth it**: `code` predicates and the per-protocol request builders are already named, and a shared TP helper would hide the protocol difference.~~
- [x] ~~**Move Function:** CPTruth view helpers, engineCtx file, unify `landBody`/`rstBody` — **not worth it**: fakeio and netsim land different completions. One body would take a flag or a hook and not delete a branch.~~

### sys

- [x] ~~**Extract Function** / **Split Phase:** net `Shutdown`, executor timeout, `NewMetrics`, `Observe`, seglog, `HandleFinalize`, s3store `Get`, `waitForWork`, and manifest publish are already one phase each. A second split does not hide a decision.~~
- [x] ~~**Move Function:** conn loops, fact accessors, S3 writer overlap — **not worth it**: each already lives next to the type that owns it.~~
- [x] ~~**Decompose Conditional** / **Guard Clauses:** enqueue, getFailed, classify ladder — **not worth it**: the ladders are the function. Pulling a clause out leaves a one-caller wrapper.~~
- [x] ~~**Consolidate Duplicate Conditional Fragments:** inbox Control/Data Pop — **not worth it**: the two pops differ by lane. A shared pop would take the lane as a flag.~~
- [x] ~~**Extract Function:** node `wireBundle` — **not worth it**: the bundle is the constructor's argument list. A named bundle type is P4.~~

### veritas

- [x] ~~**Split Phase** / **Extract Function:** `execCommit` is `replayCommit` or `freshCommit`. Get's range check is `sliceRange`. Lockstep shrink passes were already `dropChunks`, `shrinkOne`, and `shrinkAll`.~~
- [x] ~~**Form Template Method:** `extendPrefix` is the shared prefix fold for `ObjectStore.StateHash` and `ControlPlane.StateHash`. `Partition.StateHash` folds a record list and does not use the prefix cache.~~

### cmd

- [x] ~~**Split Phase:** manibroker and manicp `serve` are already a sequence of named calls (`nodeConfig`, `openObjects`, `bind`, `Start`, `Publish`, `Wait`). `openLog`, manibench `run`, and crapscore `analyse` are the same shape.~~
- [x] ~~**Extract Function:** openS3Objects, wireFacts, applySegment/BufferLimits, shouldFinalCheckpoint, Evaluate classify, ParseTree, coverageFor, openClients, formatFailure, fail, ReportView — **not worth it**: the bodies are already those calls, or a helper would have one caller and no second shape.~~
- [x] ~~**Introduce Parameter Object:** brokerFlags groups, worker, BaselinePolicy, logFlags nesting — **not worth it**: the flag structs are the parameter objects. Nesting them again hides the flag a reader looks up.~~
- [x] ~~**Preserve Whole Object:** runtimeSnapshot for manifest — **not worth it**: `manifestTuning` already returns the object the manifest stores.~~
- [x] ~~**Slide Statements:** the log-level error returns before the "starting" line. Moving it before `slog.SetDefault` would change that side effect.~~
- [x] ~~**Decompose Conditional:** tuning apply, parseArgs, improved branches — **not worth it**: each branch is the decision. A table would not delete a case.~~
- [x] ~~**Combine Functions into Class:** pebbleState lifecycle — **not worth it**: open and close are two calls on the pebble DB. A lifecycle type adds a layer.~~
- [x] ~~**Replace Conditional with Polymorphism** (light): scenario registry, bufferPolicy, store-kind — **not worth it**: this is the P4 polymorphism pass. The cmd switches are small and local.~~

### tools

- [x] ~~**Parameterize Function:** `checkFile` takes a `[]fileCheck`. `run` passes `checkImports`, `checkGlobals`, and `checkAST`. `loadAllow`, json2yaml, and `api/openapi` stay closed.~~

---

## P4 — Structure (Extract Class / polymorphism / ACL)

After P1–P3 seams exist.

- [ ] Broker collaborators — **Extract Class** — conn table, segment pipeline, op table
- [ ] op by kind — **Replace Type Code with Subclasses** / **Replace Conditional with Polymorphism** — `internal/broker/ops.go`
- [ ] segState machine — **Replace Conditional with Polymorphism** — `internal/broker/segment.go`
- [ ] `internal/controlplane/statemachine.Machine` — **Extract Class** (optional; file seams already exist as `commit.go` / catch-up in `machine.go` / `checkpoint.go` / `role.go`) — only if a collaborator earns a type boundary
- [x] ~~`logcp.Service` / shared replica↔dynlog finish-P2 — **obsolete** (no `Service`; no twin)~~
- [ ] `sim.run` / `logengine.Engine` / `netsim.Engine` / `kafka.Workload` — **Extract Class** — lifecycle, dispatcher, replicaSet, clientSession, linkTable
- [ ] cstate/opState/mutants — **Replace Type Code** / **Polymorphism** — kafka + echo
- [ ] executor Obj* switch — **Replace Conditional with Polymorphism** — `internal/system/production/executor.go`
- [ ] Dynamo/S3/gRPC ACL — **Encapsulate Record** / **Hide Delegate** — edges are `exec/dynlog` items, `exec/objects`, `apiconv`, `applier/keys` (`ckptPointer` and JSON wire are gone)
- [ ] Bounded context formalize — hard ACL between `internal/controlplane/statemachine` (deterministic Step) and `internal/controlplane/executor` / AWS I/O; echo vs kafka only after P2 dedupe choice

---

## Suggested calendar

1. **Now:** clear P0. *(done — tests green on touched packages)*
2. **P1 VO helpers:** done.
3. **CP-DSM program:** done (stack landed; twin + JSON/OpenAPI husk removed).
4. **P2 shared kernels:** done (kwire, runinfo, campaign, session, codec framing, boundary Draw).
5. **P3:** done (hot-path extracts, or struck where a helper would only add a layer).
6. **Last:** P4 Extract Class / polymorphism / ACL / formal bounded contexts (**test + green per item/commit**).

---

## Cross-cutting reminders (from earlier review)

Highest leverage themes still live (mapped into waves above):

1. **Extract Class** on god types after seams (P4), especially broker + `internal/controlplane/statemachine`
2. ~~Shared serve/manifest helpers~~ — **done** (`internal/system/runinfo.ServeMetrics` / `Publish`)
3. ~~Echo ↔ Kafka client FSM / campaign~~ — **done** (`internal/verify/simulator/internal/session`, `internal/verify/simulator/internal/campaign`)
4. ~~kwire framing + boundary fault Draw~~ — **done** (`appendFlexibleTag`/`readBody`, `fakeio.Params.Draw`)
5. ~~Share or formally fork logsm ↔ logcp~~ — **done by deletion**; one machine + exec remains
6. ~~Shared binary reader across log twins~~ — **gone with the twins**
7. ~~Remove Dead Code / flags (P0)~~ — **done**
