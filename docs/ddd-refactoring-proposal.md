# DDD refactoring proposal

*Note: the DynamoDB-transaction control plane (`cp/dynamo`) named in this proposal was removed (decision 54); the log-based CP (`cp/logcp`, `cp/logsm`) is the only one, so references to `cp/dynamo` are historical.*

Status: proposal for discussion, nothing applied. Constraints are decisions 7, 10, 11, 12 (frozen fact
names), 21 (spec independence), 22 and 28. "Hash-neutral" means no trace hash or `StateHash` output
changes.

## Summary

- **Already good:** the strategic shape is close to DDD. `core` is the published language, `veritas` the
  executable spec, `controlplane/api/v1` + `apiconv` the gRPC wire boundary, and `format/` and `kwire/` clean context
  modules.
- **Weak spots are tactical:**
  1. The same concept has several names and types.
  2. `broker/` is one ~1,740-line struct holding four or five aggregates, with Kafka error codes inside
     commit logic.
  3. Primitives stand in for value objects.
  4. Boilerplate is copied: six FNV hashers, three retry and backoff skeletons, fact names as string
     literals.
- **Hash impact:** almost everything below is hash-neutral. Only a new `ErrCode` or a fact rename
  re-baselines the golden hashes.
- **Packages:** no new packages. `broker/` stays one package.

## 1. Language drift

| Concept | Today | Problem |
|---|---|---|
| Segment | `broker.segment` is an in-flight upload; `veritas.segment` is a committed request and answer; `format` calls it an object; the (removed) DynamoDB transaction CP called it `G#<segkey>`; the log CP keeps it as a key in the applier window | Three things share one word. |
| Segment name | `SegmentKey string` field, but `format.SegmentKey()` is a function | Not a typed key. Rename the function to `format.NewSegmentKey`. |
| Byte range | `core.SegmentRef`, also called `Loc` and `Ref` | It is a range inside an object. Call it `ObjectRange`. |
| Batch | `pbatch`, `PendingBatch`, `format.Batch`, `kwire.Batch`, `cp.Batch`, `kmsg.RecordBatch`, and `ProduceOp.Batch` (a serial number) | Six or seven meanings. Always qualify it: RecordBatch, AdmittedBatch, PendingBatch. |
| `core.BatchRange` | One struct is an assignment, a duplicate, a rejection (`Base = -1`) and a Lookup entry | A rejection called a "range" is the strongest smell. |
| Producer id | `pbatch.pid int64` and `pbatch.producer ProducerID` side by side (for `BugNoIdempotence`) | The reason is not visible in the types. |
| Epoch | `core.Epoch` is the broker's registration epoch; `PEpoch` is the producer's; plus Kafka's leader epoch; the fact key `"epoch"` means the broker's | "Epoch" alone is ambiguous. Rename the Go type to `BrokerEpoch`; never rename the fact key. |
| **`ErrFenced`** | Means both "stale broker epoch, so fail-stop" (`broker/segment.go:168`) and "stale producer epoch, so reject one batch" (`:192`) | **The most important language problem.** |
| Incarnation | Carried in segment keys and `Owner`, but the CP fences on `Owner.Node` and epoch only | lessons §4 says a stale incarnation is fenced; the code never checks it. Decide which is right. |
| `release` | Means "answer with an error" | Rename to `reject`. |
| `op` | Has four meanings across the code | The broker's `op` is really a request part. |

Proposed glossary, to go in `docs/glossary.md` and be cited by doc comments:

- **Segment:** one uploaded object holding batches for many partitions.
- **SegmentKey:** the segment's name.
- **ObjectRange:** where one batch's bytes are.
- **RecordBatch, AdmittedBatch, PendingBatch:** the same batch at different stages.
- **Assignment** and **Verdict** (Assigned, Duplicate, OutOfOrder, ProducerFenced): the CP's answer for each batch.
- **High watermark.**
- **Broker epoch** and **Producer epoch.**
- **Incarnation.**
- **Trip.**
- **Ack.**
- **Request part.**
- **Slot.**

## 2. Bounded contexts

| Context | Packages | Status |
|---|---|---|
| Kafka protocol | `kwire`, `core/kafka.go`, the response half of `broker/` | Conformist to Kafka. Correct. |
| Ingest (produce pipeline) | `broker/segment.go`, `broker/produce.go` | **Leaks.** Kafka codes are chosen inside `committed()` and `isolate()`, and `pbatch.part` holds a `*kmsg` response struct. |
| Read path | `broker/fetch.go`, `broker/ops.go` | Same leak: `op.lpart` and `op.fpart`. |
| Log and idempotence | `core/controlplane.go` (contract), `veritas` (spec), `cp/logsm` and `cp/logcp` (implementation), `cpengine` | Duplicated on purpose (decision 21). |
| Membership and fencing | `Register` and `broker.trip` | |
| Object layout | `format`, `veritas.ObjectStore`, `sys/s3store` | Shared kernel. Fine. |
| Runtime | `core`, `sys`, the sim kernel | |
| Verification | `sim`, `sim/kafka`, `veritastest`, `detcheck` | |

**Shared pure "idempotence rules" module?** No for the rules, yes for the vocabulary.

- If `veritas` and the log state machine (`cp/logsm`) called one decide function, a bug in it would be in both, and lockstep would only test the plumbing.
- Keep the duplicated constants (`firstProducer = 1000`, `dupWindow = 5`) on purpose.
- Instead, number the spec rules (R1..R9) and reference them from `decide.go` and from lockstep failure messages.

## 3. Aggregates and value objects

| Aggregate | Invariants | Enforcement today |
|---|---|---|
| PartitionLog (CP) | Offsets contiguous; Base = high watermark | Parallel code in spec and implementation, plus the oracle. |
| ProducerSession (TP, producer) | PEpoch monotonic; next seq; 5-entry dup window | Three independent restatements: spec, DynamoDB implementation, oracle. Intended. |
| CommittedSegment | Atomic; idempotent by key; ErrConflict if the request differs | |
| BrokerRegistration | Epoch strictly increasing; commits carry the current epoch | Incarnation is not an invariant. |
| **IngestBuffer + SegmentPipeline** (broker) | Commits in seal order; ack only after commit; `bufBytes` balanced | Scattered: `bufBytes` is added in `addBatch` and subtracted in `endSegment`. |
| **ConnectionPipeline** (broker) | Responses in request order; `pending` counter | Open-coded `pending++` in `produce.go` and `ops.go`. |
| **RequestPart** (`broker.op`) | One call in flight; retry with backoff; give up | `if o.kind == ...` branches. |

Missing value objects:

- `core.BatchIdentity{Producer, PEpoch, BaseSeq}` with `Idempotent()`.
- `core.SegmentKey`, a named string.
- `core.OffsetRange{Base, Count}` with `End()` and `Contains()`.
- A `Verdict` in place of `Err`, `Dup` and `Base == -1`.
- `kcode`, a named int16 for Kafka codes.

Facts are already past-tense domain events. The only problems are the edges:

- Fact names are string literals on both the emit side and the invariant side, so a typo makes an invariant vacuous instead of failing.
- The fact field `"epoch"` is ambiguous (the broker's, not the producer's).

## 4. Opportunities, ranked by value over risk

| # | What | Effort | Hash | Sign-off |
|---|---|---|---|---|
| 1 | `docs/glossary.md`, doc comments, unexported broker renames (`release`→`reject`, `op`→`part`, `pbatch`→`admitted`) | S | none | |
| 2 | One place translates CP verdicts to Kafka codes (`broker/kafkaerr.go`: `kcode`, `verdictCode`, `exhaustCode`) | S | none | |
| 3 | Value objects in `core` (`BatchIdentity`, `SegmentKey`, `OffsetRange`). Touches core, cp, veritas, sim, broker; land alone after other work merges | M | none (tags hash field by field; verify with golden hashes) | |
| 4 | Broker aggregates as types in the same package (`ingest.go`, `connpipe.go`, segment transition methods, an `answer` handle that cuts the kmsg leak), one aggregate per commit | M–L | none if the `StateHash` order is kept | |
| 5 | Fact name constants in `core`, typed constructors, and a test that pins the names | S | none | |
| 6 | One `core.Hasher` in place of five copies, and a shared retry and backoff helper (`retryState`) | S–M | none | |
| 7 | Split the overloaded `ErrFenced`. **A:** typed accessors only (recommended). **B:** a new `ErrProducerFenced`, which changes the HTTP wire format and re-baselines | L | B re-baselines | yes |
| 8 | Rule numbering between `veritas` and `cp/logsm`; decide whether Incarnation should fence (lessons §4 vs code) | S | none (a behaviour change if it fences) | open question |

Validation for every step:

- Golden trace hashes are unchanged.
- Each of the four broken-broker tests is still caught at the same first seed.
- `TestDeterminismCanary` passes.
- Lockstep against memstore and DynamoDB passes.

**Split `broker/` into sub-packages?** Not now.

- Decision 22 rejected toys3's full package split.
- `StateHash` reads every private field.
- `pbatch` → `slot` → `conn` is a pointer cycle.
- `Step` routes completions by scanning `segs` and `ops`.

Use files instead.

## 5. Do not

- **Repositories over the broker:** state is in memory in `Step`; `cp.Store` already is the repository.
- **Domain services with I/O:** all I/O stays Actions (decisions 10, 11).
- **Event sourcing or an event bus:** facts are an observation trail, not the source of state.
- **Handler dispatchers with funcs or maps:** detcheck forbids them.
- **A shared idempotence-rules package:** it breaks decision 21.
- **An oracle that imports veritas logic.**
- **Renaming facts, `ErrCode` strings or enum order:** they are frozen.
- **A broker package split.**
- **A generic entity base type.**
- **Kafka error numbers in `core`:** translate at the broker boundary.

## 6. Suggested order

1. Items 1, 5 and 8: documentation and constants.
2. Item 2: error translation.
3. Items 6 and 4: one aggregate per commit, golden hashes unchanged each time.
4. Item 3, after the current `sim/` and `sys/` work has merged.
5. Item 7, after you choose A or B (A recommended).
