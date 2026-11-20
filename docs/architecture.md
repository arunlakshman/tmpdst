# Architecture

MDS has two runtime pieces: a Kafka-protocol broker (`mdsbroker`) and a control
plane (`mdscontrolplane`). Clients speak Kafka to the broker. The broker stores
record batches in an object store and asks the control plane for metadata,
offsets, and classic consumer-group state.

## Runtime picture

    Kafka client
         |
         v
      mdsbroker  ----object get/put---->  object store (mem / S3-compatible)
         |
         | gRPC
         v
    mdscontrolplane  ----append/read---->  log store (mem / DynamoDB / Postgres)
         |
         +---- metadata / topics / groups / committed offsets

- **Broker:** Kafka wire, Produce/Fetch path, object I/O, forwards group and
  topic RPCs to the control plane. Every live broker names itself leader for
  every partition (clients may still connect to the FindCoordinator target).
- **Control plane:** replicated command log, topic and group state, segment
  commit and lookup, broker liveness leases.
- **Stores:** demo `mds-all` uses in-memory log and objects. Production shape uses
  separate images and real backends (see [operations.md](operations.md)).

## Module DAG

Four Go modules in one repo. A module may import only modules to its left.
`make check-deps` enforces that.

    dst <- shared <- controlplane <- broker

| Module | Role |
|---|---|
| `dst/` | Deterministic simulation library (no product words) |
| `shared/` | Cross-product helpers with no product words |
| `controlplane/` | Control plane service and client |
| `broker/` | Kafka-protocol broker |

## Core vs edge

Deterministic **core** packages take events and emit actions: no wall clock, no
goroutine scheduling, no Prometheus. Simulation and production share that core.

The **edge** (cmd, production adapters, metrics, real gRPC and object I/O) talks
to the outside world and turns core facts into logs and metrics. That split is
why DST can drive the same rules the binaries run. See
[contribute/determinism.md](contribute/determinism.md).

## Kafka protocol surface

If this table disagrees with
`broker/internal/wire/kafkawire/supported.go`, **the code wins**.

| API | Key | Versions |
|---|---|---|
| Produce | 0 | 3 to 9 |
| Fetch | 1 | 4 to 12 |
| ListOffsets | 2 | 1 to 7 |
| Metadata | 3 | 1 to 12 |
| OffsetCommit | 8 | 0 to 8 |
| OffsetFetch | 9 | 0 to 8 |
| FindCoordinator | 10 | 0 to 4 |
| JoinGroup | 11 | 0 to 7 |
| Heartbeat | 12 | 0 to 4 |
| LeaveGroup | 13 | 0 to 5 |
| SyncGroup | 14 | 0 to 5 |
| DescribeGroups | 15 | 0 to 5 |
| ListGroups | 16 | 0 to 4 |
| ApiVersions | 18 | 0 to 3 |
| CreateTopics | 19 | 0 to 7 |
| DescribeConfigs | 32 | 0 to 4 |

FindCoordinator: group keys only. DescribeConfigs: topics only, empty config
list. Classic groups only (no KIP-848). Compression is opaque bytes (gzip,
snappy, lz4, zstd accepted; broker never decompresses). Unsupported API
key/version: connection closed with no error body (ApiVersions still answers
`UNSUPPORTED_VERSION`).

CreateTopics ignores replication factor and configs. Fetch honors MinBytes /
MaxWait; no fetch sessions; ListOffsets earliest/latest only; earliest offset
is 0; no retention delete or compaction in 0.1.0.

### Classic groups

Group state lives in the control plane. FindCoordinator hashes the group name
(FNV-1a 32-bit) over the sorted live broker list. Transaction keys get
`COORDINATOR_NOT_AVAILABLE`. Manual OffsetCommit (generation `-1`, no member)
only when the group has no members. `MEMBER_ID_REQUIRED` is not used.

Intentional diffs vs Apache Kafka classic:

| Topic | Apache Kafka | MDS 0.1.0 |
|---|---|---|
| Unchanged follower rejoin in Stable / CompletingRebalance | Often answered at once; leader rejoin starts a round | Any JoinGroup opens a new rebalance immediately |
| Empty round | Generation advances when Empty | Generation kept |
| Protocol choice | Vote among shared protocols | First protocol in the leader's list everyone has |
| Protocol mismatch | Refuse immediately | Park, then end round with `INCONSISTENT_GROUP_PROTOCOL` |
| SyncGroup | Missing members get empty assignment | Unknown/duplicate assignment is an error |
| Manual / simple groups | Can create a "simple group" | No group created; Describe Dead; List omits |
| `MEMBER_ID_REQUIRED` | Challenge then second join | Empty id joins in one step |
| Static membership | Supported | Instance id stored, unused |

Smoke check: `ops/broker/scripts/e2e-kafka-cli.sh`.

## Terms (control plane)

Plain one-liners. Code map: `controlplane/internal/controlplane/doc.go`.

- **command** / **event**: client change stored in the log vs fact `Decide` makes
  at apply time (never stored).
- **log** / **slot** / **entry**: ordered shared commands; numbered place written
  once; protobuf payload (or heartbeat/fence) at a slot.
- **Decide** / **Apply** / **view**: pure `Decide(view, command) -> events`;
  `Apply` writes; view is read-only state.
- **metadb** / **engine** / **schema** / **table**: state DB on an ordered KV
  engine; schemas own key prefixes.
- **checkpoint** / **state hash** / **trim**: object-store snapshot at a slot;
  64-bit digest of applied state; delete covered log slots.
- **segment** / **commit key** / **idempotency window** / **offset**: object-stored
  batches; client key for CommitSegment dedupe; remembered keys; partition position.
- **replica** / **role** / **epoch** / **fence** / **takeover**: node copy of the
  log; writer vs standby; writer generation; stop old writer; standby claim after silence.
- **lease**: broker liveness heartbeat (memory only).
- **group** / **member** / **generation** / **leader** / **assignment**: classic
  Kafka consumer group vocabulary; rebalance machine is `domain/rebalance`.
- **seed** / **campaign** / **oracle** / **trace hash**: one number for a sim run;
  many seeds; post-run checks; digest of every step.
