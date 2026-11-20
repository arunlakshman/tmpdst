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

Shipped Kafka surface: [what-works-in-0.1.0.md](what-works-in-0.1.0.md).
