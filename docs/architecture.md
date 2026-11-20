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
  separate images and real backends (see [operations/run.md](operations/run.md)).

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
why DST can drive the same rules the binaries run.

## Related

- [Glossary](reference/glossary.md)  -  log, slot, Decide/Apply, term, lease
- [Kafka compatibility](kafka-compatibility.md)  -  API ranges and classic-group diffs
- [What works in 0.1.0](what-works-in-0.1.0.md)  -  shipped surface
