// Package veritas holds executable specs of the outside world: the object
// store (ObjectStore), the control plane (ControlPlane) and one Kafka
// partition (Partition). Each is sequential, fault-free, plain data and short
// enough to be obviously correct (decision 21). ObjectStore and ControlPlane
// answer core.IOAction with core.Completion via Exec; all three have a
// StateHash. Nothing here iterates a map for behavior.
//
// The specs double as test fakes (production-facing adapters over them are in
// veritas/vsys) and are what implementations are checked against, in lockstep
// with veritastest. Four checks:
//
//	C1  broker with no faults vs Partition: produce/fetch/ListOffsets through
//	    the real state machine, driven by a fault-free executor over these
//	    specs (broker/conformance_test.go). Its Bug mutants must fail it.
//	C2  each sim engine (object store, control plane) with faults off vs its
//	    spec: same completions, same StateHash after every operation
//	    (sim/engines/conformance_test.go).
//	C3  the sys adapters (S3, the control-plane HTTP client) vs these specs.
//	    The only check that executes sys. Lives with the adapters.
//	C4  the Kafka oracle vs Partition. The oracle never imports this package;
//	    C4 is what keeps the two independent. Wired: sim/kafka/c4_test.go
//	    (accepts correct histories built from these specs, rejects 28 faults and fenced acks).
//
// What is checked today: each spec's rules (unit tests), ObjectStore against
// a sorted-slice model, and ControlPlane invariants (contiguous offsets,
// end offset == committed counts, compare-and-set by base, idempotent commit), all via veritastest.
package veritas
