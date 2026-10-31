// Package speccheck holds the specs of the outside world: the object store
// (ObjectStore), the control plane (ControlPlane) and one Kafka partition
// (Partition). A spec is a small program that shows the correct behavior. It
// has no faults, it runs one operation at a time, and it keeps plain data. It
// is short so that a reader can see that it is correct.
//
// ObjectStore and ControlPlane answer a core.IOCall with a core.IOResult
// through Exec. Partition has Append and Read. All three have a StateHash.
//
// A spec is also used as a test fake (the adapters over the specs are in
// speccheck/specsystem). The main use is as an oracle: a checker we trust to say if a
// result is correct. Each real component is compared with its spec, step by
// step, using the helpers in checker. There are four checks:
//
//	C1  The broker state machine runs with no faults, over the ObjectStore
//	    and ControlPlane specs. Its answers to produce, fetch and ListOffsets
//	    are compared with Partition. The test also checks that the injected
//	    bugs InjectedBugWrongOffset and InjectedBugServeCorrupt are caught.
//	    Test: internal/broker/conformance_test.go.
//	C2  Each simulated engine (object store, control plane) runs with all
//	    faults off. It must give the same completion and the same StateHash
//	    as its spec after every operation.
//	    Test: internal/verify/simulator/fakeservices/conformance_test.go.
//	C3  The real adapters are compared with the specs: the S3 object store
//	    against ObjectStore, and the control-plane stores (in-memory store,
//	    gRPC client, applier on MemoryKeyValueStore and on Pebble) against ControlPlane.
//	    This is the only check that runs the real code in internal/system and
//	    the control-plane stores. The S3 test needs MDS_S3_ENDPOINT.
//	    Tests: internal/system/s3store/conform_test.go and
//	    internal/controlplane/controlplanetest/conform_test.go.
//	C4  The Kafka oracle (in sim/kafka) is compared with Partition and
//	    ControlPlane. The oracle must accept the histories built from the
//	    specs and must reject every fault that the test plants in a history.
//	    The oracle never imports this package. C4 keeps the two independent.
//	    Test: internal/verify/simulator/kafka/c4_test.go and c4_faults_test.go.
//
// The table below shows, for each check, what is compared and which side is
// real code and which side is simulated or a spec.
//
//	check | what is tested                    | compared with        | real or simulated
//	C1    | broker answers (produce, fetch,   | Partition            | broker code is real;
//	      | ListOffsets)                      |                      | storage and control plane are specs
//	C2    | simulated object store and        | ObjectStore and      | engines are simulated;
//	      | control plane, faults off         | ControlPlane         | specs are the reference
//	C3    | S3 adapter; control-plane stores  | ObjectStore and      | adapters are real code (S3 needs
//	      |                                   | ControlPlane         | a live endpoint); specs are the reference
//	C4    | Kafka oracle (accepts and         | Partition and        | oracle is simulation-side code;
//	      | rejects histories)                | ControlPlane         | specs are the reference
//
// The specs themselves are checked in this package. The unit tests check each
// rule. ObjectStore is compared with a sorted-slice model. ControlPlane is
// compared with a naive model made of maps and loops (controlplane_model_test.go)
// and its invariants are checked: offsets are contiguous, the end offset equals
// the committed counts, and a repeated commit gives the same answer. Partition
// is compared with a slice model (partition_model_test.go).
//
// Terms:
//
//	oracle  a checker we trust to say if a result is correct.
//	spec    a short, fault-free program that shows the correct behavior.
package speccheck
