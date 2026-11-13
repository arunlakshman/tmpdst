// Package speccheck holds the tools that compare the broker with the specs of
// the outside world. It holds no spec itself. There is one spec for each
// thing, and each lives with the thing it describes:
//
//	control plane   controlplane/spec    (the one spec of the control plane)
//	object store    shared/objectstore   (objectstore.Spec)
//	broker          broker/spec          (spec.Broker, one Kafka partition)
//
// A spec is a small program that shows the correct behavior. It has no
// faults, it runs one operation at a time, and it keeps plain data. It is
// short so that a reader can see that it is correct. A spec shares types with
// the code and never shares logic, and it never imports the package it is the
// spec of.
//
// The broker kernel speaks core.IOCall and core.IOResult. The control plane
// spec speaks contract.Operation and contract.Done. The package specbridge
// converts between the two and adds no rule. The package specsystem puts the
// specs behind the production seams, so a production Executor can run against
// them. The package conformance compares a subject with a spec, step by step, with
// spectest.Lockstep (dst/check/spectest); the package injectedbug holds the
// planted bugs that prove that it can fail.
//
// A spec is also used as a test fake. The main use is as an oracle: a checker
// we trust to say if a result is correct. There are four checks:
//
//	C1  The broker state machine runs with no faults, over the object store
//	    spec and the control plane spec. Its answers to produce, fetch and
//	    ListOffsets are compared with the broker spec. The test also checks
//	    that the planted bugs InjectedBugWrongOffset and
//	    InjectedBugServeCorrupt are caught.
//	    Test: internal/broker/conformance_test.go.
//	C2  Each simulated engine (object store, control plane) runs with all
//	    faults off. It must give the same completion and the same StateHash
//	    as its spec after every operation.
//	    Test: internal/verify/simulator/fakeservices/conformance_test.go.
//	C3  The real adapters are compared with the specs: the S3 object store
//	    against the object store spec. Control-plane store conformance (mem,
//	    gRPC, applier) lives in controlplane/test/conformance.
//	    The S3 test needs MDS_S3_ENDPOINT.
//	    Test: internal/verify/speccheck/s3conform/conform_test.go (the adapter is
//	    shared/objectstore/s3).
//	C4  The Kafka oracle (in simulator/kafka) is compared with the broker spec
//	    and the control plane spec. The oracle must accept the histories built
//	    from the specs and must reject every fault that the test plants in a
//	    history. The oracle never imports a spec package. C4 keeps the two
//	    independent. A test and a linter rule (depguard, kafka-oracle-no-spec
//	    in .golangci.yml) both check it.
//	    Test: internal/verify/simulator/kafka/c4_test.go and c4_faults_test.go.
//
// The table below shows, for each check, what is compared and which side is
// real code and which side is simulated or a spec.
//
//	check | what is tested                    | compared with        | real or simulated
//	C1    | broker answers (produce, fetch,   | broker spec          | broker code is real;
//	      | ListOffsets)                      |                      | storage and control plane are specs
//	C2    | simulated object store and        | object store spec    | engines are simulated;
//	      | control plane, faults off         | and control plane    | specs are the reference
//	      |                                   | spec                 |
//	C3    | S3 adapter; control-plane stores  | object store spec    | adapters are real code (S3 needs
//	      |                                   | and control plane    | a live endpoint); specs are the reference
//	      |                                   | spec                 |
//	C4    | Kafka oracle (accepts and         | broker spec and      | oracle is simulation-side code;
//	      | rejects histories)                | control plane spec   | specs are the reference
//
// Each spec is checked in its own package. The unit tests check each rule. The
// object store spec is compared with a sorted-slice model (shared/objectstore,
// spec_test.go). The control plane spec is checked by its unit tests and by the
// conformance tests of the control plane, which compare the real code with it.
// The broker spec is compared with a slice model (broker/spec/broker_model_test.go).
//
// Terms:
//
//	oracle  a checker we trust to say if a result is correct.
//	spec    a short, fault-free program that shows the correct behavior.
package speccheck
