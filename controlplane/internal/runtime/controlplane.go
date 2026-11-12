package runtime

// ControlPlaneOperation is a control-plane operation as seen by a broker (decision 3).
// The concrete operations are plain data types in internal/controlplane/contract.
// They implement this interface with the marker method, so runtime does not need
// to import them.
//
// The wire protocol for new work is gRPC (`controlplane/api/controlplane/v1`, decision 57);
// the contract types remain the in-process vocabulary until cutover. The metadb is still invisible
// to the broker. The invariants below are not provisional.
//
// Brokers are stateless (decision 44): there is no registration, epoch or
// fencing, and no idempotent producers (decision 41). The control plane
// assigns offsets (decision 42): the broker never proposes one. How a store
// stays correct under concurrent committers (a compare-and-set on the run base
// with an internal retry) is the store's business, not part of this contract.
type ControlPlaneOperation interface{ IsControlPlaneOperation() }

// ControlPlaneResponse is the answer to a ControlPlaneOperation. The concrete
// responses live in internal/controlplane/contract.
type ControlPlaneResponse interface{ IsControlPlaneResponse() }
