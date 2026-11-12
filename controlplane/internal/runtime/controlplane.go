package runtime

import "github.com/manifesterio/mds/controlplane/contract"

// ControlPlaneOperation is a control-plane operation as seen by a broker. It is
// the sealed sum contract.Operation; this name is kept for the code of this module.
// The concrete operations are plain data types in contract.
//
// The wire protocol is gRPC (`controlplane/api/controlplane/v1`, decision 57);
// the contract types are the in-process vocabulary on both sides of it. The metadb is
// invisible to the broker. The invariants in contract.Operation are not provisional.
type ControlPlaneOperation = contract.Operation

// ControlPlaneResponse is the answer to a ControlPlaneOperation. It is the sealed
// sum contract.Response; the concrete responses live in contract.
type ControlPlaneResponse = contract.Response
