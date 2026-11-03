// Package controlplane is the control-plane service tree (renamed from cp/).
// Production mdscontrolplane serves gRPC via controlplane/server over controlplane/runloop
// and controlplane/statemachine (CP-DSM-7). Brokers dial through controlplane/controlplaneclient.
//
// The gRPC wire contract is controlplane/api/v1 (decision 57). Veritas remains
// the independent oracle and shares schema types via wireboundary, not apply code
// (decision 21).
//
// Broker membership (heartbeat and the live broker set) is one feature in
// several packages. Its file is brokers.go in each; the map is at the top of
// server/brokers.go.
//
// All tests and test support live in controlplanetest (see its doc.go). Helpers under
// this tree (stateupdater, executor) support the
// state machine's fold and IO. Not deterministic (goroutines, net): it is not
// checked by ops/tools/determinismcheck.
//
// # Map of the packages
//
//   - api/v1: the gRPC wire contract, defined in controlplane.proto.
//   - controlplaneclient: the broker-side adapter. It is a system.ControlPlane
//     over the gRPC API.
//   - wireboundary: the only place that maps between core types and the wire
//     (api/v1) messages. It holds no logic.
//   - server: the gRPC adapter for the control-plane machine.
//   - runloop: the production event loop. One goroutine owns Step, timers and
//     inject work. It imports only core and the standard library.
//   - statemachine: the control-plane replica state machine. It implements
//     core.Machine.
//   - stateupdater: the state updater. It applies commands in order to an
//     ordered key-value store.
//   - stateupdater/pebblestore: the Pebble-backed key-value store that
//     stateupdater uses.
//   - executor: runs the control-plane IOCalls outside Step. These are Dynamo
//     log slots and object-store checkpoints.
//   - groupcoordinator: the consumer group coordinator of the active node. It
//     speaks the classic group protocol only.
//   - controlplanemetrics: the Prometheus instrumentation. It covers HTTP
//     handler time, DynamoDB call time, in-flight requests and request sizes.
//   - controlplanetest: the control-plane tests only.
//
// How they fit together, from the imports:
//
//   - server imports runloop, statemachine, executor and groupcoordinator.
//   - statemachine imports stateupdater. executor imports runloop.
//   - controlplaneclient imports api/v1 and wireboundary, and calls the gRPC API.
package controlplane
