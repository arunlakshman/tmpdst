// Package controlplane is the control-plane service tree (renamed from cp/).
// Production mdscontrolplane serves gRPC via controlplane/server over controlplane/runloop
// and controlplane/replica (CP-DSM-7). Brokers dial through controlplane/controlplaneclient.
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
// this tree (metadata, executor) support the
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
//   - replica: one node's copy of the replicated log: leader or standby,
//     catch-up, checkpoint and restore. It implements core.Machine.
//   - metadata: the control-plane data model (topics, segments, groups and
//     offsets) and the rules for changing it. It applies commands in order to an
//     ordered key-value store.
//   - kv: the small ordered key-value seam under metadata and groups/storage,
//     with an in-memory store and the record codec they share.
//   - kv/pebblestore: the Pebble-backed key-value store that
//     metadata uses.
//   - executor: runs the control-plane IOCalls outside Step, and owns the Log
//     and ObjectStore ports.
//   - executor/dynamolog and executor/sqllog: the DynamoDB and Postgres
//     implementations of executor.Log. executor/logtest is their shared test
//     suite.
//   - groups/storage: how consumer group state is kept in kv (the G, M and O
//     keys): the fold of the group writes and the group reads. It does not
//     know about topics.
//   - groups: the consumer group coordinator of the active node. It
//     speaks the classic group protocol only.
//   - controlplanemetrics: the Prometheus instrumentation. It covers HTTP
//     handler time, DynamoDB call time, in-flight requests and request sizes.
//   - controlplanetest: the control-plane tests only.
//
// Time: all time comes from one interface, core.Clock (Mono, Wall, AfterFunc).
// system/realclock is the production clock and system/fakeclock is the one for
// tests and simulation. The domain and server packages may not call time.Now,
// time.AfterFunc and the like (TestControlPlaneTimeComesFromClock).
//
// How they fit together, from the imports:
//
//   - server imports runloop, replica, executor and groups.
//   - replica imports metadata. metadata imports kv and groups/storage.
//     groups/storage imports kv. executor imports runloop. dynamolog and sqllog
//     import executor. Domain packages import no third-party package; see
//     TestControlPlaneLayers in architecture_test.go.
//   - controlplaneclient imports api/v1 and wireboundary, and calls the gRPC API.
package controlplane
