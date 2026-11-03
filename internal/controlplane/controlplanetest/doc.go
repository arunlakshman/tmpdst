// Package controlplanetest is the one home of control-plane testing: the tests
// themselves (handler and client request-and-reply tests, and conformance C3,
// which runs the memorystore and the gRPC stack against speccheck.ControlPlane)
// and the test support they share.
//
// Test support, used by tests here and in other packages:
//   - StartMem and Start (grpcserver.go) run an in-process gRPC control plane node.
//   - fakedynamodb is an in-memory DynamoDB for the log tests.
//   - memorystore is an in-memory store over the speccheck spec.
//   - updaterstore runs a stateupdater as one node on a local slot counter with
//     no log. It is also the GroupStore of the groupcoordinator tests.
//   - storecall gives tests a typed view of the single Exec method of the stores.
//
// Nothing outside tests imports this tree.
package controlplanetest
