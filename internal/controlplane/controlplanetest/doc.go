// Package controlplanetest is the one home of control-plane testing: the tests
// themselves (handler and client request-and-reply tests, and conformance C3,
// which runs the memorystore and the gRPC stack against speccheck.ControlPlane)
// and the test support they share.
//
// Test support, used by tests here and in other packages:
//   - StartMem and Start (grpcserver.go) run an in-process gRPC control plane node.
//   - fakedynamodb is an in-memory DynamoDB for the log tests.
//   - memorystore is an in-memory store over the speccheck spec.
//   - updaterstore adapts a stateupdater to the store and group-store calls, on a
//     local slot counter with no log.
//
// Nothing outside tests imports this tree.
package controlplanetest
