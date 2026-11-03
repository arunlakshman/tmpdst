// Package controlplanetest is the one home of control-plane testing: the tests
// themselves (handler and client request-and-reply tests, and conformance C3,
// which runs the memorystore and the gRPC stack against speccheck.ControlPlane)
// and the test support they share.
//
// Test support, used by tests here and in other packages:
//   - StartMem and Start (grpcserver.go) run an in-process gRPC control plane node.
//   - fakedynamodb is an in-memory DynamoDB for the log tests.
//   - memorystore is an in-memory controlplane.Store over the speccheck spec.
//
// Nothing outside tests imports this tree.
package controlplanetest
