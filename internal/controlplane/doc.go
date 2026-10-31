// Package controlplane is the control-plane service tree (renamed from cp/).
// Production mdscontrolplane serves gRPC via controlplane/server over controlplane/runloop
// and controlplane/statemachine (CP-DSM-7). Brokers dial through controlplane/controlplaneclient.
//
// The gRPC wire contract is controlplane/api/v1 (decision 57). Veritas remains
// the independent oracle and shares schema types via apiconvert, not apply code
// (decision 21).
//
// Helpers under this tree (memorystore, stateupdater, executor) support tests and the
// state machine's fold and IO. Not deterministic (goroutines, net): it is not
// checked by ops/tools/determinismcheck.
package controlplane
