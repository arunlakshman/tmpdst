// Package controlplane is the control-plane service tree (renamed from cp/).
// Production manicp serves gRPC via controlplane/server over controlplane/run
// and controlplane/machine (CP-DSM-7). Brokers dial through controlplane/cpclient.
//
// The gRPC wire contract is controlplane/api/v1 (decision 57). Veritas remains
// the independent oracle and shares schema types via apiconv, not apply code
// (decision 21).
//
// Helpers under this tree (memstore, applier, exec) support tests and the
// machine's fold and IO. Not deterministic (goroutines, net): it is not
// checked by ops/tools/detcheck.
package controlplane
