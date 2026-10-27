// Package controlplane is the control-plane service tree (renamed from cp/).
// Production manicp serves gRPC via controlplane/server over controlplane/run
// and controlplane/machine (CP-DSM-7). Brokers dial through controlplane/cpclient.
//
// The gRPC wire contract is controlplane/api/v1 (decision 57). Veritas remains
// the independent oracle and shares schema types via apiconv, not apply code
// (decision 21).
//
// Stores and helpers under this tree (memstore, logcp, applier) remain for
// tests and for the machine's fold. Not deterministic (goroutines, net): it is
// not in scripts/ci-determinism.sh.
package controlplane
