// Package controlplane is the control-plane service tree (renamed from cp/).
// The JSON-over-HTTP handler (NewHandler) over a Store remains until the gRPC
// cutover. It is the production twin of veritas.ControlPlane, which is its
// executable spec; the stores are checked against it op for op
// (controlplane/cptest). Not deterministic (goroutines, net/http): it is not
// in scripts/ci-determinism.sh.
//
// The gRPC wire contract is controlplane/api/v1 (decision 57). The HTTP OpenAPI
// description in api/openapi/controlplane.yaml stays in sync with this
// package's routes and JSON wire types until that cutover.
//
// Stores: controlplane/memstore (the spec behind a mutex, for tests and demos)
// and controlplane/logcp (the log-backed control plane over a DynamoDB log and
// an object store of checkpoints, decisions 48-53; docs/log-cp-service.md),
// which is what cmd/manicp serves in production. The broker side is
// controlplane/cpclient.
//
// A commit has no size limit in the stores: the broker bounds a segment by
// MaxSegmentPartitions and MaxSegmentBatches so one commit stays a small log
// entry (broker.Config). The spec has no such limit. A refusal (ErrTooLarge)
// is not retried by the broker.
package controlplane
