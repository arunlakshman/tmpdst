// Package cp is the control-plane service: a JSON-over-HTTP handler (NewHandler)
// over a Store. It is the production twin of veritas.ControlPlane, which is its
// executable spec; the stores are checked against it op for op (cp/cptest).
// Not deterministic (goroutines, net/http): it is not in scripts/ci-determinism.sh.
//
// The HTTP API is described in api/openapi/controlplane.yaml (OpenAPI 3.1); a
// test keeps it in sync with this package's routes and wire types.
//
// Stores: cp/memstore (the spec behind a mutex, for tests and demos) and
// cp/logcp (the log-backed control plane over a DynamoDB log and an object
// store of checkpoints, decisions 48-53; docs/log-cp-service.md), which is
// what cmd/manicp serves in production. The earlier DynamoDB-transaction
// store and its sequencer were removed (see docs/decisions.md). The broker side
// is cp/cpclient.
//
// A commit has no size limit in the stores: the broker bounds a segment by
// MaxSegmentPartitions and MaxSegmentBatches so one commit stays a small log
// entry (broker.Config). The spec has no such limit. A refusal (ErrTooLarge)
// is not retried by the broker.
package cp
