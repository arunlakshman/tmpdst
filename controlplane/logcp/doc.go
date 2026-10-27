// Package logcp is the production control-plane service of design A
// (decisions 48-52, docs/log-cp-service.md): a DynamoDB log of raw commands
// (CommitSegment, CreateTopic), one conditional PutItem per slot, applied in
// slot order by cp/applier on an embedded KV store, answered from the applied
// state only.
//
// One node, the active one, appends (K appends in flight, several commands
// grouped in one slot, a hole filler); the others are hot standbys that tail
// the log and apply it. The log slot is the fence: two appenders race on the
// same slot with an if-absent write and the loser retries, so no lease is
// needed for safety. Reads are fresh: a Lookup reads the tail (shared between
// concurrent reads), applies through it and answers from memory.
//
// The package is I/O shell code (wall clock, goroutines, DynamoDB, S3). All
// blocking and time go through Env so the seeded harness can drive the real
// code. Service implements cp.Store; Service.Handler is the HTTP handler.
package logcp
