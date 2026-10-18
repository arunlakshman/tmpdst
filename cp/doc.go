// Package cp is the control-plane service: a JSON-over-HTTP handler (NewHandler)
// over a Store. It is the production twin of veritas.ControlPlane, which is its
// executable spec; the stores are checked against it op for op (cp/cptest).
// Not deterministic (goroutines, net/http): it is not in scripts/ci-determinism.sh.
//
// Stores: cp/memstore (the spec behind a mutex) and cp/dynamo (DynamoDB, safe
// across many replicas). The broker side is cp/cpclient.
//
// DynamoDB design: one table <prefix>cp, string keys pk and sk, on demand.
//
//	pk              sk            attributes
//	N#<node>        -             epoch     (Register: UpdateItem ADD)
//	PID             -             n         (InitProducer: ADD; ID = 999+n)
//	TOPICS          <topic>       parts     (CreateTopic and growth: max-merge)
//	G#<segkey>      -             blob      (request batches + full answer, JSON)
//	R#<topic>#<p>   !             hwm       (next offset; "!" sorts before digits)
//	R#<topic>#<p>   <base,%020d>  base cnt lk lo ll   (one item per assigned batch)
//	S#<topic>#<p>   <producer>    pepoch nxt win(JSON, last 5) v (version)
//
// Lookup is a consistent GetItem of the HWM plus a Query on R#<topic>#<p>
// (sk BETWEEN): the range holding From is the last item with base <= From.
//
// CommitSegment: replay check (GetItem G#key), validation, fence check, a
// consistent BatchGet of the HWM, topic and producer-state items, a pure
// decision (decide.go), then one TransactWriteItems:
//
//	ConditionCheck N#<node>      epoch = request epoch
//	Put            G#<segkey>    attribute_not_exists (the idempotency key)
//	Update         each changed R#..#p / hwm          hwm = value read (or absent)
//	Update         each grown TOPICS item             absent or parts < new
//	Update/Check   each S# item read                  v = value read (or absent)
//	Put            one R# range item per assigned batch
//
// TransactionCanceled (a lost optimistic race) re-reads and retries with
// jittered backoff, 20 attempts, then cp.ErrUnavailable (HTTP 503, NotApplied).
// The 100-item transaction limit gives ErrTooLarge: a commit needs
// 2 + changed partitions + grown topics + producer states read + assigned
// batches items, so roughly 30 partitions per segment with idempotent
// producers and 48 without. The spec has no such limit; brokers must cap.
// The coupling: broker.Config.MaxSegmentPartitions (24) and MaxSegmentBatches
// (32) bound a segment to 2 + 24 + 32 + 32 = 90 items plus one per grown topic,
// under 100. Raising either needs this limit raised or range items compacted
// (docs/TODO.md). A refusal (ErrTooLarge) is not retried by the broker, and the
// sim CP engine refuses the same commits (sim.ControlPlaneFaults.MaxCommitItems).
package cp
