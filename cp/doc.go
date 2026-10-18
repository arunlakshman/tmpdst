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
//	R#<topic>#<p>   <base,%020d>  base cnt runs        (one item per partition and segment: a run)
//	S#<topic>#<p>   <producer>    pepoch nxt win(JSON, last 5) v (version)
//
// A run holds the batches one commit assigned to one partition: their offsets
// are contiguous, the sort key is the first offset, cnt is the run's total
// records and runs is a packed list of the batches (segment key table, then per
// batch: key index, record count, byte offset, byte length; varints; see
// cp/dynamo/runs.go). Batch offsets inside the run are the running sum of the
// counts. A run is a few bytes per batch, far below the 400 KB item limit;
// maxRun (200 KB) is the explicit bound, and the G# blob (maxBlob, 300 KB, about
// 1000 batches) is the tighter one. The contract (core.BatchRange, Lookup) is
// unchanged: Lookup expands runs into the same per-batch ranges.
//
// Lookup is a consistent GetItem of the HWM plus a Query on R#<topic>#<p>
// (sk BETWEEN): the run holding From is the last item with base <= From; its
// batches wholly below From are dropped, and MaxBytes cuts per batch.
// On-disk change (decision 39): the old table held one item per batch with
// lk/lo/ll attributes; this system is pre-release, so there is no migration.
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
//	Put            one R# run item per partition with newly assigned batches
//
// TransactionCanceled (a lost optimistic race) re-reads and retries with
// jittered backoff, 20 attempts, then cp.ErrUnavailable (HTTP 503, NotApplied).
// The 100-item transaction limit gives ErrTooLarge: a commit needs
// 2 + 2 x (partitions with a new batch: HWM and run) + grown topics + producer
// states read (one per idempotent (partition, producer)) items. The number of
// batches does not appear; the producer states are the only per-batch growth,
// so the broker bounds a segment by its item cost (broker.Config.
// MaxSegmentItems, default 96, plus MaxSegmentPartitions 24 and
// MaxSegmentBatches 256 for the G# blob). The spec has no such limit. A
// refusal (ErrTooLarge) is not retried by the broker, and the sim CP engine
// refuses the same commits (sim.ControlPlaneFaults.MaxCommitItems).
package cp
