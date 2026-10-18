// Package cp is the control-plane service: a JSON-over-HTTP handler (NewHandler)
// over a Store. It is the production twin of veritas.ControlPlane, which is its
// executable spec; the stores are checked against it op for op (cp/cptest).
// Not deterministic (goroutines, net/http): it is not in scripts/ci-determinism.sh.
//
// The HTTP API is described in api/openapi/controlplane.yaml (OpenAPI 3.1); a
// test keeps it in sync with this package's routes and wire types.
//
// Stores: cp/memstore (the spec behind a mutex) and cp/dynamo (DynamoDB, safe
// across many replicas). The broker side is cp/cpclient.
//
// DynamoDB design (decisions 39, 41-44): one table <prefix>cp, string keys pk
// and sk, on demand. There is no node item, producer state, segment-to-offset
// counter or high-watermark item: the next offset of a partition is the end
// of its newest run.
//
//	pk              sk            attributes
//	TOPICS          <topic>       parts     (CreateTopic and growth: max-merge)
//	G#<segkey>      -             blob      (request batches + full answer, JSON; the idempotency key)
//	R#<topic>#<p>   <base,%020d>  base cnt runs   (one item per partition and segment: a run)
//
// A run holds the batches one commit assigned to one partition: their offsets
// are contiguous, the sort key is the first offset, cnt is the run's total
// records and runs is a packed list of the batches (segment key table, then per
// batch: key index, record count, byte offset, byte length; varints; see
// cp/dynamo/runs.go). Batch offsets inside the run are the running sum of the
// counts. The end of a partition is base+cnt of its newest run (0 if none).
// A run is a few bytes per batch, far below the 400 KB item limit; maxRun
// (200 KB) is the explicit bound, and the G# blob (maxBlob, 300 KB, about 1000
// batches) is the tighter one. The segment key is an opaque string here
// (format.SegmentKey, decision 43).
//
// Lookup is a descending Query (Limit 1, consistent) on R#<topic>#<p> for the
// end, then a Query (sk BETWEEN) for the runs: the run holding From is the last
// item with base <= From; its batches wholly below From are dropped, and
// MaxBytes cuts per batch.
//
// CommitSegment, per attempt:
//
//  1. Reads, concurrently (at most 16 in flight): one consistent BatchGetItem
//     of G#<segkey> (replay) and the TOPICS items of the batches, and one
//     consistent descending Query with Limit 1 on R#<topic>#<p> per distinct
//     partition (the newest run: its base+cnt is the end). On a retry only the
//     partitions that lost the race are queried again.
//
//  2. G# present: the stored answer if the request is identical, ErrConflict
//     if not (a malformed request reads G# only and can still replay or
//     conflict; otherwise it is ErrInvalid).
//
//  3. Bases: each batch gets its partition's end, in request order, the end
//     growing by its records.
//
//  4. One TransactWriteItems:
//
//     Put    G#<segkey>      attribute_not_exists(pk)   the idempotency key
//     Put    R#..#p / base   attribute_not_exists(sk)   one run per partition: the compare-and-set
//     Update each grown TOPICS item   absent or parts < new
//
// The run item is written at sk = the base read; if another replica committed
// to the partition since, an item already sits at that base, the condition
// fails, TransactionCanceled follows and the commit reads those partitions
// again and retries (jittered backoff, 20 attempts, then cp.ErrUnavailable,
// HTTP 503, NotApplied). A lost race on G# (same key committed concurrently)
// retries into a replay.
//
// Cost per commit with P distinct partitions and no retry: 1 BatchGetItem + P
// Queries + 1 TransactWriteItems = P + 2 calls. The 100-item transaction limit
// gives ErrTooLarge: a commit needs 1 + P + (grown topics) items,
// whatever the number of batches (batches of one partition share one run
// item). For the sim: sim.DynamoMaxCommitItems accounting is
//
//	items = 1 + distinct partitions + topics that must grow (<= 100)
//
// The broker bounds a segment by MaxSegmentPartitions (24) and
// MaxSegmentBatches (256, for the G# blob) well below that. The spec has no
// such limit. A refusal (ErrTooLarge) is not retried by the broker.
package cp
