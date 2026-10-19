// Package cp is the control-plane service: a JSON-over-HTTP handler (NewHandler)
// over a Store. It is the production twin of veritas.ControlPlane, which is its
// executable spec; the stores are checked against it op for op (cp/cptest).
// Not deterministic (goroutines, net/http): it is not in scripts/ci-determinism.sh.
//
// The HTTP API is described in api/openapi/controlplane.yaml (OpenAPI 3.1); a
// test keeps it in sync with this package's routes and wire types.
//
// Stores: cp/memstore (the spec behind a mutex) and cp/dynamo (DynamoDB: the
// Store, safe across many replicas but contended when they share partitions,
// and the Sequencer over it, which is what cmd/manicp serves; see "Sequencer"
// below). The broker side is cp/cpclient.
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
// # Sequencer (decision 46)
//
// dynamo.Sequencer is a cp.Store over a dynamo.Store. It owns the sequencing:
// brokers never retry because of contention, the sequencer retries inside and
// returns the assigned offsets. Deploy exactly ONE manicp (every broker's
// -cp-url points at it); correctness does not depend on that, the compare-and-set
// of the commit transaction is unchanged and a second or stale sequencer, an old
// Store, a restart or an out-of-band CreateTopic only cost internal retries.
//
//   - One loop goroutine takes queued CommitSegment requests in order and forms
//     groups. A group is committed by ONE TransactWriteItems: 1 G# item per
//     segment, 1 run item per distinct partition (the batches of one partition
//     from several segments are merged into one run; the packed run keeps a
//     table of distinct segment keys), 1 Update per grown topic; at most
//     MaxItems (100) items and 1 MiB of G# blobs. There is no wait: a lone
//     commit goes immediately; groups form while a transaction is in flight.
//   - Each request is answered with its own ranges in request order. A replay,
//     conflict, ErrTooLarge or malformed request is answered alone and the
//     rest of the group still commits; a request that does not fit the budget
//     waits for the next group (with every later request that shares one of its
//     partitions, so per-partition order is the queue order).
//   - State in memory (never an authority): the next offset of each partition
//     (loaded with the descending Query on first touch), the topic partition
//     counts and a bounded LRU of recently committed segment keys with their
//     answers (replay without a read). A new key needs no read: the G# Put is
//     conditional; if it fails, that G# is read and the request answered as a
//     replay or conflict, and the others retry.
//   - Failure handling. TransactionCanceled: the per-item reasons say what to
//     refresh (run item: that partition, topic item: that topic, G#: read it),
//     the group is rebuilt and retried with jittered backoff, at most 40
//     attempts / 8 s, then cp.ErrUnavailable (503, the broker resends the same
//     key). A rejection before apply (throttling, TransactionConflict) retries
//     with the state kept. Any other error may have applied (5xx, timeout, lost
//     answer): the group's partitions and topics are forgotten and every G# of
//     the group is read first, so an applied commit is answered with its stored
//     ranges and never assigned twice.
//   - Pipelining (SeqOptions.Pipeline, default 4): groups on disjoint
//     partitions, disjoint growing topics and disjoint segment keys may be in
//     flight together. A group reserves those resources until it ends and the
//     planner never forms a group that touches a reserved one, so no transaction
//     is issued with a base chained after another in flight on the same
//     partition: a failed transaction cannot leave a hole behind it.
//   - A context cancelled while the request is queued withdraws it; once its
//     group is formed it may still commit and the broker's resend replays it.
//
// Metrics: manicp_seq_* (cp/cpobs); the per-request commit_attempts_total and
// retries_total keep counting.
package cp
