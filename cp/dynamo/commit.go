package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpobs"
)

// readParallel bounds the concurrent DynamoDB reads of one commit (one
// newest-run Query per partition, plus the segment/topic BatchGetItem).
const readParallel = 16

// segBlob is the segment item's payload: the request and the full original
// answer, so a replay returns it verbatim and a different request conflicts.
type segBlob struct {
	Batches []cp.Batch
	Ranges  []cp.Range
}

// commit is the state of one CommitSegment call across its attempts. ends are
// the partition end offsets last read (a newest-run Query each); only the
// partitions in stale are read again, the others are pinned by the run item's
// condition anyway.
type commit struct {
	s     *Store
	op    core.CommitSegment
	tps   []core.TopicPartition // distinct partitions, first-use order
	ends  map[core.TopicPartition]int64
	stale map[core.TopicPartition]bool
}

// CommitSegment follows the spec's rules in order. The offsets are computed
// from the newest run of each partition and committed in one transaction whose
// run items are written only if absent (a compare-and-set on the run base); a
// lost race (TransactionCanceled) reads the lost partitions again and retries,
// bounded, then cp.ErrUnavailable.
func (s *Store) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	cpobs.FromContext(ctx).SetSegment(op.SegmentKey)
	if !wellFormed(op) {
		return s.replayOnly(ctx, op)
	}
	c := &commit{s: s, op: op, tps: distinctPartitions(op.Batches),
		ends: map[core.TopicPartition]int64{}, stale: map[core.TopicPartition]bool{}}
	for _, tp := range c.tps {
		c.stale[tp] = true
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, time.Duration(rand.IntN(5<<min(attempt, 6))+1)*time.Millisecond); err != nil {
				return core.CommitSegmentResp{}, 0, err
			}
		}
		s.Obs.AttemptCtx(ctx, "commit")
		resp, code, retry, err := c.once(ctx)
		if !retry {
			return resp, code, err
		}
	}
	return core.CommitSegmentResp{}, 0, fmt.Errorf("%w: commit lost the race %d times", cp.ErrUnavailable, maxAttempts)
}

// wellFormed: a key, at least one batch, every batch representable.
func wellFormed(op core.CommitSegment) bool {
	if op.SegmentKey == "" || len(op.Batches) == 0 {
		return false
	}
	for _, b := range op.Batches {
		if b.Records <= 0 || b.TP.Topic == "" || b.TP.Partition < 0 {
			return false
		}
	}
	return true
}

// replayOnly answers a malformed op: only a committed key can still win
// (replay, or conflict); anything else is ErrInvalid. Its keys may be
// unrepresentable, so nothing but the segment item is read.
func (s *Store) replayOnly(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	if op.SegmentKey == "" {
		return core.CommitSegmentResp{}, core.ErrInvalid, nil
	}
	it, err := s.getItem(ctx, segKey(op.SegmentKey))
	if err != nil {
		return core.CommitSegmentResp{}, 0, err
	}
	if it == nil {
		return core.CommitSegmentResp{}, core.ErrInvalid, nil
	}
	return replay(op, it)
}

func distinctPartitions(bs []core.PendingBatch) []core.TopicPartition {
	var out []core.TopicPartition
	for _, b := range bs {
		if !slices.Contains(out, b.TP) {
			out = append(out, b.TP)
		}
	}
	return out
}

func distinctTopics(tps []core.TopicPartition) []string {
	var out []string
	for _, tp := range tps {
		if !slices.Contains(out, tp.Topic) {
			out = append(out, tp.Topic)
		}
	}
	return out
}

// once is one attempt: read, replay check, assign, write one transaction.
// retry means the transaction lost a race (or the service was unavailable)
// and the caller should go round again.
func (c *commit) once(ctx context.Context) (resp core.CommitSegmentResp, code core.ErrCode, retry bool, err error) {
	t0 := time.Now()
	seg, topics, err := c.read(ctx)
	c.s.Obs.Prep("reads", time.Since(t0))
	if err != nil {
		return resp, 0, false, err
	}
	if seg != nil { // already committed: replay or conflict
		resp, code, err = replay(c.op, seg)
		return resp, code, false, err
	}
	t1 := time.Now()
	ranges, grown := assign(c.op.Batches, c.ends, topics)
	items, code := c.s.txItems(c.op, ranges, grown)
	c.s.Obs.Prep("decide_encode", time.Since(t1))
	if code != core.OK {
		return resp, code, false, nil
	}
	if retry, err = c.write(ctx, items); retry || err != nil {
		return resp, 0, retry, err
	}
	return core.CommitSegmentResp{Assigned: ranges}, core.OK, false, nil
}

// read is the attempt's read round: one consistent BatchGetItem (segment item
// for replay, topic items for growth) and, concurrently, one newest-run Query
// per stale partition.
func (c *commit) read(ctx context.Context) (seg item, topics map[string]int32, err error) {
	stale := slices.DeleteFunc(slices.Clone(c.tps), func(tp core.TopicPartition) bool { return !c.stale[tp] })
	ends := make([]int64, len(stale))
	var got map[string]item
	jobs := []func(context.Context) error{func(ctx context.Context) (err error) {
		got, err = c.s.batchGet(ctx, c.readKeys())
		return err
	}}
	for i, tp := range stale {
		jobs = append(jobs, func(ctx context.Context) (err error) {
			ends[i], err = c.s.endOf(ctx, tp)
			return err
		})
	}
	if err = runAll(ctx, readParallel, jobs); err != nil {
		return nil, nil, err
	}
	for i, tp := range stale {
		c.ends[tp] = ends[i]
		delete(c.stale, tp)
	}
	topics = map[string]int32{}
	for _, t := range distinctTopics(c.tps) {
		if it := lookupKey(got, topicKey(t)); it != nil {
			n, _ := getNum(it, "parts")
			topics[t] = int32(n)
		}
	}
	return lookupKey(got, segKey(c.op.SegmentKey)), topics, nil
}

// readKeys: the segment item and the topic items of the batches.
func (c *commit) readKeys() []item {
	keys := []item{segKey(c.op.SegmentKey)}
	for _, t := range distinctTopics(c.tps) {
		keys = append(keys, topicKey(t))
	}
	return keys
}

// runAll runs the jobs with at most limit in flight and returns the first
// error (the others are cancelled).
func runAll(ctx context.Context, limit int, jobs []func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
		sem   = make(chan struct{}, limit)
	)
	for _, job := range jobs {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if err := job(ctx); err != nil {
				mu.Lock()
				if first == nil {
					first = err
					cancel()
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}

// endOf is the partition's next offset: the end of its newest run (0 if none),
// one consistent descending Query with Limit 1.
func (s *Store) endOf(ctx context.Context, tp core.TopicPartition) (int64, error) {
	newest, err := s.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String("pk = :p"),
		ExpressionAttributeValues: item{":p": str(partKey("R#", tp))},
		ScanIndexForward:          aws.Bool(false),
		Limit:                     aws.Int32(1),
	}, 1)
	if err != nil || len(newest) == 0 {
		return 0, err
	}
	base, _ := getNum(newest[0], "base")
	cnt, _ := getNum(newest[0], "cnt")
	return base + cnt, nil
}

// assign gives every batch its partition's end as base, in request order, the
// end growing by Records, and lists the topics that must grow to cover the
// partitions. ends and topics are not modified.
func assign(batches []core.PendingBatch, ends map[core.TopicPartition]int64, topics map[string]int32) ([]core.BatchRange, map[string]int32) {
	next := make(map[core.TopicPartition]int64, len(ends))
	maps.Copy(next, ends)
	grown := map[string]int32{}
	var out []core.BatchRange
	for _, b := range batches {
		out = append(out, core.BatchRange{TP: b.TP, Base: core.Offset(next[b.TP]), Count: b.Records, Loc: b.Loc})
		next[b.TP] += b.Records
		if want := b.TP.Partition + 1; want > max(topics[b.TP.Topic], grown[b.TP.Topic]) {
			grown[b.TP.Topic] = want
		}
	}
	return out, grown
}

// replay answers a segment that is already committed: the stored answer if the
// request is the same, ErrConflict if not.
func replay(op core.CommitSegment, it item) (resp core.CommitSegmentResp, code core.ErrCode, err error) {
	var sb segBlob
	if err := json.Unmarshal(getBlob(it, "blob"), &sb); err != nil {
		return resp, 0, fmt.Errorf("dynamo: corrupt segment %q: %w", op.SegmentKey, err)
	}
	if !slices.Equal(fromWireBatches(sb.Batches), op.Batches) {
		return resp, core.ErrConflict, nil
	}
	for _, r := range sb.Ranges {
		resp.Assigned = append(resp.Assigned, cp.RangeFromWire(r))
	}
	return resp, core.OK, nil
}

// write sends the commit transaction. retry: it lost a race
// (TransactionCanceled; the partitions whose run item was in the way are read
// again) or the service was unavailable; any other error is returned.
func (c *commit) write(ctx context.Context, items []types.TransactWriteItem) (retry bool, err error) {
	s := c.s
	t2 := time.Now()
	tout, err := s.db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
	var cc []types.ConsumedCapacity
	if tout != nil {
		cc = tout.ConsumedCapacity
	}
	s.Obs.CallCtx(ctx, "TransactWriteItems", t2, cc)
	if err == nil {
		return false, nil
	}
	var tc *types.TransactionCanceledException
	if errors.As(err, &tc) {
		s.noteCanceled(ctx, c.op, items, tc, err)
		c.markStale(tc)
		return true, nil
	}
	if errors.Is(infra(err), cp.ErrUnavailable) && !errors.Is(err, context.Canceled) {
		s.Obs.RetryCtx(ctx, "commit", "unavailable", nil)
		s.note(ctx, "TransactWriteItems", err)
		return true, nil
	}
	return false, err
}

// markStale flags the partitions whose run item failed its condition (item i
// of the transaction is the run of tps[i-1]); a cancellation without reasons
// flags them all.
func (c *commit) markStale(tc *types.TransactionCanceledException) {
	if len(tc.CancellationReasons) == 0 {
		for _, tp := range c.tps {
			c.stale[tp] = true
		}
		return
	}
	for i, r := range tc.CancellationReasons {
		if i >= 1 && i <= len(c.tps) && aws.ToString(r.Code) == "ConditionalCheckFailed" {
			c.stale[c.tps[i-1]] = true
		}
	}
}

// noteCanceled records a lost race: the retry metric with the per-item codes
// and a WARN line with the first few failing items.
func (s *Store) noteCanceled(ctx context.Context, op core.CommitSegment, items []types.TransactWriteItem, tc *types.TransactionCanceledException, err error) {
	codes, detail := cancelReasons(tc, items)
	s.Obs.RetryCtx(ctx, "commit", "transaction_canceled", codes)
	if s.Obs != nil {
		s.Obs.Logger().Warn("commit transaction canceled", "event", "commit_txn_canceled", "segment", op.SegmentKey,
			"items", len(items), "reasons", detail, "err", err.Error())
	}
}

// cancelReasons lists the codes of the items that did not commit and, for the
// log, "index:code:pk/sk" of the first few of them. Codes "None" (the item was
// fine) are skipped.
func cancelReasons(tc *types.TransactionCanceledException, items []types.TransactWriteItem) (codes, detail []string) {
	for i, r := range tc.CancellationReasons {
		c := aws.ToString(r.Code)
		if c == "" || c == "None" {
			continue
		}
		codes = append(codes, c)
		if len(detail) < 8 {
			d := fmt.Sprintf("%d:%s", i, c)
			if i < len(items) {
				d += ":" + txKey(items[i])
			}
			detail = append(detail, d)
		}
	}
	return
}

func txKey(t types.TransactWriteItem) string {
	var k item
	switch {
	case t.Put != nil:
		k = t.Put.Item
	case t.Update != nil:
		k = t.Update.Key
	case t.Delete != nil:
		k = t.Delete.Key
	case t.ConditionCheck != nil:
		k = t.ConditionCheck.Key
	}
	return getStr(k, "pk") + "/" + getStr(k, "sk")
}

func fromWireBatches(bs []cp.Batch) []core.PendingBatch {
	out := make([]core.PendingBatch, len(bs))
	for i, b := range bs {
		out[i] = cp.BatchFromWire(b)
	}
	return out
}

func keyID(k item) string { return getStr(k, "pk") + "\x00" + getStr(k, "sk") }

// txItems builds the commit transaction, or ErrTooLarge. Item 0 is the segment
// item, items 1..n the run items in first-use order of the partitions, then
// the grown topics.
func (s *Store) txItems(op core.CommitSegment, ranges []core.BatchRange, grown map[string]int32) ([]types.TransactWriteItem, core.ErrCode) {
	seg, ok := s.segmentItem(op, ranges)
	if !ok {
		return nil, core.ErrTooLarge
	}
	runs, ok := s.runItems(ranges)
	if !ok {
		return nil, core.ErrTooLarge
	}
	items := append([]types.TransactWriteItem{seg}, runs...)
	items = append(items, s.topicItems(grown)...)
	if len(items) > MaxTxItems {
		return nil, core.ErrTooLarge
	}
	return items, core.OK
}

// segmentItem is the idempotency key: request and answer, written only if
// absent. ok is false if the blob is too large.
func (s *Store) segmentItem(op core.CommitSegment, ranges []core.BatchRange) (types.TransactWriteItem, bool) {
	sb := segBlob{}
	for _, b := range op.Batches {
		sb.Batches = append(sb.Batches, cp.BatchToWire(b))
	}
	for _, r := range ranges {
		sb.Ranges = append(sb.Ranges, cp.RangeToWire(r))
	}
	payload, _ := json.Marshal(sb)
	if len(payload) > maxBlob {
		return types.TransactWriteItem{}, false
	}
	return types.TransactWriteItem{Put: &types.Put{
		TableName: &s.table, Item: merge(segKey(op.SegmentKey), item{"blob": blob(payload)}),
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	}}, true
}

// topicItems grows the topics whose partition count must rise.
func (s *Store) topicItems(grown map[string]int32) []types.TransactWriteItem {
	var items []types.TransactWriteItem
	for _, topic := range slices.Sorted(maps.Keys(grown)) {
		items = append(items, types.TransactWriteItem{Update: &types.Update{
			TableName: &s.table, Key: topicKey(topic),
			UpdateExpression:          aws.String("SET parts = :n"),
			ConditionExpression:       aws.String("attribute_not_exists(parts) OR parts < :n"),
			ExpressionAttributeValues: item{":n": num(int64(grown[topic]))},
		}})
	}
	return items
}

// runItems is one run item per partition, written only if absent: the sort
// key is the base offset, so a concurrent committer that took the same base
// makes this a lost race (the compare-and-set). ok is false if a run is too
// large.
func (s *Store) runItems(ranges []core.BatchRange) ([]types.TransactWriteItem, bool) {
	var items []types.TransactWriteItem
	for _, run := range runsOf(ranges) {
		it := runItem(run[0].TP, run)
		if len(getBlob(it, "runs")) > maxRun {
			return nil, false
		}
		items = append(items, putIfAbsent(s.table, it, "attribute_not_exists(sk)"))
	}
	return items, true
}

func merge(a, b item) item {
	for k, v := range b {
		a[k] = v
	}
	return a
}

// putIfAbsent is a Put guarded by cond (attribute_not_exists).
func putIfAbsent(table string, it item, cond string) types.TransactWriteItem {
	return types.TransactWriteItem{Put: &types.Put{TableName: &table, Item: it, ConditionExpression: aws.String(cond)}}
}
