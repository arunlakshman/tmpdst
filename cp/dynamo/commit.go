package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp"
)

// segBlob is the segment item's payload: the request and the full original
// answer, so a replay returns it verbatim and a different request conflicts.
type segBlob struct {
	Batches []cp.Batch
	Ranges  []cp.Range
}

// CommitSegment follows the spec's rules in order. Everything it decides is
// computed from consistent reads and committed in one transaction whose
// conditions pin those reads; a lost race (TransactionCanceled) re-reads and
// retries, bounded, then cp.ErrUnavailable.
func (s *Store) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, time.Duration(rand.IntN(5<<min(attempt, 6))+1)*time.Millisecond); err != nil {
				return core.CommitSegmentResp{}, 0, err
			}
		}
		resp, code, retry, err := s.commitOnce(ctx, op)
		if !retry {
			return resp, code, err
		}
	}
	return core.CommitSegmentResp{}, 0, fmt.Errorf("%w: commit lost the race %d times", cp.ErrUnavailable, maxAttempts)
}

func (s *Store) commitOnce(ctx context.Context, op core.CommitSegment) (resp core.CommitSegmentResp, code core.ErrCode, retry bool, err error) {
	// 1. Already committed: replay or conflict, whatever the epoch says now.
	if it, err := s.getItem(ctx, segKey(op.SegmentKey)); err != nil {
		return resp, 0, false, err
	} else if it != nil {
		var sb segBlob
		if err := json.Unmarshal(getBlob(it, "blob"), &sb); err != nil {
			return resp, 0, false, fmt.Errorf("dynamo: corrupt segment %q: %w", op.SegmentKey, err)
		}
		if !slices.Equal(fromWireBatches(sb.Batches), op.Batches) {
			return resp, core.ErrConflict, false, nil
		}
		for _, r := range sb.Ranges {
			br, err := cp.RangeFromWire(r)
			if err != nil {
				return resp, 0, false, err
			}
			resp.Assigned = append(resp.Assigned, br)
		}
		return resp, core.OK, false, nil
	}
	// 2. Malformed (producer IDs are checked against the allocation counter).
	if op.SegmentKey == "" || len(op.Batches) == 0 {
		return resp, core.ErrInvalid, false, nil
	}
	hasProducer := false
	for _, b := range op.Batches {
		if b.Records <= 0 || b.TP.Topic == "" || b.TP.Partition < 0 || b.PEpoch < 0 || b.BaseSeq < 0 {
			return resp, core.ErrInvalid, false, nil
		}
		hasProducer = hasProducer || b.Producer != core.NoProducer
	}
	if hasProducer {
		it, err := s.getItem(ctx, pidKey())
		if err != nil {
			return resp, 0, false, err
		}
		n, _ := getNum(it, "n")
		next := core.ProducerID(firstProducer + n)
		for _, b := range op.Batches {
			if b.Producer != core.NoProducer && (b.Producer < firstProducer || b.Producer >= next) {
				return resp, core.ErrInvalid, false, nil
			}
		}
	}
	// 3. Fencing.
	nit, err := s.getItem(ctx, nodeKey(op.Owner.Node))
	if err != nil {
		return resp, 0, false, err
	}
	if cur, ok := getNum(nit, "epoch"); !ok || uint64(cur) != uint64(op.Epoch) {
		return resp, core.ErrFenced, false, nil
	}
	// 4. Read, decide, commit.
	snap, err := s.snapshot(ctx, op.Batches)
	if err != nil {
		return resp, 0, false, err
	}
	d := decide(op.Batches, snap)
	items, code := s.txItems(op, snap, d)
	if code != core.OK {
		return resp, code, false, nil
	}
	_, err = s.db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err != nil {
		var tc *types.TransactionCanceledException
		if errors.As(err, &tc) || errors.Is(infra(err), cp.ErrUnavailable) && !errors.Is(err, context.Canceled) {
			return resp, 0, true, nil
		}
		return resp, 0, false, err
	}
	return core.CommitSegmentResp{Assigned: d.ranges}, core.OK, false, nil
}

func fromWireBatches(bs []cp.Batch) []core.PendingBatch {
	out := make([]core.PendingBatch, len(bs))
	for i, b := range bs {
		out[i] = cp.BatchFromWire(b)
	}
	return out
}

// snapshot reads the HWM, producer state and topic items the batches touch.
func (s *Store) snapshot(ctx context.Context, batches []core.PendingBatch) (*snapshot, error) {
	var keys []item
	seen := map[string]bool{}
	add := func(k item) {
		id := getStr(k, "pk") + "\x00" + getStr(k, "sk")
		if !seen[id] {
			seen[id] = true
			keys = append(keys, k)
		}
	}
	for _, b := range batches {
		add(hwmKey(b.TP))
		add(topicKey(b.TP.Topic))
		if b.Producer != core.NoProducer {
			add(stateKey(skey{b.TP, b.Producer}))
		}
	}
	got, err := s.batchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	snap := &snapshot{hwm: map[core.TopicPartition]int64{}, state: map[skey]*pstate{}, topics: map[string]int32{}, ver: map[skey]int64{}}
	for _, b := range batches {
		if it := lookupKey(got, hwmKey(b.TP)); it != nil {
			snap.hwm[b.TP], _ = getNum(it, "hwm")
		}
		if it := lookupKey(got, topicKey(b.TP.Topic)); it != nil {
			n, _ := getNum(it, "parts")
			snap.topics[b.TP.Topic] = int32(n)
		}
		if b.Producer == core.NoProducer {
			continue
		}
		k := skey{b.TP, b.Producer}
		if it := lookupKey(got, stateKey(k)); it != nil {
			st := &pstate{}
			pe, _ := getNum(it, "pepoch")
			st.PEpoch = int16(pe)
			st.Next, _ = getNum(it, "nxt")
			if err := json.Unmarshal(getBlob(it, "win"), &st.Window); err != nil {
				return nil, fmt.Errorf("dynamo: corrupt producer state %v: %w", k, err)
			}
			snap.state[k] = st
			snap.ver[k], _ = getNum(it, "v")
		}
	}
	return snap, nil
}

// txItems builds the commit transaction, or ErrTooLarge.
func (s *Store) txItems(op core.CommitSegment, snap *snapshot, d *decision) ([]types.TransactWriteItem, core.ErrCode) {
	sb := segBlob{}
	for _, b := range op.Batches {
		sb.Batches = append(sb.Batches, cp.BatchToWire(b))
	}
	for _, r := range d.ranges {
		sb.Ranges = append(sb.Ranges, cp.RangeToWire(r))
	}
	payload, _ := json.Marshal(sb)
	if len(payload) > maxBlob {
		return nil, core.ErrTooLarge
	}
	t := &s.table
	// The fence and the segment itself.
	items := []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName: t, Key: nodeKey(op.Owner.Node),
			ConditionExpression:       aws.String("epoch = :e"),
			ExpressionAttributeValues: item{":e": num(int64(op.Epoch))},
		}},
		{Put: &types.Put{
			TableName: t, Item: merge(segKey(op.SegmentKey), item{"blob": blob(payload)}),
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		}},
	}
	for tp, hwm := range d.hwm {
		u := &types.Update{TableName: t, Key: hwmKey(tp), UpdateExpression: aws.String("SET hwm = :new")}
		if old, ok := snap.hwm[tp]; ok {
			u.ConditionExpression = aws.String("hwm = :old")
			u.ExpressionAttributeValues = item{":new": num(hwm), ":old": num(old)}
		} else {
			u.ConditionExpression = aws.String("attribute_not_exists(pk)")
			u.ExpressionAttributeValues = item{":new": num(hwm)}
		}
		items = append(items, types.TransactWriteItem{Update: u})
	}
	for topic, parts := range d.topics {
		items = append(items, types.TransactWriteItem{Update: &types.Update{
			TableName: t, Key: topicKey(topic),
			UpdateExpression:          aws.String("SET parts = :n"),
			ConditionExpression:       aws.String("attribute_not_exists(parts) OR parts < :n"),
			ExpressionAttributeValues: item{":n": num(int64(parts))},
		}})
	}
	for k, old := range snap.versions(op.Batches) {
		st, changed := d.state[k]
		if !changed {
			// Read but unchanged (dup or reject): pin it so the answer holds.
			c := &types.ConditionCheck{TableName: t, Key: stateKey(k)}
			if old < 0 {
				c.ConditionExpression = aws.String("attribute_not_exists(pk)")
			} else {
				c.ConditionExpression = aws.String("v = :v")
				c.ExpressionAttributeValues = item{":v": num(old)}
			}
			items = append(items, types.TransactWriteItem{ConditionCheck: c})
			continue
		}
		win, _ := json.Marshal(st.Window)
		u := &types.Update{
			TableName: t, Key: stateKey(k),
			UpdateExpression: aws.String("SET pepoch = :pe, nxt = :nx, win = :w, v = :v1"),
			ExpressionAttributeValues: item{
				":pe": num(int64(st.PEpoch)), ":nx": num(st.Next), ":w": blob(win), ":v1": num(max(old, 0) + 1),
			},
		}
		if old < 0 {
			u.ConditionExpression = aws.String("attribute_not_exists(pk)")
		} else {
			u.ConditionExpression = aws.String("v = :v0")
			u.ExpressionAttributeValues[":v0"] = num(old)
		}
		items = append(items, types.TransactWriteItem{Update: u})
	}
	for _, r := range d.ranges {
		if r.Err != core.OK || r.Dup {
			continue
		}
		items = append(items, types.TransactWriteItem{Put: &types.Put{
			TableName: t,
			Item: merge(key(partKey("R#", r.TP), offSK(int64(r.Base))), item{
				"base": num(int64(r.Base)), "cnt": num(r.Count),
				"lk": str(r.Loc.Key), "lo": num(r.Loc.Off), "ll": num(r.Loc.Len),
			}),
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		}})
	}
	if len(items) > MaxTxItems {
		return nil, core.ErrTooLarge
	}
	return items, core.OK
}

func merge(a, b item) item {
	for k, v := range b {
		a[k] = v
	}
	return a
}
