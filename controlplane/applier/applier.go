package applier

import (
	"bytes"
	"fmt"
	"slices"

	"manifester/core"
)

// Applier is the control-plane state machine over a KV. Writes (commands) must
// be applied by one goroutine in log order; reads may run concurrently.
type Applier struct{ kv KV }

// New returns an applier over kv (empty, or restored from a checkpoint).
func New(kv KV) *Applier { return &Applier{kv: kv} }

// Close closes the store.
func (a *Applier) Close() error { return a.kv.Close() }

// Exec runs one CPCall like veritas.ControlPlane.Exec. For a CommitSegment the
// call's Req is the log slot (it orders the idempotency window for Trim). A
// store failure panics.
func (a *Applier) Exec(act core.IOAction) core.Completion {
	call, ok := act.(core.CPCall)
	if !ok {
		panic("applier: not a CPCall")
	}
	var d core.CPDone
	var err error
	switch op := call.Op.(type) {
	case core.CommitSegment:
		d, err = a.Commit(int64(call.Req), op)
	case core.Lookup:
		d, err = a.Lookup(op)
	case core.DescribeTopics:
		d, err = a.Describe(op)
	default:
		d.Err = core.ErrInvalid
	}
	if err != nil {
		panic(fmt.Errorf("applier: store failed: %w", err))
	}
	d.Req = call.Req
	return d
}

// Commit applies a CommitSegment that sits at log slot slot.
func (a *Applier) Commit(slot int64, op core.CommitSegment) (core.CPDone, error) {
	var d core.CPDone
	err := a.kv.Update(func(tx Tx) { d = commitTx(tx, slot, op) })
	return d, err
}

// Command is one CommitSegment at its log slot.
type Command struct {
	Slot int64
	Op   core.CommitSegment
}

// CommitBatch applies commands in order in ONE store transaction (each sees
// the writes of the ones before it) and returns their answers. It is what a
// replica uses to apply the slots one tail read delivered: the same state as
// applying them one by one, with far fewer page writes in the store.
func (a *Applier) CommitBatch(cmds []Command) ([]core.CPDone, error) {
	var out []core.CPDone
	err := a.kv.Update(func(tx Tx) {
		out = make([]core.CPDone, len(cmds))
		for i, c := range cmds {
			out[i] = commitTx(tx, c.Slot, c.Op)
		}
	})
	return out, err
}

// Lookup answers a Lookup from the stored runs.
func (a *Applier) Lookup(op core.Lookup) (core.CPDone, error) {
	if op.From < 0 {
		return core.CPDone{Err: core.ErrInvalid}, nil
	}
	var d core.CPDone
	err := a.kv.View(func(tx Tx) { d = lookupTx(tx, op) })
	return d, err
}

// Describe answers a DescribeTopics.
func (a *Applier) Describe(op core.DescribeTopics) (core.CPDone, error) {
	var d core.CPDone
	err := a.kv.View(func(tx Tx) { d = describeTx(tx, op) })
	return d, err
}

// CreateTopic creates name with at least partitions partitions (never
// shrinks). ErrInvalid for an empty name or partitions <= 0.
func (a *Applier) CreateTopic(name string, partitions int32) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	if err := a.kv.Update(func(tx Tx) { growTopic(tx, map[string]int32{}, name, partitions) }); err != nil {
		panic(fmt.Errorf("applier: store failed: %w", err))
	}
	return core.OK
}

// End is the next offset of tp (0 if unknown).
func (a *Applier) End(tp core.TopicPartition) core.Offset {
	var end core.Offset
	err := a.kv.View(func(tx Tx) { end = core.Offset(getUvarint(tx.Get(endKey(tp)))) })
	if err != nil {
		panic(fmt.Errorf("applier: store failed: %w", err))
	}
	return end
}

func validBatch(b core.PendingBatch) bool {
	return b.Records > 0 && b.TP.Topic != "" && b.TP.Partition >= 0
}

func validCommit(op core.CommitSegment) bool {
	return op.SegmentKey != "" && len(op.Batches) > 0 && slices.IndexFunc(op.Batches, func(b core.PendingBatch) bool { return !validBatch(b) }) < 0
}

func commitTx(tx Tx, slot int64, op core.CommitSegment) core.CPDone {
	dig := digest(op.Batches)
	if rec := tx.Get(idemKey(op.SegmentKey)); rec != nil {
		return replay(rec, dig, op)
	}
	if !validCommit(op) {
		return core.CPDone{Err: core.ErrInvalid}
	}
	assigned := assign(tx, op.Batches)
	tx.Set(idemKey(op.SegmentKey), idemRecord(slot, dig, assigned))
	tx.Set(slotKey(slot, op.SegmentKey), []byte{1})
	return core.CPDone{Resp: core.CommitSegmentResp{Assigned: assigned}, Outcome: core.Applied}
}

// replay answers a commit whose key is known: the original ranges, or
// ErrConflict if the request differs.
func replay(rec []byte, dig [digestLen]byte, op core.CommitSegment) core.CPDone {
	was, bases, err := parseIdem(rec)
	if err != nil {
		panic(fmt.Errorf("applier: key %q: %w", op.SegmentKey, err))
	}
	if was != dig {
		return core.CPDone{Err: core.ErrConflict}
	}
	if len(bases) != len(op.Batches) {
		panic(fmt.Errorf("applier: key %q: %w", op.SegmentKey, errCorrupt))
	}
	rs := make([]core.BatchRange, len(op.Batches))
	for i, b := range op.Batches {
		rs[i] = core.BatchRange{TP: b.TP, Base: core.Offset(bases[i]), Count: b.Records, Loc: b.Loc}
	}
	return core.CPDone{Resp: core.CommitSegmentResp{Assigned: rs}, Outcome: core.Applied}
}

// assign hands out offsets: each batch's Base is its partition's end, in
// request order, the end then growing by Records. It writes the new ends, one
// packed run per partition and grown topics, and returns the ranges.
func assign(tx Tx, bs []core.PendingBatch) []core.BatchRange {
	var order []core.TopicPartition
	ends := map[core.TopicPartition]int64{}
	runs := map[core.TopicPartition][]core.BatchRange{}
	out := make([]core.BatchRange, 0, len(bs))
	for _, b := range bs {
		e, seen := ends[b.TP]
		if !seen {
			e = getUvarint(tx.Get(endKey(b.TP)))
			order = append(order, b.TP)
		}
		r := core.BatchRange{TP: b.TP, Base: core.Offset(e), Count: b.Records, Loc: b.Loc}
		ends[b.TP], runs[b.TP] = e+b.Records, append(runs[b.TP], r)
		out = append(out, r)
	}
	topics := map[string]int32{}
	for _, tp := range order {
		tx.Set(endKey(tp), putUvarintVal(ends[tp]))
		tx.Set(runKey(tp, int64(runs[tp][0].Base)), encodeRun(runs[tp]))
		growTopic(tx, topics, tp.Topic, tp.Partition+1)
	}
	return out
}

// growTopic raises topic's partition count to at least n; seen caches the
// counts read in this transaction.
func growTopic(tx Tx, seen map[string]int32, topic string, n int32) {
	cur, ok := seen[topic]
	if !ok {
		cur = int32(getUvarint(tx.Get(topicKey(topic))))
	}
	if n > cur {
		cur = n
		tx.Set(topicKey(topic), putUvarintVal(int64(cur)))
	}
	seen[topic] = cur
}

func lookupTx(tx Tx, op core.Lookup) core.CPDone {
	resp := core.LookupResp{End: core.Offset(getUvarint(tx.Get(endKey(op.TP))))}
	lo := runKey(op.TP, 0)
	hi := prefixEnd(tpPrefix(pRun, op.TP))
	if k, _ := tx.Floor(lo, append(runKey(op.TP, int64(op.From)), 0)); k != nil {
		lo = k // the run holding From starts at or before it
	}
	var size int64
	var bad error
	tx.Ascend(lo, hi, func(k, v []byte) bool {
		rs, err := decodeRun(op.TP, runBase(k), v)
		if err != nil {
			bad = err
			return false
		}
		for _, r := range rs {
			if r.Base+core.Offset(r.Count) <= op.From {
				continue
			}
			resp.Ranges = append(resp.Ranges, r)
			size += r.Loc.Len
			if op.MaxBytes > 0 && size >= int64(op.MaxBytes) {
				return false
			}
		}
		return true
	})
	if bad != nil {
		panic(fmt.Errorf("applier: %v: %w", op.TP, bad))
	}
	return core.CPDone{Resp: resp}
}

func describeTx(tx Tx, op core.DescribeTopics) core.CPDone {
	var resp core.DescribeTopicsResp
	lo := []byte{pTopic}
	tx.Ascend(lo, prefixEnd(lo), func(k, v []byte) bool {
		name := string(bytes.Clone(k[1:]))
		if len(op.Topics) == 0 || slices.Contains(op.Topics, name) {
			resp.Topics = append(resp.Topics, core.TopicInfo{Name: name, Partitions: int32(getUvarint(v))})
		}
		return true
	})
	return core.CPDone{Resp: resp}
}
