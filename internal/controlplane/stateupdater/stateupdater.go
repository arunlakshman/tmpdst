package stateupdater

import (
	"bytes"
	"fmt"
	"slices"

	"manifester/internal/core"
)

// StateUpdater is the control-plane state machine over a KeyValueStore. Writes (commands) must
// be applied by one goroutine in log order; reads may run concurrently.
type StateUpdater struct{ kv KeyValueStore }

// New returns a state updater over kv (empty, or restored from a checkpoint).
func New(kv KeyValueStore) *StateUpdater { return &StateUpdater{kv: kv} }

// Close closes the store.
func (a *StateUpdater) Close() error { return a.kv.Close() }

// Exec runs one ControlPlaneCall like speccheck.ControlPlane.Exec. For a CommitSegment the
// call's Req is the log slot (it orders the idempotency window for Trim). A
// store failure panics.
func (a *StateUpdater) Exec(act core.IOCall) core.IOResult {
	call, ok := act.(core.ControlPlaneCall)
	if !ok {
		panic("stateupdater: not a ControlPlaneCall")
	}
	var d core.ControlPlaneDone
	var err error
	switch op := call.Op.(type) {
	case core.CommitSegment:
		d, err = a.Commit(int64(call.Req), op)
	case core.FindSegments:
		d, err = a.Lookup(op)
	case core.DescribeTopics:
		d, err = a.Describe(op)
	case core.CreateTopic:
		d.Err = a.CreateTopicIf(op.Name, op.Partitions, op.FailIfExists)
		if d.Err == core.OK {
			d.Outcome = core.Applied
		}
	case core.GroupInstall, core.GroupAssign, core.GroupRemove, core.OffsetCommit:
		d, err = a.groupWrite(op)
	case core.OffsetFetch:
		d, err = a.OffsetFetch(op)
	case core.DescribeGroup:
		d, err = a.DescribeGroup(op)
	case core.ListGroups:
		d, err = a.ListGroups(op)
	default:
		d.Err = core.ErrInvalid
	}
	if err != nil {
		panic(fmt.Errorf("stateupdater: store failed: %w", err))
	}
	d.Req = call.Req
	return d
}

// Commit applies a CommitSegment that sits at log slot slot.
func (a *StateUpdater) Commit(slot int64, op core.CommitSegment) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.kv.Update(func(tx Transaction) { d = commitTransaction(tx, slot, op) })
	return d, err
}

// KnownCommit answers op from the idempotency window when the key is already
// present. ok is false when the key is unknown (caller must append).
func (a *StateUpdater) KnownCommit(op core.CommitSegment) (core.ControlPlaneDone, bool, error) {
	var d core.ControlPlaneDone
	var ok bool
	err := a.kv.View(func(tx Transaction) {
		rec := tx.Get(duplicateCheckKey(op.SegmentKey))
		if rec == nil {
			return
		}
		ok = true
		d = replay(rec, checksum(op.Batches), op)
	})
	return d, ok, err
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
func (a *StateUpdater) CommitBatch(cmds []Command) ([]core.ControlPlaneDone, error) {
	var out []core.ControlPlaneDone
	err := a.kv.Update(func(tx Transaction) {
		out = make([]core.ControlPlaneDone, len(cmds))
		for i, c := range cmds {
			out[i] = commitTransaction(tx, c.Slot, c.Op)
		}
	})
	return out, err
}

// Lookup answers a Lookup from the stored runs.
func (a *StateUpdater) Lookup(op core.FindSegments) (core.ControlPlaneDone, error) {
	if op.From < 0 {
		return core.ControlPlaneDone{Err: core.ErrInvalid}, nil
	}
	var d core.ControlPlaneDone
	err := a.kv.View(func(tx Transaction) { d = lookupTransaction(tx, op) })
	return d, err
}

// Describe answers a DescribeTopics.
//
//nolint:dupl // same shape as groupWrite, but a read transaction
func (a *StateUpdater) Describe(op core.DescribeTopics) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.kv.View(func(tx Transaction) { d = describeTransaction(tx, op) })
	return d, err
}

// CreateTopic creates name with at least partitions partitions (never
// shrinks). ErrInvalid for an empty name or partitions <= 0.
func (a *StateUpdater) CreateTopic(name string, partitions int32) core.ErrCode {
	return a.CreateTopicIf(name, partitions, false)
}

// CreateTopicIf is CreateTopic. With failIfExists, an existing topic gives
// ErrTopicExists and nothing changes. The check and the write are one
// transaction, so the answer depends only on the state before this command.
func (a *StateUpdater) CreateTopicIf(name string, partitions int32, failIfExists bool) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	code := core.OK
	err := a.kv.Update(func(tx Transaction) {
		if failIfExists && tx.Get(topicKey(name)) != nil {
			code = core.ErrTopicExists
			return
		}
		addPartitions(tx, map[string]int32{}, name, partitions)
	})
	if err != nil {
		panic(fmt.Errorf("stateupdater: store failed: %w", err))
	}
	return code
}

// End is the next offset of tp (0 if unknown).
func (a *StateUpdater) End(tp core.TopicPartition) core.Offset {
	var end core.Offset
	err := a.kv.View(func(tx Transaction) { end = core.Offset(readVarint(tx.Get(endKey(tp)))) })
	if err != nil {
		panic(fmt.Errorf("stateupdater: store failed: %w", err))
	}
	return end
}

func validBatch(b core.PendingBatch) bool {
	return b.Records > 0 && b.TopicPartition.Topic != "" && b.TopicPartition.Partition >= 0
}

func validCommit(op core.CommitSegment) bool {
	return op.SegmentKey != "" && len(op.Batches) > 0 && slices.IndexFunc(op.Batches, func(b core.PendingBatch) bool { return !validBatch(b) }) < 0
}

func commitTransaction(tx Transaction, slot int64, op core.CommitSegment) core.ControlPlaneDone {
	dig := checksum(op.Batches)
	if rec := tx.Get(duplicateCheckKey(op.SegmentKey)); rec != nil {
		return replay(rec, dig, op)
	}
	if !validCommit(op) {
		return core.ControlPlaneDone{Err: core.ErrInvalid}
	}
	return storeCommit(tx, slot, op, dig, assign(tx, op.Batches))
}

// commitWithBases is the BugWriterOffsets apply path: bases come from the
// writer instead of partition ends.
func (a *StateUpdater) commitWithBases(slot int64, op core.CommitSegment, bases []core.Offset) core.ControlPlaneDone {
	var d core.ControlPlaneDone
	err := a.kv.Update(func(tx Transaction) {
		dig := checksum(op.Batches)
		if rec := tx.Get(duplicateCheckKey(op.SegmentKey)); rec != nil {
			d = replay(rec, dig, op)
			return
		}
		if !validCommit(op) || len(bases) != len(op.Batches) {
			d = core.ControlPlaneDone{Err: core.ErrInvalid}
			return
		}
		d = storeCommit(tx, slot, op, dig, assignAt(tx, op.Batches, bases))
	})
	if err != nil {
		panic(fmt.Errorf("stateupdater: store failed: %w", err))
	}
	return d
}

// replay answers a commit whose key is known: the original ranges, or
// ErrConflict if the request differs.
func replay(rec []byte, dig [checksumLength]byte, op core.CommitSegment) core.ControlPlaneDone {
	was, bases, err := parseDuplicateCheck(rec)
	if err != nil {
		panic(fmt.Errorf("stateupdater: key %q: %w", op.SegmentKey, err))
	}
	if was != dig {
		return core.ControlPlaneDone{Err: core.ErrConflict}
	}
	if len(bases) != len(op.Batches) {
		panic(fmt.Errorf("stateupdater: key %q: %w", op.SegmentKey, errCorrupt))
	}
	rs := make([]core.BatchRange, len(op.Batches))
	for i, b := range op.Batches {
		rs[i] = core.BatchRange{TopicPartition: b.TopicPartition, Base: core.Offset(bases[i]), Count: b.Records, Loc: b.Loc}
	}
	return core.ControlPlaneDone{Resp: core.CommitSegmentResp{Assigned: rs}, Outcome: core.Applied}
}

func storeCommit(tx Transaction, slot int64, op core.CommitSegment, dig [checksumLength]byte, assigned []core.BatchRange) core.ControlPlaneDone {
	tx.Set(duplicateCheckKey(op.SegmentKey), duplicateCheckRecord(slot, dig, assigned))
	tx.Set(slotKey(slot, op.SegmentKey), []byte{1})
	return core.ControlPlaneDone{Resp: core.CommitSegmentResp{Assigned: assigned}, Outcome: core.Applied}
}

// offsetPlan is the per-partition end and run built while handing out offsets.
type offsetPlan struct {
	order []core.TopicPartition
	ends  map[core.TopicPartition]int64
	runs  map[core.TopicPartition][]core.BatchRange
	seen  map[core.TopicPartition]bool
}

func newOffsetPlan() *offsetPlan {
	return &offsetPlan{
		ends: map[core.TopicPartition]int64{},
		runs: map[core.TopicPartition][]core.BatchRange{},
		seen: map[core.TopicPartition]bool{},
	}
}

func (p *offsetPlan) add(b core.PendingBatch, base int64) core.BatchRange {
	if !p.seen[b.TopicPartition] {
		p.order = append(p.order, b.TopicPartition)
		p.seen[b.TopicPartition] = true
	}
	r := core.BatchRange{TopicPartition: b.TopicPartition, Base: core.Offset(base), Count: b.Records, Loc: b.Loc}
	if end := base + b.Records; end > p.ends[b.TopicPartition] {
		p.ends[b.TopicPartition] = end
	}
	p.runs[b.TopicPartition] = append(p.runs[b.TopicPartition], r)
	return r
}

func (p *offsetPlan) write(tx Transaction) { writeAssign(tx, p.order, p.ends, p.runs) }

// assign hands out offsets: each batch's Base is its partition's end, in
// request order, the end then growing by Records. It writes the new ends, one
// packed run per partition and grown topics, and returns the ranges.
func assign(tx Transaction, bs []core.PendingBatch) []core.BatchRange {
	p := newOffsetPlan()
	out := make([]core.BatchRange, 0, len(bs))
	for _, b := range bs {
		base := p.ends[b.TopicPartition]
		if !p.seen[b.TopicPartition] {
			base = readVarint(tx.Get(endKey(b.TopicPartition)))
		}
		out = append(out, p.add(b, base))
	}
	p.write(tx)
	return out
}

// assignAt is assign with writer-chosen bases (BugWriterOffsets).
func assignAt(tx Transaction, bs []core.PendingBatch, bases []core.Offset) []core.BatchRange {
	p := newOffsetPlan()
	out := make([]core.BatchRange, 0, len(bs))
	for i, b := range bs {
		out = append(out, p.add(b, int64(bases[i])))
	}
	p.write(tx)
	return out
}

func writeAssign(tx Transaction, order []core.TopicPartition, ends map[core.TopicPartition]int64, runs map[core.TopicPartition][]core.BatchRange) {
	topics := map[string]int32{}
	for _, tp := range order {
		tx.Set(endKey(tp), writeVarint(ends[tp]))
		tx.Set(batchRunKey(tp, int64(runs[tp][0].Base)), encodeBatchRun(runs[tp]))
		addPartitions(tx, topics, tp.Topic, tp.Partition+1)
	}
}

// addPartitions raises topic's partition count to at least n; seen caches the
// counts read in this transaction.
func addPartitions(tx Transaction, seen map[string]int32, topic string, n int32) {
	cur, ok := seen[topic]
	if !ok {
		cur = int32(readVarint(tx.Get(topicKey(topic))))
	}
	if n > cur {
		cur = n
		tx.Set(topicKey(topic), writeVarint(int64(cur)))
	}
	seen[topic] = cur
}

func lookupTransaction(tx Transaction, op core.FindSegments) core.ControlPlaneDone {
	resp := core.FindSegmentsResp{End: core.Offset(readVarint(tx.Get(endKey(op.TopicPartition))))}
	lo := batchRunKey(op.TopicPartition, 0)
	hi := prefixEnd(topicPartitionPrefix(prefixRun, op.TopicPartition))
	if k, _ := tx.Floor(lo, append(batchRunKey(op.TopicPartition, int64(op.From)), 0)); k != nil {
		lo = k // the run holding From starts at or before it
	}
	var size int64
	var bad error
	tx.Ascend(lo, hi, func(k, v []byte) bool {
		rs, err := decodeBatchRun(op.TopicPartition, batchRunBase(k), v)
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
		panic(fmt.Errorf("stateupdater: %v: %w", op.TopicPartition, bad))
	}
	return core.ControlPlaneDone{Resp: resp}
}

func describeTransaction(tx Transaction, op core.DescribeTopics) core.ControlPlaneDone {
	var resp core.DescribeTopicsResp
	lo := []byte{prefixTopic}
	tx.Ascend(lo, prefixEnd(lo), func(k, v []byte) bool {
		name := string(bytes.Clone(k[1:]))
		if len(op.Topics) == 0 || slices.Contains(op.Topics, name) {
			resp.Topics = append(resp.Topics, core.TopicInfo{Name: name, Partitions: int32(readVarint(v))})
		}
		return true
	})
	return core.ControlPlaneDone{Resp: resp}
}
