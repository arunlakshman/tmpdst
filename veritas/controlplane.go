package veritas

import (
	"slices"

	"manifester/core"
)

// ControlPlane is the spec of the control plane as a broker sees it
// (core.CPOp). Sequential, fault-free; Outcome is Applied for a state change
// and NotApplied for errors and reads. Rules:
//
//	Register       epoch for the node = previous + 1, from 1: Epoch is
//	               monotonic per NodeID, whatever the incarnation.
//	CommitSegment  1. SegmentKey already committed: the original ranges again
//	                  (OK, Applied), whatever Epoch now says; ErrConflict if
//	                  the batches differ from the original commit.
//	               2. Empty key or batches, Records <= 0, empty topic or
//	                  negative partition: ErrInvalid, no change.
//	               3. Epoch != the owner node's current epoch, or the node
//	                  never registered: ErrFenced, no change.
//	               4. Else every batch gets Base = the partition's high
//	                  watermark, in order, which then grows by Records:
//	                  offsets are contiguous per partition, and all
//	                  partitions are assigned in one step. Topics are created
//	                  implicitly and grow to cover the partition.
//	Lookup         From < 0: ErrInvalid. Else the batches of TP whose last
//	               offset is >= From, in offset order, plus the high
//	               watermark. MaxBytes > 0 stops after the batch that takes
//	               the summed Loc.Len to MaxBytes or more, but always returns
//	               one if any match. An unknown partition is empty, HWM 0.
//	DescribeTopics sorted by name; empty request = all; unknown names are
//	               omitted.
//
// Topics can also be made with CreateTopic. Exec panics on a non-CPCall.
type ControlPlane struct {
	epochs map[string]core.Epoch
	segs   map[string]segment
	parts  map[string]map[int32][]core.BatchRange // topic -> partition -> ranges
	topics map[string]int32                       // topic -> partition count
}

type segment struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

// NewControlPlane returns an empty control plane.
func NewControlPlane() *ControlPlane {
	return &ControlPlane{
		epochs: map[string]core.Epoch{},
		segs:   map[string]segment{},
		parts:  map[string]map[int32][]core.BatchRange{},
		topics: map[string]int32{},
	}
}

// CreateTopic creates name with at least partitions partitions (it never
// shrinks one). ErrInvalid for an empty name or partitions <= 0.
func (c *ControlPlane) CreateTopic(name string, partitions int32) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	c.topics[name] = max(c.topics[name], partitions)
	return core.OK
}

// HighWatermark is the next offset of tp (0 if unknown).
func (c *ControlPlane) HighWatermark(tp core.TopicPartition) core.Offset {
	rs := c.parts[tp.Topic][tp.Partition]
	if len(rs) == 0 {
		return 0
	}
	last := rs[len(rs)-1]
	return last.Base + core.Offset(last.Count)
}

// Exec runs one CPCall and returns its CPDone.
func (c *ControlPlane) Exec(a core.IOAction) core.Completion {
	call, ok := a.(core.CPCall)
	if !ok {
		panic("veritas.ControlPlane: not a CPCall")
	}
	fail := func(e core.ErrCode) core.Completion { return core.CPDone{Req: call.Req, Err: e} }
	switch op := call.Op.(type) {
	case core.Register:
		c.epochs[string(op.Self.Node)]++
		resp := core.RegisterResp{Epoch: c.epochs[string(op.Self.Node)]}
		return core.CPDone{Req: call.Req, Resp: resp, Outcome: core.Applied}
	case core.CommitSegment:
		if seg, done := c.segs[op.SegmentKey]; done {
			if !slices.Equal(seg.batches, op.Batches) {
				return fail(core.ErrConflict)
			}
			resp := core.CommitSegmentResp{Assigned: slices.Clone(seg.ranges)}
			return core.CPDone{Req: call.Req, Resp: resp, Outcome: core.Applied}
		}
		if op.SegmentKey == "" || len(op.Batches) == 0 {
			return fail(core.ErrInvalid)
		}
		for _, b := range op.Batches {
			if b.Records <= 0 || b.TP.Topic == "" || b.TP.Partition < 0 {
				return fail(core.ErrInvalid)
			}
		}
		if cur, ok := c.epochs[string(op.Owner.Node)]; !ok || op.Epoch != cur {
			return fail(core.ErrFenced)
		}
		seg := segment{batches: slices.Clone(op.Batches)}
		for _, b := range op.Batches {
			r := core.BatchRange{TP: b.TP, Base: c.HighWatermark(b.TP), Count: b.Records, Loc: b.Loc}
			if c.parts[b.TP.Topic] == nil {
				c.parts[b.TP.Topic] = map[int32][]core.BatchRange{}
			}
			c.parts[b.TP.Topic][b.TP.Partition] = append(c.parts[b.TP.Topic][b.TP.Partition], r)
			c.topics[b.TP.Topic] = max(c.topics[b.TP.Topic], b.TP.Partition+1)
			seg.ranges = append(seg.ranges, r)
		}
		c.segs[op.SegmentKey] = seg
		resp := core.CommitSegmentResp{Assigned: slices.Clone(seg.ranges)}
		return core.CPDone{Req: call.Req, Resp: resp, Outcome: core.Applied}
	case core.Lookup:
		if op.From < 0 {
			return fail(core.ErrInvalid)
		}
		resp := core.LookupResp{HighWatermark: c.HighWatermark(op.TP)}
		var bytes int64
		for _, r := range c.parts[op.TP.Topic][op.TP.Partition] {
			if r.Base+core.Offset(r.Count) <= op.From {
				continue
			}
			resp.Ranges = append(resp.Ranges, r)
			bytes += r.Loc.Len
			if op.MaxBytes > 0 && bytes >= int64(op.MaxBytes) {
				break
			}
		}
		return core.CPDone{Req: call.Req, Resp: resp}
	case core.DescribeTopics:
		var resp core.DescribeTopicsResp
		for _, name := range sortedKeys(c.topics) {
			if len(op.Topics) == 0 || slices.Contains(op.Topics, name) {
				resp.Topics = append(resp.Topics, core.TopicInfo{Name: name, Partitions: c.topics[name]})
			}
		}
		return core.CPDone{Req: call.Req, Resp: resp}
	}
	return fail(core.ErrInvalid)
}

// StateHash folds epochs, committed segments and topics, each in sorted order.
func (c *ControlPlane) StateHash() uint64 {
	h := newHasher()
	for _, n := range sortedKeys(c.epochs) {
		h.str(n)
		h.u64(uint64(c.epochs[n]))
	}
	for _, k := range sortedKeys(c.segs) {
		h.str(k)
		h.ranges(c.segs[k].ranges)
	}
	for _, t := range sortedKeys(c.topics) {
		h.str(t)
		h.u64(uint64(c.topics[t]))
	}
	return uint64(h)
}
