package veritas

import (
	"slices"

	"manifester/core"
)

// ControlPlane is the spec of the control plane as a broker sees it
// (core.CPOp). Sequential, fault-free; Outcome is Applied for a state change
// and NotApplied for errors and reads. Rules:
//
//	Log            A partition's log is a list of runs (core.BatchRange), each
//	               [Base, Base+Count). Its end offset is the newest run's
//	               Base+Count, 0 when empty. There is no separate counter.
//	CommitSegment  1. SegmentKey already committed: the original ranges again
//	                  (OK, Applied); ErrConflict if the request differs.
//	               2. Empty key or batches, Records <= 0, empty topic or
//	                  negative partition: ErrInvalid, no change.
//	               3. Else every batch gets Base = its partition's end offset
//	                  (newest run's Base+Count, 0 if empty), in request order,
//	                  the end then growing by Records: offsets are contiguous
//	                  per partition, all partitions are assigned in one step,
//	                  all or nothing. Topics are created implicitly and grow
//	                  to cover the partition.
//	               A failed commit records nothing: its key is reusable.
//	               Real stores guard step 3 with a compare-and-set on the run
//	               base and retry internally; the spec is sequential.
//	Lookup         From < 0: ErrInvalid. Else the runs of TP whose last offset
//	               is >= From, in offset order, plus the end offset. MaxBytes > 0
//	               stops after the batch that takes the summed Loc.Len to
//	               MaxBytes or more, but always returns one if any match. An
//	               unknown partition is empty, End 0.
//	DescribeTopics sorted by name; empty request = all; unknown names are
//	               omitted.
//
// Topics can also be made with CreateTopic. Exec panics on a non-CPCall.
type ControlPlane struct {
	segs   map[string]segment
	parts  map[string]map[int32][]core.BatchRange // topic -> partition -> runs
	topics map[string]int32                       // topic -> partition count

	// segKeys is every committed segment key, sorted. segPre[i] is the
	// StateHash state after segKeys[:i]; segPre[0] is the empty state. A
	// commit at index i truncates it to i+1 entries.
	segKeys []string
	segPre  []hasher
}

type segment struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

// NewControlPlane returns an empty control plane.
func NewControlPlane() *ControlPlane {
	return &ControlPlane{
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

// End is the next offset of tp: the end of its newest run (0 if unknown).
func (c *ControlPlane) End(tp core.TopicPartition) core.Offset {
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
	switch op := call.Op.(type) {
	case core.CommitSegment:
		return c.execCommit(call.Req, op)
	case core.Lookup:
		return c.execLookup(call.Req, op)
	case core.DescribeTopics:
		return c.execDescribe(call.Req, op)
	}
	return core.CPDone{Req: call.Req, Err: core.ErrInvalid}
}

// validBatch reports whether b is well formed on its own.
func validBatch(b core.PendingBatch) bool {
	return b.Records > 0 && b.TP.Topic != "" && b.TP.Partition >= 0
}

func (c *ControlPlane) execCommit(req core.ReqID, op core.CommitSegment) core.Completion {
	fail := func(e core.ErrCode) core.Completion { return core.CPDone{Req: req, Err: e} }
	if seg, done := c.segs[op.SegmentKey]; done {
		if !slices.Equal(seg.batches, op.Batches) {
			return fail(core.ErrConflict)
		}
		resp := core.CommitSegmentResp{Assigned: slices.Clone(seg.ranges)}
		return core.CPDone{Req: req, Resp: resp, Outcome: core.Applied}
	}
	if op.SegmentKey == "" || len(op.Batches) == 0 {
		return fail(core.ErrInvalid)
	}
	for _, b := range op.Batches {
		if !validBatch(b) {
			return fail(core.ErrInvalid)
		}
	}
	seg := segment{batches: slices.Clone(op.Batches)}
	for _, b := range op.Batches {
		r := core.BatchRange{TP: b.TP, Base: c.End(b.TP), Count: b.Records, Loc: b.Loc}
		c.append(r)
		seg.ranges = append(seg.ranges, r)
	}
	i, _ := slices.BinarySearch(c.segKeys, op.SegmentKey)
	c.segKeys = slices.Insert(c.segKeys, i, op.SegmentKey)
	if len(c.segPre) > i+1 {
		c.segPre = c.segPre[:i+1]
	}
	c.segs[op.SegmentKey] = seg
	resp := core.CommitSegmentResp{Assigned: slices.Clone(seg.ranges)}
	return core.CPDone{Req: req, Resp: resp, Outcome: core.Applied}
}

func (c *ControlPlane) execLookup(req core.ReqID, op core.Lookup) core.Completion {
	if op.From < 0 {
		return core.CPDone{Req: req, Err: core.ErrInvalid}
	}
	resp := core.LookupResp{End: c.End(op.TP)}
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
	return core.CPDone{Req: req, Resp: resp}
}

func (c *ControlPlane) execDescribe(req core.ReqID, op core.DescribeTopics) core.Completion {
	var resp core.DescribeTopicsResp
	for _, name := range sortedKeys(c.topics) {
		if len(op.Topics) == 0 || slices.Contains(op.Topics, name) {
			resp.Topics = append(resp.Topics, core.TopicInfo{Name: name, Partitions: c.topics[name]})
		}
	}
	return core.CPDone{Req: req, Resp: resp}
}

func (c *ControlPlane) append(r core.BatchRange) {
	if c.parts[r.TP.Topic] == nil {
		c.parts[r.TP.Topic] = map[int32][]core.BatchRange{}
	}
	c.parts[r.TP.Topic][r.TP.Partition] = append(c.parts[r.TP.Topic][r.TP.Partition], r)
	c.topics[r.TP.Topic] = max(c.topics[r.TP.Topic], r.TP.Partition+1)
}

// StateHash folds committed segments (sorted by key, with their ranges) and
// topics (sorted by name).
func (c *ControlPlane) StateHash() uint64 {
	if len(c.segPre) == 0 {
		c.segPre = append(c.segPre, newHasher())
	}
	for i := len(c.segPre) - 1; i < len(c.segKeys); i++ {
		h := c.segPre[i]
		h.str(c.segKeys[i])
		h.ranges(c.segs[c.segKeys[i]].ranges)
		c.segPre = append(c.segPre, h)
	}
	return c.tailHash(c.segPre[len(c.segKeys)])
}

// fullStateHash is StateHash without the segment prefix cache (for tests).
func (c *ControlPlane) fullStateHash() uint64 {
	h := newHasher()
	for _, k := range sortedKeys(c.segs) {
		h.str(k)
		h.ranges(c.segs[k].ranges)
	}
	return c.tailHash(h)
}

// tailHash folds topics into h.
func (c *ControlPlane) tailHash(h hasher) uint64 {
	for _, t := range sortedKeys(c.topics) {
		h.str(t)
		h.u64(uint64(c.topics[t]))
	}
	return uint64(h)
}
