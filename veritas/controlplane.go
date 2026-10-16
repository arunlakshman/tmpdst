package veritas

import (
	"slices"
	"strconv"

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
//	InitProducer   ProducerID = the next one, from 1000, +1 per call; PEpoch 0.
//	               Applied, never fails.
//	Idempotence    Within step 4 of CommitSegment, per batch, in request order
//	               (an earlier batch's effect is seen by a later one):
//	               - Producer == NoProducer: assigned as above, Err OK, Dup
//	                 false. Otherwise a Producer InitProducer never returned,
//	                 or PEpoch < 0 or BaseSeq < 0, is ErrInvalid in step 2.
//	               - State is per (TP, Producer): PEpoch, the next expected
//	                 sequence, and the last 5 assigned batches (BaseSeq,
//	                 Records, range). Sequences are int64 here: int32
//	                 wraparound is ignored.
//	               - No state: BaseSeq != 0 is ErrOutOfOrder.
//	               - PEpoch < state's: ErrFenced. PEpoch > state's: BaseSeq != 0
//	                 is ErrOutOfOrder, else the state restarts at the new PEpoch.
//	               - Same PEpoch: a window entry with this BaseSeq answers Dup
//	                 (Err OK, that entry's Base/Count/Loc, no change) if its
//	                 Records match, else ErrOutOfOrder. Else BaseSeq == next
//	                 expected is assigned, enters the window, and advances next
//	                 by Records; anything else is ErrOutOfOrder.
//	               - A rejected batch has Base -1, Count = Records, Loc as
//	                 requested, and changes nothing. A segment of only dups and
//	                 rejects still counts as committed (Applied).
//	Lookup         From < 0: ErrInvalid. Else the assigned batches of TP
//	               (never dups or rejects) whose last offset is >= From, in
//	               offset order, plus the high watermark. MaxBytes > 0 stops after the batch that takes
//	               the summed Loc.Len to MaxBytes or more, but always returns
//	               one if any match. An unknown partition is empty, HWM 0.
//	DescribeTopics sorted by name; empty request = all; unknown names are
//	               omitted.
//
// Topics can also be made with CreateTopic. Exec panics on a non-CPCall.
type ControlPlane struct {
	epochs  map[string]core.Epoch
	segs    map[string]segment
	parts   map[string]map[int32][]core.BatchRange // topic -> partition -> ranges
	topics  map[string]int32                       // topic -> partition count
	nextPID core.ProducerID
	prods   map[string]*prodState // pkey(tp, producer) -> state
}

const (
	firstProducer = 1000
	dupWindow     = 5
)

type prodState struct {
	pepoch int16
	next   int64
	window []winEntry // oldest first
}

type winEntry struct {
	seq     int64
	records int64
	r       core.BatchRange
}

func pkey(tp core.TopicPartition, p core.ProducerID) string {
	return tp.Topic + "\x00" + strconv.Itoa(int(tp.Partition)) + "\x00" + strconv.FormatInt(int64(p), 10)
}

type segment struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

// NewControlPlane returns an empty control plane.
func NewControlPlane() *ControlPlane {
	return &ControlPlane{
		epochs:  map[string]core.Epoch{},
		segs:    map[string]segment{},
		parts:   map[string]map[int32][]core.BatchRange{},
		topics:  map[string]int32{},
		nextPID: firstProducer,
		prods:   map[string]*prodState{},
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
	case core.InitProducer:
		resp := core.InitProducerResp{Producer: c.nextPID}
		c.nextPID++
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
			if b.Producer != core.NoProducer && (b.Producer < firstProducer || b.Producer >= c.nextPID || b.PEpoch < 0 || b.BaseSeq < 0) {
				return fail(core.ErrInvalid)
			}
		}
		if cur, ok := c.epochs[string(op.Owner.Node)]; !ok || op.Epoch != cur {
			return fail(core.ErrFenced)
		}
		seg := segment{batches: slices.Clone(op.Batches)}
		for _, b := range op.Batches {
			r, assigned := c.idempotent(b)
			if assigned {
				c.append(r)
			}
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

// idempotent decides one batch against the producer state and updates it. It
// reports whether the returned range is a new assignment to append.
func (c *ControlPlane) idempotent(b core.PendingBatch) (core.BatchRange, bool) {
	assign := func() core.BatchRange {
		return core.BatchRange{TP: b.TP, Base: c.HighWatermark(b.TP), Count: b.Records, Loc: b.Loc}
	}
	if b.Producer == core.NoProducer {
		return assign(), true
	}
	rej := func(e core.ErrCode) (core.BatchRange, bool) {
		return core.BatchRange{TP: b.TP, Base: -1, Count: b.Records, Loc: b.Loc, Err: e}, false
	}
	key := pkey(b.TP, b.Producer)
	st := c.prods[key]
	seq := int64(b.BaseSeq)
	switch {
	case st == nil || b.PEpoch > st.pepoch:
		if seq != 0 {
			return rej(core.ErrOutOfOrder)
		}
		r := assign()
		c.prods[key] = &prodState{pepoch: b.PEpoch, next: b.Records, window: []winEntry{{seq, b.Records, r}}}
		return r, true
	case b.PEpoch < st.pepoch:
		return rej(core.ErrFenced)
	}
	for _, w := range st.window {
		if w.seq == seq {
			if w.records != b.Records {
				return rej(core.ErrOutOfOrder)
			}
			r := w.r
			r.Dup = true
			return r, false
		}
	}
	if seq != st.next {
		return rej(core.ErrOutOfOrder)
	}
	r := assign()
	st.window = append(st.window, winEntry{seq, b.Records, r})
	if len(st.window) > dupWindow {
		st.window = slices.Delete(st.window, 0, 1)
	}
	st.next += b.Records
	return r, true
}

func (c *ControlPlane) append(r core.BatchRange) {
	if c.parts[r.TP.Topic] == nil {
		c.parts[r.TP.Topic] = map[int32][]core.BatchRange{}
	}
	c.parts[r.TP.Topic][r.TP.Partition] = append(c.parts[r.TP.Topic][r.TP.Partition], r)
	c.topics[r.TP.Topic] = max(c.topics[r.TP.Topic], r.TP.Partition+1)
}

// StateHash folds epochs, committed segments, topics and producer state, each in sorted order.
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
	h.u64(uint64(c.nextPID))
	for _, k := range sortedKeys(c.prods) {
		st := c.prods[k]
		h.str(k)
		h.u64(uint64(st.pepoch))
		h.u64(uint64(st.next))
		h.u64(uint64(len(st.window)))
		for _, w := range st.window {
			h.u64(uint64(w.seq))
			h.u64(uint64(w.records))
			h.ranges([]core.BatchRange{w.r})
		}
	}
	return uint64(h)
}
