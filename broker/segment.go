package broker

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/format"
)

// pbatch is one admitted partition batch on its way to a segment.
type pbatch struct {
	s        *slot
	part     *kmsg.ProduceResponseTopicPartition
	tp       core.TopicPartition
	data     []byte
	records  int64
	pid      int64           // the client's producer id (-1: none), for facts
	producer core.ProducerID // as committed (BugNoIdempotence zeroes it)
	pepoch   int16
	baseSeq  int32
	loc      core.SegmentRef
	answered bool // BugEarlyAck already answered it
}

// openSegment is the buffer that the next flush seals.
type openSegment struct {
	batches []*pbatch
	bytes   int
	parts   []core.TopicPartition // distinct partitions buffered
	gen     core.TimerGen         // flush timer generation; 0 = none armed
}

type segState uint8

const (
	segPut segState = iota // ObjPut in flight
	segPutBackoff
	segPutDone // uploaded, waiting for its turn to commit
	segCommit  // CommitSegment in flight
	segCommitBackoff
)

// segment is a sealed upload: one object, one CommitSegment.
type segment struct {
	id       uint64
	batches  []*pbatch
	key      string
	data     []byte
	state    segState
	req      core.ReqID
	gen      core.TimerGen
	attempts int
}

// addBatch appends an admitted batch to the open segment and flushes at the
// size limit; the first batch arms the flush timer.
func (b *Broker) addBatch(pb *pbatch) []core.Action {
	var acts []core.Action
	b.open.batches = append(b.open.batches, pb)
	b.open.bytes += len(pb.data)
	b.bufBytes += len(pb.data)
	if !slices.Contains(b.open.parts, pb.tp) {
		b.open.parts = append(b.open.parts, pb.tp)
	}
	if len(b.open.batches) == 1 {
		b.timerGen++
		b.open.gen = b.timerGen
		acts = append(acts, core.SetTimer{ID: timerFlush, Gen: b.timerGen, After: b.cfg.BatchWindow})
	}
	if b.open.bytes >= b.cfg.MaxSegmentBytes || len(b.open.parts) >= b.cfg.MaxSegmentPartitions {
		acts = append(acts, b.seal()...)
	}
	return acts
}

// seal turns the open buffer into a segment and starts its upload.
func (b *Broker) seal() []core.Action {
	if len(b.open.batches) == 0 {
		return nil
	}
	bs := b.open.batches
	acts := []core.Action{core.CancelTimer{ID: timerFlush}}
	b.open = openSegment{}
	return append(acts, b.sealBatches(bs)...)
}

func (b *Broker) sealBatches(bs []*pbatch) []core.Action {
	fb := make([]format.Batch, len(bs))
	for i, pb := range bs {
		fb[i] = format.Batch{TP: pb.tp, Records: uint32(pb.records), Data: pb.data}
	}
	data, entries, err := format.Encode(fb)
	if err != nil {
		b.env.Unreachable("broker: admitted batches cannot be encoded")
		for _, pb := range bs {
			b.release(pb, errUnknownServer)
		}
		return nil
	}
	s := &segment{id: b.id(), batches: bs, data: data,
		key: format.SegmentKey(b.self.Node, b.self.Inc, b.env.NewID())}
	for i, pb := range bs {
		pb.loc = entries[i].Ref(s.key)
	}
	b.segs = append(b.segs, s)
	return b.put(s)
}

func (b *Broker) put(s *segment) []core.Action {
	s.state, s.req = segPut, b.req()
	return []core.Action{core.ObjPut{Req: s.req, Key: s.key, Data: s.data}}
}

func (b *Broker) onPutDone(s *segment, e core.ObjPutDone) []core.Action {
	if e.Err == core.ErrInvalid {
		return b.failSegment(s, errUnknownServer)
	}
	if e.Err != core.OK {
		return b.retry(s, segPut)
	}
	s.state, s.req, s.attempts = segPutDone, 0, 0
	var acts []core.Action
	if b.cfg.Bug == BugEarlyAck {
		for _, pb := range s.batches {
			b.guess += uint64(pb.records)
			b.ack(pb, core.BatchRange{TP: pb.tp, Base: core.Offset(b.guess) - core.Offset(pb.records), Count: pb.records}, s.key)
			pb.answered = true
		}
	}
	return append(acts, b.pump()...)
}

// pump starts the commit of the head segment once it is uploaded. Only the
// head commits, so segments reach the control plane in seal order.
func (b *Broker) pump() []core.Action {
	if len(b.segs) == 0 || b.segs[0].state != segPutDone {
		return nil
	}
	return b.commit(b.segs[0])
}

func (b *Broker) commit(s *segment) []core.Action {
	s.state, s.req = segCommit, b.req()
	pend := make([]core.PendingBatch, len(s.batches))
	for i, pb := range s.batches {
		pend[i] = core.PendingBatch{TP: pb.tp, Records: pb.records, Loc: pb.loc,
			Producer: pb.producer, PEpoch: pb.pepoch, BaseSeq: pb.baseSeq}
	}
	return []core.Action{core.CPCall{Req: s.req, Op: core.CommitSegment{
		SegmentKey: s.key, Owner: b.self, Epoch: b.epoch, Batches: pend}}}
}

func (b *Broker) onCommit(s *segment, e core.CPDone) []core.Action {
	switch {
	case e.Err == core.OK:
		resp, ok := e.Resp.(core.CommitSegmentResp)
		shape := ok && len(resp.Assigned) == len(s.batches)
		for i := 0; shape && i < len(s.batches); i++ {
			shape = resp.Assigned[i].TP == s.batches[i].tp
		}
		b.env.AlwaysOrUnreachable(shape, "broker: commit answers every batch in order")
		if !shape {
			return b.failSegment(s, errUnknownServer)
		}
		return b.committed(s, resp.Assigned)
	case e.Err == core.ErrFenced:
		return b.trip("fenced")
	case e.Err == core.ErrInvalid:
		return b.isolate(s)
	case e.Err == core.ErrConflict:
		return b.failSegment(s, errUnknownServer)
	}
	return b.retry(s, segCommit) // idempotent per SegmentKey (decision 3)
}

// committed answers every batch of s from the control plane's verdicts.
func (b *Broker) committed(s *segment, rs []core.BatchRange) []core.Action {
	acts := []core.Action{emit("segment_committed",
		core.Field{Key: "segment", Value: core.Str(s.key)},
		core.Field{Key: "batches", Value: core.I64(len(s.batches))},
		core.Field{Key: "epoch", Value: core.U64(b.epoch)})}
	for i, pb := range s.batches {
		r := rs[i]
		switch r.Err {
		case core.OK:
			// Dup is a success at the original offset (decision 28).
			b.ack(pb, r, s.key)
		case core.ErrOutOfOrder:
			b.release(pb, errOutOfOrderSeq)
		case core.ErrFenced:
			b.release(pb, errInvalidProducerEpoch)
		default:
			b.release(pb, errUnknownServer)
		}
	}
	return append(acts, b.endSegment(s)...)
}

// isolate handles a commit the control plane rejected as invalid (a batch with
// a producer id it never issued): it splits the segment so only the offender
// fails.
func (b *Broker) isolate(s *segment) []core.Action {
	if len(s.batches) == 1 {
		pb := s.batches[0]
		code := errUnknownServer
		if pb.producer != core.NoProducer {
			code = errInvalidPIDMapping
		}
		return b.failSegment(s, code)
	}
	b.remove(s)
	var acts []core.Action
	for _, pb := range s.batches {
		acts = append(acts, b.sealBatches([]*pbatch{pb})...)
	}
	return append(acts, b.pump()...)
}

func (b *Broker) remove(s *segment) {
	for i, x := range b.segs {
		if x == s {
			b.segs = append(b.segs[:i:i], b.segs[i+1:]...)
			return
		}
	}
}

// endSegment retires s and starts the next commit.
func (b *Broker) endSegment(s *segment) []core.Action {
	b.remove(s)
	for _, pb := range s.batches {
		b.bufBytes -= len(pb.data)
	}
	return b.pump()
}

// failSegment answers every unanswered batch of s with code.
func (b *Broker) failSegment(s *segment, code int16) []core.Action {
	for _, pb := range s.batches {
		b.release(pb, code)
	}
	return b.endSegment(s)
}

// retry backs off and repeats the PUT or commit under the same key.
func (b *Broker) retry(s *segment, from segState) []core.Action {
	if s.attempts++; s.attempts >= b.cfg.MaxAttempts {
		// The write may still land later: unbounded MaybeApplied (decision 15).
		b.env.Reachable("broker: segment retries exhausted")
		return b.failSegment(s, errRequestTimedOut)
	}
	s.state, s.req = segPutBackoff, 0
	if from == segCommit {
		s.state = segCommitBackoff
	}
	t, g := b.timer(core.TimerID(s.id), s.attempts)
	s.gen = g
	return []core.Action{t}
}

func (b *Broker) segTimer(s *segment, gen core.TimerGen) []core.Action {
	if s.gen != gen {
		return nil
	}
	switch s.state {
	case segPutBackoff:
		return b.put(s)
	case segCommitBackoff:
		b.env.Reachable("broker: commit retried")
		return b.commit(s)
	}
	return nil
}

// ack records a success for pb at range r: the response entry, and the
// "produce_acked" fact that drain emits right before the Send (decision 12).
func (b *Broker) ack(pb *pbatch, r core.BatchRange, key string) {
	if pb.answered {
		return
	}
	pb.part.ErrorCode, pb.part.BaseOffset = errNone, int64(r.Base)
	pb.s.facts = append(pb.s.facts, core.Fact{Name: "produce_acked", Fields: []core.Field{
		{Key: "topic", Value: core.Str(pb.tp.Topic)},
		{Key: "partition", Value: core.I64(pb.tp.Partition)},
		{Key: "base_offset", Value: core.I64(r.Base)},
		{Key: "count", Value: core.I64(pb.records)},
		{Key: "producer", Value: core.I64(pb.pid)},
		{Key: "pepoch", Value: core.I64(pb.pepoch)},
		{Key: "base_seq", Value: core.I64(pb.baseSeq)},
		{Key: "dup", Value: core.Bool(r.Dup)},
		{Key: "segment", Value: core.Str(key)},
	}})
	b.finishPart(pb.s)
}

// release answers pb with an error code (unless already answered).
func (b *Broker) release(pb *pbatch, code int16) {
	if pb.answered {
		return
	}
	pb.part.ErrorCode, pb.part.BaseOffset = code, -1
	b.finishPart(pb.s)
}

// trip is the fail-stop (decision 7): nothing may be acked after this.
func (b *Broker) trip(cause string) []core.Action {
	b.env.Reachable("broker: tripped on fencing")
	b.tripped = true
	return []core.Action{
		emit("broker_fenced", core.Field{Key: "epoch", Value: core.U64(b.epoch)}),
		core.Trip{Cause: cause},
	}
}
