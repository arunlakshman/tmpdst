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
	loc      core.SegmentRef
	answered bool // BugEarlyAck already answered it
}

// commitItems is what the DynamoDB CP's commit transaction costs for the open
// segment (cp/doc.go): one segment item and one range run item per distinct
// partition. Batches do not count, and grown topics are not counted: they are
// created before use, and the partition cap keeps spare items for them.
func (o *openSegment) commitItems() int { return 1 + len(o.parts) }

// openSegment is the buffer that the next flush seals.
type openSegment struct {
	batches []*pbatch
	bytes   int
	parts   []core.TopicPartition // distinct partitions buffered
	gen     core.TimerGen         // flush timer generation; 0 = none armed
	since   core.Time             // when the first batch arrived (age of the segment)
}

// Why a segment was sealed (the "reason" of the segment_sealed fact).
const (
	sealSize       = "size"
	sealTimer      = "timer"
	sealBatches    = "batches"
	sealPartitions = "partitions"
	sealStop       = "stop"
)

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

	// Timing for facts only (derived from event times, never read by
	// behaviour, so not part of StateHash). The PUT starts at the seal.
	sealedAt  core.Time // when the segment was sealed (its PUT was issued)
	putDoneAt core.Time // when the PUT finally succeeded
	commitAt  core.Time // when the first CommitSegment was issued
	committed bool      // commitAt is set
}

// dataBytes is the produce data of the segment (what bufBytes counts).
func (s *segment) dataBytes() int {
	n := 0
	for _, pb := range s.batches {
		n += len(pb.data)
	}
	return n
}

// addBatch appends an admitted batch to the open segment and seals it at the
// size, partition or batch cap; the first batch arms the flush timer, which
// later batches do not move (the window runs from the first batch).
func (b *Broker) addBatch(pb *pbatch) []core.Action {
	var acts []core.Action
	before := b.open.bytes
	b.open.batches = append(b.open.batches, pb)
	b.open.bytes += len(pb.data)
	b.bufBytes += len(pb.data)
	if !slices.Contains(b.open.parts, pb.tp) {
		b.open.parts = append(b.open.parts, pb.tp)
	}
	if len(b.open.batches) == 1 || b.cfg.Bug == BugTimerFromLastBatch {
		if len(b.open.batches) == 1 {
			b.open.since = b.now
		}
		b.timerGen++
		b.open.gen = b.timerGen
		acts = append(acts, core.SetTimer{ID: timerFlush, Gen: b.timerGen, After: b.cfg.BatchWindow})
	}
	sizeBytes := b.open.bytes
	if b.cfg.Bug == BugSealOffByOne {
		sizeBytes = before
	}
	switch {
	case sizeBytes >= b.cfg.MaxSegmentBytes:
		return append(acts, b.seal(sealSize)...)
	case len(b.open.parts) >= b.cfg.MaxSegmentPartitions && b.cfg.Bug != BugNoPartitionCap:
		return append(acts, b.seal(sealPartitions)...)
	case len(b.open.batches) >= b.cfg.MaxSegmentBatches && b.cfg.Bug != BugNoBatchCap:
		return append(acts, b.seal(sealBatches)...)
	}
	return acts
}

// seal turns the open buffer into a segment and starts its upload.
func (b *Broker) seal(why string) []core.Action {
	if len(b.open.batches) == 0 {
		return nil
	}
	bs := b.open.batches
	switch why {
	case sealSize:
		b.env.Reachable("broker: segment sealed by size")
		if len(bs) == 1 {
			b.env.Reachable("broker: oversize batch sealed alone")
		}
	case sealTimer:
		b.env.Reachable("broker: segment sealed by timer")
	case sealBatches:
		b.env.Reachable("broker: segment sealed by batch cap")
	case sealPartitions:
		b.env.Reachable("broker: segment sealed by partition cap")
	}
	acts := []core.Action{core.CancelTimer{ID: timerFlush}, emit("segment_sealed",
		core.Field{Key: "reason", Value: core.Str(why)},
		core.Field{Key: "bytes", Value: core.I64(b.open.bytes)},
		core.Field{Key: "batches", Value: core.I64(len(bs))},
		core.Field{Key: "parts", Value: core.I64(len(b.open.parts))},
		core.Field{Key: "last", Value: core.I64(len(bs[len(bs)-1].data))},
		core.Field{Key: "age", Value: core.I64(b.now - b.open.since)},
		core.Field{Key: "inflight", Value: core.I64(len(b.segs) + 1)},
		core.Field{Key: "buffered", Value: core.I64(b.bufBytes)})}
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
		key: format.SegmentKey(b.env.NewID()), sealedAt: b.now}
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
	attempts := s.attempts + 1
	s.state, s.req, s.attempts, s.putDoneAt = segPutDone, 0, 0, b.now
	acts := []core.Action{emit("segment_put_done",
		core.Field{Key: "segment", Value: core.Str(s.key)},
		core.Field{Key: "bytes", Value: core.I64(len(s.data))},
		core.Field{Key: "put_ns", Value: core.I64(b.now - s.sealedAt)},
		core.Field{Key: "attempts", Value: core.I64(attempts)})}
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
	if !s.committed {
		s.committed, s.commitAt = true, b.now
	}
	pend := make([]core.PendingBatch, len(s.batches))
	for i, pb := range s.batches {
		pend[i] = core.PendingBatch{TP: pb.tp, Records: pb.records, Loc: pb.loc}
	}
	key := s.key
	if b.cfg.Bug == BugFreshKeyOnRetry && s.attempts > 0 {
		key = format.SegmentKey(b.env.NewID())
	}
	return []core.Action{core.CPCall{Req: s.req, Op: core.CommitSegment{
		SegmentKey: key, Batches: pend}}}
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
	case e.Err == core.ErrInvalid:
		return b.failSegment(s, errUnknownServer)
	case e.Err == core.ErrConflict:
		// The same key with a different request: a broker bug, not a race.
		b.env.Unreachable("broker: commit key reused with a different request")
		return b.failSegment(s, errUnknownServer)
	case e.Err == core.ErrTooLarge:
		// The segment exceeds the CP's commit limit; retrying cannot help.
		b.env.Unreachable("broker: segment exceeds the control plane's commit size")
		return b.failSegment(s, errUnknownServer)
	}
	// MaybeApplied or a transient error: resend the identical request under
	// the same key until definite; a replay returns the same answer.
	return b.retry(s, segCommit)
}

// committed answers every batch of s from the control plane's verdicts.
func (b *Broker) committed(s *segment, rs []core.BatchRange) []core.Action {
	acts := []core.Action{emit("segment_committed",
		core.Field{Key: "segment", Value: core.Str(s.key)},
		core.Field{Key: "batches", Value: core.I64(len(s.batches))},
		// Timing, in nanoseconds: put + wait + commit = total. wait is the time
		// the uploaded segment queued behind earlier segments' commits.
		core.Field{Key: "bytes", Value: core.I64(len(s.data))},
		core.Field{Key: "put_ns", Value: core.I64(s.putDoneAt - s.sealedAt)},
		core.Field{Key: "wait_ns", Value: core.I64(s.commitAt - s.putDoneAt)},
		core.Field{Key: "commit_ns", Value: core.I64(b.now - s.commitAt)},
		core.Field{Key: "total_ns", Value: core.I64(b.now - s.sealedAt)},
		core.Field{Key: "inflight", Value: core.I64(len(b.segs) - 1)},
		core.Field{Key: "buffered", Value: core.I64(b.bufBytes - s.dataBytes())})}
	for i, pb := range s.batches {
		b.ack(pb, rs[i], s.key)
	}
	return append(acts, b.endSegment(s)...)
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
	acts := []core.Action{emit("segment_failed",
		core.Field{Key: "segment", Value: core.Str(s.key)},
		core.Field{Key: "code", Value: core.I64(code)},
		core.Field{Key: "total_ns", Value: core.I64(b.now - s.sealedAt)},
		core.Field{Key: "inflight", Value: core.I64(len(b.segs) - 1)},
		core.Field{Key: "buffered", Value: core.I64(b.bufBytes - s.dataBytes())})}
	for _, pb := range s.batches {
		b.release(pb, code)
	}
	return append(acts, b.endSegment(s)...)
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
