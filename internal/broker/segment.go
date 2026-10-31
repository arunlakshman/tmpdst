package broker

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
	"manifester/internal/wire/segmentformat"
)

// partitionBatch is one admitted partition batch on its way to a segment.
type partitionBatch struct {
	s        *slot
	part     *kmsg.ProduceResponseTopicPartition
	tp       core.TopicPartition
	data     []byte
	records  int64
	loc      core.SegmentRef
	answered bool // answerPart already ran (early under InjectedBugEarlyAck)
}

// openSegment is the buffer that the next flush seals.
type openSegment struct {
	batches []*partitionBatch
	bytes   int
	parts   []core.TopicPartition // distinct partitions buffered
	// Flush timer generation matched by onTimer. 0 means none is armed, an
	// empty buffer or one just sealed.
	gen core.TimerVersion
	// Time of the first batch, for the segment_sealed age fact and
	// SegmentCaps. Not used to schedule. Does not move when
	// InjectedBugTimerFromLastBatch re-arms gen.
	since core.NodeTime
}

// Why a segment was sealed (the "reason" of the segment_sealed fact).
const (
	closeOnSize       = "size"
	closeOnTimer      = "timer"
	closeOnBatches    = "batches"
	closeOnPartitions = "partitions"
	closeOnStop       = "stop"
)

type segmentState uint8

const (
	segmentUpload segmentState = iota // ObjectPut in flight
	segmentUploadRetry
	segmentUploadDone // uploaded, waiting for its turn to commit
	segmentCommit     // CommitSegment in flight
	segmentCommitRetry
)

// segment is a sealed upload: one object, one CommitSegment.
type segment struct {
	id       uint64
	batches  []*partitionBatch
	key      string
	data     []byte
	state    segmentState
	request  core.RequestID
	gen      core.TimerVersion
	attempts int

	// sealedAt and putDoneAt are facts only, and StateHash omits them.
	// The PUT starts at the seal. commitAt is the time of the first
	// CommitSegment, and commitStarted keeps that time fixed.
	// retryCommit reads commitAt for the deadline. StateHash omits both.
	sealedAt      core.NodeTime
	putDoneAt     core.NodeTime
	commitAt      core.NodeTime
	commitStarted bool
}

// dataBytes is the produce data of the segment (what bufBytes counts).
func (s *segment) dataBytes() int {
	n := 0
	for _, pb := range s.batches {
		n += len(pb.data)
	}
	return n
}

// addBatch appends an admitted batch. The first batch arms a fixed
// BatchWindow timer per decision 35, and a cap may seal earlier.
func (b *Broker) addBatch(pb *partitionBatch) []core.Action {
	var acts []core.Action
	before := b.open.bytes
	b.open.batches = append(b.open.batches, pb)
	b.open.bytes += len(pb.data)
	b.bufBytes += len(pb.data)
	if !slices.Contains(b.open.parts, pb.tp) {
		b.open.parts = append(b.open.parts, pb.tp)
	}
	first := len(b.open.batches) == 1
	// since must not move when the mutant re-arms the timer, or the age
	// invariant cannot catch it.
	if first {
		b.open.since = b.now
	}
	if first || b.cfg.InjectedBug == InjectedBugTimerFromLastBatch {
		b.timerGen++
		b.open.gen = b.timerGen
		acts = append(acts, core.SetTimer{ID: timerFlush, Gen: b.timerGen, After: b.cfg.BatchWindow})
	}
	sizeBytes := b.open.bytes
	if b.cfg.InjectedBug == InjectedBugSealOffByOne {
		sizeBytes = before
	}
	switch {
	case sizeBytes >= b.cfg.MaxSegmentBytes:
		return append(acts, b.seal(closeOnSize)...)
	case len(b.open.parts) >= b.cfg.MaxSegmentPartitions && b.cfg.InjectedBug != InjectedBugNoPartitionCap:
		return append(acts, b.seal(closeOnPartitions)...)
	case len(b.open.batches) >= b.cfg.MaxSegmentBatches && b.cfg.InjectedBug != InjectedBugNoBatchCap:
		return append(acts, b.seal(closeOnBatches)...)
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
	case closeOnSize:
		b.env.Reachable("broker: segment sealed by size")
		if len(bs) == 1 {
			b.env.Reachable("broker: oversize batch sealed alone")
		}
	case closeOnTimer:
		b.env.Reachable("broker: segment sealed by timer")
	case closeOnBatches:
		b.env.Reachable("broker: segment sealed by batch cap")
	case closeOnPartitions:
		b.env.Reachable("broker: segment sealed by partition cap")
	}
	acts := []core.Action{core.CancelTimer{ID: timerFlush}, recordFact("segment_sealed",
		core.Field{Key: "reason", Value: core.StringValue(why)},
		core.Field{Key: "bytes", Value: core.Int64Value(b.open.bytes)},
		core.Field{Key: "batches", Value: core.Int64Value(len(bs))},
		core.Field{Key: "parts", Value: core.Int64Value(len(b.open.parts))},
		core.Field{Key: "last", Value: core.Int64Value(len(bs[len(bs)-1].data))},
		core.Field{Key: "age", Value: core.Int64Value(b.now - b.open.since)},
		core.Field{Key: "inflight", Value: core.Int64Value(len(b.segs) + 1)},
		core.Field{Key: "buffered", Value: core.Int64Value(b.bufBytes)})}
	b.open = openSegment{}
	return append(acts, b.sealBatches(bs)...)
}

func (b *Broker) sealBatches(bs []*partitionBatch) []core.Action {
	fb := make([]segmentformat.Batch, len(bs))
	for i, pb := range bs {
		fb[i] = segmentformat.Batch{TopicPartition: pb.tp, Records: uint32(pb.records), Data: pb.data}
	}
	data, entries, err := segmentformat.Encode(fb)
	if err != nil {
		b.env.Unreachable("broker: admitted batches cannot be encoded")
		for _, pb := range bs {
			b.answerPart(pb, errUnknownServer, core.BatchRange{}, "")
		}
		return nil
	}
	s := &segment{id: b.id(), batches: bs, data: data,
		key: segmentformat.SegmentKey(b.env.NewID()), sealedAt: b.now}
	for i, pb := range bs {
		pb.loc = entries[i].Ref(s.key)
	}
	b.segs = append(b.segs, s)
	return b.put(s)
}

func (b *Broker) put(s *segment) []core.Action {
	s.state, s.request = segmentUpload, b.nextRequestID()
	return []core.Action{core.ObjectPut{Req: s.request, Key: s.key, Data: s.data}}
}

func (b *Broker) onPutDone(s *segment, e core.ObjectPutDone) []core.Action {
	if e.Err == core.ErrInvalid {
		return b.failSegment(s, errUnknownServer)
	}
	if e.Err != core.OK {
		return b.retryPut(s)
	}
	attempts := s.attempts + 1
	s.state, s.request, s.attempts, s.putDoneAt = segmentUploadDone, 0, 0, b.now
	acts := []core.Action{recordFact("segment_put_done",
		core.Field{Key: "segment", Value: core.StringValue(s.key)},
		core.Field{Key: "bytes", Value: core.Int64Value(len(s.data))},
		core.Field{Key: "put_ns", Value: core.Int64Value(b.now - s.sealedAt)},
		core.Field{Key: "attempts", Value: core.Int64Value(attempts)})}
	if b.cfg.InjectedBug == InjectedBugEarlyAck {
		for _, pb := range s.batches {
			b.guess += uint64(pb.records)
			b.answerPart(pb, errNone, core.BatchRange{TopicPartition: pb.tp, Base: core.Offset(b.guess) - core.Offset(pb.records), Count: pb.records}, s.key)
		}
	}
	return append(acts, b.pump()...)
}

// pump starts the commit of the head segment once it is uploaded. Only the
// head commits, so segments reach the control plane in seal order.
func (b *Broker) pump() []core.Action {
	if len(b.segs) == 0 || b.segs[0].state != segmentUploadDone {
		return nil
	}
	return b.commit(b.segs[0])
}

func (b *Broker) commit(s *segment) []core.Action {
	s.state, s.request = segmentCommit, b.nextRequestID()
	if !s.commitStarted {
		s.commitStarted, s.commitAt = true, b.now
	}
	pend := make([]core.PendingBatch, len(s.batches))
	for i, pb := range s.batches {
		pend[i] = core.PendingBatch{TopicPartition: pb.tp, Records: pb.records, Loc: pb.loc}
	}
	key := s.key
	if b.cfg.InjectedBug == InjectedBugFreshKeyOnRetry && s.attempts > 0 {
		key = segmentformat.SegmentKey(b.env.NewID())
	}
	return []core.Action{core.ControlPlaneCall{Req: s.request, Op: core.CommitSegment{
		SegmentKey: key, Batches: pend}}}
}

func (b *Broker) onCommit(s *segment, e core.ControlPlaneDone) []core.Action {
	switch e.Err {
	case core.OK:
		resp, ok := e.Resp.(core.CommitSegmentResp)
		shape := ok && len(resp.Assigned) == len(s.batches)
		for i := 0; shape && i < len(s.batches); i++ {
			shape = resp.Assigned[i].TopicPartition == s.batches[i].tp
		}
		b.env.AlwaysOrUnreachable(shape, "broker: commit answers every batch in order")
		if !shape {
			return b.failSegment(s, errUnknownServer)
		}
		if s.attempts >= b.cfg.MaxAttempts {
			// Past the old six-attempt budget (audit item 8): the outage was
			// ridden out instead of failing the segment.
			b.env.Reachable("broker: commit succeeded after the old retry budget")
		}
		return b.committed(s, resp.Assigned)
	case core.ErrInvalid:
		return b.failSegment(s, errUnknownServer)
	case core.ErrConflict:
		// The same key with a different request: a broker bug, not a race.
		b.env.Unreachable("broker: commit key reused with a different request")
		return b.failSegment(s, errUnknownServer)
	case core.ErrTooLarge:
		// The segment exceeds the CP's commit limit; retrying cannot help.
		b.env.Unreachable("broker: segment exceeds the control plane's commit size")
		return b.failSegment(s, errUnknownServer)
	}
	// MaybeApplied or a transient error: resend the identical request under
	// the same key until definite; a replay returns the same answer.
	return b.retryCommit(s)
}

// committed answers every batch of s from the control plane's verdicts.
// Timing is in nanoseconds. put plus wait plus commit equals total. wait is
// the time the uploaded segment queued behind earlier segments' commits.
func (b *Broker) committed(s *segment, rs []core.BatchRange) []core.Action {
	acts := []core.Action{recordFact("segment_committed",
		core.Field{Key: "segment", Value: core.StringValue(s.key)},
		core.Field{Key: "batches", Value: core.Int64Value(len(s.batches))},
		core.Field{Key: "bytes", Value: core.Int64Value(len(s.data))},
		core.Field{Key: "put_ns", Value: core.Int64Value(s.putDoneAt - s.sealedAt)},
		core.Field{Key: "wait_ns", Value: core.Int64Value(s.commitAt - s.putDoneAt)},
		core.Field{Key: "commit_ns", Value: core.Int64Value(b.now - s.commitAt)},
		core.Field{Key: "total_ns", Value: core.Int64Value(b.now - s.sealedAt)},
		core.Field{Key: "inflight", Value: core.Int64Value(len(b.segs) - 1)},
		core.Field{Key: "buffered", Value: core.Int64Value(b.bufBytes - s.dataBytes())})}
	for i, pb := range s.batches {
		b.answerPart(pb, errNone, rs[i], s.key)
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
func (b *Broker) failSegment(s *segment, code kafkaErrorCode) []core.Action {
	acts := []core.Action{recordFact("segment_failed",
		core.Field{Key: "segment", Value: core.StringValue(s.key)},
		core.Field{Key: "code", Value: core.Int64Value(int64(code))},
		core.Field{Key: "total_ns", Value: core.Int64Value(b.now - s.sealedAt)},
		core.Field{Key: "inflight", Value: core.Int64Value(len(b.segs) - 1)},
		core.Field{Key: "buffered", Value: core.Int64Value(b.bufBytes - s.dataBytes())})}
	for _, pb := range s.batches {
		b.answerPart(pb, code, core.BatchRange{}, "")
	}
	return append(acts, b.endSegment(s)...)
}

// retryPut backs off and repeats the PUT under the same key.
func (b *Broker) retryPut(s *segment) []core.Action {
	if s.attempts++; s.attempts >= b.cfg.MaxAttempts {
		// The write may still land later: unbounded MaybeApplied (decision 15).
		b.env.Reachable("broker: segment retries exhausted")
		return b.failSegment(s, errRequestTimedOut)
	}
	s.state, s.request = segmentUploadRetry, 0
	t, g := b.timer(core.TimerID(s.id), s.attempts)
	s.gen = g
	return []core.Action{t}
}

// retryCommit repeats a commit under the same key until CommitDeadline has
// passed since the first CommitSegment: capped exponential backoff with
// jitter (drawn from the Environment, so deterministic in simulation), never a count
// limit. Past the deadline the segment fails; the write may still land later
// (decision 15).
func (b *Broker) retryCommit(s *segment) []core.Action {
	left := b.cfg.CommitDeadline - core.Duration(b.now-s.commitAt)
	if left <= 0 {
		b.env.Reachable("broker: commit deadline exceeded")
		return b.failSegment(s, errRequestTimedOut)
	}
	s.attempts++
	d := b.cfg.RetryBase
	for n := s.attempts; n > 1 && d < b.cfg.CommitRetryMax; n-- {
		d *= 2
	}
	d = min(d, b.cfg.CommitRetryMax)
	d = d/2 + core.Duration(b.env.Intn(int(d/2)+1)) // jitter in [d/2, d]
	d = min(d, left)
	s.state, s.request = segmentCommitRetry, 0
	b.timerGen++
	s.gen = b.timerGen
	return []core.Action{core.SetTimer{ID: core.TimerID(s.id), Gen: b.timerGen, After: d}}
}

func (b *Broker) segmentTimer(s *segment, gen core.TimerVersion) []core.Action {
	if s.gen != gen {
		return nil
	}
	switch s.state {
	case segmentUploadRetry:
		return b.put(s)
	case segmentCommitRetry:
		b.env.Reachable("broker: commit retried")
		return b.commit(s)
	}
	return nil
}

// answerPart answers pb once, with code or, on errNone, with success at range
// r of segment key. A success also records the "produce_acked" fact that
// drain emits right before the Send (decision 12).
func (b *Broker) answerPart(pb *partitionBatch, code kafkaErrorCode, r core.BatchRange, key string) {
	if pb.answered {
		return
	}
	pb.answered = true
	pb.part.ErrorCode, pb.part.BaseOffset = int16(code), -1
	if code == errNone {
		pb.part.BaseOffset = int64(r.Base)
		pb.s.facts = append(pb.s.facts, core.Fact{Name: "produce_acked", Fields: []core.Field{
			{Key: "topic", Value: core.StringValue(pb.tp.Topic)},
			{Key: "partition", Value: core.Int64Value(pb.tp.Partition)},
			{Key: "base_offset", Value: core.Int64Value(r.Base)},
			{Key: "count", Value: core.Int64Value(pb.records)},
			{Key: "segment", Value: core.StringValue(key)},
		}})
	}
	b.finishPart(pb.s)
}
