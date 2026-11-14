package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// maxFetchWait caps MaxWait, so one Fetch cannot hold a connection for long.
const maxFetchWait = 30 * machine.Second

// fetch serves each partition independently: FindSegments, then ranged GETs (one
// per run of adjacent batches in a segment), verified and patched to each
// batch's committed offset (decision 30).
// A part that finds no data is parked and polled again (long poll, decision 29
// and Q8): the Fetch answers once it has MinBytes, or when MaxWait ends.
func (b *Broker) fetch(s *slot, r *kmsg.FetchRequest) []core.Action {
	response := kmsg.NewPtrFetchResponse()
	s.response = response
	if r.SessionEpoch > 0 { // incremental sessions are never granted
		response.ErrorCode = int16(errFetchSessionNotFound)
		return nil
	}
	s.fetchMax = r.MaxBytes
	s.fetchMin = r.MinBytes
	s.fetchUntil = b.now + machine.NodeTime(fetchWait(r.MaxWaitMillis))
	fillFetchSkeleton(response, r)
	var acts []core.Action
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &response.Topics[i].Partitions[j]
			topicPartition := contract.NewTopicPartition(t.Topic, p.Partition)
			switch {
			case !b.knows(topicPartition):
				rp.ErrorCode = int16(errUnknownTopicOrPart)
			case p.FetchOffset < 0:
				rp.ErrorCode = int16(errOffsetOutOfRange)
			default:
				lim := p.PartitionMaxBytes
				if lim <= 0 {
					lim = b.config.DefaultFetchBytes
				}
				acts = append(acts, b.startOp(&operation{kind: opFetch, s: s, topicPartition: topicPartition, from: core.Offset(p.FetchOffset), maxBytes: lim, fpart: rp})...)
			}
		}
	}
	return acts
}

func fillFetchSkeleton(response *kmsg.FetchResponse, r *kmsg.FetchRequest) {
	response.Topics = make([]kmsg.FetchResponseTopic, len(r.Topics))
	for i, t := range r.Topics {
		rt := &response.Topics[i]
		rt.Topic = t.Topic
		rt.Partitions = make([]kmsg.FetchResponseTopicPartition, len(t.Partitions))
		for j := range t.Partitions {
			rp := &rt.Partitions[j]
			rp.Partition = t.Partitions[j].Partition
			rp.HighWatermark, rp.LastStableOffset, rp.LogStartOffset, rp.PreferredReadReplica = -1, -1, 0, -1
		}
	}
}

// onLookup picks the ranges that fit the partition limit (at least one) and
// starts reading them.
func (b *Broker) onLookup(o *operation, r contract.FindSegmentsResp) []core.Action {
	o.fpart.HighWatermark, o.fpart.LastStableOffset = int64(r.End), int64(r.End)
	if o.from > r.End {
		o.fpart.ErrorCode = int16(errOffsetOutOfRange)
		return b.endOp(o)
	}
	var total int64
	for _, rg := range r.Ranges {
		if len(o.ranges) > 0 && total+rg.Loc.Len > int64(o.maxBytes) {
			break
		}
		o.ranges = append(o.ranges, rg)
		total += rg.Loc.Len
	}
	if o.from == r.End || len(o.ranges) == 0 {
		return b.waitOrEnd(o)
	}
	o.state, o.attempts, o.next = opGet, 0, 0
	return b.issue(o)
}

// fetchWait is how long a Fetch may wait: MaxWait, capped. Zero means answer at once.
func fetchWait(maxWaitMillis int32) machine.Duration {
	if maxWaitMillis <= 0 {
		return 0
	}
	return min(machine.Duration(maxWaitMillis)*machine.Millisecond, maxFetchWait)
}

// fetchOver reports whether a Fetch must answer now: it has MinBytes, its
// MaxWait is spent, the client is gone, or the broker is stopping.
func (b *Broker) fetchOver(s *slot) bool {
	return s.fetchMin <= 0 || s.fetchGot >= int(s.fetchMin) || b.now >= s.fetchUntil ||
		s.conn.closed || b.stopping
}

// waitOrEnd parks a part that found no data, or ends it if the Fetch is over.
func (b *Broker) waitOrEnd(o *operation) []core.Action {
	if b.fetchOver(o.s) {
		return b.endOp(o)
	}
	b.env.Reachable(fetchParkedForPoll)
	return b.park(o)
}

// fetchPartData runs when a part has its batches. The bytes count toward
// MinBytes. If that meets MinBytes, the Fetch answers now: parked parts end
// too. If not, this part waits with its data (held), and the Fetch answers
// once MinBytes is reached or MaxWait ends.
func (b *Broker) fetchPartData(o *operation) []core.Action {
	s := o.s
	s.fetchGot += len(o.fpart.RecordBatches)
	if s.fetchGot >= int(s.fetchMin) {
		b.endParked(func(p *operation) bool { return p.s == s })
		return b.endOp(o)
	}
	if b.fetchOver(s) {
		return b.endOp(o)
	}
	o.held = true
	return b.park(o)
}

// park waits until the next poll of o: FetchPollInterval, or the end of
// MaxWait if that comes first.
func (b *Broker) park(o *operation) []core.Action {
	o.state, o.attempts = opWait, 0
	after := min(b.config.FetchPollInterval, machine.Duration(o.s.fetchUntil-b.now))
	b.timerGen++
	o.gen = b.timerGen
	return []core.Action{core.SetTimer{ID: machine.TimerID(o.id), Gen: o.gen, After: after}}
}

// pollOp runs when a parked part's timer fires. A part that holds data only
// waits for the clock. A part with no data runs its next Lookup, so only the
// partitions still short are polled.
func (b *Broker) pollOp(o *operation) []core.Action {
	if b.fetchOver(o.s) {
		return b.endOp(o)
	}
	if o.held {
		return b.park(o)
	}
	o.state = opControlPlane
	return b.issue(o)
}

// endParked ends each parked part that match, with no further polls. A held
// part ends with the data it holds. Its bytes were counted when it arrived.
func (b *Broker) endParked(match func(*operation) bool) {
	var parked []*operation
	for _, o := range b.ops {
		if o.state == opWait && match(o) {
			parked = append(parked, o)
		}
	}
	for _, o := range parked {
		b.endOp(o)
	}
}

// readRanges returns the ranges that the next GET reads, from o.next on. They
// are the ranges that follow each other in one segment object, so one GET
// reads them all. The ranges were already cut to the partition limit.
func (o *operation) readRanges() []contract.BatchRange {
	end := o.next + 1
	for end < len(o.ranges) && adjacent(o.ranges[end-1], o.ranges[end]) {
		end++
	}
	return o.ranges[o.next:end]
}

// adjacent reports whether next starts where prev ends, in the same object.
// Lengths are positive: a length of 0 or less means "to the end" (SegmentRef).
func adjacent(prev, next contract.BatchRange) bool {
	return prev.Loc.Len > 0 && next.Loc.Len > 0 &&
		prev.Loc.Key == next.Loc.Key && prev.Loc.Off+prev.Loc.Len == next.Loc.Off
}

// readRef is one ranged GET that covers rgs. rgs must be adjacent.
func readRef(rgs []contract.BatchRange) contract.SegmentRef {
	ref := rgs[0].Loc
	for _, rg := range rgs[1:] {
		ref.Len += rg.Loc.Len
	}
	return ref
}

// onFetchGet verifies one read from the object store. A read holds one or
// more batches (see readRanges). A corrupt read is never returned: the whole
// read is retried (the object itself is intact; S3 checksums cover whole
// objects but not byte ranges, decision 27).
func (b *Broker) onFetchGet(o *operation, e objectstore.GetDone) []core.Action {
	rgs := o.readRanges()
	if e.Err == core.ErrNotFound { // committed segments are never deleted
		b.env.Unreachable(committedSegmentMissing)
		return b.failOp(o, errKafkaStorage)
	}
	if e.Err != core.OK {
		o.corrupt = false
		return b.retryOp(o)
	}
	parts := splitRead(e.Data, rgs)
	if b.config.InjectedBug != InjectedBugServeCorrupt && !allIntact(e.Data, parts, rgs) {
		return b.retryCorrupt(o, rgs[0])
	}
	return b.acceptFetch(o, parts, rgs)
}

func (b *Broker) retryCorrupt(o *operation, rg contract.BatchRange) []core.Action {
	b.env.Reachable(corruptGetDetected)
	o.corrupt = true
	acts := []core.Action{recordFact("corruption_detected", machine.Field{Key: "segment", Value: machine.StringValue(rg.Loc.Key)})}
	return append(acts, b.retryOp(o)...)
}

// acceptFetch takes the batches of one read, patches each one to its
// committed offset, and moves on to the next read.
func (b *Broker) acceptFetch(o *operation, parts [][]byte, rgs []contract.BatchRange) []core.Action {
	for i, rg := range rgs {
		base := int64(rg.Base)
		if b.config.InjectedBug == InjectedBugWrongOffset {
			base++
		}
		o.out = append(o.out, kafkawire.ChangeBatchOffset(parts[i], base, 0)...)
	}
	o.next += len(rgs)
	o.attempts, o.corrupt = 0, false
	if o.next < len(o.ranges) {
		return b.issue(o)
	}
	o.fpart.RecordBatches = o.out
	return b.fetchPartData(o)
}

// splitRead cuts the bytes of one read into one part per range, by Loc.Len.
// A short read gives short parts, so the checks below fail on it.
func splitRead(data []byte, rgs []contract.BatchRange) [][]byte {
	parts := make([][]byte, len(rgs))
	for i, rg := range rgs {
		n := min(rg.Loc.Len, int64(len(data)))
		parts[i], data = data[:n], data[n:]
	}
	return parts
}

// allIntact reports whether data is exactly the batches of rgs, and each part
// is the one valid batch that its range committed.
func allIntact(data []byte, parts [][]byte, rgs []contract.BatchRange) bool {
	var total int64
	for _, rg := range rgs {
		total += rg.Loc.Len
	}
	if int64(len(data)) != total {
		return false
	}
	for i, rg := range rgs {
		if !isIntact(parts[i], rg) {
			return false
		}
	}
	return true
}

// isIntact reports whether data is the one valid batch that rg committed.
func isIntact(data []byte, rg contract.BatchRange) bool {
	bt, err := kafkawire.ParseBatch(data)
	return err == nil && int64(len(data)) == rg.Loc.Len && int64(bt.NumRecords) == rg.Count
}
