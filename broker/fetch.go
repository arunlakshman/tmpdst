package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
)

// fetch serves each partition independently: Lookup, then one ranged GET per
// committed batch, verified and patched to its committed offset (decision 30).
// It answers at once, ignoring MinBytes and MaxWait (decision 29).
func (b *Broker) fetch(s *slot, r *kmsg.FetchRequest) []core.Action {
	resp := kmsg.NewPtrFetchResponse()
	s.resp = resp
	if r.SessionEpoch > 0 { // incremental sessions are never granted
		resp.ErrorCode = int16(errFetchSessionNotFound)
		return nil
	}
	s.fetchMax = r.MaxBytes
	fillFetchSkeleton(resp, r)
	var acts []core.Action
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &resp.Topics[i].Partitions[j]
			tp := core.TP(t.Topic, p.Partition)
			switch {
			case !b.knows(tp):
				rp.ErrorCode = int16(errUnknownTopicOrPart)
			case p.FetchOffset < 0:
				rp.ErrorCode = int16(errOffsetOutOfRange)
			default:
				lim := p.PartitionMaxBytes
				if lim <= 0 {
					lim = b.cfg.DefaultFetchBytes
				}
				acts = append(acts, b.startOp(&op{kind: opFetch, s: s, tp: tp, from: core.Offset(p.FetchOffset), maxBytes: lim, fpart: rp})...)
			}
		}
	}
	return acts
}

func fillFetchSkeleton(resp *kmsg.FetchResponse, r *kmsg.FetchRequest) {
	resp.Topics = make([]kmsg.FetchResponseTopic, len(r.Topics))
	for i, t := range r.Topics {
		rt := &resp.Topics[i]
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
func (b *Broker) onLookup(o *op, r core.LookupResp) []core.Action {
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
		return b.endOp(o)
	}
	o.state, o.attempts, o.next = opGet, 0, 0
	return b.issue(o)
}

// onFetchGet verifies one batch read from the object store. A corrupt GET is
// never returned: it is retried (the object itself is intact; S3 checksums
// cover whole objects but not byte ranges, decision 27).
func (b *Broker) onFetchGet(o *op, e core.ObjGetDone) []core.Action {
	rg := o.ranges[o.next]
	if e.Err == core.ErrNotFound { // committed segments are never deleted
		b.env.Unreachable("broker: committed segment missing")
		return b.failOp(o, errKafkaStorage)
	}
	if e.Err != core.OK {
		o.corrupt = false
		return b.retryOp(o)
	}
	if b.cfg.Bug != BugServeCorrupt && !intact(e.Data, rg) {
		return b.retryCorrupt(o, rg)
	}
	return b.acceptFetch(o, e.Data, rg)
}

func (b *Broker) retryCorrupt(o *op, rg core.BatchRange) []core.Action {
	b.env.Reachable("broker: corrupt GET detected")
	o.corrupt = true
	acts := []core.Action{emit("corruption_detected", core.Field{Key: "segment", Value: core.Str(rg.Loc.Key)})}
	return append(acts, b.retryOp(o)...)
}

func (b *Broker) acceptFetch(o *op, data []byte, rg core.BatchRange) []core.Action {
	base := int64(rg.Base)
	if b.cfg.Bug == BugWrongOffset {
		base++
	}
	o.out = append(o.out, kwire.PatchBatch(data, base, 0)...)
	o.next++
	o.attempts, o.corrupt = 0, false
	if o.next < len(o.ranges) {
		return b.issue(o)
	}
	o.fpart.RecordBatches = o.out
	return b.endOp(o)
}

// intact reports whether data is the one valid batch that rg committed.
func intact(data []byte, rg core.BatchRange) bool {
	bt, err := kwire.ParseBatch(data)
	return err == nil && int64(len(data)) == rg.Loc.Len && int64(bt.NumRecords) == rg.Count
}
