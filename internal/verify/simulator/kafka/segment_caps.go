package kafka

import (
	"slices"

	"mds/internal/broker"
	"mds/internal/core"
	"mds/internal/verify/simulator"
)

// timerExtraTime is how late a flush may fire past its window: a node busy with a
// paused Step (CostParams.Pause, up to 100 ms) runs its timer late.
const timerExtraTime = 150 * core.Millisecond

// segmentCaps checks every sealed segment (the broker's segment_sealed fact)
// against this seed's flush rule: at most MaxSegmentBatches batches and
// MaxSegmentPartitions partitions with an implied commit cost (1 + partitions,
// decisions 39, 41, 44) of at most the store's limit (none for the log store), the size rule applied exactly (the bytes
// before the last batch stayed under MaxSegmentBytes, so a segment overshoots
// the cap by at most its last batch), and an age of at most the window plus
// timerExtraTime. Every committed segment was sealed first.
//
// AtEnd repeats the partition and batch checks on the control plane's truth,
// independent of the broker's facts: the committed ranges grouped by segment.
type segmentCaps struct {
	config broker.Config
	tps    []core.TopicPartition
	items  int // the store's commit item limit; 0 = unbounded (the log store)
	seal   simulator.Cursor
}

func newSegmentCaps(config broker.Config, items int, tps []core.TopicPartition) *segmentCaps {
	return &segmentCaps{config: config, items: items, tps: tps}
}

// fits reports whether a commit of parts partitions is within the store's item
// limit (1 + parts, decisions 39, 41, 44); an unbounded store always fits.
func (i *segmentCaps) fits(parts int64) bool { return i.items <= 0 || 1+parts <= int64(i.items) }

func (*segmentCaps) Name() string { return "SegmentCaps" }

// AtEnd checks every committed segment: at most MaxSegmentBatches batches and
// MaxSegmentPartitions distinct partitions, so at most 1 + partitions items.
func (i *segmentCaps) AtEnd(v simulator.View) {
	type use struct {
		batches int
		parts   []int
	}
	var keys []string // first-seen order, so reports are deterministic
	segments := map[string]*use{}
	for pi, topicPartition := range i.tps {
		for _, r := range v.CP().Committed(topicPartition) {
			u := segments[r.Loc.Key]
			if u == nil {
				u = &use{}
				segments[r.Loc.Key] = u
				keys = append(keys, r.Loc.Key)
			}
			u.batches++
			if !slices.Contains(u.parts, pi) {
				u.parts = append(u.parts, pi)
			}
		}
	}
	a := v.Assert()
	for _, k := range keys {
		u := segments[k]
		d := core.ContextPair{Key: "segment", Value: k}
		a.Always(u.batches <= i.config.MaxSegmentBatches, "kafka: a committed segment holds at most MaxSegmentBatches batches",
			d, core.ContextPair{Key: "batches", Value: u.batches}, core.ContextPair{Key: "cap", Value: i.config.MaxSegmentBatches})
		a.Always(len(u.parts) <= i.config.MaxSegmentPartitions, "kafka: a committed segment spans at most MaxSegmentPartitions partitions",
			d, core.ContextPair{Key: "parts", Value: len(u.parts)}, core.ContextPair{Key: "cap", Value: i.config.MaxSegmentPartitions})
		a.Always(i.fits(int64(len(u.parts))), "kafka: a committed segment's commit cost (1 + partitions) fits the store's commit item limit",
			d, core.ContextPair{Key: "parts", Value: len(u.parts)})
	}
}

func (i *segmentCaps) Observe(v simulator.View) {
	for _, tf := range v.Timeline().Since("segment_sealed", &i.seal) {
		n := func(k string) int64 { x, _ := tf.Fact.Field(k).(core.Int64Value); return int64(x) }
		a := v.Assert()
		why, _ := tf.Fact.Field("reason").(core.StringValue)
		d := core.ContextPair{Key: "reason", Value: string(why)}
		a.Always(n("batches") <= int64(i.config.MaxSegmentBatches), "kafka: a sealed segment holds at most MaxSegmentBatches batches",
			d, core.ContextPair{Key: "batches", Value: n("batches")}, core.ContextPair{Key: "cap", Value: i.config.MaxSegmentBatches})
		a.Always(n("parts") <= int64(i.config.MaxSegmentPartitions), "kafka: a sealed segment spans at most MaxSegmentPartitions partitions",
			d, core.ContextPair{Key: "parts", Value: n("parts")}, core.ContextPair{Key: "cap", Value: i.config.MaxSegmentPartitions})
		a.Always(i.fits(n("parts")), "kafka: a sealed segment's implied commit cost (1 + partitions) fits the store's commit item limit",
			d, core.ContextPair{Key: "parts", Value: n("parts")})
		a.Always(n("bytes")-n("last") < int64(i.config.MaxSegmentBytes), "kafka: a sealed segment overshoots MaxSegmentBytes by at most its last batch",
			d, core.ContextPair{Key: "bytes", Value: n("bytes")}, core.ContextPair{Key: "last", Value: n("last")}, core.ContextPair{Key: "cap", Value: i.config.MaxSegmentBytes})
		a.Always(n("age") <= int64(i.config.BatchWindow+timerExtraTime), "kafka: a segment is sealed within the window of its first batch",
			d, core.ContextPair{Key: "age", Value: n("age")}, core.ContextPair{Key: "window", Value: int64(i.config.BatchWindow)})
	}
}
