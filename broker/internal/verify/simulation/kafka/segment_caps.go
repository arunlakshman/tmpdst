package kafka

import (
	"slices"

	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/truth"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// timerExtraTime is how late a flush may fire past its window: a node busy with a
// paused Step (CostParams.Pause, up to 100 ms) runs its timer late.
const timerExtraTime = 150 * machine.Millisecond

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
	seal   kernel.Cursor
	cp     truth.ControlPlane // what the control plane really committed
}

func newSegmentCaps(config broker.Config, items int, tps []core.TopicPartition, cp truth.ControlPlane) *segmentCaps {
	return &segmentCaps{config: config, items: items, tps: tps, cp: cp}
}

// fits reports whether a commit of parts partitions is within the store's item
// limit (1 + parts, decisions 39, 41, 44); an unbounded store always fits.
func (i *segmentCaps) fits(parts int64) bool { return i.items <= 0 || 1+parts <= int64(i.items) }

func (*segmentCaps) Name() string { return "SegmentCaps" }

// AtEnd checks every committed segment: at most MaxSegmentBatches batches and
// MaxSegmentPartitions distinct partitions, so at most 1 + partitions items.
func (i *segmentCaps) AtEnd(v kernel.View) {
	type use struct {
		batches int
		parts   []int
	}
	var keys []string // first-seen order, so reports are deterministic
	segments := map[string]*use{}
	for pi, topicPartition := range i.tps {
		for _, r := range i.cp.Committed(topicPartition) {
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
		d := assert.Pair{Key: "segment", Value: k}
		a.Always(u.batches <= i.config.MaxSegmentBatches, committedSegmentWithinBatchCap,
			d, assert.Pair{Key: "batches", Value: u.batches}, assert.Pair{Key: "cap", Value: i.config.MaxSegmentBatches})
		a.Always(len(u.parts) <= i.config.MaxSegmentPartitions, committedSegmentWithinPartitionCap,
			d, assert.Pair{Key: "parts", Value: len(u.parts)}, assert.Pair{Key: "cap", Value: i.config.MaxSegmentPartitions})
		a.Always(i.fits(int64(len(u.parts))), committedSegmentCommitCostFits,
			d, assert.Pair{Key: "parts", Value: len(u.parts)})
	}
}

func (i *segmentCaps) Observe(v kernel.View) {
	for _, tf := range v.Timeline().Since("segment_sealed", &i.seal) {
		n := func(k string) int64 { x, _ := tf.Fact.Field(k).(machine.Int64Value); return int64(x) }
		a := v.Assert()
		why, _ := tf.Fact.Field("reason").(machine.StringValue)
		d := assert.Pair{Key: "reason", Value: string(why)}
		a.Always(n("batches") <= int64(i.config.MaxSegmentBatches), sealedSegmentWithinBatchCap,
			d, assert.Pair{Key: "batches", Value: n("batches")}, assert.Pair{Key: "cap", Value: i.config.MaxSegmentBatches})
		a.Always(n("parts") <= int64(i.config.MaxSegmentPartitions), sealedSegmentWithinPartitionCap,
			d, assert.Pair{Key: "parts", Value: n("parts")}, assert.Pair{Key: "cap", Value: i.config.MaxSegmentPartitions})
		a.Always(i.fits(n("parts")), sealedSegmentCommitCostFits,
			d, assert.Pair{Key: "parts", Value: n("parts")})
		a.Always(n("bytes")-n("last") < int64(i.config.MaxSegmentBytes), sealedSegmentOvershootsBytesByLastBatch,
			d, assert.Pair{Key: "bytes", Value: n("bytes")}, assert.Pair{Key: "last", Value: n("last")}, assert.Pair{Key: "cap", Value: i.config.MaxSegmentBytes})
		a.Always(n("age") <= int64(i.config.BatchWindow+timerExtraTime), segmentSealedWithinBatchWindow,
			d, assert.Pair{Key: "age", Value: n("age")}, assert.Pair{Key: "window", Value: int64(i.config.BatchWindow)})
	}
}
