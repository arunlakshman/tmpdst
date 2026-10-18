package kafka

import (
	"manifester/broker"
	"manifester/core"
	"manifester/sim"
)

// timerSlack is how late a flush may fire past its window: a node busy with a
// paused Step (CostParams.Pause, up to 100 ms) runs its timer late.
const timerSlack = 150 * core.Millisecond

// segmentCaps checks every sealed segment (the broker's segment_sealed fact)
// against this seed's flush rule: at most MaxSegmentBatches batches and
// MaxSegmentPartitions partitions, the size rule applied exactly (the bytes
// before the last batch stayed under MaxSegmentBytes, so a segment overshoots
// the cap by at most its last batch), and an age of at most the window plus
// timerSlack. Every committed segment was sealed first.
type segmentCaps struct {
	cfg  broker.Config
	seal sim.Cursor
}

func newSegmentCaps(cfg broker.Config) *segmentCaps { return &segmentCaps{cfg: cfg} }

func (*segmentCaps) Name() string   { return "SegmentCaps" }
func (*segmentCaps) AtEnd(sim.View) {}

func (i *segmentCaps) Observe(v sim.View) {
	for _, tf := range v.Timeline().Since("segment_sealed", &i.seal) {
		n := func(k string) int64 { x, _ := fieldOf(tf.Fact, k).(core.I64); return int64(x) }
		a := v.Assert()
		why, _ := fieldOf(tf.Fact, "reason").(core.Str)
		d := core.Detail{Key: "reason", Value: string(why)}
		a.Always(n("batches") <= int64(i.cfg.MaxSegmentBatches), "kafka: a sealed segment holds at most MaxSegmentBatches batches",
			d, core.Detail{Key: "batches", Value: n("batches")}, core.Detail{Key: "cap", Value: i.cfg.MaxSegmentBatches})
		a.Always(n("parts") <= int64(i.cfg.MaxSegmentPartitions), "kafka: a sealed segment spans at most MaxSegmentPartitions partitions",
			d, core.Detail{Key: "parts", Value: n("parts")}, core.Detail{Key: "cap", Value: i.cfg.MaxSegmentPartitions})
		a.Always(n("bytes")-n("last") < int64(i.cfg.MaxSegmentBytes), "kafka: a sealed segment overshoots MaxSegmentBytes by at most its last batch",
			d, core.Detail{Key: "bytes", Value: n("bytes")}, core.Detail{Key: "last", Value: n("last")}, core.Detail{Key: "cap", Value: i.cfg.MaxSegmentBytes})
		a.Always(n("age") <= int64(i.cfg.BatchWindow+timerSlack), "kafka: a segment is sealed within the window of its first batch",
			d, core.Detail{Key: "age", Value: n("age")}, core.Detail{Key: "window", Value: int64(i.cfg.BatchWindow)})
	}
}
