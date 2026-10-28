package kafka

import (
	"manifester/internal/core"
	"manifester/internal/verify/sim"
)

// segmentTiming checks the timing facts that the production metrics are built
// from: durations are never negative, a committed segment's put, queue wait and
// commit add up exactly to its seal-to-commit total, and the occupancy fields
// (segments in flight, buffered bytes) are never negative. A wrong metric would
// send a performance investigation the wrong way.
type segmentTiming struct {
	put, commit, fail, off sim.Cursor
}

func (*segmentTiming) Name() string   { return "SegmentTiming" }
func (*segmentTiming) AtEnd(sim.View) {}

func (i *segmentTiming) Observe(v sim.View) {
	n := func(f core.Fact, k string) int64 { x, _ := f.Field(k).(core.I64); return int64(x) }
	a := v.Assert()
	tl := v.Timeline()
	for _, tf := range tl.Since("segment_put_done", &i.put) {
		a.Always(n(tf.Fact, "put_ns") >= 0 && n(tf.Fact, "attempts") >= 1 && n(tf.Fact, "bytes") > 0,
			"kafka: a segment's put time is non-negative and it took at least one attempt",
			core.Detail{Key: "put_ns", Value: n(tf.Fact, "put_ns")})
	}
	for _, tf := range tl.Since("segment_committed", &i.commit) {
		f := tf.Fact
		put, wait, com, total := n(f, "put_ns"), n(f, "wait_ns"), n(f, "commit_ns"), n(f, "total_ns")
		d := []core.Detail{{Key: "put_ns", Value: put}, {Key: "wait_ns", Value: wait},
			{Key: "commit_ns", Value: com}, {Key: "total_ns", Value: total}}
		a.Always(put >= 0 && wait >= 0 && com >= 0, "kafka: segment put, queue wait and commit durations are non-negative", d...)
		a.Always(put+wait+com == total, "kafka: segment put + queue wait + commit equals its seal-to-commit total", d...)
		a.Always(n(f, "inflight") >= 0 && n(f, "buffered") >= 0 && n(f, "bytes") > 0,
			"kafka: a committed segment reports non-negative occupancy",
			core.Detail{Key: "inflight", Value: n(f, "inflight")}, core.Detail{Key: "buffered", Value: n(f, "buffered")})
	}
	for _, tf := range tl.Since("segment_failed", &i.fail) {
		f := tf.Fact
		a.Always(n(f, "total_ns") >= 0 && n(f, "inflight") >= 0 && n(f, "buffered") >= 0,
			"kafka: a failed segment reports non-negative time and occupancy",
			core.Detail{Key: "total_ns", Value: n(f, "total_ns")})
	}
	for _, tf := range tl.Since("backpressure_off", &i.off) {
		a.Always(n(tf.Fact, "paused_ns") >= 0 && n(tf.Fact, "buffered") >= 0,
			"kafka: a backpressure pause has a non-negative duration",
			core.Detail{Key: "paused_ns", Value: n(tf.Fact, "paused_ns")})
	}
}
