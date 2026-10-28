package prod

import (
	"log/slog"

	"manifester/internal/core"
)

// SegmentLog writes one structured log line per segment lifecycle event, built
// from the broker's facts (decision 36) so the broker core stays free of
// logging. The segment_sealed fact carries no segment key; the executor sees
// the key on the first ObjPut for it, right after the fact and in the same loop
// goroutine, so IO pairs them and the sealed line can name the segment.
//
// Events (field "event"): segment_sealed, segment_put_done, segment_committed,
// segment_failed, backpressure_on, backpressure_off. Every segment line has
// segment, bytes, batches, partitions and, as they become known, reason,
// put_ms, wait_ms, commit_ms, total_ms. At benchmark rates (tens of segments a
// second) this is a few hundred lines a second at most; raise -log-level to
// warn to keep only failures.
//
// Observe and IO must be called from one goroutine (the broker loop).
type SegmentLog struct {
	log     *slog.Logger
	pending []*segInfo
	segs    map[string]*segInfo
}

type segInfo struct {
	reason                     string
	bytes, batches, parts, age int64
	putAttempts                int64
	putMS                      float64
}

// NewSegmentLog returns a logger of segment lifecycle facts.
func NewSegmentLog(log *slog.Logger) *SegmentLog {
	return &SegmentLog{log: log, segs: map[string]*segInfo{}}
}

func ms(ns int64) float64 { return float64(ns/10_000) / 100 } // 10 us resolution

func fi(f core.Fact, k string) int64 {
	switch x := f.Field(k).(type) {
	case core.I64:
		return int64(x)
	case core.U64:
		return int64(x)
	}
	return 0
}

func fs(f core.Fact, k string) string {
	s, _ := f.Field(k).(core.Str)
	return string(s)
}

// IO notes an action the broker issued (wire to NodeConfig.OnIO).
func (s *SegmentLog) IO(a core.IOAction) {
	p, ok := a.(core.ObjPut)
	if !ok || len(s.pending) == 0 {
		return
	}
	if _, seen := s.segs[p.Key]; seen { // a retry of the same PUT
		return
	}
	si := s.pending[0]
	s.pending = s.pending[1:]
	s.segs[p.Key] = si
	s.log.Info("segment sealed", "event", "segment_sealed", "segment", p.Key, "reason", si.reason, "bytes", si.bytes,
		"batches", si.batches, "partitions", si.parts, "open_age_ms", ms(si.age))
}

// Observe consumes one broker fact.
func (s *SegmentLog) Observe(f core.Fact) {
	switch f.Name {
	case "segment_sealed":
		if len(s.pending) < 4096 {
			s.pending = append(s.pending, &segInfo{reason: fs(f, "reason"), bytes: fi(f, "bytes"),
				batches: fi(f, "batches"), parts: fi(f, "parts"), age: fi(f, "age")})
		}
	case "segment_put_done":
		key := fs(f, "segment")
		si := s.segs[key]
		if si == nil {
			si = &segInfo{}
		}
		si.putAttempts, si.putMS = fi(f, "attempts"), ms(fi(f, "put_ns"))
		s.log.Info("segment put done", "event", "segment_put_done", "segment", key, "bytes", fi(f, "bytes"),
			"batches", si.batches, "partitions", si.parts, "put_ms", si.putMS, "put_attempts", si.putAttempts)
	case "segment_committed":
		key := fs(f, "segment")
		si := s.take(key)
		s.log.Info("segment committed", "event", "segment_committed", "segment", key, "reason", si.reason,
			"bytes", fi(f, "bytes"), "batches", fi(f, "batches"), "partitions", si.parts,
			"put_ms", ms(fi(f, "put_ns")), "wait_ms", ms(fi(f, "wait_ns")), "commit_ms", ms(fi(f, "commit_ns")),
			"total_ms", ms(fi(f, "total_ns")), "put_attempts", si.putAttempts,
			"in_flight", fi(f, "inflight"), "buffered_bytes", fi(f, "buffered"))
	case "segment_failed":
		key := fs(f, "segment")
		si := s.take(key)
		s.log.Warn("segment failed", "event", "segment_failed", "segment", key, "reason", si.reason, "bytes", si.bytes,
			"batches", si.batches, "partitions", si.parts, "kafka_error_code", fi(f, "code"),
			"put_attempts", si.putAttempts, "put_ms", si.putMS, "total_ms", ms(fi(f, "total_ns")),
			"in_flight", fi(f, "inflight"), "buffered_bytes", fi(f, "buffered"))
	case "backpressure_on":
		s.log.Info("client reads paused", "event", "backpressure_on", "buffered_bytes", fi(f, "buffered"))
	case "backpressure_off":
		s.log.Info("client reads resumed", "event", "backpressure_off", "buffered_bytes", fi(f, "buffered"),
			"paused_ms", ms(fi(f, "paused_ns")))
	}
}

func (s *SegmentLog) take(key string) *segInfo {
	si := s.segs[key]
	if si == nil {
		return &segInfo{}
	}
	delete(s.segs, key)
	return si
}
