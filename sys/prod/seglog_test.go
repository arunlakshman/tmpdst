package prod

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"manifester/core"
)

func lines(t *testing.T, b *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for l := range strings.SplitSeq(strings.TrimSpace(b.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("%v: %s", err, l)
		}
		out = append(out, m)
	}
	return out
}

func TestSegmentLogLifecycle(t *testing.T) {
	var buf bytes.Buffer
	sl := NewSegmentLog(slog.New(slog.NewJSONHandler(&buf, nil)))
	const ms = 1_000_000
	// Two segments sealed back to back, then PUT, then answers.
	sl.Observe(fact("segment_sealed", "reason", "size", "bytes", 1000, "batches", 3, "parts", 2, "age", 50*ms))
	sl.IO(core.ObjPut{Key: "k1"})
	sl.Observe(fact("segment_sealed", "reason", "timer", "bytes", 10, "batches", 1, "parts", 1, "age", 200*ms))
	sl.IO(core.ObjPut{Key: "k2"})
	sl.IO(core.ObjPut{Key: "k1"}) // a retry must not consume a pending seal
	sl.Observe(fact("segment_put_done", "segment", "k1", "bytes", 1100, "put_ns", 30*ms, "attempts", 2))
	sl.Observe(fact("segment_committed", "segment", "k1", "batches", 3, "bytes", 1100, "put_ns", 30*ms,
		"wait_ns", 5*ms, "commit_ns", 15*ms, "total_ns", 50*ms, "inflight", 1, "buffered", 7))
	sl.Observe(fact("segment_failed", "segment", "k2", "code", 7, "total_ns", 9*ms))
	got := lines(t, &buf)
	var events []string
	for _, l := range got {
		events = append(events, l["event"].(string))
	}
	want := "segment_sealed segment_sealed segment_put_done segment_committed segment_failed"
	if strings.Join(events, " ") != want {
		t.Fatalf("events %v", events)
	}
	c := got[3]
	if c["segment"] != "k1" || c["reason"] != "size" || c["partitions"] != float64(2) || c["batches"] != float64(3) ||
		c["put_ms"] != float64(30) || c["wait_ms"] != float64(5) || c["commit_ms"] != float64(15) || c["total_ms"] != float64(50) ||
		c["put_attempts"] != float64(2) {
		t.Errorf("committed line: %v", c)
	}
	f := got[4]
	if f["level"] != "WARN" || f["reason"] != "timer" || f["kafka_error_code"] != float64(7) {
		t.Errorf("failed line: %v", f)
	}
	if len(sl.segs) != 0 || len(sl.pending) != 0 {
		t.Errorf("leak: %d segs %d pending", len(sl.segs), len(sl.pending))
	}
}
