package prod

import (
	"net/http/httptest"
	"strings"
	"testing"

	"manifester/core"
)

func fact(name string, kv ...any) core.Fact {
	f := core.Fact{Name: name}
	for i := 0; i < len(kv); i += 2 {
		var v core.FieldValue
		switch x := kv[i+1].(type) {
		case string:
			v = core.Str(x)
		case int:
			v = core.I64(x)
		}
		f.Fields = append(f.Fields, core.Field{Key: kv[i].(string), Value: v})
	}
	return f
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	return rec.Body.String()
}

func TestMetricsFromFacts(t *testing.T) {
	m := NewMetrics()
	const ms = 1_000_000
	m.Observe(fact("segment_sealed", "reason", "size", "bytes", 1000, "parts", 3, "age", 100*ms, "inflight", 2, "buffered", 5000))
	m.Observe(fact("segment_sealed", "reason", "timer", "bytes", 500, "parts", 100, "age", 200*ms, "inflight", 1, "buffered", 5500))
	m.Observe(fact("segment_put_done", "put_ns", 40*ms, "attempts", 2))
	m.Observe(fact("segment_committed", "put_ns", 40*ms, "wait_ns", 60*ms, "commit_ns", 10*ms, "total_ns", 110*ms,
		"bytes", 1100, "inflight", 1, "buffered", 4500))
	m.Observe(fact("segment_failed", "total_ns", 7*ms, "inflight", 0, "buffered", 0))
	m.Observe(fact("backpressure_on", "buffered", 9000))
	out := scrape(t, m)
	for _, want := range []string{
		`manibroker_segments_sealed_total{reason="size"} 1`,
		`manibroker_segments_sealed_total{reason="timer"} 1`,
		`manibroker_segment_sealed_batch_bytes_total 1500`,
		`manibroker_segment_put_seconds_count 1`,
		`manibroker_segment_put_seconds_sum 0.04`,
		`manibroker_segment_put_attempts_total 2`,
		`manibroker_segment_queue_wait_seconds_sum 0.06`,
		`manibroker_segment_commit_seconds_sum 0.01`,
		`manibroker_segment_seal_to_ack_seconds_count 2`, // committed and failed
		`manibroker_segments_in_flight_at_seal_bucket{le="1"} 1`,
		`manibroker_segments_in_flight_at_seal_bucket{le="2"} 2`,
		`manibroker_segment_partitions_bucket{le="2"} 0`,
		`manibroker_segment_partitions_bucket{le="4"} 1`,
		`manibroker_segment_partitions_bucket{le="128"} 2`,
		`manibroker_segment_partitions_sum 103`,
		`manibroker_segments_committed_total 1`,
		`manibroker_segment_committed_bytes_total 1100`,
		`manibroker_segments_failed_total 1`,
		`manibroker_segments_in_flight 0`,
		`manibroker_buffered_bytes 9000`,
		`manibroker_reads_paused 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q", want)
		}
	}
	m.Observe(fact("backpressure_off", "buffered", 100, "paused_ns", 1500*ms))
	m.Observe(fact("produce_acked")) // unknown facts are ignored
	out = scrape(t, m)
	for _, want := range []string{`manibroker_reads_paused 0`, `manibroker_reads_paused_seconds_total 1.5`} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q", want)
		}
	}
}
