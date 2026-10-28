package cpobs

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestMiddlewareAndNil(t *testing.T) {
	m := New()
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/commit-segment", strings.NewReader("hello")))
	m.Call("TransactWriteItems", time.Now())
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	for _, want := range []string{`manicp_handler_seconds_count{op="commit-segment"} 1`, `manicp_request_body_bytes_sum{op="commit-segment"} 5`,
		`manicp_dynamodb_call_seconds_count{call="TransactWriteItems"} 1`, "manicp_inflight_requests 0"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	var nm *Metrics
	nm.Call("x", time.Now())
	nm.Middleware(http.NotFoundHandler())
}

func gather(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestCallLogLine(t *testing.T) {
	var buf bytes.Buffer
	m := NewWithLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		c := FromContext(r.Context())
		c.SetSegment("seg-1")
		m.CallCtx(r.Context(), "BatchGetItem", time.Now().Add(-4*time.Millisecond), nil)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"err":"ErrConflict"}` + "\n"))
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/commit-segment", strings.NewReader("abc")))
	var l map[string]any
	if err := json.Unmarshal(buf.Bytes(), &l); err != nil {
		t.Fatalf("%v: %s", err, buf.String())
	}
	if l["event"] != "cp_call" || l["op"] != "commit-segment" || l["segment"] != "seg-1" || l["outcome"] != "ErrConflict" ||
		l["request_bytes"] != float64(3) {
		t.Errorf("line: %v", l)
	}
	if l["dynamo_ms"].(float64) < 4 {
		t.Errorf("timings: %v", l)
	}
	out := gather(t, m)
	for _, want := range []string{`manicp_calls_total{op="commit-segment",outcome="ErrConflict"} 1`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestConsumedCapacity(t *testing.T) {
	m := New()
	four, half := 4.0, 0.5
	m.CallCtx(t.Context(), "TransactWriteItems", time.Now(), []types.ConsumedCapacity{{CapacityUnits: &four}})
	m.CallCtx(t.Context(), "BatchGetItem", time.Now(), []types.ConsumedCapacity{{CapacityUnits: &half}, {CapacityUnits: &half}})
	m.CallCtx(t.Context(), "GetItem", time.Now(), nil) // ExtendDB: no capacity
	out := gather(t, m)
	for _, want := range []string{`manicp_dynamo_consumed_capacity_units_total{kind="write",op="TransactWriteItems"} 4`,
		`manicp_dynamo_consumed_capacity_units_total{kind="read",op="BatchGetItem"} 1`,
		`manicp_dynamo_consumed_capacity_missing_total{op="GetItem"} 1`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}

func TestStatusOutcome(t *testing.T) {
	m := NewWithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/lookup", nil))
	if !strings.Contains(gather(t, m), `manicp_calls_total{op="lookup",outcome="http_503"} 1`) {
		t.Error("503 outcome")
	}
}

// At the default INFO level a request logs nothing but the sampled summary:
// 5000 requests inside one second make no line, the first request after the
// second makes exactly one with the counts.
func TestInfoLevelLogsOnlyTheSummary(t *testing.T) {
	var buf bytes.Buffer
	m := NewWithLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	for range 5000 {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/lookup", strings.NewReader("")))
	}
	if buf.Len() != 0 {
		t.Fatalf("per-request lines at INFO: %s", buf.String())
	}
	m.sum.mu.Lock()
	m.sum.start = m.sum.start.Add(-2 * time.Second) // the interval is over
	m.sum.mu.Unlock()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/lookup", strings.NewReader("")))
	var l map[string]any
	if err := json.Unmarshal(buf.Bytes(), &l); err != nil || bytes.Count(buf.Bytes(), []byte("\n")) != 1 {
		t.Fatalf("%v: %s", err, buf.String())
	}
	got := l["lookup"].(map[string]any)
	if l["event"] != "cp_summary" || got["calls"] != float64(5001) || got["not_ok"] != float64(0) {
		t.Errorf("summary: %v", l)
	}
}

// A 5xx stays a WARN line of its own, whatever the level above WARN allows.
func TestFailureStaysWarn(t *testing.T) {
	var buf bytes.Buffer
	m := NewWithLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/commit-segment", strings.NewReader("")))
	if !strings.Contains(buf.String(), `"level":"WARN"`) || !strings.Contains(buf.String(), "cp_call") {
		t.Fatalf("%s", buf.String())
	}
}
