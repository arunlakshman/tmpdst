package cpobs

import (
	"bytes"
	"context"
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
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/commit-segment", strings.NewReader("hello")))
	m.Call("TransactWriteItems", time.Now())
	m.Prep("reads", time.Millisecond)
	m.Retry("commit", "transaction_canceled")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{`manicp_handler_seconds_count{op="commit-segment"} 1`, `manicp_request_body_bytes_sum{op="commit-segment"} 5`,
		`manicp_dynamodb_call_seconds_count{call="TransactWriteItems"} 1`, `manicp_retries_total{op="commit",reason="transaction_canceled"} 1`, "manicp_inflight_requests 0"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	var nm *Metrics
	nm.Call("x", time.Now())
	nm.Retry("a", "b")
	nm.Middleware(http.NotFoundHandler())
}

func gather(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

func TestCallLogLine(t *testing.T) {
	var buf bytes.Buffer
	m := NewWithLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		c := FromContext(r.Context())
		c.SetSegment("seg-1")
		m.AttemptCtx(r.Context(), "commit")
		m.CallCtx(r.Context(), "BatchGetItem", time.Now().Add(-4*time.Millisecond), nil)
		m.RetryCtx(r.Context(), "commit", "transaction_canceled", []string{"ConditionalCheckFailed"})
		m.AttemptCtx(r.Context(), "commit")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"err":"ErrFenced"}` + "\n"))
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/commit-segment", strings.NewReader("abc")))
	var l map[string]any
	if err := json.Unmarshal(buf.Bytes(), &l); err != nil {
		t.Fatalf("%v: %s", err, buf.String())
	}
	if l["event"] != "cp_call" || l["op"] != "commit-segment" || l["segment"] != "seg-1" || l["outcome"] != "ErrFenced" ||
		l["attempts"] != float64(2) || l["retries"] != float64(1) || l["request_bytes"] != float64(3) {
		t.Errorf("line: %v", l)
	}
	if l["dynamo_ms"].(float64) < 4 {
		t.Errorf("timings: %v", l)
	}
	out := gather(t, m)
	for _, want := range []string{`manicp_calls_total{op="commit-segment",outcome="ErrFenced"} 1`,
		`manicp_dynamo_txn_cancel_reasons_total{code="ConditionalCheckFailed"} 1`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestConsumedCapacity(t *testing.T) {
	m := New()
	four, half := 4.0, 0.5
	m.CallCtx(context.Background(), "TransactWriteItems", time.Now(), []types.ConsumedCapacity{{CapacityUnits: &four}})
	m.CallCtx(context.Background(), "BatchGetItem", time.Now(), []types.ConsumedCapacity{{CapacityUnits: &half}, {CapacityUnits: &half}})
	m.CallCtx(context.Background(), "GetItem", time.Now(), nil) // ExtendDB: no capacity
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
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/lookup", nil))
	if !strings.Contains(gather(t, m), `manicp_calls_total{op="lookup",outcome="http_503"} 1`) {
		t.Error("503 outcome")
	}
}
