package cpobs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
