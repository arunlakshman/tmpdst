// Package cpobs is the control-plane service's Prometheus instrumentation:
// HTTP handler time, DynamoDB call time, commit preparation time, retries and
// cancellations, in-flight requests and request sizes. It lives at the
// non-deterministic edge (wall clocks, atomics, no core import) and every
// method is safe on a nil *Metrics, so stores and handlers run uninstrumented.
package cpobs

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the collectors, all prefixed manicp_.
type Metrics struct {
	reg *prometheus.Registry

	handler  *prometheus.HistogramVec // op
	dyn      *prometheus.HistogramVec // call
	prep     *prometheus.HistogramVec // stage
	body     *prometheus.HistogramVec // op
	retries  *prometheus.CounterVec   // op, reason
	attempts *prometheus.CounterVec   // op
	inflight prometheus.Gauge
}

// New registers every collector on a private registry (plus Go runtime and
// process collectors).
func New() *Metrics {
	secs := prometheus.ExponentialBuckets(0.0005, 2, 16) // 0.5 ms .. 16 s
	hv := func(name, help, label string, b []float64) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "manicp", Name: name, Help: help, Buckets: b}, []string{label})
	}
	m := &Metrics{
		reg:     prometheus.NewRegistry(),
		handler: hv("handler_seconds", "Whole HTTP handler time (body read to response written), by op.", "op", secs),
		dyn:     hv("dynamodb_call_seconds", "Time inside one DynamoDB SDK call (TransactWriteItems, GetItem, BatchGetItem, ...).", "call", secs),
		prep:    hv("commit_prep_seconds", "CommitSegment time outside the transaction: reads (consistent gets) and decide_encode (decide + build items), per attempt.", "stage", secs),
		body:    hv("request_body_bytes", "HTTP request body size, by op.", "op", prometheus.ExponentialBuckets(64, 4, 10)),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Name: "retries_total",
			Help: "Internal retries by op and reason (transaction_canceled, unavailable, conditional_check)."}, []string{"op", "reason"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Name: "commit_attempts_total",
			Help: "CommitSegment attempts (first tries plus retries), by op."}, []string{"op"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "manicp", Name: "inflight_requests", Help: "HTTP requests being served."}),
	}
	m.reg.MustRegister(m.handler, m.dyn, m.prep, m.body, m.retries, m.attempts, m.inflight,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler { return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}) }

// Middleware times every request, counts it in flight and records its body size.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimPrefix(r.URL.Path, "/v1/")
		if op == r.URL.Path {
			op = "other"
		}
		m.inflight.Inc()
		start := time.Now()
		cb := &countBody{ReadCloser: r.Body}
		r.Body = cb
		defer func() {
			m.inflight.Dec()
			m.handler.WithLabelValues(op).Observe(time.Since(start).Seconds())
			m.body.WithLabelValues(op).Observe(float64(cb.n))
		}()
		next.ServeHTTP(w, r)
	})
}

type countBody struct {
	io.ReadCloser
	n int64
}

func (c *countBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n += int64(n)
	return n, err
}

// Call records one DynamoDB SDK call that began at start.
func (m *Metrics) Call(call string, start time.Time) {
	if m != nil {
		m.dyn.WithLabelValues(call).Observe(time.Since(start).Seconds())
	}
}

// Prep records the time of a commit stage ("reads" or "decide_encode").
func (m *Metrics) Prep(stage string, d time.Duration) {
	if m != nil {
		m.prep.WithLabelValues(stage).Observe(d.Seconds())
	}
}

// Retry counts an internal retry of op for reason.
func (m *Metrics) Retry(op, reason string) {
	if m != nil {
		m.retries.WithLabelValues(op, reason).Inc()
	}
}

// Attempt counts one attempt of op.
func (m *Metrics) Attempt(op string) {
	if m != nil {
		m.attempts.WithLabelValues(op).Inc()
	}
}
