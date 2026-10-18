// Package cpobs is the control-plane service's Prometheus instrumentation:
// HTTP handler time, DynamoDB call time, commit preparation time, retries and
// cancellations, in-flight requests and request sizes. It lives at the
// non-deterministic edge (wall clocks, atomics, no core import) and every
// method is safe on a nil *Metrics, so stores and handlers run uninstrumented.
package cpobs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"manifester/sys/awsobs"
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

	// AWS measures every DynamoDB HTTP attempt (manicp_dynamo_*): pass
	// AWS.APIOptions() to the client.
	AWS *awsobs.Recorder

	consumed  *prometheus.CounterVec // op, kind
	noCap     *prometheus.CounterVec // op
	cancels   *prometheus.CounterVec // code
	callsDone *prometheus.CounterVec // op, outcome

	log *slog.Logger
}

// New is NewWithLogger(nil).
func New() *Metrics { return NewWithLogger(nil) }

// NewWithLogger registers every collector on a private registry (plus Go
// runtime and process collectors). log (default slog.Default) receives one
// line per CP call and the warnings; see docs/observability.md.
func NewWithLogger(log *slog.Logger) *Metrics {
	if log == nil {
		log = slog.Default()
	}
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
		log:      log,
	}
	m.consumed = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "dynamo", Name: "consumed_capacity_units_total",
		Help: "Capacity units DynamoDB reported (ReturnConsumedCapacity=TOTAL), by call and kind (read, write). Absent from servers that do not report it."},
		[]string{"op", "kind"})
	m.noCap = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "dynamo", Name: "consumed_capacity_missing_total",
		Help: "Successful calls whose response carried no ConsumedCapacity (ExtendDB, local emulators), by call."}, []string{"op"})
	m.cancels = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "dynamo", Name: "txn_cancel_reasons_total",
		Help: "Per-item cancellation reasons of TransactionCanceledException (ConditionalCheckFailed, TransactionConflict, ThrottlingError, ProvisionedThroughputExceeded, ...)."},
		[]string{"code"})
	m.callsDone = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Name: "calls_total",
		Help: "HTTP calls served, by op and outcome (ok, the business error code such as ErrConflict, http_503, http_500)."}, []string{"op", "outcome"})
	m.AWS = awsobs.New(m.reg, "manicp", "dynamo", log)
	m.AWS.QuietCodes = map[string]bool{"TransactionCanceledException": true} // reported by CommitSegment with reasons
	m.reg.MustRegister(m.consumed, m.noCap, m.cancels, m.callsDone, m.handler, m.dyn, m.prep, m.body, m.retries, m.attempts, m.inflight,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler { return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}) }

// Registry is the private registry (for build_info and similar).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// APIOptions are the SDK middleware for the DynamoDB client (nil-safe).
func (m *Metrics) APIOptions() []func(*middleware.Stack) error {
	if m == nil {
		return nil
	}
	return m.AWS.APIOptions()
}

// Logger is the logger of CP lines (slog.Default on a nil Metrics).
func (m *Metrics) Logger() *slog.Logger {
	if m == nil || m.log == nil {
		return slog.Default()
	}
	return m.log
}

// Call is what one HTTP request to the control plane did inside the store. The
// middleware creates it, the store fills it through the request context, and
// the middleware logs it as the request's one line. All methods are safe on nil.
type Call struct {
	mu       sync.Mutex
	segment  string
	attempts int
	dynamo   time.Duration
	retries  []string
	cancels  []string
	units    float64
}

type callKey struct{}

// FromContext is the request's Call, or nil outside the middleware.
func FromContext(ctx context.Context) *Call {
	c, _ := ctx.Value(callKey{}).(*Call)
	return c
}

// SetSegment names the segment the request commits.
func (c *Call) SetSegment(k string) {
	if c != nil {
		c.mu.Lock()
		c.segment = k
		c.mu.Unlock()
	}
}

func (c *Call) attempt() {
	if c != nil {
		c.mu.Lock()
		c.attempts++
		c.mu.Unlock()
	}
}

func (c *Call) addDynamo(d time.Duration, units float64) {
	if c != nil {
		c.mu.Lock()
		c.dynamo += d
		c.units += units
		c.mu.Unlock()
	}
}

func (c *Call) retry(reason string, cancels []string) {
	if c != nil {
		c.mu.Lock()
		c.retries = append(c.retries, reason)
		c.cancels = append(c.cancels, cancels...)
		c.mu.Unlock()
	}
}

// Middleware times every request, counts it in flight, records its body size
// and logs one line per /v1 call: op, attempts, handler_ms, dynamo_ms, outcome.
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
		call := &Call{}
		sw := &statusWriter{ResponseWriter: w, status: 200}
		defer func() {
			el := time.Since(start)
			m.inflight.Dec()
			m.handler.WithLabelValues(op).Observe(el.Seconds())
			m.body.WithLabelValues(op).Observe(float64(cb.n))
			if op == "other" {
				return
			}
			outcome := sw.outcome()
			m.callsDone.WithLabelValues(op, outcome).Inc()
			call.mu.Lock()
			defer call.mu.Unlock()
			attempts := call.attempts
			if attempts == 0 {
				attempts = 1
			}
			lvl := slog.LevelInfo
			if sw.status >= 500 {
				lvl = slog.LevelWarn
			}
			m.log.Log(r.Context(), lvl, "cp call", "event", "cp_call", "op", op, "segment", call.segment, "outcome", outcome,
				"http_status", sw.status, "attempts", attempts, "retries", len(call.retries),
				"handler_ms", ms(el), "dynamo_ms", ms(call.dynamo), "consumed_units", call.units,
				"request_bytes", cb.n, "response_bytes", sw.n)
		}()
		next.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), callKey{}, call)))
	})
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// statusWriter remembers the status and (small) body so the middleware can
// report the business error code of an HTTP 200 answer.
type statusWriter struct {
	http.ResponseWriter
	status int
	n      int64
	body   bytes.Buffer
}

func (w *statusWriter) WriteHeader(c int) {
	w.status = c
	w.ResponseWriter.WriteHeader(c)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.body.Len() < 1<<20 {
		w.body.Write(p)
	}
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *statusWriter) outcome() string {
	if w.status != 200 {
		return "http_" + strconv.Itoa(w.status)
	}
	var r struct {
		Err string `json:"err"`
	}
	if json.Unmarshal(w.body.Bytes(), &r) == nil && r.Err != "" {
		return r.Err
	}
	return "ok"
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

// Call records one DynamoDB SDK call that began at start. Use CallCtx inside
// a request so the time is also added to the request's log line.
func (m *Metrics) Call(call string, start time.Time) {
	m.CallCtx(context.Background(), call, start, nil)
}

// CallCtx is Call plus the request's own tally. cc is the response's
// ConsumedCapacity: nil from a server that does not report it.
func (m *Metrics) CallCtx(ctx context.Context, call string, start time.Time, cc []types.ConsumedCapacity) {
	if m == nil {
		return
	}
	d := time.Since(start)
	m.dyn.WithLabelValues(call).Observe(d.Seconds())
	units := m.consume(call, cc)
	FromContext(ctx).addDynamo(d, units)
}

// consume counts reported capacity; the kind comes from the call.
func (m *Metrics) consume(call string, cc []types.ConsumedCapacity) float64 {
	if len(cc) == 0 {
		m.noCap.WithLabelValues(call).Inc()
		return 0
	}
	kind := "read"
	if call == "TransactWriteItems" || call == "PutItem" || call == "UpdateItem" || call == "DeleteItem" {
		kind = "write"
	}
	var total float64
	for _, c := range cc {
		switch {
		case c.CapacityUnits != nil:
			total += *c.CapacityUnits
		case kind == "write" && c.WriteCapacityUnits != nil:
			total += *c.WriteCapacityUnits
		case c.ReadCapacityUnits != nil:
			total += *c.ReadCapacityUnits
		}
	}
	m.consumed.WithLabelValues(call, kind).Add(total)
	return total
}

// Prep records the time of a commit stage ("reads" or "decide_encode").
func (m *Metrics) Prep(stage string, d time.Duration) {
	if m != nil {
		m.prep.WithLabelValues(stage).Observe(d.Seconds())
	}
}

// Retry counts an internal retry of op for reason. cancels are the
// per-item cancellation codes of a TransactionCanceledException, if any.
func (m *Metrics) Retry(op, reason string) { m.RetryCtx(context.Background(), op, reason, nil) }

// RetryCtx is Retry plus the request's own tally.
func (m *Metrics) RetryCtx(ctx context.Context, op, reason string, cancels []string) {
	if m == nil {
		return
	}
	m.retries.WithLabelValues(op, reason).Inc()
	for _, c := range cancels {
		m.cancels.WithLabelValues(c).Inc()
	}
	FromContext(ctx).retry(reason, cancels)
}

// Attempt counts one attempt of op.
func (m *Metrics) Attempt(op string) { m.AttemptCtx(context.Background(), op) }

// AttemptCtx is Attempt plus the request's own tally.
func (m *Metrics) AttemptCtx(ctx context.Context, op string) {
	if m != nil {
		m.attempts.WithLabelValues(op).Inc()
		FromContext(ctx).attempt()
	}
}
