// Package cpobs is the control-plane service's Prometheus instrumentation:
// HTTP handler time, DynamoDB call time, in-flight requests and request sizes. It lives at the
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
	"slices"
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
	body     *prometheus.HistogramVec // op
	inflight prometheus.Gauge

	// AWS measures every DynamoDB HTTP attempt (manicp_dynamo_*): pass
	// AWS.APIOptions() to the client.
	AWS *awsobs.Recorder

	consumed  *prometheus.CounterVec // op, kind
	noCap     *prometheus.CounterVec // op
	callsDone *prometheus.CounterVec // op, outcome

	log *slog.Logger
	sum summary
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
	m := &Metrics{reg: prometheus.NewRegistry(), log: log}
	m.registerHTTP(secs)
	m.registerDynamo(secs)
	m.AWS = awsobs.New(m.reg, "manicp", "dynamo", log)
	m.reg.MustRegister(m.consumed, m.noCap, m.callsDone, m.handler, m.dyn, m.body, m.inflight,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func hist(name, help, label string, b []float64) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "manicp", Name: name, Help: help, Buckets: b}, []string{label})
}

func (m *Metrics) registerHTTP(secs []float64) {
	m.handler = hist("handler_seconds", "Whole HTTP handler time (body read to response written), by op.", "op", secs)
	m.body = hist("request_body_bytes", "HTTP request body size, by op.", "op", prometheus.ExponentialBuckets(64, 4, 10))
	m.inflight = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "manicp", Name: "inflight_requests", Help: "HTTP requests being served."})
	m.callsDone = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Name: "calls_total",
		Help: "HTTP calls served, by op and outcome (ok, the business error code such as ErrConflict, http_503, http_500)."}, []string{"op", "outcome"})
}

func (m *Metrics) registerDynamo(secs []float64) {
	m.dyn = hist("dynamodb_call_seconds", "Time inside one DynamoDB SDK call (TransactWriteItems, GetItem, BatchGetItem, ...).", "call", secs)
	m.consumed = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "dynamo", Name: "consumed_capacity_units_total",
		Help: "Capacity units DynamoDB reported (ReturnConsumedCapacity=TOTAL), by call and kind (read, write). Absent from servers that do not report it."},
		[]string{"op", "kind"})
	m.noCap = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "dynamo", Name: "consumed_capacity_missing_total",
		Help: "Successful calls whose response carried no ConsumedCapacity (ExtendDB, local emulators), by call."}, []string{"op"})
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
	mu      sync.Mutex
	segment string
	dynamo  time.Duration
	units   float64
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

func (c *Call) addDynamo(d time.Duration, units float64) {
	if c != nil {
		c.mu.Lock()
		c.dynamo += d
		c.units += units
		c.mu.Unlock()
	}
}

// summaryEvery is the least time between two cp_summary lines.
const summaryEvery = time.Second

// summary aggregates /v1 calls into one INFO line per summaryEvery. There is
// no timer: the line is written by the first request that finds the interval
// over, so an idle process logs nothing and the last partial interval of a
// run is not written (the metrics have it).
type summary struct {
	mu    sync.Mutex
	start time.Time
	ops   map[string]*opSum
}

type opSum struct {
	n, notOK int64
	sum, max time.Duration
}

func (s *summary) add(log *slog.Logger, op, outcome string, el time.Duration, now time.Time) {
	s.mu.Lock()
	if s.ops == nil {
		s.ops, s.start = map[string]*opSum{}, now
	}
	o := s.ops[op]
	if o == nil {
		o = &opSum{}
		s.ops[op] = o
	}
	o.n++
	o.sum += el
	o.max = max(o.max, el)
	if outcome != "ok" {
		o.notOK++
	}
	if now.Sub(s.start) < summaryEvery {
		s.mu.Unlock()
		return
	}
	window, ops := now.Sub(s.start), s.ops
	s.ops, s.start = map[string]*opSum{}, now
	s.mu.Unlock()
	names := make([]string, 0, len(ops))
	for k := range ops {
		names = append(names, k)
	}
	slices.Sort(names)
	attrs := []any{"event", "cp_summary", "window_ms", ms(window)}
	for _, k := range names {
		o := ops[k]
		attrs = append(attrs, k, map[string]any{"calls": o.n, "not_ok": o.notOK,
			"mean_handler_ms": ms(o.sum / time.Duration(o.n)), "max_handler_ms": ms(o.max)})
	}
	log.Info("cp summary", attrs...)
}

// Middleware times every request, counts it in flight, records its body size
// and logs each /v1 call (DEBUG; WARN on HTTP 5xx) plus a sampled INFO summary.
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
			// Audit item 9: a consume stage is tens of thousands of requests a
			// second, so the per-request line is DEBUG; failures stay WARN and
			// an INFO summary (at most one a second) carries the counts.
			lvl := slog.LevelDebug
			if sw.status >= 500 {
				lvl = slog.LevelWarn
			}
			m.sum.add(m.log, op, outcome, el, time.Now())
			m.log.Log(r.Context(), lvl, "cp call", "event", "cp_call", "op", op, "segment", call.segment, "outcome", outcome,
				"http_status", sw.status, "handler_ms", ms(el), "dynamo_ms", ms(call.dynamo), "consumed_units", call.units,
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
