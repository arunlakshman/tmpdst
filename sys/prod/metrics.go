package prod

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"manifester/core"
)

// Metrics turns the broker's timing facts into Prometheus collectors. The
// broker core knows nothing of Prometheus (decision 36): it emits facts whose
// durations come from event times, and this observer, at the non-deterministic
// edge, counts them. Pass Observe as NodeConfig.Facts (or call it from a
// wrapper) and serve Handler.
//
// Observe is called from the broker's single loop goroutine; the collectors are
// safe to scrape concurrently.
type Metrics struct {
	reg *prometheus.Registry

	putSeconds, commitSeconds, waitSeconds, ackSeconds, ageSeconds prometheus.Histogram
	inflightAtSeal, partitions                                     prometheus.Histogram

	sealed        *prometheus.CounterVec
	sealedBytes   prometheus.Counter
	committedSegs prometheus.Counter
	committedByte prometheus.Counter
	failedSegs    prometheus.Counter
	putAttempts   prometheus.Counter
	pausedSeconds prometheus.Counter

	buffered, inflight, paused prometheus.Gauge

	cpCall *prometheus.HistogramVec
}

const metricsNS = "manibroker"

// NewMetrics registers every collector on a private registry (plus the Go
// runtime and process collectors).
func NewMetrics() *Metrics {
	secs := prometheus.ExponentialBuckets(0.005, 2, 14) // 5 ms .. 41 s
	hist := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: metricsNS, Name: name, Help: help, Buckets: secs})
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNS, Name: name, Help: help})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNS, Name: name, Help: help})
	}
	m := &Metrics{
		reg:           prometheus.NewRegistry(),
		putSeconds:    hist("segment_put_seconds", "Seal to successful object PUT, retries and backoff included."),
		commitSeconds: hist("segment_commit_seconds", "First CommitSegment issued to its answer, retries included."),
		waitSeconds: hist("segment_queue_wait_seconds",
			"Uploaded segment waiting for earlier segments to commit (commits run strictly in seal order)."),
		ackSeconds: hist("segment_seal_to_ack_seconds",
			"Seal to commit answer: put + queue wait + commit. Excludes the time the batches sat in the open buffer."),
		ageSeconds: hist("segment_open_age_seconds", "Time from a segment's first batch to its seal."),
		inflightAtSeal: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: metricsNS, Name: "segments_in_flight_at_seal",
			Help:    "Segments sealed and not yet answered, counting the one just sealed. Above 1 means PUTs overlap.",
			Buckets: []float64{1, 2, 3, 4, 6, 8, 12, 16, 32}}),
		partitions: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: metricsNS, Name: "segment_partitions",
			Help:    "Distinct topic-partitions per sealed segment.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 48, 64, 96, 128, 192, 288, 512, 1024, 4096}}),
		sealed: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNS, Name: "segments_sealed_total",
			Help: "Segments sealed, by reason (size, timer, batches, partitions, stop)."}, []string{"reason"}),
		sealedBytes:   counter("segment_sealed_batch_bytes_total", "Produce batch bytes sealed into segments."),
		committedSegs: counter("segments_committed_total", "Segments committed."),
		committedByte: counter("segment_committed_bytes_total", "Object bytes (batch data plus index) of committed segments."),
		failedSegs:    counter("segments_failed_total", "Segments that ended in an error answer."),
		putAttempts:   counter("segment_put_attempts_total", "Object PUT attempts that ended in success, counting retries."),
		pausedSeconds: counter("reads_paused_seconds_total", "Time client reads were paused by backpressure."),
		buffered:      gauge("buffered_bytes", "Produce bytes admitted and not yet answered, as of the last segment event."),
		inflight:      gauge("segments_in_flight", "Sealed segments not yet answered, as of the last segment event."),
		paused:        gauge("reads_paused", "1 while client reads are paused by backpressure."),
	}
	m.cpCall = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: metricsNS, Name: "cp_call_seconds",
		Help:    "Control-plane HTTP exchange as the broker sees it (send to full response read), by op.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}, []string{"op"})
	m.reg.MustRegister(m.cpCall, m.putSeconds, m.commitSeconds, m.waitSeconds, m.ackSeconds, m.ageSeconds, m.inflightAtSeal, m.partitions,
		m.sealed, m.sealedBytes, m.committedSegs, m.committedByte, m.failedSegs, m.putAttempts, m.pausedSeconds,
		m.buffered, m.inflight, m.paused,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveCPCall records one control-plane exchange (wire to cpclient.Client.Observe).
func (m *Metrics) ObserveCPCall(op string, d time.Duration) {
	m.cpCall.WithLabelValues(op).Observe(d.Seconds())
}

func seconds(ns int64) float64 { return float64(ns) / 1e9 }

// Observe consumes one broker fact; facts it does not know are ignored.
func (m *Metrics) Observe(f core.Fact) {
	n := func(k string) int64 {
		v, _ := f.Field(k).(core.I64)
		return int64(v)
	}
	switch f.Name {
	case "segment_sealed":
		reason := "unknown"
		if s, ok := f.Field("reason").(core.Str); ok {
			reason = string(s)
		}
		m.sealed.WithLabelValues(reason).Inc()
		m.sealedBytes.Add(float64(n("bytes")))
		m.ageSeconds.Observe(seconds(n("age")))
		m.inflightAtSeal.Observe(float64(n("inflight")))
		m.partitions.Observe(float64(n("parts")))
		m.inflight.Set(float64(n("inflight")))
		m.buffered.Set(float64(n("buffered")))
	case "segment_put_done":
		m.putSeconds.Observe(seconds(n("put_ns")))
		m.putAttempts.Add(float64(n("attempts")))
	case "segment_committed":
		m.commitSeconds.Observe(seconds(n("commit_ns")))
		m.waitSeconds.Observe(seconds(n("wait_ns")))
		m.ackSeconds.Observe(seconds(n("total_ns")))
		m.committedSegs.Inc()
		m.committedByte.Add(float64(n("bytes")))
		m.inflight.Set(float64(n("inflight")))
		m.buffered.Set(float64(n("buffered")))
	case "segment_failed":
		m.failedSegs.Inc()
		m.ackSeconds.Observe(seconds(n("total_ns")))
		m.inflight.Set(float64(n("inflight")))
		m.buffered.Set(float64(n("buffered")))
	case "backpressure_on":
		m.paused.Set(1)
		m.buffered.Set(float64(n("buffered")))
	case "backpressure_off":
		m.paused.Set(0)
		m.pausedSeconds.Add(seconds(n("paused_ns")))
		m.buffered.Set(float64(n("buffered")))
	}
}

// Registry is the private registry, for adding collectors that need objects
// created after NewMetrics (for example the open-connections gauge).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
