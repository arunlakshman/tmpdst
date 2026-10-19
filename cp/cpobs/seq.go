package cpobs

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// seqMetrics are the sequencer's collectors (manicp_seq_*): the queue in
// front of the single sequencing loop, the groups it forms, the transactions
// it runs and its in-memory state. All methods are nil-safe like the rest.
type seqMetrics struct {
	depth    prometheus.Gauge
	inflight prometheus.Gauge
	wait     prometheus.Histogram
	segs     prometheus.Histogram
	items    prometheus.Histogram
	tx       *prometheus.HistogramVec // outcome
	cache    *prometheus.CounterVec   // cache, result
	retries  *prometheus.CounterVec   // reason
}

func newSeqMetrics(reg *prometheus.Registry, secs []float64) *seqMetrics {
	h := func(name, help string, b []float64) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "manicp", Subsystem: "seq", Name: name, Help: help, Buckets: b})
	}
	g := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "manicp", Subsystem: "seq", Name: name, Help: help})
	}
	s := &seqMetrics{
		depth:    g("queue_depth", "Commit requests waiting for the sequencer."),
		inflight: g("inflight_groups", "Group transactions in flight (at most -seq-pipeline)."),
		wait:     h("queue_wait_seconds", "Time a commit request waited in the sequencer queue before its group was formed.", secs),
		segs:     h("group_segments", "Segments (commit requests) per group transaction.", prometheus.LinearBuckets(1, 1, 24)),
		items:    h("group_items", "Items per group transaction (limit 100).", prometheus.LinearBuckets(5, 5, 20)),
		tx: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "manicp", Subsystem: "seq", Name: "tx_seconds",
			Help: "TransactWriteItems time of a group, by outcome (ok, canceled, unavailable, unknown, error).", Buckets: secs}, []string{"outcome"}),
		cache: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "seq", Name: "cache_total",
			Help: "In-memory state lookups by cache (end, topic, segment) and result (hit, miss). A miss costs a read."}, []string{"cache", "result"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "seq", Name: "retries_total",
			Help: "Group retries by reason (transaction_canceled, unavailable, unknown_outcome, read_failed). One count per group, not per segment."}, []string{"reason"}),
	}
	reg.MustRegister(s.depth, s.inflight, s.wait, s.segs, s.items, s.tx, s.cache, s.retries)
	return s
}

// SeqQueueDepth sets the number of queued commit requests.
func (m *Metrics) SeqQueueDepth(n int) {
	if m != nil {
		m.seq.depth.Set(float64(n))
	}
}

// SeqInflight sets the number of group transactions in flight.
func (m *Metrics) SeqInflight(n int) {
	if m != nil {
		m.seq.inflight.Set(float64(n))
	}
}

// SeqQueueWait records how long a request waited for its group.
func (m *Metrics) SeqQueueWait(d time.Duration) {
	if m != nil {
		m.seq.wait.Observe(d.Seconds())
	}
}

// SeqGroup records the size of a group when it is formed.
func (m *Metrics) SeqGroup(segments, items int) {
	if m != nil {
		m.seq.segs.Observe(float64(segments))
		m.seq.items.Observe(float64(items))
	}
}

// SeqTx records one group transaction.
func (m *Metrics) SeqTx(outcome string, d time.Duration) {
	if m != nil {
		m.seq.tx.WithLabelValues(outcome).Observe(d.Seconds())
	}
}

// SeqCache counts a state lookup.
func (m *Metrics) SeqCache(cache string, hit bool) {
	if m != nil {
		r := "miss"
		if hit {
			r = "hit"
		}
		m.seq.cache.WithLabelValues(cache, r).Inc()
	}
}

// SeqRetry counts a group retry.
func (m *Metrics) SeqRetry(reason string) {
	if m != nil {
		m.seq.retries.WithLabelValues(reason).Inc()
	}
}

// TallyDynamo adds DynamoDB time spent on behalf of a request to its log
// line (a group's transaction is charged to every request in it).
func (m *Metrics) TallyDynamo(ctx context.Context, d time.Duration) {
	if m != nil {
		FromContext(ctx).addDynamo(d, 0)
	}
}
