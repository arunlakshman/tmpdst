package logcp

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the manicp_log_* collectors (docs/observability.md). Every
// method is safe on a nil *Metrics, so the service runs uninstrumented in tests.
type Metrics struct {
	attempts   *prometheus.CounterVec // result
	lost       prometheus.Counter
	slots      prometheus.Counter
	appendSec  prometheus.Histogram
	cmdsPer    prometheus.Histogram
	entryBytes prometheus.Histogram
	holeFills  prometheus.Counter
	pulls      *prometheus.CounterVec // result
	applied    prometheus.Gauge
	lag        prometheus.Gauge
	standbyLag prometheus.Gauge
	applySec   prometheus.Histogram
	windowKeys prometheus.Gauge
	blocked    prometheus.Gauge
	queueDepth prometheus.Gauge
	queueWait  prometheus.Histogram
	rejected   *prometheus.CounterVec // reason
	role       prometheus.Gauge
	roleChange *prometheus.CounterVec // to
	tailChecks *prometheus.CounterVec // result
	freshReads prometheus.Counter
	freshWait  prometheus.Histogram
	ckptSlot   prometheus.Gauge
	ckptAge    prometheus.Gauge
	ckptBytes  prometheus.Gauge
	ckptSec    prometheus.Histogram
	ckptErrs   prometheus.Counter
	trimmed    prometheus.Counter
	idxMark    prometheus.Gauge
	idxLag     prometheus.Gauge
	idxSec     prometheus.Histogram
	idxRows    prometheus.Counter
	idxUnproc  prometheus.Counter
	idxThrot   prometheus.Counter
	idxLost    prometheus.Counter
	idxErrs    prometheus.Counter
}

// NewMetrics registers the collectors on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	secs := prometheus.ExponentialBuckets(0.0005, 2, 16)
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "log", Name: name, Help: help})
	}
	cv := func(name, help, label string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "manicp", Subsystem: "log", Name: name, Help: help}, []string{label})
	}
	g := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "manicp", Subsystem: "log", Name: name, Help: help})
	}
	h := func(name, help string, b []float64) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "manicp", Subsystem: "log", Name: name, Help: help, Buckets: b})
	}
	m := &Metrics{
		attempts:   cv("slot_attempts_total", "Slot append attempts by result: won, lost (another writer had the slot), error (outcome unknown, retried at the same slot).", "result"),
		lost:       c("lost_slots_total", "Slots lost to another writer or a hole filler; the entry is appended again at a later slot. Persistently nonzero means two appenders."),
		slots:      c("slots_total", "Slots this node won. rate() of it is this node's slot rate; the budget is about 200/s for one hot key."),
		appendSec:  h("append_seconds", "One slot append from first attempt to a definite answer, retries of unknown outcomes included.", secs),
		cmdsPer:    h("entries_per_slot", "Commands grouped into one slot. Grows with load; 1 at low load.", prometheus.ExponentialBuckets(1, 2, 9)),
		entryBytes: h("entry_bytes", "Encoded size of a slot's entry (limit 350 KB).", prometheus.ExponentialBuckets(128, 2, 12)),
		holeFills:  c("hole_fills_total", "No-ops this node wrote into an empty slot that blocked apply for HoleAfter."),
		pulls:      cv("tail_reads_total", "Log reads by the tailing loop, by result (ok, error).", "result"),
		applied:    g("applied_slot", "Slots applied (the next slot to apply)."),
		lag:        g("apply_lag_slots", "Slots known to exist but not yet applied (read and waiting behind a hole, or seen at the tail)."),
		standbyLag: g("standby_lag_slots", "Same as apply_lag_slots, exported only while this node is a standby."),
		applySec:   h("apply_seconds", "One apply batch (consecutive slots in one store transaction).", secs),
		windowKeys: g("window_keys", "Segment keys in the idempotency window."),
		blocked:    g("blocked", "1 while the node cannot apply (a slot in an unknown format, or a store failure): it serves 503."),
		queueDepth: g("queue_depth", "Commands waiting for a slot on the active node."),
		queueWait:  h("queue_wait_seconds", "Time a command waited in the queue before its slot append began.", secs),
		rejected:   cv("rejected_total", "Requests refused: not_active, overloaded, blocked, too_large.", "reason"),
		role:       g("active", "1 while this node is the active appender, 0 as a standby."),
		roleChange: cv("role_changes_total", "Role changes, by new role (active, standby).", "to"),
		tailChecks: cv("tail_checks_total", "Tail checks (CheckTail) made for fresh reads, by result.", "result"),
		freshReads: c("fresh_reads_total", "Fresh reads served. Divided by tail_checks_total it is the batching factor."),
		freshWait:  h("fresh_wait_seconds", "Time a fresh read waited for its tail check and for apply to reach it.", secs),
		ckptSlot:   g("checkpoint_slot", "Slot of the newest checkpoint this node knows."),
		ckptAge:    g("checkpoint_age_seconds", "Seconds since the newest checkpoint was written."),
		ckptBytes:  g("checkpoint_bytes", "Size of the newest checkpoint written by this node."),
		ckptSec:    h("checkpoint_seconds", "Time to write one checkpoint (snapshot, S3 put, pointer).", secs),
		ckptErrs:   c("checkpoint_errors_total", "Failed checkpoint attempts."),
		trimmed:    c("trimmed_slots_total", "Log slots deleted behind the checkpoint."),
		idxMark:    g("index_watermark_slot", "Slots below this are in the R# rows."),
		idxLag:     g("index_lag_slots", "Applied slots not yet in the R# rows (index watermark behind applied)."),
		idxSec:     h("index_flush_seconds", "One index flush (plan, BatchWriteItem rows, watermark).", secs),
		idxRows:    c("index_rows_total", "R# rows written."),
		idxUnproc:  c("index_unprocessed_total", "Requests DynamoDB returned in UnprocessedItems (retried)."),
		idxThrot:   c("index_throttles_total", "BatchWriteItem calls throttled and retried."),
		idxLost:    c("index_lost_runs_total", "Commands whose runs could not be indexed because their key had left the window (index stalled for longer than the window)."),
		idxErrs:    c("index_errors_total", "Failed index flushes (retried at the next flush)."),
	}
	reg.MustRegister(m.attempts, m.lost, m.slots, m.appendSec, m.cmdsPer, m.entryBytes, m.holeFills, m.pulls, m.applied, m.lag,
		m.standbyLag, m.applySec, m.windowKeys, m.blocked, m.queueDepth, m.queueWait, m.rejected, m.role, m.roleChange,
		m.tailChecks, m.freshReads, m.freshWait, m.ckptSlot, m.ckptAge, m.ckptBytes, m.ckptSec, m.ckptErrs, m.trimmed,
		m.idxMark, m.idxLag, m.idxSec, m.idxRows, m.idxUnproc, m.idxThrot, m.idxLost, m.idxErrs)
	return m
}

func secs(d time.Duration) float64 { return d.Seconds() }

func (m *Metrics) attempt(result string, d time.Duration, cmds, bytes int) {
	if m == nil {
		return
	}
	m.attempts.WithLabelValues(result).Inc()
	switch result {
	case "won":
		m.appendSec.Observe(secs(d))
		m.slots.Inc()
		m.cmdsPer.Observe(float64(cmds))
		m.entryBytes.Observe(float64(bytes))
	case "lost":
		m.appendSec.Observe(secs(d))
		m.lost.Inc()
	}
}

func (m *Metrics) holeFill() {
	if m != nil {
		m.holeFills.Inc()
	}
}

func (m *Metrics) pull(ok bool) {
	if m == nil {
		return
	}
	r := "ok"
	if !ok {
		r = "error"
	}
	m.pulls.WithLabelValues(r).Inc()
}

// progress reports the replica's position after an apply batch or a pull.
func (m *Metrics) progress(applied, lag int64, standby bool) {
	if m == nil {
		return
	}
	m.applied.Set(float64(applied))
	m.lag.Set(float64(lag))
	if standby {
		m.standbyLag.Set(float64(lag))
	} else {
		m.standbyLag.Set(0)
	}
}

func (m *Metrics) applyBatch(d time.Duration) {
	if m != nil {
		m.applySec.Observe(secs(d))
	}
}

func (m *Metrics) window(keys int) {
	if m != nil {
		m.windowKeys.Set(float64(keys))
	}
}

func (m *Metrics) block(on bool) {
	if m == nil {
		return
	}
	v := 0.0
	if on {
		v = 1
	}
	m.blocked.Set(v)
}

func (m *Metrics) queue(depth int) {
	if m != nil {
		m.queueDepth.Set(float64(depth))
	}
}

func (m *Metrics) queued(d time.Duration) {
	if m != nil {
		m.queueWait.Observe(secs(d))
	}
}

func (m *Metrics) reject(reason string) {
	if m != nil {
		m.rejected.WithLabelValues(reason).Inc()
	}
}

func (m *Metrics) roleIs(active bool) {
	if m == nil {
		return
	}
	to, v := "standby", 0.0
	if active {
		to, v = "active", 1
	}
	m.role.Set(v)
	m.roleChange.WithLabelValues(to).Inc()
}

func (m *Metrics) tailCheck(ok bool) {
	if m == nil {
		return
	}
	r := "ok"
	if !ok {
		r = "error"
	}
	m.tailChecks.WithLabelValues(r).Inc()
}

func (m *Metrics) fresh(d time.Duration) {
	if m != nil {
		m.freshReads.Inc()
		m.freshWait.Observe(secs(d))
	}
}

func (m *Metrics) checkpoint(slot int64, bytes int, d time.Duration) {
	if m != nil {
		m.ckptSlot.Set(float64(slot))
		m.ckptAge.Set(0)
		m.ckptBytes.Set(float64(bytes))
		m.ckptSec.Observe(secs(d))
	}
}

func (m *Metrics) checkpointAge(d time.Duration) {
	if m != nil {
		m.ckptAge.Set(secs(d))
	}
}

func (m *Metrics) checkpointErr() {
	if m != nil {
		m.ckptErrs.Inc()
	}
}

func (m *Metrics) trim(n int) {
	if m != nil {
		m.trimmed.Add(float64(n))
	}
}

func (m *Metrics) indexed(mark, applied int64, rows int, d time.Duration) {
	if m != nil {
		m.idxMark.Set(float64(mark))
		m.idxLag.Set(float64(max(applied-mark, 0)))
		m.idxRows.Add(float64(rows))
		m.idxSec.Observe(secs(d))
	}
}

func (m *Metrics) indexLag(mark, applied int64) {
	if m != nil {
		m.idxMark.Set(float64(mark))
		m.idxLag.Set(float64(max(applied-mark, 0)))
	}
}

func (m *Metrics) indexUnprocessed(n int) {
	if m != nil {
		m.idxUnproc.Add(float64(n))
	}
}

func (m *Metrics) indexThrottle() {
	if m != nil {
		m.idxThrot.Inc()
	}
}

func (m *Metrics) indexLost(n int) {
	if m != nil {
		m.idxLost.Add(float64(n))
	}
}

func (m *Metrics) indexErr() {
	if m != nil {
		m.idxErrs.Inc()
	}
}
