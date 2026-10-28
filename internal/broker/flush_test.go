package broker

import (
	"testing"

	"manifester/internal/core"
)

func flushTimers(h *harness) []core.SetTimer {
	var out []core.SetTimer
	for _, a := range h.all {
		if s, ok := a.(core.SetTimer); ok && s.ID == timerFlush {
			out = append(out, s)
		}
	}
	return out
}

func sealReasons(h *harness) []string {
	var out []string
	for _, f := range factsNamed(h.all, "segment_sealed") {
		out = append(out, string(f.Field("reason").(core.Str)))
	}
	return out
}

func TestDefaultFlushRule(t *testing.T) {
	d := DefaultConfig()
	if d.MaxSegmentBytes != 16<<20 || d.BatchWindow != 200*core.Millisecond || d.MaxSegmentBatches != 256 || d.MaxSegmentPartitions != 4096 {
		t.Fatalf("%+v", d)
	}
	z := Config{}.withDefaults()
	if z.MaxSegmentBytes != d.MaxSegmentBytes || z.MaxSegmentBatches != 256 || z.BatchWindow != d.BatchWindow {
		t.Fatalf("zero config: %+v", z)
	}
}

// The size rule seals when the open segment reaches the cap, not before.
func TestSizeRuleSealsAtExactlyTheCap(t *testing.T) {
	n := len(plain(t, "a"))
	cfg := DefaultConfig()
	cfg.MaxSegmentBytes = 3 * n
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a")), e("t", 0, plain(t, "a"))))
	if len(h.puts) != 0 {
		t.Fatalf("sealed below the cap (%d of %d bytes)", 2*n, 3*n)
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	if len(h.puts) != 1 {
		t.Fatalf("not sealed at exactly the cap: %d puts", len(h.puts))
	}
	if r := sealReasons(h); len(r) != 1 || r[0] != "size" {
		t.Fatalf("reasons %v", r)
	}
	produced(h.next(c))
	produced(h.next(c))
}

// A batch larger than the cap is sealed alone, at once.
func TestOversizeBatchSealedAlone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSegmentBytes = 10
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	if len(h.puts) != 1 {
		t.Fatalf("%d puts", len(h.puts))
	}
	produced(h.next(c))
}

// The window runs from the first batch; later batches neither re-arm nor
// extend it, and the timer seals the segment.
func TestTimerRuleFromFirstBatch(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.send(c, verProduce, prodReq(-1, e("t", 1, plain(t, "b"))))
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "c"))))
	ts := flushTimers(h)
	if len(ts) != 1 || ts[0].After != 200*core.Millisecond {
		t.Fatalf("flush timers %+v (want one of 200ms)", ts)
	}
	if len(h.puts) != 0 {
		t.Fatal("sealed before the timer")
	}
	h.flush()
	if len(h.puts) != 1 {
		t.Fatalf("%d puts after the timer", len(h.puts))
	}
	f := factsNamed(h.all, "segment_sealed")
	if len(f) != 1 || f[0].Field("reason").(core.Str) != "timer" || f[0].Field("batches").(core.I64) != 3 {
		t.Fatalf("%+v", f)
	}
	if age := f[0].Field("age").(core.I64); age <= 0 {
		t.Fatalf("age %d", age)
	}
	for range 3 {
		produced(h.next(c))
	}
}

// The timer re-arms for the next segment.
func TestFlushTimerRearms(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.produceOne(c, e("t", 0, plain(t, "a")))
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "b"))))
	ts := flushTimers(h)
	if len(ts) != 2 || ts[1].Gen == ts[0].Gen || ts[1].After != 200*core.Millisecond {
		t.Fatalf("%+v", ts)
	}
	if _, ok := h.timers[timerFlush]; !ok {
		t.Fatal("not armed")
	}
	h.flush()
	if len(h.puts) != 2 {
		t.Fatalf("%d puts", len(h.puts))
	}
	produced(h.next(c))
}

func TestBatchCapSeals(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSegmentBatches = 32
	h := startedHarness(t, cfg)
	c := h.open(1)
	var es []ent
	for i := range 31 {
		es = append(es, e("t", int32(i%2), plain(t, "a")))
	}
	h.send(c, verProduce, prodReq(-1, es...))
	if len(h.puts) != 0 {
		t.Fatal("sealed below the batch cap")
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	if len(h.puts) != 1 {
		t.Fatalf("batch cap: %d puts", len(h.puts))
	}
	if r := sealReasons(h); len(r) != 1 || r[0] != "batches" {
		t.Fatalf("reasons %v", r)
	}
	h.next(c)
	h.next(c)
}

func TestPartitionCapReason(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSegmentPartitions = 2
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a")), e("t", 0, plain(t, "a")), e("t", 1, plain(t, "a"))))
	if r := sealReasons(h); len(r) != 1 || r[0] != "partitions" || len(h.puts) != 1 {
		t.Fatalf("reasons %v puts %d", r, len(h.puts))
	}
	produced(h.next(c))
}

// A control plane that refuses the segment as too large is not retried: the
// batches fail with UNKNOWN_SERVER_ERROR and the broker flags the bug.
func TestCommitTooLargeFailsBatches(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	commits := 0
	h.cpHook = func(a core.ControlPlaneCall, run func() core.IOResult) core.IOResult {
		if _, ok := a.Op.(core.CommitSegment); ok {
			commits++
			return core.ControlPlaneDone{Req: a.Req, Err: core.ErrTooLarge, Outcome: core.NotApplied}
		}
		return run()
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a")), e("t", 1, plain(t, "b"))))
	h.flush()
	h.fireAll()
	for _, p := range produced(h.next(c)) {
		if kafkaErrorCode(p.ErrorCode) != errUnknownServer || p.BaseOffset != -1 {
			t.Fatalf("code %d offset %d", p.ErrorCode, p.BaseOffset)
		}
	}
	if commits != 1 {
		t.Fatalf("%d commits: a too-large segment must not be retried", commits)
	}
	if len(h.env.viol) != 1 || h.env.viol[0] != "broker: segment exceeds the control plane's commit size" {
		t.Fatalf("assertions %v", h.env.viol)
	}
	if h.b.bufBytes != 0 || len(h.b.segs) != 0 {
		t.Fatalf("buffered %d, segments %d", h.b.bufBytes, len(h.b.segs))
	}
	if h.cp.End(core.NewTopicPartition("t", 0)) != 0 {
		t.Fatal("refused segment changed the log")
	}
}

// A graceful stop flushes the open segment before the timer.
func TestStopFlushesOpenSegment(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	if len(h.puts) != 0 {
		t.Fatal("sealed early")
	}
	h.do(core.StopRequest{})
	if len(h.puts) != 1 || !h.exited {
		t.Fatalf("puts %d exited %v", len(h.puts), h.exited)
	}
	if r := sealReasons(h); len(r) != 1 || r[0] != "stop" {
		t.Fatalf("reasons %v", r)
	}
	if r := produced(h.next(c))[0]; r.ErrorCode != 0 {
		t.Fatal(r.ErrorCode)
	}
}

// With the CP storing one range run per partition and segment (decision 39),
// many small batches no longer hit the batch cap at 32: the segment seals at
// the byte cap.
func TestManySmallBatchesSealAtByteCapNotBatchCap(t *testing.T) {
	n := len(plain(t, "a"))
	cfg := DefaultConfig()
	if cfg.MaxSegmentBatches < 200 {
		t.Fatalf("default batch cap %d", cfg.MaxSegmentBatches)
	}
	cfg.MaxSegmentBytes = 200 * n
	h := startedHarness(t, cfg)
	c := h.open(1)
	var es []ent
	for i := range 199 {
		es = append(es, e("t", int32(i%2), plain(t, "a")))
	}
	h.send(c, verProduce, prodReq(-1, es...))
	if len(h.puts) != 0 {
		t.Fatalf("sealed below the byte cap with %d puts", len(h.puts))
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	if r := sealReasons(h); len(h.puts) != 1 || len(r) != 1 || r[0] != "size" {
		t.Fatalf("reasons %v puts %d", r, len(h.puts))
	}
	f := factsNamed(h.all, "segment_sealed")[0]
	if b := f.Field("batches").(core.I64); b != 200 {
		t.Fatalf("%d batches in the segment", b)
	}
	if n := f.Field("parts").(core.I64); n != 2 {
		t.Fatalf("%d partitions in the segment, want 2", n)
	}
	h.next(c)
	h.next(c)
}
