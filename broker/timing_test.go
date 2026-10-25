package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
)

func ns(f core.Fact, k string) int64 { return int64(f.Field(k).(core.I64)) }

// A segment's timing facts add up: put + queue wait + commit = seal-to-commit.
// The second of two segments uploaded together waits for the first one's commit.
func TestSegmentTimingFacts(t *testing.T) {
	n := len(plain(t, "a"))
	cfg := DefaultConfig()
	cfg.MaxSegmentBytes = n // every batch is a segment
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.holdPut = true
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.send(c, verProduce, prodReq(-1, e("t", 1, plain(t, "b"))))
	h.now += core.Time(40 * core.Millisecond) // the PUTs take a while
	h.holdPut = false
	h.releaseHeld()
	produced(h.next(c))
	produced(h.next(c))

	puts := factsNamed(h.all, "segment_put_done")
	cs := factsNamed(h.all, "segment_committed")
	if len(puts) != 2 || len(cs) != 2 {
		t.Fatalf("%d put_done, %d committed", len(puts), len(cs))
	}
	for i, f := range cs {
		put, wait, com, total := ns(f, "put_ns"), ns(f, "wait_ns"), ns(f, "commit_ns"), ns(f, "total_ns")
		if put < int64(40*core.Millisecond) || com <= 0 || wait < 0 || put+wait+com != total {
			t.Fatalf("segment %d: put %d wait %d commit %d total %d", i, put, wait, com, total)
		}
		if ns(puts[i], "put_ns") != put || ns(puts[i], "attempts") != 1 || ns(f, "bytes") != ns(puts[i], "bytes") {
			t.Fatalf("segment %d: put_done %+v vs committed %+v", i, puts[i], f)
		}
		if ns(f, "inflight") != int64(1-i) || ns(f, "buffered") < 0 {
			t.Fatalf("segment %d: inflight %d buffered %d", i, ns(f, "inflight"), ns(f, "buffered"))
		}
	}
	if ns(cs[0], "wait_ns") != 0 || ns(cs[1], "wait_ns") <= 0 {
		t.Fatalf("queue wait: first %d, second %d", ns(cs[0], "wait_ns"), ns(cs[1], "wait_ns"))
	}
	h.noViolations()
}

// A retried PUT reports its attempts, and the time includes the backoff.
func TestPutDoneCountsAttempts(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	failed := false
	h.objHook = func(a core.IOAction, run func() core.Completion) core.Completion {
		if p, ok := a.(core.ObjPut); ok && !failed {
			failed = true
			return core.ObjPutDone{Req: p.Req, Err: core.ErrUnavailable}
		}
		return run()
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.flush()
	h.fireAll()
	produced(h.next(c))
	f := factsNamed(h.all, "segment_put_done")
	if len(f) != 1 || ns(f[0], "attempts") != 2 {
		t.Fatalf("%+v", f)
	}
}

// Backpressure facts bracket the pause and report its duration.
func TestBackpressureFacts(t *testing.T) {
	n := len(plain(t, "a"))
	cfg := DefaultConfig()
	cfg.MaxBufferedBytes, cfg.ResumeBufferedBytes = n, 0
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.call(c, verMetadata, kmsg.NewPtrMetadataRequest())
	h.holdCP = true
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.flush()
	h.now += core.Time(30 * core.Millisecond)
	h.holdCP = false
	h.releaseHeld()
	produced(h.next(c))
	on, off := factsNamed(h.all, "backpressure_on"), factsNamed(h.all, "backpressure_off")
	if len(on) != 1 || len(off) != 1 || ns(on[0], "buffered") < int64(n) || ns(off[0], "paused_ns") < int64(30*core.Millisecond) {
		t.Fatalf("on %+v off %+v", on, off)
	}
}
