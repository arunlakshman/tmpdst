package fakenetwork

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// ---- test world ----

// harness is a kernel world with the callers a, b and c and the network service. It
// keeps a view of what the network delivered: bytes, closes, opens and answers.
type harness struct {
	*servicetest.Harness
	t   *testing.T
	eng *Engine
	rnd *rng.Rand
	// the view, built from Delivered by sync
	seen   int
	log    []string
	data   map[string][]byte // received bytes per (node, conn)
	closed map[string]core.ConnClosed
	opened map[string]core.ConnOpened
	dials  map[machine.RequestID]core.ConnectDone
	sends  map[machine.RequestID]core.SendDone
	reqN   machine.RequestID
}

func newHarness(t *testing.T, seed uint64, f Faults) *harness {
	t.Helper()
	th := servicetest.New(t, seed, "a", "b", "c")
	h := &harness{Harness: th, t: t, eng: New(th.W, f), rnd: th.W.Rand(),
		data: map[string][]byte{}, closed: map[string]core.ConnClosed{},
		opened: map[string]core.ConnOpened{}, dials: map[machine.RequestID]core.ConnectDone{}, sends: map[machine.RequestID]core.SendDone{}}
	for _, n := range []string{"a", "b", "c"} {
		h.eng.AddNode(th.Addr(n))
	}
	svc := kernel.AddService[Call, core.EventData](th.W, h.eng)
	th.Route = func(r *servicetest.Router, out core.Action) {
		switch x := out.(type) {
		case core.Connect:
			kernel.Call(r, svc, Call(Dial{x}))
		case core.Send:
			kernel.Call(r, svc, Call(Write{x}))
		case core.CloseConn:
			kernel.Call(r, svc, Call(Close{x}))
		case core.PauseReads:
			kernel.Call(r, svc, Call(Pause{x}))
		case core.ResumeReads:
			kernel.Call(r, svc, Call(Resume{x}))
		}
	}
	return h
}

func key(n machine.NodeID, c core.ConnID) string { return fmt.Sprintf("%s/%d", n, c) }

// sync reads the deliveries that came since the last sync into the view.
func (h *harness) sync() {
	for _, d := range h.Delivered[h.seen:] {
		n := d.To
		switch b := d.Body.(type) {
		case core.BytesReceived:
			k := key(n, b.Conn)
			h.data[k] = append(h.data[k], b.Data...)
			h.log = append(h.log, fmt.Sprintf("%d %s bytes %d", d.At, k, len(b.Data)))
		case core.ConnClosed:
			h.closed[key(n, b.Conn)] = b
			h.log = append(h.log, fmt.Sprintf("%d %s closed %v", d.At, key(n, b.Conn), b.Abort))
		case core.ConnOpened:
			h.opened[key(n, b.Conn)] = b
			h.log = append(h.log, fmt.Sprintf("%d %s opened", d.At, key(n, b.Conn)))
		case core.ConnectDone:
			h.dials[b.Req] = b
			h.log = append(h.log, fmt.Sprintf("%d dial %d %v %d", d.At, b.Req, b.Err, b.Conn))
		case core.SendDone:
			h.sends[b.Req] = b
			h.log = append(h.log, fmt.Sprintf("%d send %d %v", d.At, b.Req, b.Err))
		}
	}
	h.seen = len(h.Delivered)
}

func (h *harness) run() {
	h.Run()
	h.sync()
}

// runUntil processes entries up to time t.
func (h *harness) runUntil(t kernel.Time) {
	h.RunTo(t)
	h.sync()
}

func (h *harness) nextRequestID() machine.RequestID { h.reqN++; return h.reqN }

// crash ends the live incarnation of the node.
func (h *harness) crash(n string) { h.Crash(n) }

// boot starts a new incarnation of a node that is down and returns it.
func (h *harness) boot(n string) machine.NodeInstance {
	h.Boot(n)
	return h.Ref(n)
}

func (h *harness) dial(from, addr string) (core.ConnectDone, bool) {
	r := h.nextRequestID()
	h.Issue(from, core.Connect{Req: r, Addr: addr})
	h.run()
	d, ok := h.dials[r]
	return d, ok
}

func (h *harness) send(from string, c core.ConnID, data []byte) machine.RequestID {
	r := h.nextRequestID()
	h.Issue(from, core.Send{Req: r, Conn: c, Data: data})
	return r
}

var lat = kernel.FastWithRareSlow{FastMin: 1000, FastMax: 5000, SlowMin: 100000, SlowMax: 200000, SlowProb: 0.1}

// connect dials a->b and returns both conn ids.
func (h *harness) connect(a, b string) (core.ConnID, core.ConnID) {
	d, ok := h.dial(a, b)
	if !ok || d.Err != core.OK {
		h.t.Fatalf("dial failed: %+v %v", d, ok)
	}
	// listener conn id: the only opened key for b not yet claimed
	cb := h.lastOpened(machine.NodeID(b))
	return d.Conn, cb
}

func (h *harness) lastOpened(n machine.NodeID) core.ConnID {
	var best core.ConnID
	for k := range h.opened {
		var name string
		var c core.ConnID
		for i := len(k) - 1; i >= 0; i-- {
			if k[i] == '/' {
				name = k[:i]
				_, _ = fmt.Sscanf(k[i+1:], "%d", &c) // best-effort parse; c stays zero on malformed keys
				break
			}
		}
		if machine.NodeID(name) == n && c > best {
			best = c
		}
	}
	return best
}

// ---- tests ----

func TestDial(t *testing.T) {
	h := newHarness(t, 1, Faults{Latency: lat})
	d, _ := h.dial("a", "b")
	if d.Err != core.OK || d.Conn == 0 {
		t.Fatalf("dial live: %+v", d)
	}
	if o, ok := h.opened[key("b", h.lastOpened("b"))]; !ok || o.Peer != "a" {
		t.Fatalf("listener not told: %+v", h.opened)
	}
	d2, _ := h.dial("a", "ghost")
	if d2.Err != core.ErrConnClosed {
		t.Fatalf("dial down: %+v", d2)
	}
	h.crash("b")
	d3, _ := h.dial("a", "b")
	if d3.Err != core.ErrConnClosed {
		t.Fatalf("dial crashed: %+v", d3)
	}
}

func TestOrderedDeliveryProperty(t *testing.T) {
	f := Faults{Latency: lat, PartialIOMax: 7, PairLatencyMax: 50000, ClogProb: 0.1, ClogDur: [2]machine.Duration{1000, 300000}}
	for seed := uint64(1); seed <= 300; seed++ {
		h := newHarness(t, seed, f)
		ca, cb := h.connect("a", "b")
		var wantAB, wantBA []byte
		for i := range 20 {
			n := h.rnd.Intn(40)
			buf := make([]byte, n)
			for j := range buf {
				buf[j] = byte(h.rnd.Uint64())
			}
			if i%3 == 0 {
				wantBA = append(wantBA, buf...)
				h.send("b", cb, buf)
			} else {
				wantAB = append(wantAB, buf...)
				h.send("a", ca, buf)
			}
			h.runUntil(h.Now() + kernel.Time(h.rnd.Intn(8000)))
		}
		h.run()
		if !bytes.Equal(h.data[key("b", cb)], wantAB) || !bytes.Equal(h.data[key("a", ca)], wantBA) {
			t.Fatalf("seed %d: stream mismatch", seed)
		}
	}
}

func TestGracefulCloseAfterLastBytes(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		h := newHarness(t, seed, Faults{Latency: lat, PartialIOMax: 3})
		ca, cb := h.connect("a", "b")
		h.send("a", ca, []byte("hello world"))
		h.Issue("a", core.CloseConn{Conn: ca})
		r := h.send("a", ca, []byte("late"))
		h.run()
		if string(h.data[key("b", cb)]) != "hello world" {
			t.Fatalf("seed %d: got %q", seed, h.data[key("b", cb)])
		}
		if c, ok := h.closed[key("b", cb)]; !ok || c.Abort {
			t.Fatalf("seed %d: want graceful close, got %+v %v", seed, c, ok)
		}
		if h.sends[r].Err != core.ErrConnClosed {
			t.Fatalf("send after close: %+v", h.sends[r])
		}
		// the close came after the last byte
		last := ""
		for _, l := range h.log {
			if len(l) > 0 && (contains(l, "b/") && (contains(l, "bytes") || contains(l, "closed"))) {
				last = l
			}
		}
		if !contains(last, "closed") {
			t.Fatalf("seed %d: close not last: %v", seed, h.log)
		}
	}
}

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }

func TestAbortDropsQueued(t *testing.T) {
	h := newHarness(t, 3, Faults{Latency: lat, PartialIOMax: 2})
	ca, cb := h.connect("a", "b")
	h.send("a", ca, bytes.Repeat([]byte("x"), 200))
	h.Issue("a", core.CloseConn{Conn: ca, Abort: true})
	h.run()
	if len(h.data[key("b", cb)]) != 0 {
		t.Fatalf("queued bytes delivered after abort: %d", len(h.data[key("b", cb)]))
	}
	if c := h.closed[key("b", cb)]; !c.Abort {
		t.Fatalf("want abort, got %+v", c)
	}
	if len(h.eng.pairs) != 0 && !h.eng.pairs[0].end[1].closed {
		t.Fatal("peer end not closed")
	}
}

func TestPauseResume(t *testing.T) {
	h := newHarness(t, 4, Faults{Latency: lat, PartialIOMax: 4})
	ca, cb := h.connect("a", "b")
	h.Issue("b", core.PauseReads{Conn: cb})
	r := h.send("a", ca, []byte("0123456789abcdef"))
	h.run()
	if len(h.data[key("b", cb)]) != 0 {
		t.Fatal("delivered while paused")
	}
	if h.sends[r].Err != core.OK {
		t.Fatal("SendDone should still complete while receiver paused")
	}
	h0 := h.eng.StateHash()
	h.Issue("b", core.ResumeReads{Conn: cb})
	h.run()
	if string(h.data[key("b", cb)]) != "0123456789abcdef" {
		t.Fatalf("got %q", h.data[key("b", cb)])
	}
	if h.eng.StateHash() == h0 {
		t.Fatal("hash unchanged by resume")
	}
}

func TestCrashClosesPeers(t *testing.T) {
	h := newHarness(t, 5, Faults{Latency: lat})
	ca, cb := h.connect("a", "b")
	h.send("a", ca, []byte("in flight"))
	h.crash("b")
	h.run()
	if c := h.closed[key("a", ca)]; !c.Abort {
		t.Fatalf("peer not reset: %+v", h.closed)
	}
	// new incarnation never sees old conn; old conn id is dead
	h.boot("b")
	r := h.send("b", cb, []byte("x"))
	h.run()
	if h.sends[r].Err != core.ErrConnClosed {
		t.Fatalf("old conn usable from new incarnation: %+v", h.sends[r])
	}
	_, cb2 := h.connect("a", "b")
	if cb2 <= cb {
		t.Fatalf("ConnID reused: %d <= %d", cb2, cb)
	}
	// crashed side's own survivor sends fail
	r2 := h.send("a", ca, []byte("y"))
	h.run()
	if h.sends[r2].Err != core.ErrConnClosed {
		t.Fatalf("send on reset conn: %+v", h.sends[r2])
	}
}

func TestPartitionFreezesThenHeals(t *testing.T) {
	f := Faults{Latency: kernel.FastWithRareSlow{FastMin: 1000, FastMax: 2000}, PartitionProb: 1,
		PartitionDur: [2]machine.Duration{1_000_000, 1_000_001}}
	h := newHarness(t, 6, Faults{Latency: f.Latency})
	ca, cb := h.connect("a", "b")
	h.eng.f = f // partition from here on
	h.send("a", ca, []byte("frozen"))
	h.runUntil(h.Now() + 500_000)
	if len(h.data[key("b", cb)]) != 0 {
		t.Fatal("delivered during partition")
	}
	h.Recover()
	h.run()
	if string(h.data[key("b", cb)]) != "frozen" {
		t.Fatalf("after heal got %q", h.data[key("b", cb)])
	}
	if h.Now() < 1_000_000 {
		t.Fatalf("healed too early at %d", h.Now())
	}
	if !h.Hit("net: partition healed") || !h.Hit("net: partition") {
		t.Fatal("coverage not recorded")
	}
}

func TestBlackHoleAndRandomClose(t *testing.T) {
	h := newHarness(t, 7, Faults{Latency: lat})
	ca, cb := h.connect("a", "b")
	h.eng.f.BlackHoleProb = 1
	r := h.send("a", ca, []byte("gone"))
	h.Recover()
	h.run()
	if h.sends[r].Err != core.OK || len(h.data[key("b", cb)]) != 0 {
		t.Fatal("black hole should ack but not deliver")
	}
	h2 := newHarness(t, 8, Faults{Latency: lat, RandomCloseProb: 1})
	ca, cb = h2.connect("a", "b")
	h2.send("a", ca, []byte("x"))
	h2.run()
	if !h2.closed[key("a", ca)].Abort || !h2.closed[key("b", cb)].Abort {
		t.Fatalf("both sides should see abort: %+v", h2.closed)
	}
}

func scenario(t *testing.T, seed uint64) ([]string, uint64) {
	f := Faults{Latency: lat, PartialIOMax: 5, PairLatencyMax: 20000, PartitionProb: 0.05,
		PartitionDur: [2]machine.Duration{10000, 400000}, ClogProb: 0.1, ClogDur: [2]machine.Duration{1000, 100000},
		RandomCloseProb: 0.02, BlackHoleProb: 0.02}
	h := newHarness(t, seed, f)
	d, _ := h.dial("a", "b")
	cb := h.lastOpened("b")
	for i := range 30 {
		h.send("a", d.Conn, fmt.Appendf(nil, "msg-%d-%d", seed, i))
		h.send("b", cb, []byte("reply"))
		h.runUntil(h.Now() + kernel.Time(h.rnd.Intn(30000)))
		if i == 15 {
			h.Issue("b", core.PauseReads{Conn: cb})
		}
		if i == 22 {
			h.Issue("b", core.ResumeReads{Conn: cb})
		}
	}
	h.Issue("a", core.CloseConn{Conn: d.Conn})
	h.run()
	faults := h.Faults()
	slices.Sort(faults)
	return append(h.log, faults...), h.eng.StateHash()
}

func TestDeterminism(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		l1, h1 := scenario(t, seed)
		l2, h2 := scenario(t, seed)
		if h1 != h2 || fmt.Sprint(l1) != fmt.Sprint(l2) {
			t.Fatalf("seed %d diverged", seed)
		}
	}
	_, h1 := scenario(t, 1)
	_, h2 := scenario(t, 2)
	if h1 == h2 {
		t.Log("note: seeds 1 and 2 share a final hash")
	}
}
