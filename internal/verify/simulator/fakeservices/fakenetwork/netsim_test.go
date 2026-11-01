package fakenetwork

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/simulator"
)

// ---- test doubles ----

type asserts struct{ some map[core.PropertyID]bool }

func (a *asserts) Always(bool, core.PropertyID, ...core.ContextPair)              {}
func (a *asserts) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (a *asserts) Sometimes(c bool, p core.PropertyID) {
	if c {
		a.some[p] = true
	}
}
func (a *asserts) Reachable(core.PropertyID)                        {}
func (a *asserts) Unreachable(core.PropertyID, ...core.ContextPair) {}

type entry struct {
	at   simulator.Time
	seq  uint64
	id   simulator.EntryID
	to   core.NodeInstance
	self bool
	body any
}

// harness is a fake EngineCtx plus a tiny event loop. Order: (at, seq).
type harness struct {
	t      *testing.T
	eng    *Engine
	rnd    simulator.CountedRand
	lanes  map[string]simulator.DeliveryLane
	now    simulator.Time
	q      []entry
	seq    uint64
	live   map[core.NodeID]core.NodeInstance
	as     *asserts
	faults []string
	log    []string
	// received data per (node, conn); closes per (node, conn)
	data   map[string][]byte
	closed map[string]core.ConnClosed
	opened map[string]core.ConnOpened
	dials  map[core.RequestID]core.ConnectDone
	sends  map[core.RequestID]core.SendDone
	reqN   core.RequestID
}

func newHarness(t *testing.T, seed uint64, f simulator.NetworkFaults) *harness {
	return &harness{t: t, eng: New(f), rnd: simulator.NewRand(seed), lanes: map[string]simulator.DeliveryLane{}, live: map[core.NodeID]core.NodeInstance{},
		as: &asserts{some: map[core.PropertyID]bool{}}, data: map[string][]byte{}, closed: map[string]core.ConnClosed{},
		opened: map[string]core.ConnOpened{}, dials: map[core.RequestID]core.ConnectDone{}, sends: map[core.RequestID]core.SendDone{}}
}

func (h *harness) Now() simulator.Time         { return h.now }
func (h *harness) Rand() simulator.CountedRand { return h.rnd }
func (h *harness) Lane(key string) simulator.DeliveryLane {
	if l, ok := h.lanes[key]; ok {
		return l
	}
	l := simulator.DeliveryLane(len(h.lanes) + 1)
	h.lanes[key] = l
	return l
}
func (h *harness) add(e entry) simulator.EntryID {
	h.seq++
	e.seq = h.seq
	e.id = simulator.EntryID(h.seq)
	if e.at < h.now {
		e.at = h.now
	}
	h.q = append(h.q, e)
	return e.id
}
func (h *harness) Deliver(to core.NodeInstance, at simulator.Time, _ simulator.DeliveryLane, b core.EventData) simulator.EntryID {
	return h.add(entry{at: at, to: to, body: b})
}
func (h *harness) Self(at simulator.Time, _ simulator.DeliveryLane, b any) simulator.EntryID {
	return h.add(entry{at: at, self: true, body: b})
}
func (h *harness) Cancel(id simulator.EntryID) {
	for i, e := range h.q {
		if e.id == id {
			h.q = append(h.q[:i], h.q[i+1:]...)
			return
		}
	}
}
func (h *harness) Fault(kind string, f ...core.Field) { h.faults = append(h.faults, kind) }
func (h *harness) Assert() core.Assertions            { return h.as }
func (h *harness) Current(n core.NodeID) (core.NodeInstance, bool) {
	r, ok := h.live[n]
	return r, ok
}

var _ simulator.NodeCurrent = (*harness)(nil)

func (h *harness) boot(n string) core.NodeInstance {
	r := core.NodeInstance{Node: core.NodeID(n), Inc: core.BootNumber(h.rnd.NewID())}
	h.live[r.Node] = r
	return r
}

func (h *harness) crash(r core.NodeInstance) {
	delete(h.live, r.Node)
	h.eng.OnCrash(h, r)
}

func key(n core.NodeID, c core.ConnID) string { return fmt.Sprintf("%s/%d", n, c) }

// step pops one entry; false when empty.
func (h *harness) step() bool {
	if len(h.q) == 0 {
		return false
	}
	best := 0
	for i, e := range h.q {
		if e.at < h.q[best].at || (e.at == h.q[best].at && e.seq < h.q[best].seq) {
			best = i
		}
	}
	e := h.q[best]
	h.q = append(h.q[:best], h.q[best+1:]...)
	h.now = e.at
	if e.self {
		h.eng.OnInternal(h, e.body)
		return true
	}
	if cur, ok := h.live[e.to.Node]; !ok || cur != e.to {
		return true // dropped: addressed to a dead incarnation
	}
	n := e.to.Node
	switch b := e.body.(type) {
	case core.BytesReceived:
		k := key(n, b.Conn)
		h.data[k] = append(h.data[k], b.Data...)
		h.log = append(h.log, fmt.Sprintf("%d %s bytes %d", h.now, k, len(b.Data)))
	case core.ConnClosed:
		h.closed[key(n, b.Conn)] = b
		h.log = append(h.log, fmt.Sprintf("%d %s closed %v", h.now, key(n, b.Conn), b.Abort))
	case core.ConnOpened:
		h.opened[key(n, b.Conn)] = b
		h.log = append(h.log, fmt.Sprintf("%d %s opened", h.now, key(n, b.Conn)))
	case core.ConnectDone:
		h.dials[b.Req] = b
		h.log = append(h.log, fmt.Sprintf("%d dial %d %v %d", h.now, b.Req, b.Err, b.Conn))
	case core.SendDone:
		h.sends[b.Req] = b
		h.log = append(h.log, fmt.Sprintf("%d send %d %v", h.now, b.Req, b.Err))
	}
	return true
}

func (h *harness) run() {
	for i := 0; h.step(); i++ {
		if i > 1_000_000 {
			h.t.Fatal("runaway")
		}
	}
}

// runUntil processes entries with at <= t.
func (h *harness) runUntil(t simulator.Time) {
	for {
		min := simulator.Time(1<<62 - 1)
		for _, e := range h.q {
			if e.at < min {
				min = e.at
			}
		}
		if min > t {
			break
		}
		h.step()
	}
	if h.now < t {
		h.now = t
	}
}

func (h *harness) nextRequestID() core.RequestID { h.reqN++; return h.reqN }

func (h *harness) dial(from core.NodeInstance, addr string) (core.ConnectDone, bool) {
	r := h.nextRequestID()
	h.eng.Execute(h, from, core.Connect{Req: r, Addr: addr})
	h.run()
	d, ok := h.dials[r]
	return d, ok
}

func (h *harness) send(from core.NodeInstance, c core.ConnID, data []byte) core.RequestID {
	r := h.nextRequestID()
	h.eng.Execute(h, from, core.Send{Req: r, Conn: c, Data: data})
	return r
}

var lat = simulator.FastWithRareSlow{FastMin: 1000, FastMax: 5000, SlowMin: 100000, SlowMax: 200000, SlowProb: 0.1}

// connect dials a->b and returns both conn ids.
func (h *harness) connect(a, b core.NodeInstance) (core.ConnID, core.ConnID) {
	d, ok := h.dial(a, string(b.Node))
	if !ok || d.Err != core.OK {
		h.t.Fatalf("dial failed: %+v %v", d, ok)
	}
	// listener conn id: the only opened key for b not yet claimed
	cb := h.lastOpened(b.Node)
	return d.Conn, cb
}

func (h *harness) lastOpened(n core.NodeID) core.ConnID {
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
		if core.NodeID(name) == n && c > best {
			best = c
		}
	}
	return best
}

// ---- tests ----

func TestDial(t *testing.T) {
	h := newHarness(t, 1, simulator.NetworkFaults{Latency: lat})
	a := h.boot("a")
	b := h.boot("b")
	d, _ := h.dial(a, "b")
	if d.Err != core.OK || d.Conn == 0 {
		t.Fatalf("dial live: %+v", d)
	}
	if o, ok := h.opened[key("b", h.lastOpened("b"))]; !ok || o.Peer != "a" {
		t.Fatalf("listener not told: %+v", h.opened)
	}
	_ = b
	d2, _ := h.dial(a, "ghost")
	if d2.Err != core.ErrConnClosed {
		t.Fatalf("dial down: %+v", d2)
	}
	h.crash(b)
	d3, _ := h.dial(a, "b")
	if d3.Err != core.ErrConnClosed {
		t.Fatalf("dial crashed: %+v", d3)
	}
}

func TestOrderedDeliveryProperty(t *testing.T) {
	f := simulator.NetworkFaults{Latency: lat, PartialIOMax: 7, PairLatencyMax: 50000, ClogProb: 0.1, ClogDur: [2]core.Duration{1000, 300000}}
	for seed := uint64(1); seed <= 300; seed++ {
		h := newHarness(t, seed, f)
		a, b := h.boot("a"), h.boot("b")
		ca, cb := h.connect(a, b)
		var wantAB, wantBA []byte
		for i := range 20 {
			n := int(h.rnd.Intn(40))
			buf := make([]byte, n)
			for j := range buf {
				buf[j] = byte(h.rnd.Uint64())
			}
			if i%3 == 0 {
				wantBA = append(wantBA, buf...)
				h.send(b, cb, buf)
			} else {
				wantAB = append(wantAB, buf...)
				h.send(a, ca, buf)
			}
			h.runUntil(h.now + simulator.Time(h.rnd.Intn(8000)))
		}
		h.run()
		if !bytes.Equal(h.data[key("b", cb)], wantAB) || !bytes.Equal(h.data[key("a", ca)], wantBA) {
			t.Fatalf("seed %d: stream mismatch", seed)
		}
	}
}

func TestGracefulCloseAfterLastBytes(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		h := newHarness(t, seed, simulator.NetworkFaults{Latency: lat, PartialIOMax: 3})
		a, b := h.boot("a"), h.boot("b")
		ca, cb := h.connect(a, b)
		h.send(a, ca, []byte("hello world"))
		h.eng.Execute(h, a, core.CloseConn{Conn: ca})
		r := h.send(a, ca, []byte("late"))
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
	h := newHarness(t, 3, simulator.NetworkFaults{Latency: lat, PartialIOMax: 2})
	a, b := h.boot("a"), h.boot("b")
	ca, cb := h.connect(a, b)
	h.send(a, ca, bytes.Repeat([]byte("x"), 200))
	h.eng.Execute(h, a, core.CloseConn{Conn: ca, Abort: true})
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
	h := newHarness(t, 4, simulator.NetworkFaults{Latency: lat, PartialIOMax: 4})
	a, b := h.boot("a"), h.boot("b")
	ca, cb := h.connect(a, b)
	h.eng.Execute(h, b, core.PauseReads{Conn: cb})
	r := h.send(a, ca, []byte("0123456789abcdef"))
	h.run()
	if len(h.data[key("b", cb)]) != 0 {
		t.Fatal("delivered while paused")
	}
	if h.sends[r].Err != core.OK {
		t.Fatal("SendDone should still complete while receiver paused")
	}
	h0 := h.eng.StateHash()
	h.eng.Execute(h, b, core.ResumeReads{Conn: cb})
	h.run()
	if string(h.data[key("b", cb)]) != "0123456789abcdef" {
		t.Fatalf("got %q", h.data[key("b", cb)])
	}
	if h.eng.StateHash() == h0 {
		t.Fatal("hash unchanged by resume")
	}
}

func TestCrashClosesPeers(t *testing.T) {
	h := newHarness(t, 5, simulator.NetworkFaults{Latency: lat})
	a, b := h.boot("a"), h.boot("b")
	ca, cb := h.connect(a, b)
	h.send(a, ca, []byte("in flight"))
	h.crash(b)
	h.run()
	if c := h.closed[key("a", ca)]; !c.Abort {
		t.Fatalf("peer not reset: %+v", h.closed)
	}
	// new incarnation never sees old conn; old conn id is dead
	b2 := h.boot("b")
	r := h.send(b2, cb, []byte("x"))
	h.run()
	if h.sends[r].Err != core.ErrConnClosed {
		t.Fatalf("old conn usable from new incarnation: %+v", h.sends[r])
	}
	_, cb2 := h.connect(a, b2)
	if cb2 <= cb {
		t.Fatalf("ConnID reused: %d <= %d", cb2, cb)
	}
	// crashed side's own survivor sends fail
	r2 := h.send(a, ca, []byte("y"))
	h.run()
	if h.sends[r2].Err != core.ErrConnClosed {
		t.Fatalf("send on reset conn: %+v", h.sends[r2])
	}
}

func TestPartitionFreezesThenHeals(t *testing.T) {
	f := simulator.NetworkFaults{Latency: simulator.FastWithRareSlow{FastMin: 1000, FastMax: 2000}, PartitionProb: 1,
		PartitionDur: [2]core.Duration{1_000_000, 1_000_001}}
	h := newHarness(t, 6, simulator.NetworkFaults{Latency: f.Latency})
	a, b := h.boot("a"), h.boot("b")
	ca, cb := h.connect(a, b)
	h.eng.f = f // partition from here on
	h.send(a, ca, []byte("frozen"))
	h.runUntil(h.now + 500_000)
	if len(h.data[key("b", cb)]) != 0 {
		t.Fatal("delivered during partition")
	}
	h.eng.EnterRecovery()
	h.run()
	if string(h.data[key("b", cb)]) != "frozen" {
		t.Fatalf("after heal got %q", h.data[key("b", cb)])
	}
	if h.now < 1_000_000 {
		t.Fatalf("healed too early at %d", h.now)
	}
	if !h.as.some["net: partition healed"] || !h.as.some["net: partition"] {
		t.Fatal("coverage not recorded")
	}
}

func TestBlackHoleAndRandomClose(t *testing.T) {
	h := newHarness(t, 7, simulator.NetworkFaults{Latency: lat})
	a, b := h.boot("a"), h.boot("b")
	ca, cb := h.connect(a, b)
	h.eng.f.BlackHoleProb = 1
	r := h.send(a, ca, []byte("gone"))
	h.eng.EnterRecovery()
	h.run()
	if h.sends[r].Err != core.OK || len(h.data[key("b", cb)]) != 0 {
		t.Fatal("black hole should ack but not deliver")
	}
	h2 := newHarness(t, 8, simulator.NetworkFaults{Latency: lat, RandomCloseProb: 1})
	a, b = h2.boot("a"), h2.boot("b")
	ca, cb = h2.connect(a, b)
	h2.send(a, ca, []byte("x"))
	h2.run()
	if !h2.closed[key("a", ca)].Abort || !h2.closed[key("b", cb)].Abort {
		t.Fatalf("both sides should see abort: %+v", h2.closed)
	}
}

func scenario(seed uint64) ([]string, uint64) {
	f := simulator.NetworkFaults{Latency: lat, PartialIOMax: 5, PairLatencyMax: 20000, PartitionProb: 0.05,
		PartitionDur: [2]core.Duration{10000, 400000}, ClogProb: 0.1, ClogDur: [2]core.Duration{1000, 100000},
		RandomCloseProb: 0.02, BlackHoleProb: 0.02}
	h := newHarness(&testing.T{}, seed, f)
	a, b := h.boot("a"), h.boot("b")
	d, _ := h.dial(a, "b")
	cb := h.lastOpened("b")
	for i := range 30 {
		h.send(a, d.Conn, []byte(fmt.Sprintf("msg-%d-%d", seed, i)))
		h.send(b, cb, []byte("reply"))
		h.runUntil(h.now + simulator.Time(h.rnd.Intn(30000)))
		if i == 15 {
			h.eng.Execute(h, b, core.PauseReads{Conn: cb})
		}
		if i == 22 {
			h.eng.Execute(h, b, core.ResumeReads{Conn: cb})
		}
	}
	h.eng.Execute(h, a, core.CloseConn{Conn: d.Conn})
	h.run()
	sort.Strings(h.faults)
	return append(h.log, h.faults...), h.eng.StateHash()
}

func TestDeterminism(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		l1, h1 := scenario(seed)
		l2, h2 := scenario(seed)
		if h1 != h2 || fmt.Sprint(l1) != fmt.Sprint(l2) {
			t.Fatalf("seed %d diverged", seed)
		}
	}
	_, h1 := scenario(1)
	_, h2 := scenario(2)
	if h1 == h2 {
		t.Log("note: seeds 1 and 2 share a final hash")
	}
}
