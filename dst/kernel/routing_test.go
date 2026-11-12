package kernel_test

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// Ports of the dst/sim runner tests and of the broker kernel tests about
// routing, timers, services, time and the cost model.

func TestServiceCallRouting(t *testing.T) {
	var done []uint64
	h := newHarness(kernel.Config{Seed: 1})
	h.svc.lat = kernel.FastWithRareSlow{FastMin: machine.Millisecond, FastMax: machine.Millisecond}
	h.spawn("b-0", func(_ *scriptM, _ machine.Now, in tin) []tout {
		switch b := in.(type) {
		case started:
			return []tout{put{req: 7}}
		case putDone:
			done = append(done, b.req)
		}
		return nil
	}, nil)
	res := h.run()
	if !res.OK() || len(done) != 1 || done[0] != 7 || h.svc.applied != 1 {
		t.Fatalf("done=%v applied=%d %+v", done, h.svc.applied, res.Failures)
	}
	if stat(res, "engine_steps") != 1 {
		t.Fatalf("engine_steps = %d", stat(res, "engine_steps"))
	}

	// A zero ServiceAddr is a protocol failure, not a crash of the simulator.
	h = newHarness(kernel.Config{Seed: 1})
	h.echo = kernel.ServiceAddr[echoReq, tin]{}
	h.spawn("b-0", func(_ *scriptM, _ machine.Now, in tin) []tout {
		if _, ok := in.(started); ok {
			return []tout{put{req: 1}}
		}
		return nil
	}, nil)
	if res = h.run(); !hasFailure(res, kernel.FailProtocol, "zero address") {
		t.Fatalf("%+v", res.Failures)
	}
}

func TestZeroAndForeignAddress(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1})
	kernel.Inject[tin](h.w, kernel.Addr[tin]{}, 0, note{"x"})
	other := newHarness(kernel.Config{Seed: 1})
	foreign := other.spawn("n", ping, nil)
	kernel.Inject[tin](h.w, foreign, 0, note{"x"})
	res := h.run()
	if !hasFailure(res, kernel.FailProtocol, "zero address") || !hasFailure(res, kernel.FailProtocol, "foreign address") {
		t.Fatalf("%+v", res.Failures)
	}
}

// relay is a service that, on a put, makes one later step and then delivers a
// note to node "p" (a node that did not call it).
type relay struct {
	h     *harness
	later bool
}

func (s *relay) Name() string                                              { return "relay" }
func (s *relay) StateHash() uint64                                         { return 0 }
func (s *relay) EnterRecovery()                                            {}
func (s *relay) OnCrash(*kernel.ServiceContext[tin], machine.NodeInstance) {}

func (s *relay) Handle(cx *kernel.ServiceContext[tin], _ kernel.Ref[tin], req echoReq) {
	switch req.(type) {
	case put:
		cx.Later(cx.Now(), kernel.NoLane, func(cx *kernel.ServiceContext[tin]) {
			s.later = true
			if to, ok := cx.Current(s.h.addr["p"]); ok {
				cx.Deliver(to, cx.Now(), kernel.NoLane, note{"hello"})
			}
		})
	case idle:
	}
}

func TestServiceLaterAndDeliver(t *testing.T) {
	h := &harness{w: kernel.New(kernel.Config{Seed: 9}), addr: map[string]kernel.Addr[tin]{}}
	s := &relay{h: h}
	h.echo = kernel.AddService[echoReq, tin](h.w, s)
	got := false
	h.spawn("p", func(_ *scriptM, _ machine.Now, in tin) []tout {
		if n, ok := in.(note); ok && n.s == "hello" {
			got = true
		}
		return nil
	}, nil)
	h.spawn("c", func(_ *scriptM, _ machine.Now, in tin) []tout {
		if _, ok := in.(started); ok {
			return []tout{put{}}
		}
		return nil
	}, nil)
	res := h.run()
	if !res.OK() || !s.later || !got {
		t.Fatalf("later=%v got=%v %+v", s.later, got, res.Failures)
	}
}

func TestServiceDeliverToADeadIncarnationDrawsNothing(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1})
	p := &probe{}
	kernel.AddService[echoReq, tin](h.w, p)
	a := h.spawn("n", ping, nil)
	h.w.Start()
	old, _ := kernel.CurrentRef(h.w, a)
	h.w.Crash(a, "x") // the probe keeps the ServiceContext it gets in OnCrash
	h.w.Boot(a)
	before := h.w.Rand().Calls()
	if id := p.cx.Deliver(old, 0, kernel.NoLane, note{"late"}); id != 0 || h.w.Rand().Calls() != before {
		t.Fatalf("Deliver to a dead incarnation: id=%d draws=%d", id, h.w.Rand().Calls()-before)
	}
	cur, _ := kernel.CurrentRef(h.w, a)
	if id := p.cx.Deliver(cur, 0, kernel.NoLane, note{"now"}); id == 0 || h.w.Rand().Calls() != before+1 {
		t.Fatal("Deliver to the current incarnation must queue one entry with one tie")
	}
}

// probe is a service that keeps the ServiceContext it gets in OnCrash.
type probe struct{ cx *kernel.ServiceContext[tin] }

func (p *probe) Name() string                                                 { return "probe" }
func (p *probe) StateHash() uint64                                            { return 0 }
func (p *probe) EnterRecovery()                                               {}
func (p *probe) Handle(*kernel.ServiceContext[tin], kernel.Ref[tin], echoReq) {}
func (p *probe) OnCrash(cx *kernel.ServiceContext[tin], _ machine.NodeInstance) {
	p.cx = cx
}

func TestClockSkewAndWall(t *testing.T) {
	const wallStart = machine.RealTime(1_700_000_000 * 1_000_000_000)
	var monos []machine.NodeTime
	h := newHarness(kernel.Config{Seed: 1, ClockSkewMax: 100 * machine.Millisecond, WallStart: wallStart})
	for i := range 6 {
		h.spawn(fmt.Sprintf("b-%d", i), func(_ *scriptM, now machine.Now, in tin) []tout {
			if _, ok := in.(started); ok {
				monos = append(monos, now.Mono)
				if now.Wall != wallStart {
					t.Errorf("Wall = %d, want the start time at virtual time 0", now.Wall)
				}
			}
			return nil
		}, nil)
	}
	h.run()
	distinct := map[machine.NodeTime]bool{}
	for _, m := range monos {
		if m < 0 || m > machine.NodeTime(100*machine.Millisecond) {
			t.Fatalf("skew out of range: %d", m)
		}
		distinct[m] = true
	}
	if len(monos) != 6 || len(distinct) < 2 {
		t.Fatalf("skews: %v", monos)
	}
}

func TestWallIsWallStartPlusNow(t *testing.T) {
	var got []int64
	h := newHarness(kernel.Config{Seed: 1, WallStart: 1_000_000})
	a := h.spawn("p", func(_ *scriptM, now machine.Now, _ tin) []tout {
		got = append(got, int64(now.Mono), int64(now.Wall))
		return nil
	}, func(s *kernel.NodeSpec[tin, tout]) { s.Boot = nil })
	kernel.Inject[tin](h.w, a, 5, note{"a"})
	h.run()
	if len(got) != 2 || got[0] != 5 || got[1] != 1_000_005 {
		t.Fatalf("mono/wall = %v", got)
	}
}

func TestCostModelQueuesAndControlFirst(t *testing.T) {
	var order []string
	run := func(cost kernel.CostParams) kernel.Result {
		order = nil
		h := newHarness(kernel.Config{Seed: 1, End: ms(1200)})
		h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
			order = append(order, fmt.Sprintf("%T", in))
			return ping(m, now, in)
		}, func(s *kernel.NodeSpec[tin, tout]) { s.Cost = cost })
		return h.run()
	}
	free := run(kernel.CostParams{})
	busy := run(kernel.CostParams{PerEvent: 40 * machine.Millisecond})
	if len(order) == 0 || order[0] != "kernel_test.started" {
		t.Fatalf("order: %v", order)
	}
	if busy.Steps >= free.Steps || stat(busy, "wakes") == 0 {
		t.Fatalf("a busy node must step less: busy=%d free=%d", busy.Steps, free.Steps)
	}
}

func TestClassPicksInbox(t *testing.T) {
	var order []string
	h := newHarness(kernel.Config{Seed: 1})
	a := h.spawn("n", func(_ *scriptM, _ machine.Now, in tin) []tout {
		switch x := in.(type) {
		case note:
			order = append(order, "data:"+x.s)
		case urgent:
			order = append(order, "control:"+x.s)
		}
		return nil
	}, func(s *kernel.NodeSpec[tin, tout]) { s.Cost = kernel.CostParams{PerEvent: machine.Millisecond} })
	// All arrive while the node is busy with its boot input, so they wait in
	// the inboxes; Control is taken first, each inbox in FIFO order.
	for _, m := range []tin{note{"1"}, urgent{"a"}, note{"2"}, urgent{"b"}} {
		kernel.InjectOn(h.w, a, 7, m)
	}
	res := h.run()
	want := []string{"control:a", "control:b", "data:1", "data:2"}
	if !res.OK() || fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("order %v, want %v", order, want)
	}
}

func TestTimerCancel(t *testing.T) {
	fired := 0
	h := newHarness(kernel.Config{Seed: 1})
	h.spawn("b-0", func(_ *scriptM, _ machine.Now, in tin) []tout {
		switch in.(type) {
		case started:
			return []tout{setTimer{id: 7, after: 10 * machine.Millisecond, msg: tick{1}}, cancelTimer{id: 7}}
		case tick:
			fired++
		}
		return nil
	}, nil)
	res := h.run()
	if fired != 0 || stat(res, "timers_cancelled") != 1 {
		t.Fatalf("fired=%d %+v", fired, res.Stats)
	}
}

// A timer that was replaced must never fire, even after the new timer fired
// and a third one was armed with the same id.
func TestReplacedTimerNeverFiresAfterIDIsReused(t *testing.T) {
	var seen []uint64
	h := newHarness(kernel.Config{Seed: 1})
	h.spawn("p", func(_ *scriptM, _ machine.Now, in tin) []tout {
		switch x := in.(type) {
		case started:
			return []tout{
				setTimer{id: 1, after: 100, msg: tick{1}}, // replaced at once, must not fire
				setTimer{id: 1, after: 10, msg: tick{2}},
			}
		case tick:
			seen = append(seen, x.gen)
			if x.gen == 2 {
				return []tout{setTimer{id: 1, after: 500, msg: tick{3}}} // reuses the id after it fired
			}
		}
		return nil
	}, nil)
	res := h.run()
	if !res.OK() || fmt.Sprint(seen) != "[2 3]" {
		t.Fatalf("seen = %v, want [2 3]", seen)
	}
}

// ioService answers a put to the caller after 10 ns.
type ioService struct{}

func (ioService) Name() string                                              { return "io" }
func (ioService) StateHash() uint64                                         { return 0 }
func (ioService) EnterRecovery()                                            {}
func (ioService) OnCrash(*kernel.ServiceContext[tin], machine.NodeInstance) {}
func (ioService) Handle(cx *kernel.ServiceContext[tin], from kernel.Ref[tin], req echoReq) {
	switch r := req.(type) {
	case put:
		cx.Deliver(from, cx.Now()+10, kernel.NoLane, putDone{req: r.req})
	case idle:
	}
}

func ioHarness() *harness {
	h := &harness{w: kernel.New(kernel.Config{Seed: 1}), addr: map[string]kernel.Addr[tin]{}}
	h.echo = kernel.AddService[echoReq, tin](h.w, ioService{})
	return h
}

// bootThenCall arms a timer at 20 and calls io (answer at 10) on its boot.
func bootThenCall(seen *[]string, name string) script {
	return func(_ *scriptM, _ machine.Now, in tin) []tout {
		*seen = append(*seen, fmt.Sprintf("%s:%T", name, in))
		if _, ok := in.(started); ok && name == "old" {
			return []tout{setTimer{id: 1, after: 20, msg: tick{}}, put{}}
		}
		return nil
	}
}

func TestRestartDropsStaleTimerAndReply(t *testing.T) {
	var seen []string
	h := ioHarness()
	incarnation := 0
	a := h.spawn("n", func(m *scriptM, now machine.Now, in tin) []tout {
		name := "old"
		if incarnation > 1 {
			name = "new"
		}
		return bootThenCall(&seen, name)(m, now, in)
	}, func(s *kernel.NodeSpec[tin, tout]) {
		next := s.New
		s.New = func(env machine.Env, self machine.NodeInstance) machine.Machine[tin, tout] {
			incarnation++
			return next(env, self)
		}
	})
	// At t=5: after the old boot sent its call and armed its timer, before the
	// answer (t=10) and the timer (t=20).
	h.w.At(5, kernel.HostLane, func() { h.w.Crash(a, "restart"); h.w.Boot(a) })
	res := h.run()
	want := "[old:kernel_test.started new:kernel_test.started]"
	if !res.OK() || fmt.Sprint(seen) != want {
		t.Fatalf("seen = %v, want %s", seen, want)
	}
}

func TestCurrentIncarnationStillGetsItsReplyAndTimer(t *testing.T) {
	var seen []string
	h := ioHarness()
	h.spawn("n", bootThenCall(&seen, "old"), nil)
	res := h.run()
	want := "[old:kernel_test.started old:kernel_test.putDone old:kernel_test.tick]"
	if !res.OK() || fmt.Sprint(seen) != want {
		t.Fatalf("seen = %v, want %s", seen, want)
	}
}

func TestAfterStepSeesInputAndOutput(t *testing.T) {
	var log []string
	state := 0
	h := newHarness(kernel.Config{Seed: 1})
	h.spawn("p", func(_ *scriptM, _ machine.Now, in tin) []tout {
		state++
		if n, ok := in.(note); ok && n.s == "a" {
			return []tout{sendSelf{note{"b"}}}
		}
		return nil
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Boot = nil
		s.AfterStep = func(self kernel.Ref[tin], in tin, out []tout) {
			log = append(log, fmt.Sprintf("%s:%v:%d:%d", self.Instance().Node, in, len(out), state))
		}
	})
	kernel.Inject[tin](h.w, h.addr["p"], 0, note{"a"})
	res := h.run()
	if !res.OK() || fmt.Sprint(log) != "[p:{a}:1:1 p:{b}:0:2]" {
		t.Fatalf("log = %v", log)
	}
}

func TestRunUntilStopsOnPredicate(t *testing.T) {
	n := 0
	h := newHarness(kernel.Config{Seed: 1})
	h.spawn("p", func(*scriptM, machine.Now, tin) []tout {
		n++
		return []tout{sendSelf{note{"again"}}} // never ends by itself
	}, nil)
	stop := func() bool { return n >= 5 }
	if got := h.w.RunUntil(stop); got != kernel.StopAsked || n != 5 {
		t.Fatalf("RunUntil = %d n=%d", got, n)
	}
}

func TestRouterAfterItsStepIsProtocolFailure(t *testing.T) {
	var kept *kernel.Router[tin]
	h := newHarness(kernel.Config{Seed: 1})
	s := h.spec("p", func(_ *scriptM, _ machine.Now, in tin) []tout {
		if _, ok := in.(started); ok {
			return []tout{fact("x", 0)}
		}
		return nil
	})
	s.Route = func(r *kernel.Router[tin], out tout) {
		kept = r
		h.route(r, out)
	}
	kernel.Spawn(h.w, s)
	h.w.At(ms(1), kernel.NoLane, func() { kept.SendSelf(note{"late"}) })
	if res := h.run(); !hasFailure(res, kernel.FailProtocol, "router_after_step") {
		t.Fatalf("%+v", res.Failures)
	}
}

// changeBehindTheKernel is a machine whose state host code changes directly.
type hidden struct{ v uint64 }

func (m *hidden) Step(machine.Now, tin) []tout { return nil }
func (m *hidden) StateHash() uint64            { return m.v }

func TestVerifyHashesCatchesAHiddenChange(t *testing.T) {
	for _, verify := range []bool{false, true} {
		m := &hidden{}
		h := newHarness(kernel.Config{Seed: 1, VerifyHashes: verify})
		h.spawn("p", nil, func(s *kernel.NodeSpec[tin, tout]) {
			s.New = func(machine.Env, machine.NodeInstance) machine.Machine[tin, tout] { return m }
		})
		h.w.At(ms(1), kernel.NoLane, func() { m.v = 42 })
		h.w.At(ms(2), kernel.NoLane, func() {})
		res := h.run()
		if got := hasFailure(res, kernel.FailProtocol, "stale_state_hash"); got != verify {
			t.Fatalf("verify=%v: failure=%v %+v", verify, got, res.Failures)
		}
	}
}

func TestInjectOnOneLaneKeepsOrder(t *testing.T) {
	for seed := range uint64(32) {
		var got []string
		h := newHarness(kernel.Config{Seed: seed})
		a := h.spawn("n", func(_ *scriptM, _ machine.Now, in tin) []tout {
			if n, ok := in.(note); ok {
				got = append(got, n.s)
			}
			return nil
		}, nil)
		lanes := kernel.NewLanes[string](h.w)
		h.w.At(ms(1), kernel.NoLane, func() {
			for _, s := range []string{"1", "2", "3", "4"} {
				kernel.InjectOn[tin](h.w, a, lanes.Get("to n"), note{s})
				kernel.Inject[tin](h.w, a, 0, note{"x"})
			}
		})
		h.run()
		var lane []string
		for _, s := range got {
			if s != "x" {
				lane = append(lane, s)
			}
		}
		if fmt.Sprint(lane) != "[1 2 3 4]" {
			t.Fatalf("seed %d: %v", seed, got)
		}
	}
}

func TestLanesStableDisjointAndDistinct(t *testing.T) {
	w := kernel.New(kernel.Config{Seed: 1})
	type connLane struct {
		pair uint64
		dir  uint8
	}
	byName := kernel.NewLanes[string](w)
	byConn := kernel.NewLanes[connLane](w)
	seen := map[kernel.Lane]string{}
	before := w.Rand().Calls()
	for i := range 1000 {
		key := fmt.Sprintf("store/node-%d", i)
		l := byName.Get(key)
		if l >= kernel.HostLane || l == kernel.NoLane {
			t.Fatalf("%s: lane %d is in the kernel range or NoLane", key, l)
		}
		if prev, dup := seen[l]; dup {
			t.Fatalf("keys %q and %q share lane %d", prev, key, l)
		}
		seen[l] = key
		c := byConn.Get(connLane{pair: uint64(i), dir: 1})
		if _, dup := seen[c]; dup {
			t.Fatalf("two key types share lane %d", c)
		}
		seen[c] = "conn"
	}
	for i := range 1000 {
		key := fmt.Sprintf("store/node-%d", i)
		if seen[byName.Get(key)] != key {
			t.Fatalf("the lane of %q is not stable", key)
		}
	}
	if w.Rand().Calls() != before {
		t.Fatal("making a lane must draw nothing")
	}
}

// Kernel tags: a run that differs only in an input differs in hash.
func TestInputsAndOutputsAreInTheHash(t *testing.T) {
	run := func(s string) uint64 {
		h := newHarness(kernel.Config{Seed: 1, End: ms(100)})
		a := h.spawn("n", ping, nil)
		kernel.Inject[tin](h.w, a, 3, note{s})
		return h.run().Hash
	}
	if run("a") == run("b") {
		t.Fatal("the input is not in the trace hash")
	}
	h1, h2 := trace.New(), trace.New()
	note{"a"}.Fold(&h1)
	note{"b"}.Fold(&h2)
	if h1 == h2 {
		t.Fatal("the test messages fold the same")
	}
}
