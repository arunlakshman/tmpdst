package simulator

import (
	"fmt"
	"testing"

	"manifester/internal/core"
)

// ---- test-only machine: a scriptable state machine ----

type scriptM struct {
	fn   func(m *scriptM, ev core.Event) []core.Action
	env  core.Environment
	self core.NodeInstance
	n    int
	h    uint64
	gen  core.TimerVersion
}

func (m *scriptM) Step(ev core.Event) []core.Action {
	m.n++
	m.h = m.h*31 + uint64(m.n)
	return m.fn(m, ev)
}
func (m *scriptM) StateHash() uint64 { return m.h }

func scripted(fn func(m *scriptM, ev core.Event) []core.Action) core.MachineFactory {
	return func(env core.Environment, self core.NodeInstance) core.Machine {
		return &scriptM{fn: fn, env: env, self: self}
	}
}

func fact(name string, v int64) core.RecordFact {
	return core.RecordFact{Fact: core.Fact{Name: name, Fields: []core.Field{{Key: "v", Value: core.I64(v)}}}}
}

// ping: timer ping-pong that emits a "started" fact on boot and a "ping" fact
// per tick, every 10ms.
func ping(m *scriptM, ev core.Event) []core.Action {
	switch b := ev.Body.(type) {
	case core.NodeStarted:
		m.gen++
		return []core.Action{fact("started", 0), core.SetTimer{ID: 1, Gen: m.gen, After: 10 * core.Millisecond}}
	case core.TimerFired:
		if b.Gen != m.gen {
			return nil
		}
		m.gen++
		return []core.Action{fact("ping", int64(m.n)), core.SetTimer{ID: 1, Gen: m.gen, After: 10 * core.Millisecond}}
	}
	return nil
}

// ---- test-only engine: echo with latency and a fault coin ----

type echoApply struct {
	from core.NodeInstance
	req  core.RequestID
	lost bool
}

type echo struct {
	lat       FastWithRareSlow
	lostProb  float64
	applied   uint64
	lost      uint64
	crashes   uint64
	recovered bool
}

func (e *echo) StateHash() uint64          { return e.applied*1000003 + e.lost*7919 + e.crashes }
func (e *echo) Name() string               { return "echo" }
func (e *echo) Handles(a core.Action) bool { _, ok := a.(core.ObjectPut); return ok }
func (e *echo) Execute(ctx FakeServiceContext, from core.NodeInstance, a core.Action) {
	put := a.(core.ObjectPut)
	lost := !e.recovered && ctx.Rand().Chance(e.lostProb)
	if lost {
		ctx.Fault("echo_response_lost")
		ctx.Assert().Sometimes(true, "echo lost a response")
	}
	ctx.Self(ctx.Now()+Time(e.lat.Sample(ctx.Rand())), 1, echoApply{from, put.Req, lost})
}
func (e *echo) OnInternal(ctx FakeServiceContext, body any) {
	a := body.(echoApply)
	e.applied++
	done := core.ObjectPutDone{Req: a.req, Outcome: core.Applied}
	if a.lost {
		e.lost++
		done.Err, done.Outcome = core.ErrTimeout, core.MaybeApplied
	}
	ctx.Deliver(a.from, ctx.Now()+Time(core.Millisecond), 2, done)
}
func (e *echo) OnCrash(ctx FakeServiceContext, ref core.NodeInstance) { e.crashes++ }
func (e *echo) EnterRecovery()                                        { e.recovered = true }

// echo is also ground truth for Objects().
func (e *echo) Object(string) ([]byte, bool)    { return nil, false }
func (e *echo) EachObject(func(string, []byte)) {}
func (e *echo) Gen() uint64                     { return e.applied }

// putter: every tick sends an ObjectPut; emits "put_done" on the completion.
func putter(m *scriptM, ev core.Event) []core.Action {
	switch b := ev.Body.(type) {
	case core.NodeStarted:
		m.gen++
		return []core.Action{fact("started", 0), core.SetTimer{ID: 1, Gen: m.gen, After: 5 * core.Millisecond}}
	case core.TimerFired:
		if b.Gen != m.gen {
			return nil
		}
		m.gen++
		return []core.Action{
			core.ObjectPut{Req: core.RequestID(m.n), Key: fmt.Sprint("k", m.n), Data: []byte{byte(m.n)}},
			core.SetTimer{ID: 1, Gen: m.gen, After: 5 * core.Millisecond},
		}
	case core.ObjectPutDone:
		return []core.Action{fact("put_done", int64(b.Err))}
	case core.StopRequest:
		return []core.Action{core.ShutdownDone{}}
	}
	return nil
}

func baseCfg(f core.MachineFactory) Config {
	return Config{
		Seeds:     []uint64{1},
		Processes: []Process{{Name: "b", Count: 1, Factory: f}},
		Phases:    Phases{Chaos: core.Second, Settling: core.Second, Settle: 10 * core.Second},
	}
}

func stat(res SeedResult, name string) uint64 {
	for _, c := range res.Stats {
		if c.Name == name {
			return c.N
		}
	}
	panic("no stat " + name)
}

func hasFailure(res SeedResult, k FailKind, prop core.PropertyID) bool {
	for _, f := range res.Failures {
		if f.Kind == k && (prop == "" || f.Prop == prop) {
			return true
		}
	}
	return false
}

// ---- tests ----

func TestStartedIsFirstStep(t *testing.T) {
	var firsts []core.EventData
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if m.n == 1 {
			firsts = append(firsts, ev.Body)
		}
		return ping(m, ev)
	})
	cfg := baseCfg(f)
	cfg.Scripted = []PlannedFault{{At: Time(300 * core.Millisecond), Node: "b-0", Kind: EntryCrash}}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || res.Crashes != 1 || len(firsts) != 2 {
		t.Fatalf("crashes=%d firsts=%d failures=%+v", res.Crashes, len(firsts), res.Failures)
	}
	for _, b := range firsts {
		if _, ok := b.(core.NodeStarted); !ok {
			t.Fatalf("first Step was %T", b)
		}
	}
}

func TestScriptedGracefulStop(t *testing.T) {
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.StopRequest); ok {
			return []core.Action{core.ShutdownDone{}}
		}
		return ping(m, ev)
	})
	cfg := baseCfg(f)
	cfg.Scripted = []PlannedFault{{At: Time(300 * core.Millisecond), Node: "b-0", Kind: EntryStop}}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || stat(res, "graceful_stops") != 1 {
		t.Fatalf("graceful_stops=%d failures=%+v", stat(res, "graceful_stops"), res.Failures)
	}
}

func TestStartedFirstEvenWithQueuedData(t *testing.T) {
	// A data event queued before the node's first Step must not overtake NodeStarted.
	var firsts []core.EventData
	cfg := baseCfg(scripted(func(m *scriptM, ev core.Event) []core.Action {
		if m.n == 1 {
			firsts = append(firsts, ev.Body)
		}
		return nil
	}))
	r := newRun(cfg, 1)
	r.setup()
	n := r.byName["b-0"]
	n.inbox.Push(core.ConnOpened{Conn: 1})
	n.inbox.Push(core.TimerFired{ID: 9})
	r.loop()
	if len(r.res.Failures) != 0 || len(firsts) != 1 {
		t.Fatalf("%+v %v", r.res.Failures, firsts)
	}
	if _, ok := firsts[0].(core.NodeStarted); !ok {
		t.Fatalf("first Step was %T", firsts[0])
	}
}

func TestTripNotLastIsProtocolFailure(t *testing.T) {
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.NodeStarted); ok {
			return []core.Action{core.StopForRestart{Cause: "x"}, fact("after", 0)}
		}
		return nil
	})
	cfg := baseCfg(f)
	cfg.HaltOnFirst = true // a zero RecoveryDelay would crash-loop at one instant
	res := RunSeed(cfg, 1)
	if !hasFailure(res, FailProtocol, "trip_not_last") || res.Trips == 0 {
		t.Fatalf("%+v trips=%d", res.Failures, res.Trips)
	}
}

func TestTripLastCrashesAndRestarts(t *testing.T) {
	boots := 0
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		switch ev.Body.(type) {
		case core.NodeStarted:
			boots++
			return []core.Action{core.SetTimer{ID: 1, Gen: 1, After: 50 * core.Millisecond}}
		case core.TimerFired:
			if boots == 1 {
				return []core.Action{fact("pre", 0), core.StopForRestart{Cause: "fenced"}}
			}
		}
		return nil
	})
	cfg := baseCfg(f)
	cfg.Attrition.RecoveryDelay = [2]core.Duration{20 * core.Millisecond, 30 * core.Millisecond}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || res.Trips != 1 || res.Crashes != 0 || boots != 2 {
		t.Fatalf("trips=%d crashes=%d boots=%d %+v", res.Trips, res.Crashes, boots, res.Failures)
	}
}

func TestCrashDropsEntriesAndRestartsWithNewIncarnation(t *testing.T) {
	var incs []core.BootNumber
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.NodeStarted); ok {
			incs = append(incs, m.self.Inc)
		}
		return ping(m, ev)
	})
	cfg := baseCfg(f)
	cfg.Scripted = []PlannedFault{{At: Time(305 * core.Millisecond), Node: "b-0", Kind: EntryCrash}}
	cfg.Attrition.RecoveryDelay = [2]core.Duration{100 * core.Millisecond, 100 * core.Millisecond}
	var crashAt Time
	var oldPingsAfter int
	cfg.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			c := v.Timeline().Snapshot("sim_crashed")
			if len(c) != 1 {
				v.Assert().Always(false, "one crash fact")
				return
			}
			crashAt = c[0].At
			for _, p := range v.Timeline().Snapshot("ping") {
				if p.Source.Inc == incs[0] && p.At > crashAt {
					oldPingsAfter++
				}
			}
			v.Assert().Always(len(v.Timeline().Snapshot("started")) == 2, "restarted once")
		}}
	}}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || res.Crashes != 1 || len(incs) != 2 || incs[0] == incs[1] {
		t.Fatalf("%+v crashes=%d incs=%v", res.Failures, res.Crashes, incs)
	}
	if oldPingsAfter != 0 {
		t.Fatalf("%d steps from the dead incarnation after the crash", oldPingsAfter)
	}
	if stat(res, "dropped_by_crash") == 0 || stat(res, "restarts") != 1 {
		t.Fatalf("stats: %+v", res.Stats)
	}
}

type fnInvariant struct {
	observe func(v View)
	atEnd   func(v View)
}

func (f *fnInvariant) Name() string { return "fn" }
func (f *fnInvariant) Observe(v View) {
	if f.observe != nil {
		f.observe(v)
	}
}
func (f *fnInvariant) AtEnd(v View) {
	if f.atEnd != nil {
		f.atEnd(v)
	}
}

func TestStopExitPath(t *testing.T) {
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.StopRequest); ok {
			return []core.Action{fact("draining", 0), core.ShutdownDone{}}
		}
		return ping(m, ev)
	})
	cfg := baseCfg(f)
	cfg.Attrition = NodeKills{MaxDead: 1, CrashProb: 1, GracefulWeight: 1, RecoveryDelay: [2]core.Duration{10 * core.Millisecond, 20 * core.Millisecond}}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || res.Exits == 0 || res.Crashes != 0 || stat(res, "deadline_kills") != 0 {
		t.Fatalf("exits=%d crashes=%d %+v", res.Exits, res.Crashes, res.Failures)
	}
}

func TestStopDeadlineKill(t *testing.T) {
	var deadlines []core.NodeTime
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if s, ok := ev.Body.(core.StopRequest); ok {
			deadlines = append(deadlines, s.Deadline-ev.At)
			return nil // ignores the Stop
		}
		return ping(m, ev)
	})
	cfg := baseCfg(f)
	cfg.Attrition = NodeKills{MaxDead: 1, CrashProb: 1, GracefulWeight: 1, RecoveryDelay: [2]core.Duration{10 * core.Millisecond, 10 * core.Millisecond}}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || res.Exits != 0 || res.Crashes == 0 || stat(res, "deadline_kills") == 0 {
		t.Fatalf("exits=%d crashes=%d %+v stats=%+v", res.Exits, res.Crashes, res.Failures, res.Stats)
	}
	if len(deadlines) == 0 || deadlines[0] != core.NodeTime(stopWaitTime) {
		t.Fatalf("deadline offset = %v", deadlines)
	}
}

func TestInvariantsSeeFactsAndReport(t *testing.T) {
	cfg := baseCfg(scripted(ping))
	cfg.Seeds = []uint64{1, 2, 3}
	seen := 0
	cfg.Invariants = []func() SafetyRule{func() SafetyRule {
		var cur Cursor
		return &fnInvariant{
			observe: func(v View) { seen += len(v.Timeline().Since("ping", &cur)) },
			atEnd: func(v View) {
				v.Assert().Always(len(v.Timeline().Snapshot("ping")) > 0, "saw pings")
				v.Assert().Always(false, "always broken")
				v.Assert().Sometimes(false, "never true")
				v.Assert().Sometimes(true, "pings happen")
				v.Assert().Reachable("end reached")
			},
		}
	}}
	rep, err := NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Fatal("Observe never saw a ping")
	}
	if len(rep.Always) != 1 || rep.Always[0].Prop != "always broken" || len(rep.Always[0].Seeds) != 3 || rep.Always[0].FirstSeed != 1 {
		t.Fatalf("Always: %+v", rep.Always)
	}
	if len(rep.Coverage) != 1 || rep.Coverage[0].Prop != "never true" {
		t.Fatalf("Coverage: %+v", rep.Coverage)
	}
	for _, s := range rep.Seeds {
		if !hasFailure(s, FailViolation, "always broken") || hasFailure(s, FailViolation, "saw pings") {
			t.Fatalf("failures: %+v", s.Failures)
		}
	}
}

func TestMaxSteps(t *testing.T) {
	cfg := baseCfg(scripted(ping))
	cfg.MaxSteps = 20
	res := RunSeed(cfg, 1)
	if !hasFailure(res, FailMaxSteps, "") || res.Steps != 20 {
		t.Fatalf("steps=%d %+v", res.Steps, res.Failures)
	}
}

func TestPanicRecovery(t *testing.T) {
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if m.n == 3 {
			panic("boom")
		}
		return ping(m, ev)
	})
	res := RunSeed(baseCfg(f), 1)
	if !hasFailure(res, FailPanic, "") || res.Crashes == 0 || res.Steps < 100 {
		t.Fatalf("crashes=%d steps=%d %+v", res.Crashes, res.Steps, res.Failures)
	}
	cfg := baseCfg(f)
	cfg.HaltOnFirst = true
	halted := RunSeed(cfg, 1)
	if !hasFailure(halted, FailPanic, "") || halted.Steps >= res.Steps || halted.Steps > 10 {
		t.Fatalf("HaltOnFirst: steps=%d", halted.Steps)
	}
	// A panicking factory is a failure, not a crash of the simulator.
	cfg = baseCfg(func(core.Environment, core.NodeInstance) core.Machine { panic("factory") })
	if r := RunSeed(cfg, 1); !hasFailure(r, FailPanic, "") {
		t.Fatalf("%+v", r.Failures)
	}
}

func TestEngineRouting(t *testing.T) {
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.NodeStarted); ok {
			return []core.Action{core.ObjectDelete{Req: 1, Key: "k"}}
		}
		return nil
	})
	cfg := baseCfg(f)
	cfg.HaltOnFirst = true
	res := RunSeed(cfg, 1)
	if !hasFailure(res, FailProtocol, "unrouted_action") {
		t.Fatalf("no engine: %+v", res.Failures)
	}
	cfg.Engines = func(SeedConfig) []FakeService { return []FakeService{&echoDel{n: "a"}, &echoDel{n: "b"}} }
	res = RunSeed(cfg, 1)
	if !hasFailure(res, FailProtocol, "unrouted_action") {
		t.Fatalf("two engines: %+v", res.Failures)
	}
	cfg.Engines = func(SeedConfig) []FakeService { return []FakeService{&echoDel{n: "a"}} }
	if res = RunSeed(cfg, 1); len(res.Failures) != 0 {
		t.Fatalf("one engine: %+v", res.Failures)
	}
}

type echoDel struct{ n string }

func (e *echoDel) StateHash() uint64                                          { return 0 }
func (e *echoDel) Name() string                                               { return e.n }
func (e *echoDel) Handles(a core.Action) bool                                 { _, ok := a.(core.ObjectDelete); return ok }
func (e *echoDel) Execute(FakeServiceContext, core.NodeInstance, core.Action) {}
func (e *echoDel) OnInternal(FakeServiceContext, any)                         {}
func (e *echoDel) OnCrash(FakeServiceContext, core.NodeInstance)              {}
func (e *echoDel) EnterRecovery()                                             {}

func TestClockSkewAndWall(t *testing.T) {
	var ats []core.NodeTime
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.NodeStarted); ok {
			ats = append(ats, ev.At)
			if ev.Wall != wallClockStart {
				t.Errorf("Wall = %d, want the scripted epoch at sim time 0", ev.Wall)
			}
		}
		return nil
	})
	cfg := baseCfg(f)
	cfg.Processes[0].Count = 6
	cfg.Network.ClockSkewMax = 100 * core.Millisecond
	RunSeed(cfg, 1)
	distinct := map[core.NodeTime]bool{}
	for _, a := range ats {
		if a < 0 || a > core.NodeTime(100*core.Millisecond) {
			t.Fatalf("skew out of range: %d", a)
		}
		distinct[a] = true
	}
	if len(ats) != 6 || len(distinct) < 2 {
		t.Fatalf("skews: %v", ats)
	}
}

func TestCostModelQueuesAndControlFirst(t *testing.T) {
	var order []string
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		order = append(order, fmt.Sprintf("%T", ev.Body))
		return ping(m, ev)
	})
	free := baseCfg(f)
	free.Phases = Phases{Chaos: 200 * core.Millisecond, Settle: core.Second}
	fres := RunSeed(free, 1)
	order = nil
	busy := free
	busy.Cost = CostParams{PerEvent: 40 * core.Millisecond}
	bres := RunSeed(busy, 1)
	if len(order) == 0 || order[0] != "core.NodeStarted" {
		t.Fatalf("order: %v", order)
	}
	if bres.Steps >= fres.Steps || stat(bres, "wakes") == 0 {
		t.Fatalf("a busy node must step less: busy=%d free=%d", bres.Steps, fres.Steps)
	}
}

func TestWorkloadRole(t *testing.T) {
	var brokers []core.NodeInstance
	var w *wl
	cfg := baseCfg(scripted(ping))
	cfg.Processes[0].Count = 2
	cfg.QuiesceEvery = 100 * core.Millisecond
	cfg.Workloads = []WorkloadFactory{func(env core.Environment, self core.NodeInstance, b []core.NodeInstance) Workload {
		brokers = b
		w = &wl{}
		return w
	}}
	var quiesce int
	cfg.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			quiesce = len(v.Timeline().Snapshot("sim_quiesce"))
			v.Assert().Always(v.History() != nil, "history exists")
		}}
	}}
	res := RunSeed(cfg, 1)
	if len(res.Failures) != 0 || len(brokers) != 2 || brokers[0].Node != "b-0" || brokers[1].Node != "b-1" {
		t.Fatalf("%+v %v", res.Failures, brokers)
	}
	if w.n == 0 || w.hist == nil || quiesce == 0 {
		t.Fatalf("workload steps=%d history=%v quiesce=%d", w.n, w.hist != nil, quiesce)
	}
	// A workload may not StopForRestart.
	cfg.Workloads = []WorkloadFactory{func(core.Environment, core.NodeInstance, []core.NodeInstance) Workload { return &wl{trip: true} }}
	cfg.Invariants = nil
	if res = RunSeed(cfg, 1); !hasFailure(res, FailProtocol, "workload_ended") {
		t.Fatalf("%+v", res.Failures)
	}
}

type wl struct {
	n    int
	trip bool
	hist History
}

func (w *wl) Step(core.Event) []core.Action {
	w.n++
	if w.trip {
		return []core.Action{core.StopForRestart{Cause: "no"}}
	}
	return nil
}
func (w *wl) StateHash() uint64    { return uint64(w.n) }
func (w *wl) Name() string         { return "wl" }
func (w *wl) Quiescent() bool      { return true }
func (w *wl) SetHistory(h History) { w.hist = h }

func TestTimerCancel(t *testing.T) {
	fired := 0
	f := scripted(func(m *scriptM, ev core.Event) []core.Action {
		switch ev.Body.(type) {
		case core.NodeStarted:
			return []core.Action{core.SetTimer{ID: 7, Gen: 1, After: 10 * core.Millisecond}, core.CancelTimer{ID: 7}}
		case core.TimerFired:
			fired++
		}
		return nil
	})
	res := RunSeed(baseCfg(f), 1)
	if fired != 0 || stat(res, "timers_cancelled") != 1 {
		t.Fatalf("fired=%d %+v", fired, res.Stats)
	}
}

// ---- determinism ----

func chaosCfg() Config {
	cfg := baseCfg(scripted(putter))
	cfg.Processes[0].Count = 3
	cfg.Engines = func(sc SeedConfig) []FakeService {
		return []FakeService{&echo{lat: sc.ObjectStore.ApplyLatency, lostProb: sc.ObjectStore.LostResponse}}
	}
	cfg.ObjectStore = ObjectStoreFaults{
		ApplyLatency: FastWithRareSlow{FastMin: core.Millisecond, FastMax: 3 * core.Millisecond, SlowMin: 50 * core.Millisecond, SlowMax: 80 * core.Millisecond, SlowProb: 0.1},
		LostResponse: 0.05,
	}
	cfg.Network.ClockSkewMax = 100 * core.Millisecond
	cfg.Attrition = NodeKills{MaxDead: 1, CrashProb: 0.01, CrashWeight: 1, GracefulWeight: 1, RecoveryDelay: [2]core.Duration{10 * core.Millisecond, 50 * core.Millisecond}}
	cfg.Cost = CostParams{PerEvent: core.Millisecond, PauseProb: 0.05, Pause: FastWithRareSlow{FastMin: 5 * core.Millisecond, FastMax: 20 * core.Millisecond}}
	cfg.Swarm = func(seed uint64, r CountedRand, c *SeedConfig) {
		if r.Chance(0.5) {
			c.ObjectStore.LostResponse = 0
		}
		c.Attrition.CrashProb *= float64(1 + r.Intn(3))
	}
	cfg.QuiesceEvery = 250 * core.Millisecond
	cfg.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			v.Assert().Always(v.Objects().Gen() > 0, "engine applied something")
			v.Assert().Always(len(v.Timeline().Snapshot("put_done")) > 0, "puts complete")
		}}
	}}
	return cfg
}

func TestDeterminismCanary(t *testing.T) {
	cfg := chaosCfg()
	var prev SeedResult
	for seed := uint64(1); seed <= 12; seed++ {
		a, b := RunSeed(cfg, seed), RunSeed(cfg, seed)
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: %x/%d/%d vs %x/%d/%d", seed, a.TraceHash, a.RandCalls, a.Steps, b.TraceHash, b.RandCalls, b.Steps)
		}
		logged := cfg
		logged.Log = true
		c := RunSeed(logged, seed)
		if c.TraceHash != a.TraceHash || c.RandCalls != a.RandCalls || c.Steps != a.Steps || len(c.Log) == 0 {
			t.Fatalf("seed %d: logging changed the run (log lines %d)", seed, len(c.Log))
		}
		if len(a.Failures) != 0 {
			t.Fatalf("seed %d: %+v", seed, a.Failures)
		}
		if seed > 1 && a.TraceHash == prev.TraceHash {
			t.Fatalf("seeds %d and %d share a trace hash", seed-1, seed)
		}
		prev = a
	}
}

func TestChaosExercisesTheKernel(t *testing.T) {
	cfg := chaosCfg()
	var crashes, exits int
	var faults, deadline uint64
	for seed := uint64(1); seed <= 12; seed++ {
		r := RunSeed(cfg, seed)
		crashes += r.Crashes
		exits += r.Exits
		faults += stat(r, "faults")
		deadline += stat(r, "deadline_kills")
	}
	if crashes == 0 || exits == 0 || faults == 0 {
		t.Fatalf("crashes=%d exits=%d faults=%d", crashes, exits, faults)
	}
}

func TestCheckDeterminismDetectsDivergence(t *testing.T) {
	cfg := chaosCfg()
	cfg.Seeds = []uint64{1, 2, 3, 4}
	cfg.CheckDeterminism = 2
	rep, err := NewRunner().Run(cfg)
	if err != nil || len(rep.Nondeterminism) != 0 {
		t.Fatalf("clean config flagged: %v %v", err, rep.Nondeterminism)
	}
	runs := 0
	bad := baseCfg(scripted(func(m *scriptM, ev core.Event) []core.Action {
		if _, ok := ev.Body.(core.NodeStarted); ok {
			runs++ // package-level state leaking across runs: the bug the check exists for
			return []core.Action{core.SetTimer{ID: 1, Gen: 1, After: core.Duration(runs) * core.Millisecond}}
		}
		return nil
	}))
	bad.Seeds = []uint64{1, 2}
	bad.CheckDeterminism = 1
	rep, _ = NewRunner().Run(bad)
	if len(rep.Nondeterminism) == 0 || !hasFailure(rep.Seeds[0], FailNotRepeatable, "") {
		t.Fatalf("divergence not detected: %+v", rep.Nondeterminism)
	}
}

func TestReplay(t *testing.T) {
	cfg := chaosCfg()
	plain := RunSeed(cfg, 5)
	rep, err := NewRunner().Replay(cfg, 5)
	if err != nil || rep.TraceHash != plain.TraceHash || len(rep.Log) == 0 {
		t.Fatalf("replay: %v hash %x vs %x log %d", err, rep.TraceHash, plain.TraceHash, len(rep.Log))
	}
	if _, err := NewRunner().Run(Config{Processes: []Process{{Name: "x"}}}); err == nil {
		t.Fatal("a Process without a Factory must be rejected")
	}
}

func TestEngineLanesStableDisjointAndDistinct(t *testing.T) {
	r := newRun(Config{}, 1)
	c := &fakeServiceContext{r: r, name: "e"}
	d := &fakeServiceContext{r: r, name: "other"}
	seen := map[DeliveryLane]string{}
	for i := range 1000 {
		key := fmt.Sprintf("fakeobjectstore/node-%d", i)
		l := c.Lane(key)
		if l >= laneForSystem || l == 0 {
			t.Fatalf("%s: lane %d in or beside the kernel-reserved range", key, l)
		}
		if prev, dup := seen[l]; dup {
			t.Fatalf("keys %q and %q share lane %d", prev, key, l)
		}
		seen[l] = key
	}
	for l, key := range seen {
		if c.Lane(key) != l || d.Lane(key) != l {
			t.Fatalf("lane for %q is not stable", key)
		}
	}
	if c.Lane("net/misc") == c.Lane("net/heal") {
		t.Fatal("distinct keys share a lane")
	}
	for _, l := range []DeliveryLane{laneForSimulator, nodeLane(&node{idx: 3}, 0), nodeLane(&node{idx: 3}, 1)} {
		if _, clash := seen[l]; clash {
			t.Fatalf("engine lane collides with reserved lane %d", l)
		}
	}
}

type phaseWl struct {
	wl
	phases []Phase
	emit   bool
}

func (w *phaseWl) OnPhase(p Phase) []core.Action {
	w.phases = append(w.phases, p)
	if !w.emit {
		return nil
	}
	return []core.Action{fact("wl_phase", int64(p))}
}

func TestPhaseAwareWorkload(t *testing.T) {
	run := func(emit bool) (SeedResult, *phaseWl, []TimedFact, []TimedFact) {
		w := &phaseWl{emit: emit}
		cfg := baseCfg(scripted(ping))
		cfg.Workloads = []WorkloadFactory{func(core.Environment, core.NodeInstance, []core.NodeInstance) Workload { return w }}
		var sp, wp []TimedFact
		cfg.Invariants = []func() SafetyRule{func() SafetyRule {
			return &fnInvariant{atEnd: func(v View) {
				sp, wp = v.Timeline().Snapshot("sim_phase"), v.Timeline().Snapshot("wl_phase")
			}}
		}}
		return RunSeed(cfg, 1), w, sp, wp
	}
	res, w, sp, wp := run(true)
	if len(res.Failures) != 0 {
		t.Fatalf("%+v", res.Failures)
	}
	if len(w.phases) != 2 || w.phases[0] != PhaseRecovering || w.phases[1] != PhaseRecover {
		t.Fatalf("OnPhase calls: %v", w.phases)
	}
	if len(sp) != 2 || sp[0].At != Time(core.Second) || sp[1].At != Time(2*core.Second) {
		t.Fatalf("sim_phase facts: %+v", sp)
	}
	if v, _ := sp[0].Fact.Fields[0].Value.(core.U64); v != core.U64(PhaseRecovering) {
		t.Fatalf("phase field: %+v", sp[0].Fact)
	}
	if len(wp) != 2 || wp[0].At != sp[0].At {
		t.Fatalf("actions not dispatched at the phase change: %+v", wp)
	}
	res2, _, _, _ := run(true)
	res3, _, _, _ := run(false)
	if res2.TraceHash != res.TraceHash || res3.TraceHash == res.TraceHash {
		t.Fatal("OnPhase actions must be deterministic and trace-hashed")
	}
}

func TestExpectReachableAndAssertions(t *testing.T) {
	cfg := baseCfg(scripted(ping))
	cfg.Seeds = []uint64{1, 2}
	cfg.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			v.Assert().Reachable("hit")
			v.Assert().Always(true, "fine")
		}}
	}}
	cfg.ExpectReachable = []core.PropertyID{"hit", "never ran", "also never"}
	rep, err := NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var cov []core.PropertyID
	for _, a := range rep.Coverage {
		cov = append(cov, a.Prop)
		if a.Kind != "reachable" || a.Checks != 0 {
			t.Fatalf("%+v", a)
		}
	}
	if len(cov) != 2 || cov[0] != "never ran" || cov[1] != "also never" {
		t.Fatalf("coverage: %v", cov)
	}
	got := map[core.PropertyID]AssertionStat{}
	for _, a := range rep.Assertions {
		got[a.Prop] = a
	}
	if a := got["hit"]; a.Kind != "reachable" || a.Checks != 2 || len(a.Seeds) != 2 {
		t.Fatalf("hit: %+v", a)
	}
	if a := got["fine"]; a.Kind != "always" || a.Passes != 2 || len(a.Seeds) != 0 {
		t.Fatalf("fine: %+v", a)
	}
	if _, ok := got["never ran"]; !ok || len(rep.Always) != 0 {
		t.Fatalf("%+v", rep)
	}
}
