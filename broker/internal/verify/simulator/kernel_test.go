package simulator

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ---- test-only machine: a scriptable state machine ----

type scriptM struct {
	fn   func(m *scriptM, event core.Event) []core.Action
	env  core.Environment
	self machine.NodeInstance
	n    int
	h    uint64
	gen  machine.TimerVersion
}

func (m *scriptM) Step(event core.Event) []core.Action {
	m.n++
	m.h = m.h*31 + uint64(m.n)
	return m.fn(m, event)
}
func (m *scriptM) StateHash() uint64 { return m.h }

func scripted(fn func(m *scriptM, event core.Event) []core.Action) core.MachineFactory {
	return func(env core.Environment, self machine.NodeInstance) core.Machine {
		return &scriptM{fn: fn, env: env, self: self}
	}
}

func fact(name string, v int64) core.RecordFact {
	return core.RecordFact{Fact: machine.Fact{Name: name, Fields: []machine.Field{{Key: "v", Value: machine.Int64Value(v)}}}}
}

// ping: timer ping-pong that emits a "started" fact on boot and a "ping" fact
// per tick, every 10ms.
func ping(m *scriptM, event core.Event) []core.Action {
	switch b := event.Body.(type) {
	case core.NodeStarted:
		m.gen++
		return []core.Action{fact("started", 0), core.SetTimer{ID: 1, Gen: m.gen, After: 10 * machine.Millisecond}}
	case core.TimerFired:
		if b.Gen != m.gen {
			return nil
		}
		m.gen++
		return []core.Action{fact("ping", int64(m.n)), core.SetTimer{ID: 1, Gen: m.gen, After: 10 * machine.Millisecond}}
	}
	return nil
}

// ---- test-only engine: echo with latency and a fault coin ----

type echoApply struct {
	from    machine.NodeInstance
	request machine.RequestID
	lost    bool
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
func (e *echo) Handles(a core.Action) bool { _, ok := putOf(a); return ok }

// putOf returns the Put inside an action, if the action is an object store Put.
func putOf(a core.Action) (objectstore.Put, bool) {
	if oc, ok := a.(core.ObjectCall); ok {
		put, isPut := oc.Call.(objectstore.Put)
		return put, isPut
	}
	return objectstore.Put{}, false
}

func (e *echo) Execute(ctx FakeServiceContext, from machine.NodeInstance, a core.Action) {
	put, _ := putOf(a)
	lost := !e.recovered && ctx.Rand().Chance(e.lostProb)
	if lost {
		ctx.Fault("echo_response_lost")
		ctx.Assert().Sometimes(true, echoLostResponse)
	}
	ctx.Self(ctx.Now()+Time(e.lat.Sample(ctx.Rand())), 1, echoApply{from, put.Req, lost})
}
func (e *echo) OnInternal(ctx FakeServiceContext, body any) {
	a := body.(echoApply)
	e.applied++
	done := objectstore.PutDone{Req: a.request, Outcome: core.Applied}
	if a.lost {
		e.lost++
		done.Err, done.Outcome = core.ErrTimeout, core.MaybeApplied
	}
	ctx.Deliver(a.from, ctx.Now()+Time(machine.Millisecond), 2, core.ObjectDone{Result: done})
}
func (e *echo) OnCrash(ctx FakeServiceContext, ref machine.NodeInstance) { e.crashes++ }
func (e *echo) EnterRecovery()                                           { e.recovered = true }

// echo is also ground truth for Objects().
func (e *echo) Object(string) ([]byte, bool)    { return nil, false }
func (e *echo) EachObject(func(string, []byte)) {}
func (e *echo) Gen() uint64                     { return e.applied }

// putter: every tick sends an ObjectPut; emits "put_done" on the completion.
func putter(m *scriptM, event core.Event) []core.Action {
	switch b := event.Body.(type) {
	case core.NodeStarted:
		m.gen++
		return []core.Action{fact("started", 0), core.SetTimer{ID: 1, Gen: m.gen, After: 5 * machine.Millisecond}}
	case core.TimerFired:
		if b.Gen != m.gen {
			return nil
		}
		m.gen++
		return []core.Action{
			core.ObjectCall{Call: objectstore.Put{Req: machine.RequestID(m.n), Key: fmt.Sprint("k", m.n), Data: []byte{byte(m.n)}}},
			core.SetTimer{ID: 1, Gen: m.gen, After: 5 * machine.Millisecond},
		}
	case core.ObjectDone:
		if put, ok := b.Result.(objectstore.PutDone); ok {
			return []core.Action{fact("put_done", int64(put.Err))}
		}
	case core.StopRequest:
		return []core.Action{core.ShutdownDone{}}
	}
	return nil
}

func baseCfg(f core.MachineFactory) Config {
	return Config{
		Seeds:     []uint64{1},
		Processes: []Process{{Name: "b", Count: 1, Factory: f}},
		Phases:    Phases{Chaos: machine.Second, Settling: machine.Second, Settle: 10 * machine.Second},
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

func hasFailure(res SeedResult, k FailKind, prop string) bool {
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
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if m.n == 1 {
			firsts = append(firsts, event.Body)
		}
		return ping(m, event)
	})
	config := baseCfg(f)
	config.Scripted = []PlannedFault{{At: Time(300 * machine.Millisecond), Node: "b-0", Kind: EntryCrash}}
	res := RunSeed(config, 1)
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
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.StopRequest); ok {
			return []core.Action{core.ShutdownDone{}}
		}
		return ping(m, event)
	})
	config := baseCfg(f)
	config.Scripted = []PlannedFault{{At: Time(300 * machine.Millisecond), Node: "b-0", Kind: EntryStop}}
	res := RunSeed(config, 1)
	if len(res.Failures) != 0 || stat(res, "graceful_stops") != 1 {
		t.Fatalf("graceful_stops=%d failures=%+v", stat(res, "graceful_stops"), res.Failures)
	}
}

func TestStartedFirstEvenWithQueuedData(t *testing.T) {
	// A data event queued before the node's first Step must not overtake NodeStarted.
	var firsts []core.EventData
	config := baseCfg(scripted(func(m *scriptM, event core.Event) []core.Action {
		if m.n == 1 {
			firsts = append(firsts, event.Body)
		}
		return nil
	}))
	r := newRun(config, 1)
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
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.NodeStarted); ok {
			return []core.Action{core.StopForRestart{Cause: "x"}, fact("after", 0)}
		}
		return nil
	})
	config := baseCfg(f)
	config.HaltOnFirst = true // a zero RecoveryDelay would crash-loop at one instant
	res := RunSeed(config, 1)
	if !hasFailure(res, FailProtocol, "trip_not_last") || res.Trips == 0 {
		t.Fatalf("%+v trips=%d", res.Failures, res.Trips)
	}
}

func TestTripLastCrashesAndRestarts(t *testing.T) {
	boots := 0
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		switch event.Body.(type) {
		case core.NodeStarted:
			boots++
			return []core.Action{core.SetTimer{ID: 1, Gen: 1, After: 50 * machine.Millisecond}}
		case core.TimerFired:
			if boots == 1 {
				return []core.Action{fact("pre", 0), core.StopForRestart{Cause: "fenced"}}
			}
		}
		return nil
	})
	config := baseCfg(f)
	config.Attrition.RecoveryDelay = [2]machine.Duration{20 * machine.Millisecond, 30 * machine.Millisecond}
	res := RunSeed(config, 1)
	if len(res.Failures) != 0 || res.Trips != 1 || res.Crashes != 0 || boots != 2 {
		t.Fatalf("trips=%d crashes=%d boots=%d %+v", res.Trips, res.Crashes, boots, res.Failures)
	}
}

func TestCrashDropsEntriesAndRestartsWithNewIncarnation(t *testing.T) {
	var incs []machine.BootNumber
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.NodeStarted); ok {
			incs = append(incs, m.self.Inc)
		}
		return ping(m, event)
	})
	config := baseCfg(f)
	config.Scripted = []PlannedFault{{At: Time(305 * machine.Millisecond), Node: "b-0", Kind: EntryCrash}}
	config.Attrition.RecoveryDelay = [2]machine.Duration{100 * machine.Millisecond, 100 * machine.Millisecond}
	var crashAt Time
	var oldPingsAfter int
	config.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			c := v.Timeline().Snapshot("sim_crashed")
			if len(c) != 1 {
				v.Assert().Always(false, oneCrashFact)
				return
			}
			crashAt = c[0].At
			for _, p := range v.Timeline().Snapshot("ping") {
				if p.Source.Inc == incs[0] && p.At > crashAt {
					oldPingsAfter++
				}
			}
			v.Assert().Always(len(v.Timeline().Snapshot("started")) == 2, restartedOnce)
		}}
	}}
	res := RunSeed(config, 1)
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
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.StopRequest); ok {
			return []core.Action{fact("draining", 0), core.ShutdownDone{}}
		}
		return ping(m, event)
	})
	config := baseCfg(f)
	config.Attrition = NodeKills{MaxDead: 1, CrashProb: 1, GracefulWeight: 1, RecoveryDelay: [2]machine.Duration{10 * machine.Millisecond, 20 * machine.Millisecond}}
	res := RunSeed(config, 1)
	if len(res.Failures) != 0 || res.Exits == 0 || res.Crashes != 0 || stat(res, "deadline_kills") != 0 {
		t.Fatalf("exits=%d crashes=%d %+v", res.Exits, res.Crashes, res.Failures)
	}
}

func TestStopDeadlineKill(t *testing.T) {
	var deadlines []machine.NodeTime
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if s, ok := event.Body.(core.StopRequest); ok {
			deadlines = append(deadlines, s.Deadline-event.At)
			return nil // ignores the Stop
		}
		return ping(m, event)
	})
	config := baseCfg(f)
	config.Attrition = NodeKills{MaxDead: 1, CrashProb: 1, GracefulWeight: 1, RecoveryDelay: [2]machine.Duration{10 * machine.Millisecond, 10 * machine.Millisecond}}
	res := RunSeed(config, 1)
	if len(res.Failures) != 0 || res.Exits != 0 || res.Crashes == 0 || stat(res, "deadline_kills") == 0 {
		t.Fatalf("exits=%d crashes=%d %+v stats=%+v", res.Exits, res.Crashes, res.Failures, res.Stats)
	}
	if len(deadlines) == 0 || deadlines[0] != machine.NodeTime(stopWaitTime) {
		t.Fatalf("deadline offset = %v", deadlines)
	}
}

func TestInvariantsSeeFactsAndReport(t *testing.T) {
	config := baseCfg(scripted(ping))
	config.Seeds = []uint64{1, 2, 3}
	seen := 0
	config.Invariants = []func() SafetyRule{func() SafetyRule {
		var cur Cursor
		return &fnInvariant{
			observe: func(v View) { seen += len(v.Timeline().Since("ping", &cur)) },
			atEnd: func(v View) {
				v.Assert().Always(len(v.Timeline().Snapshot("ping")) > 0, sawPings)
				v.Assert().Always(false, alwaysBroken)
				v.Assert().Sometimes(false, neverTrue)
				v.Assert().Sometimes(true, pingsHappen)
				v.Assert().Reachable(endReached)
			},
		}
	}}
	rep, err := NewRunner().Run(config)
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
	config := baseCfg(scripted(ping))
	config.MaxSteps = 20
	res := RunSeed(config, 1)
	if !hasFailure(res, FailMaxSteps, "") || res.Steps != 20 {
		t.Fatalf("steps=%d %+v", res.Steps, res.Failures)
	}
}

func TestPanicRecovery(t *testing.T) {
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if m.n == 3 {
			panic("boom")
		}
		return ping(m, event)
	})
	res := RunSeed(baseCfg(f), 1)
	if !hasFailure(res, FailPanic, "") || res.Crashes == 0 || res.Steps < 100 {
		t.Fatalf("crashes=%d steps=%d %+v", res.Crashes, res.Steps, res.Failures)
	}
	config := baseCfg(f)
	config.HaltOnFirst = true
	halted := RunSeed(config, 1)
	if !hasFailure(halted, FailPanic, "") || halted.Steps >= res.Steps || halted.Steps > 10 {
		t.Fatalf("HaltOnFirst: steps=%d", halted.Steps)
	}
	// A panicking factory is a failure, not a crash of the simulator.
	config = baseCfg(func(core.Environment, machine.NodeInstance) core.Machine { panic("factory") })
	if r := RunSeed(config, 1); !hasFailure(r, FailPanic, "") {
		t.Fatalf("%+v", r.Failures)
	}
}

func TestEngineRouting(t *testing.T) {
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.NodeStarted); ok {
			return []core.Action{core.ObjectCall{Call: objectstore.Delete{Req: 1, Key: "k"}}}
		}
		return nil
	})
	config := baseCfg(f)
	config.HaltOnFirst = true
	res := RunSeed(config, 1)
	if !hasFailure(res, FailProtocol, "unrouted_action") {
		t.Fatalf("no engine: %+v", res.Failures)
	}
	config.Engines = func(SeedConfig) []FakeService { return []FakeService{&echoDel{n: "a"}, &echoDel{n: "b"}} }
	res = RunSeed(config, 1)
	if !hasFailure(res, FailProtocol, "unrouted_action") {
		t.Fatalf("two engines: %+v", res.Failures)
	}
	config.Engines = func(SeedConfig) []FakeService { return []FakeService{&echoDel{n: "a"}} }
	if res = RunSeed(config, 1); len(res.Failures) != 0 {
		t.Fatalf("one engine: %+v", res.Failures)
	}
}

type echoDel struct{ n string }

func (e *echoDel) StateHash() uint64 { return 0 }
func (e *echoDel) Name() string      { return e.n }
func (e *echoDel) Handles(a core.Action) bool {
	if oc, ok := a.(core.ObjectCall); ok {
		_, isDelete := oc.Call.(objectstore.Delete)
		return isDelete
	}
	return false
}
func (e *echoDel) Execute(FakeServiceContext, machine.NodeInstance, core.Action) {}
func (e *echoDel) OnInternal(FakeServiceContext, any)                            {}
func (e *echoDel) OnCrash(FakeServiceContext, machine.NodeInstance)              {}
func (e *echoDel) EnterRecovery()                                                {}

func TestClockSkewAndWall(t *testing.T) {
	var ats []machine.NodeTime
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.NodeStarted); ok {
			ats = append(ats, event.At)
			if event.Wall != wallClockStart {
				t.Errorf("Wall = %d, want the scripted epoch at sim time 0", event.Wall)
			}
		}
		return nil
	})
	config := baseCfg(f)
	config.Processes[0].Count = 6
	config.Network.ClockSkewMax = 100 * machine.Millisecond
	RunSeed(config, 1)
	distinct := map[machine.NodeTime]bool{}
	for _, a := range ats {
		if a < 0 || a > machine.NodeTime(100*machine.Millisecond) {
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
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		order = append(order, fmt.Sprintf("%T", event.Body))
		return ping(m, event)
	})
	free := baseCfg(f)
	free.Phases = Phases{Chaos: 200 * machine.Millisecond, Settle: machine.Second}
	fres := RunSeed(free, 1)
	order = nil
	busy := free
	busy.Cost = CostParams{PerEvent: 40 * machine.Millisecond}
	bres := RunSeed(busy, 1)
	if len(order) == 0 || order[0] != "core.NodeStarted" {
		t.Fatalf("order: %v", order)
	}
	if bres.Steps >= fres.Steps || stat(bres, "wakes") == 0 {
		t.Fatalf("a busy node must step less: busy=%d free=%d", bres.Steps, fres.Steps)
	}
}

func TestWorkloadRole(t *testing.T) {
	var brokers []machine.NodeInstance
	var w *wl
	config := baseCfg(scripted(ping))
	config.Processes[0].Count = 2
	config.QuiesceEvery = 100 * machine.Millisecond
	config.Workloads = []WorkloadFactory{func(env core.Environment, self machine.NodeInstance, b []machine.NodeInstance) Workload {
		brokers = b
		w = &wl{}
		return w
	}}
	var quiesce int
	config.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			quiesce = len(v.Timeline().Snapshot("sim_quiesce"))
			v.Assert().Always(v.History() != nil, historyExists)
		}}
	}}
	res := RunSeed(config, 1)
	if len(res.Failures) != 0 || len(brokers) != 2 || brokers[0].Node != "b-0" || brokers[1].Node != "b-1" {
		t.Fatalf("%+v %v", res.Failures, brokers)
	}
	if w.n == 0 || w.history == nil || quiesce == 0 {
		t.Fatalf("workload steps=%d history=%v quiesce=%d", w.n, w.history != nil, quiesce)
	}
	// A workload may not StopForRestart.
	config.Workloads = []WorkloadFactory{func(core.Environment, machine.NodeInstance, []machine.NodeInstance) Workload { return &wl{trip: true} }}
	config.Invariants = nil
	if res = RunSeed(config, 1); !hasFailure(res, FailProtocol, "workload_ended") {
		t.Fatalf("%+v", res.Failures)
	}
}

type wl struct {
	n       int
	trip    bool
	history History
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
func (w *wl) SetHistory(h History) { w.history = h }

func TestTimerCancel(t *testing.T) {
	fired := 0
	f := scripted(func(m *scriptM, event core.Event) []core.Action {
		switch event.Body.(type) {
		case core.NodeStarted:
			return []core.Action{core.SetTimer{ID: 7, Gen: 1, After: 10 * machine.Millisecond}, core.CancelTimer{ID: 7}}
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
	config := baseCfg(scripted(putter))
	config.Processes[0].Count = 3
	config.Engines = func(sc SeedConfig) []FakeService {
		return []FakeService{&echo{lat: sc.ObjectStore.ApplyLatency, lostProb: sc.ObjectStore.LostResponse}}
	}
	config.ObjectStore = ObjectStoreFaults{
		ApplyLatency: FastWithRareSlow{FastMin: machine.Millisecond, FastMax: 3 * machine.Millisecond, SlowMin: 50 * machine.Millisecond, SlowMax: 80 * machine.Millisecond, SlowProb: 0.1},
		LostResponse: 0.05,
	}
	config.Network.ClockSkewMax = 100 * machine.Millisecond
	config.Attrition = NodeKills{MaxDead: 1, CrashProb: 0.01, CrashWeight: 1, GracefulWeight: 1, RecoveryDelay: [2]machine.Duration{10 * machine.Millisecond, 50 * machine.Millisecond}}
	config.Cost = CostParams{PerEvent: machine.Millisecond, PauseProb: 0.05, Pause: FastWithRareSlow{FastMin: 5 * machine.Millisecond, FastMax: 20 * machine.Millisecond}}
	config.Swarm = func(seed uint64, r CountedRand, c *SeedConfig) {
		if r.Chance(0.5) {
			c.ObjectStore.LostResponse = 0
		}
		c.Attrition.CrashProb *= float64(1 + r.Intn(3))
	}
	config.QuiesceEvery = 250 * machine.Millisecond
	config.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			v.Assert().Always(v.Objects().Gen() > 0, engineAppliedSomething)
			v.Assert().Always(len(v.Timeline().Snapshot("put_done")) > 0, putsComplete)
		}}
	}}
	return config
}

func TestDeterminismCanary(t *testing.T) {
	config := chaosCfg()
	var prev SeedResult
	for seed := uint64(1); seed <= 12; seed++ {
		a, b := RunSeed(config, seed), RunSeed(config, seed)
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: %x/%d/%d vs %x/%d/%d", seed, a.TraceHash, a.RandCalls, a.Steps, b.TraceHash, b.RandCalls, b.Steps)
		}
		logged := config
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
	config := chaosCfg()
	var crashes, exits int
	var faults, deadline uint64
	for seed := uint64(1); seed <= 12; seed++ {
		r := RunSeed(config, seed)
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
	config := chaosCfg()
	config.Seeds = []uint64{1, 2, 3, 4}
	config.CheckDeterminism = 2
	rep, err := NewRunner().Run(config)
	if err != nil || len(rep.Nondeterminism) != 0 {
		t.Fatalf("clean config flagged: %v %v", err, rep.Nondeterminism)
	}
	runs := 0
	bad := baseCfg(scripted(func(m *scriptM, event core.Event) []core.Action {
		if _, ok := event.Body.(core.NodeStarted); ok {
			runs++ // package-level state leaking across runs: the bug the check exists for
			return []core.Action{core.SetTimer{ID: 1, Gen: 1, After: machine.Duration(runs) * machine.Millisecond}}
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
	config := chaosCfg()
	plain := RunSeed(config, 5)
	rep, err := NewRunner().Replay(config, 5)
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
		config := baseCfg(scripted(ping))
		config.Workloads = []WorkloadFactory{func(core.Environment, machine.NodeInstance, []machine.NodeInstance) Workload { return w }}
		var sp, wp []TimedFact
		config.Invariants = []func() SafetyRule{func() SafetyRule {
			return &fnInvariant{atEnd: func(v View) {
				sp, wp = v.Timeline().Snapshot("sim_phase"), v.Timeline().Snapshot("wl_phase")
			}}
		}}
		return RunSeed(config, 1), w, sp, wp
	}
	res, w, sp, wp := run(true)
	if len(res.Failures) != 0 {
		t.Fatalf("%+v", res.Failures)
	}
	if len(w.phases) != 2 || w.phases[0] != PhaseRecovering || w.phases[1] != PhaseRecover {
		t.Fatalf("OnPhase calls: %v", w.phases)
	}
	if len(sp) != 2 || sp[0].At != Time(machine.Second) || sp[1].At != Time(2*machine.Second) {
		t.Fatalf("sim_phase facts: %+v", sp)
	}
	if v, _ := sp[0].Fact.Fields[0].Value.(machine.Uint64Value); v != machine.Uint64Value(PhaseRecovering) {
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
	config := baseCfg(scripted(ping))
	config.Seeds = []uint64{1, 2}
	config.Invariants = []func() SafetyRule{func() SafetyRule {
		return &fnInvariant{atEnd: func(v View) {
			v.Assert().Reachable(hit)
			v.Assert().Always(true, fine)
		}}
	}}
	config.ExpectReachable = []string{"hit", "never ran", "also never"}
	rep, err := NewRunner().Run(config)
	if err != nil {
		t.Fatal(err)
	}
	var cov []string
	for _, a := range rep.Coverage {
		cov = append(cov, a.Prop)
		if a.Kind != "reachable" || a.Checks != 0 {
			t.Fatalf("%+v", a)
		}
	}
	if len(cov) != 2 || cov[0] != "never ran" || cov[1] != "also never" {
		t.Fatalf("coverage: %v", cov)
	}
	got := map[string]AssertionStat{}
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

// maxStepGap is the longest time between two Steps of one process.
func maxStepGap(times []machine.NodeTime) machine.NodeTime {
	var longest machine.NodeTime
	for i := 1; i < len(times); i++ {
		longest = max(longest, times[i]-times[i-1])
	}
	return longest
}

// A frozen process takes no Step for the whole freeze, and it carries on after
// it. Without the fault the same process steps every few milliseconds.
func TestFreezeParksAProcessAndLetsItResume(t *testing.T) {
	const freeze = 400 * machine.Millisecond
	run := func(prob float64) ([]machine.NodeTime, SeedResult) {
		var times []machine.NodeTime
		f := scripted(func(m *scriptM, event core.Event) []core.Action {
			times = append(times, event.At)
			return ping(m, event)
		})
		cfg := baseCfg(f)
		cfg.Processes[0].Freezable = true
		cfg.Phases = Phases{Chaos: 300 * machine.Millisecond, Settling: machine.Second, Settle: machine.Second}
		cfg.Attrition = NodeKills{FreezeProb: prob, FreezeDur: [2]machine.Duration{freeze, freeze}}
		res := RunSeed(cfg, 1)
		return times, res
	}

	free, freeRes := run(0)
	if stat(freeRes, "freezes") != 0 || maxStepGap(free) >= machine.NodeTime(50*machine.Millisecond) {
		t.Fatalf("without the fault: freezes=%d, longest gap %v", stat(freeRes, "freezes"), maxStepGap(free))
	}

	frozen, res := run(1)
	if stat(res, "freezes") == 0 {
		t.Fatal("no freeze happened with FreezeProb 1")
	}
	if got := maxStepGap(frozen); got < machine.NodeTime(freeze) {
		t.Fatalf("longest gap between Steps is %v, want at least the freeze %v", got, freeze)
	}
	// The process steps again after the freeze: the last Step is well after the first freeze ends.
	if last := frozen[len(frozen)-1]; last < machine.NodeTime(freeze) {
		t.Fatalf("process did not resume: last Step at %v", last)
	}
}

// A process that is not marked Freezable is never frozen.
func TestFreezeSkipsProcessesThatDoNotAllowIt(t *testing.T) {
	cfg := baseCfg(scripted(ping))
	cfg.Attrition = NodeKills{FreezeProb: 1, FreezeDur: [2]machine.Duration{machine.Second, machine.Second}}
	if res := RunSeed(cfg, 1); stat(res, "freezes") != 0 {
		t.Fatalf("froze a process that is not Freezable: %d", stat(res, "freezes"))
	}
}
