package kernel_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// Ports of the broker kernel tests (broker/internal/verify/simulator/kernel_test.go)
// about boots, ends and stops.

func TestBootInputIsFirstStep(t *testing.T) {
	var firsts []tin
	h := newHarness(kernel.Config{Seed: 1, End: ms(2000)})
	a := h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
		if m.n == 1 {
			firsts = append(firsts, in)
		}
		return ping(m, now, in)
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Restart = restartAfter(machine.Millisecond, machine.Millisecond)
	})
	h.w.At(ms(300), kernel.HostLane, func() { h.w.Crash(a, "planned") })
	res := h.run()
	if !res.OK() || res.Crashes != 1 || len(firsts) != 2 {
		t.Fatalf("crashes=%d firsts=%d failures=%+v", res.Crashes, len(firsts), res.Failures)
	}
	for _, in := range firsts {
		if _, ok := in.(started); !ok {
			t.Fatalf("the first Step was %T", in)
		}
	}
}

// An input that arrives before the first Step must not overtake the boot input.
func TestBootFirstEvenWithQueuedInputs(t *testing.T) {
	var firsts []tin
	for seed := range uint64(16) {
		firsts = firsts[:0]
		h := newHarness(kernel.Config{Seed: seed})
		a := h.spawn("b-0", func(m *scriptM, _ machine.Now, in tin) []tout {
			if m.n == 1 {
				firsts = append(firsts, in)
			}
			return nil
		}, nil)
		kernel.Inject[tin](h.w, a, 0, note{"data"})
		kernel.Inject[tin](h.w, a, 0, urgent{"control"})
		res := h.run()
		if !res.OK() || len(firsts) != 1 {
			t.Fatalf("seed %d: %+v %v", seed, res.Failures, firsts)
		}
		if _, ok := firsts[0].(started); !ok {
			t.Fatalf("seed %d: the first Step was %T", seed, firsts[0])
		}
	}
}

func TestEndNotLastIsProtocolFailure(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, HaltOnFirst: true, End: ms(1000)})
	h.spawn("b-0", func(_ *scriptM, _ machine.Now, in tin) []tout {
		if _, ok := in.(started); ok {
			return []tout{endOut{how: machine.Trip, cause: "x"}, fact("after", 0)}
		}
		return nil
	}, func(s *kernel.NodeSpec[tin, tout]) { s.Restart = restartAfter(0, 0) })
	res := h.run()
	if !hasFailure(res, kernel.FailProtocol, "output_after_end") || res.Trips == 0 {
		t.Fatalf("%+v trips=%d", res.Failures, res.Trips)
	}
	if len(h.w.Timeline().Snapshot("after")) != 0 {
		t.Fatal("an output after End was routed")
	}
}

func TestTripRestartsAfterDelay(t *testing.T) {
	boots := 0
	h := newHarness(kernel.Config{Seed: 1, End: ms(2000)})
	h.spawn("b-0", func(_ *scriptM, _ machine.Now, in tin) []tout {
		switch in.(type) {
		case started:
			boots++
			return []tout{setTimer{id: 1, after: 50 * machine.Millisecond, msg: tick{1}}}
		case tick:
			if boots == 1 {
				return []tout{fact("pre", 0), endOut{how: machine.Trip, cause: "fenced"}}
			}
		}
		return nil
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Restart = restartAfter(20*machine.Millisecond, 30*machine.Millisecond)
	})
	res := h.run()
	if !res.OK() || res.Trips != 1 || res.Crashes != 0 || boots != 2 {
		t.Fatalf("trips=%d crashes=%d boots=%d %+v", res.Trips, res.Crashes, boots, res.Failures)
	}
	tripped := h.w.Timeline().Snapshot("sim_tripped")
	restarted := h.w.Timeline().Snapshot("sim_restarted")
	if len(tripped) != 1 || len(restarted) != 1 {
		t.Fatalf("facts: tripped=%d restarted=%d", len(tripped), len(restarted))
	}
	if gap := restarted[0].At - tripped[0].At; gap < ms(20) || gap > ms(30) {
		t.Fatalf("restart after %d, want 20..30 ms", gap)
	}
}

var oneCrashFact = assert.NewAlways("kernel test: one crash fact")

func TestCrashDropsEntriesAndRestartsWithNewIncarnation(t *testing.T) {
	var incs []machine.BootNumber
	h := newHarness(kernel.Config{Seed: 1, End: ms(2000)})
	a := h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
		if _, ok := in.(started); ok {
			incs = append(incs, m.self.Inc)
		}
		return ping(m, now, in)
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Restart = restartAfter(100*machine.Millisecond, 100*machine.Millisecond)
	})
	h.w.At(ms(305), kernel.HostLane, func() { h.w.Crash(a, "planned") })
	oldPingsAfter, startedFacts := 0, 0
	h.w.AddRule(&fnRule{atEnd: func(v kernel.View) {
		c := v.Timeline().Snapshot("sim_crashed")
		v.Assert().Always(len(c) == 1, oneCrashFact)
		for _, p := range v.Timeline().Snapshot("ping") {
			if p.Source.Inc == incs[0] && p.At > c[0].At {
				oldPingsAfter++
			}
		}
		startedFacts = len(v.Timeline().Snapshot("started"))
	}})
	res := h.run()
	if !res.OK() || res.Crashes != 1 || len(incs) != 2 || incs[0] == incs[1] {
		t.Fatalf("%+v crashes=%d incs=%v", res.Failures, res.Crashes, incs)
	}
	if oldPingsAfter != 0 || startedFacts != 2 {
		t.Fatalf("%d steps from the dead incarnation after the crash; %d boots", oldPingsAfter, startedFacts)
	}
	if stat(res, "dropped_by_crash") == 0 || stat(res, "restarts") != 1 {
		t.Fatalf("stats: %+v", res.Stats)
	}
}

func TestAttritionGracefulStopExits(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, Attrition: kernel.Attrition{MaxDead: 1, CrashProb: 1, GracefulWeight: 1}})
	h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
		if _, ok := in.(stopReq); ok {
			return []tout{fact("draining", 0), endOut{how: machine.Exit, cause: "stop"}}
		}
		return ping(m, now, in)
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Attrition = true
		s.Restart = restartAfter(10*machine.Millisecond, 20*machine.Millisecond)
	})
	kernel.SchedulePhases(h.w, kernel.Phases{Chaos: machine.Second, Settling: machine.Second, Settle: 10 * machine.Second})
	res := h.run()
	if !res.OK() || res.Exits == 0 || res.Crashes != 0 || stat(res, "deadline_kills") != 0 {
		t.Fatalf("exits=%d crashes=%d %+v", res.Exits, res.Crashes, res.Failures)
	}
}

func TestStopDeadlineKill(t *testing.T) {
	var offsets []machine.NodeTime
	h := newHarness(kernel.Config{Seed: 1, Attrition: kernel.Attrition{MaxDead: 1, CrashProb: 1, GracefulWeight: 1}})
	h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
		if s, ok := in.(stopReq); ok {
			offsets = append(offsets, s.deadline-now.Mono)
			return nil // ignores the stop
		}
		return ping(m, now, in)
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Attrition = true
		s.Restart = restartAfter(10*machine.Millisecond, 10*machine.Millisecond)
	})
	kernel.SchedulePhases(h.w, kernel.Phases{Chaos: machine.Second, Settling: machine.Second, Settle: 10 * machine.Second})
	res := h.run()
	if !res.OK() || res.Exits != 0 || res.Crashes == 0 || stat(res, "deadline_kills") == 0 {
		t.Fatalf("exits=%d crashes=%d %+v stats=%+v", res.Exits, res.Crashes, res.Failures, res.Stats)
	}
	if len(offsets) == 0 || offsets[0] != machine.NodeTime(5*machine.Second) {
		t.Fatalf("deadline offsets = %v, want the 5 s default grace", offsets)
	}
}

func TestHostStopGraceful(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, End: ms(2000)})
	a := h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
		if _, ok := in.(stopReq); ok {
			return []tout{endOut{how: machine.Exit, cause: "stop"}}
		}
		return ping(m, now, in)
	}, nil)
	h.w.At(ms(300), kernel.HostLane, func() { h.w.Stop(a, 5*machine.Second) })
	res := h.run()
	if !res.OK() || stat(res, "graceful_stops") != 1 || res.Exits != 1 {
		t.Fatalf("graceful_stops=%d exits=%d %+v", stat(res, "graceful_stops"), res.Exits, res.Failures)
	}
	if how, down := h.w.Down(a); !down || how != machine.Exit {
		t.Fatalf("Down = %v %v, want exit", how, down)
	}
	if stat(res, "deadline_kills") != 0 {
		t.Fatal("the kill must be dropped when the node exits first")
	}
}

func TestStopWithoutStopMsgIsProtocolFailure(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, End: ms(100)})
	a := h.spawn("b-0", ping, func(s *kernel.NodeSpec[tin, tout]) { s.StopMsg = nil })
	h.w.At(ms(30), kernel.HostLane, func() { h.w.Stop(a, machine.Second) })
	if res := h.run(); !hasFailure(res, kernel.FailProtocol, "stop_not_allowed") {
		t.Fatalf("%+v", res.Failures)
	}
}

func TestPanicRecovery(t *testing.T) {
	boom := func(m *scriptM, now machine.Now, in tin) []tout {
		if m.n == 3 {
			panic("boom")
		}
		return ping(m, now, in)
	}
	restart := func(s *kernel.NodeSpec[tin, tout]) {
		s.Restart = restartAfter(machine.Millisecond, machine.Millisecond)
	}
	h := newHarness(kernel.Config{Seed: 1, End: ms(2000)})
	h.spawn("b-0", boom, restart)
	res := h.run()
	if !hasFailure(res, kernel.FailPanic, "panic") || res.Crashes == 0 || res.Steps < 100 {
		t.Fatalf("crashes=%d steps=%d %+v", res.Crashes, res.Steps, res.Failures)
	}
	if res.Failures[0].Repeats == 0 {
		t.Fatal("later panics must raise Repeats")
	}
	h = newHarness(kernel.Config{Seed: 1, End: ms(2000), HaltOnFirst: true})
	h.spawn("b-0", boom, restart)
	if halted := h.run(); !hasFailure(halted, kernel.FailPanic, "") || halted.Steps > 10 {
		t.Fatalf("HaltOnFirst: steps=%d", halted.Steps)
	}
	// A panicking factory is a failure, not a crash of the simulator.
	h = newHarness(kernel.Config{Seed: 1})
	h.spawn("b-0", ping, func(s *kernel.NodeSpec[tin, tout]) {
		s.New = func(machine.Env, machine.NodeInstance) machine.Machine[tin, tout] { panic("factory") }
	})
	if r := h.run(); !hasFailure(r, kernel.FailPanic, "") {
		t.Fatalf("%+v", r.Failures)
	}
}

func TestSpawnRejectsBadSpec(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, End: ms(100)})
	if a := h.spawn("x", ping, func(s *kernel.NodeSpec[tin, tout]) { s.New = nil }); a.ID() != "" {
		t.Fatal("a spec without New must give the zero Addr")
	}
	h.spawn("y", ping, nil)
	h.spawn("y", ping, nil)
	res := h.run()
	if !hasFailure(res, kernel.FailProtocol, "spawn") || res.Failures[0].Repeats != 1 {
		t.Fatalf("%+v", res.Failures)
	}
}

func TestMaxSteps(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, MaxSteps: 20})
	h.spawn("b-0", ping, nil)
	if got := h.w.RunUntil(nil); got != kernel.StepLimit {
		t.Fatalf("RunUntil = %d, want StepLimit", got)
	}
	res := h.w.Finish()
	if !hasFailure(res, kernel.FailMaxSteps, "") || res.Steps != 20 {
		t.Fatalf("steps=%d %+v", res.Steps, res.Failures)
	}
}

func TestDriverRole(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1, End: ms(500)})
	b0 := h.spawn("b-0", ping, nil)
	b1 := h.spawn("b-1", ping, nil)
	var upAtBoot []bool
	driverSteps := 0
	h.spawn("workload-0", func(*scriptM, machine.Now, tin) []tout {
		driverSteps++
		return nil
	}, func(s *kernel.NodeSpec[tin, tout]) {
		s.Driver = true
		next := s.New
		s.New = func(env machine.Env, self machine.NodeInstance) machine.Machine[tin, tout] {
			// The driver boots after every other node.
			upAtBoot = append(upAtBoot, h.w.Up(b0), h.w.Up(b1))
			return next(env, self)
		}
	})
	res := h.run()
	if !res.OK() || len(upAtBoot) != 2 || !upAtBoot[0] || !upAtBoot[1] || driverSteps == 0 {
		t.Fatalf("%+v up=%v steps=%d", res.Failures, upAtBoot, driverSteps)
	}

	// A driver may not end.
	h = newHarness(kernel.Config{Seed: 1, End: ms(500)})
	h.spawn("workload-0", func(*scriptM, machine.Now, tin) []tout {
		return []tout{endOut{how: machine.Trip, cause: "no"}}
	}, func(s *kernel.NodeSpec[tin, tout]) { s.Driver = true })
	if res = h.run(); !hasFailure(res, kernel.FailProtocol, "driver_ended") {
		t.Fatalf("%+v", res.Failures)
	}

	// Host code may not crash a driver either.
	h = newHarness(kernel.Config{Seed: 1, End: ms(500)})
	d := h.spawn("workload-0", ping, func(s *kernel.NodeSpec[tin, tout]) { s.Driver = true })
	h.w.At(ms(10), kernel.HostLane, func() { h.w.Crash(d, "nemesis") })
	if res = h.run(); !hasFailure(res, kernel.FailProtocol, "driver_ended") || res.Crashes != 0 {
		t.Fatalf("%+v", res.Failures)
	}
}

func TestDriverOnPhase(t *testing.T) {
	run := func(emit bool) (kernel.Result, []kernel.Phase, []kernel.TimedFact, []kernel.TimedFact) {
		var phases []kernel.Phase
		h := newHarness(kernel.Config{Seed: 1})
		h.spawn("b-0", ping, nil)
		h.spawn("workload-0", func(_ *scriptM, _ machine.Now, in tin) []tout {
			if n, ok := in.(note); ok && emit {
				return []tout{fact("wl_phase", int64(len(n.s)))}
			}
			return nil
		}, func(s *kernel.NodeSpec[tin, tout]) {
			s.Driver = true
			s.OnPhase = func(p kernel.Phase) (tin, bool) {
				phases = append(phases, p)
				return note{string(make([]byte, p))}, true
			}
		})
		kernel.SchedulePhases(h.w, kernel.Phases{Chaos: machine.Second, Settling: machine.Second, Settle: 10 * machine.Second})
		res := h.run()
		return res, phases, h.w.Timeline().Snapshot("sim_phase"), h.w.Timeline().Snapshot("wl_phase")
	}
	res, phases, sp, wp := run(true)
	if !res.OK() {
		t.Fatalf("%+v", res.Failures)
	}
	if len(phases) != 2 || phases[0] != kernel.PhaseRecovering || phases[1] != kernel.PhaseRecover {
		t.Fatalf("OnPhase calls: %v", phases)
	}
	if len(sp) != 2 || sp[0].At != ms(1000) || sp[1].At != ms(2000) {
		t.Fatalf("sim_phase facts: %+v", sp)
	}
	if v, _ := sp[0].Fact.Field("phase").(machine.Uint64Value); v != machine.Uint64Value(kernel.PhaseRecovering) {
		t.Fatalf("phase field: %+v", sp[0].Fact)
	}
	if len(wp) != 2 || wp[0].At != sp[0].At {
		t.Fatalf("outputs not routed at the phase change: %+v", wp)
	}
	res2, _, _, _ := run(true)
	res3, _, _, _ := run(false)
	if res2.Hash != res.Hash || res3.Hash == res.Hash {
		t.Fatal("OnPhase outputs must be repeatable and in the trace hash")
	}
}

func TestPauseHoldsInputsAndResume(t *testing.T) {
	var times []machine.NodeTime
	var notes []string
	h := newHarness(kernel.Config{Seed: 1, End: ms(1000)})
	a := h.spawn("b-0", func(m *scriptM, now machine.Now, in tin) []tout {
		times = append(times, now.Mono)
		if n, ok := in.(note); ok {
			notes = append(notes, n.s)
		}
		return ping(m, now, in)
	}, nil)
	other := 0
	h.spawn("b-1", func(m *scriptM, now machine.Now, in tin) []tout {
		other++
		return ping(m, now, in)
	}, nil)
	h.w.At(ms(50), kernel.HostLane, func() { h.w.Pause(a) })
	h.w.At(ms(60), kernel.HostLane, func() {
		kernel.Inject[tin](h.w, a, 0, note{"1"})
		kernel.Inject[tin](h.w, a, machine.Millisecond, note{"2"})
	})
	h.w.At(ms(450), kernel.HostLane, func() { h.w.Resume(a) })
	res := h.run()
	if !res.OK() {
		t.Fatalf("%+v", res.Failures)
	}
	longest := machine.NodeTime(0)
	for i := 1; i < len(times); i++ {
		longest = max(longest, times[i]-times[i-1])
	}
	if longest < machine.NodeTime(400*machine.Millisecond) {
		t.Fatalf("the longest gap between Steps is %d, want the pause", longest)
	}
	if last := times[len(times)-1]; last < machine.NodeTime(900*machine.Millisecond) {
		t.Fatalf("the node did not resume: last Step at %d", last)
	}
	if len(notes) != 2 || notes[0] != "1" || notes[1] != "2" {
		t.Fatalf("held inputs: %v", notes)
	}
	if other < 90 {
		t.Fatalf("the pause must not stop another node: %d steps", other)
	}
}

func TestCrashAndBoot(t *testing.T) {
	boots := 0
	h := newHarness(kernel.Config{Seed: 3})
	a := h.spawn("n", func(*scriptM, machine.Now, tin) []tout {
		boots++
		return nil
	}, nil)
	h.w.At(ms(1), kernel.HostLane, func() { h.w.Crash(a, "kill") })
	h.w.At(ms(2), kernel.HostLane, func() {
		if h.w.Up(a) {
			t.Error("the node is up after the crash")
		}
		h.w.Boot(a)
	})
	res := h.run()
	if !res.OK() || boots != 2 || res.Crashes != 1 || !h.w.Up(a) {
		t.Fatalf("boots=%d crashes=%d %+v", boots, res.Crashes, res.Failures)
	}
}

func TestCrashOtherAndExit(t *testing.T) {
	steps := map[string]int{}
	h := newHarness(kernel.Config{Seed: 1})
	mk := func(name string, out ...tout) kernel.Addr[tin] {
		return h.spawn(name, func(*scriptM, machine.Now, tin) []tout {
			steps[name]++
			return out
		}, func(s *kernel.NodeSpec[tin, tout]) { s.Boot = nil })
	}
	victim := mk("victim")
	quitter := mk("quitter", endOut{how: machine.Exit, cause: "done"})
	h.w.At(0, kernel.HostLane, func() { h.w.Crash(victim, "test") })
	kernel.Inject[tin](h.w, quitter, 0, note{"go"})
	kernel.Inject[tin](h.w, victim, 10, note{"late"})
	kernel.Inject[tin](h.w, quitter, 10, note{"late"})
	res := h.run()
	if !res.OK() || steps["victim"] != 0 || steps["quitter"] != 1 {
		t.Fatalf("steps = %v %+v", steps, res.Failures)
	}
	if how, down := h.w.Down(victim); !down || how != machine.Crash {
		t.Fatal("the victim must be down after a crash")
	}
	if how, down := h.w.Down(quitter); !down || how != machine.Exit {
		t.Fatal("the quitter must be down after an exit")
	}
	if stat(res, "dropped_stale") != 2 {
		t.Fatalf("dropped_stale = %d, want the two late inputs", stat(res, "dropped_stale"))
	}
	h.w.Boot(quitter)
	if _, down := h.w.Down(quitter); down || !h.w.Up(quitter) {
		t.Fatal("Boot must bring the node up")
	}
}

func TestRefLifecycleCallsNeedTheCurrentIncarnation(t *testing.T) {
	h := newHarness(kernel.Config{Seed: 1})
	a := h.spawn("n", ping, nil)
	h.w.Start()
	old, ok := kernel.CurrentRef(h.w, a)
	if !ok || !h.w.Up(old) {
		t.Fatal("no current incarnation after Start")
	}
	h.w.Crash(a, "x")
	h.w.Boot(a)
	if h.w.Up(old) || !h.w.Up(a) {
		t.Fatal("an old Ref must not be up; the Addr must be")
	}
	h.w.Crash(old, "stale")
	if !h.w.Up(a) {
		t.Fatal("Crash of an old Ref must not touch the new incarnation")
	}
	cur, _ := h.w.Current(a)
	if cur == old.Instance() || cur.Node != "n" {
		t.Fatalf("Current = %+v", cur)
	}
}
