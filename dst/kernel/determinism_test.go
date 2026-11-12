package kernel_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

var (
	serviceApplied = assert.NewAlways("kernel test: the service applied something")
	putsComplete   = assert.NewAlways("kernel test: puts complete")
	sawPings       = assert.NewAlways("kernel test: saw pings")
	alwaysBroken   = assert.NewAlways("kernel test: always broken")
	neverTrue      = assert.NewSometimes("kernel test: never true")
	pingsHappen    = assert.NewSometimes("kernel test: pings happen")
	endReached     = assert.NewReachable("kernel test: end reached")
	testSite       = fault.NewSite("kernel test: site")
)

// chaosRun is one run with three putters, the echo service with latency and
// lost answers, clock skew, attrition with crashes and graceful stops, a cost
// model, phases and a per-seed swarm.
func chaosRun(seed uint64, log bool) kernel.Result {
	h := newHarness(kernel.Config{
		Seed: seed, Log: log, ClockSkewMax: 100 * machine.Millisecond,
		Attrition: kernel.Attrition{MaxDead: 1, CrashProb: 0.01, CrashWeight: 1, GracefulWeight: 1},
	})
	swarm := h.w.Rand().Derive("swarm")
	h.svc.lat = kernel.FastWithRareSlow{
		FastMin: machine.Millisecond, FastMax: 3 * machine.Millisecond,
		SlowMin: 50 * machine.Millisecond, SlowMax: 80 * machine.Millisecond, SlowProb: 0.1,
	}
	if !swarm.Chance(0.5) {
		h.svc.lostProb = 0.05
	}
	cost := kernel.CostParams{PerEvent: machine.Millisecond, PauseProb: 0.05,
		Pause: kernel.FastWithRareSlow{FastMin: 5 * machine.Millisecond, FastMax: 20 * machine.Millisecond}}
	for i := range 3 {
		h.spawn(fmt.Sprintf("b-%d", i), putter, func(s *kernel.NodeSpec[tin, tout]) {
			s.Attrition = true
			s.Cost = cost
			s.Restart = restartAfter(10*machine.Millisecond, 50*machine.Millisecond)
		})
	}
	h.w.AddRule(&fnRule{atEnd: func(v kernel.View) {
		v.Assert().Always(h.svc.applied > 0, serviceApplied)
		v.Assert().Always(len(v.Timeline().Snapshot("put_done")) > 0, putsComplete)
	}})
	kernel.SchedulePhases(h.w, kernel.Phases{Chaos: machine.Second, Settling: machine.Second, Settle: 10 * machine.Second})
	return h.run()
}

func TestSameSeedSameRun(t *testing.T) {
	var prev kernel.Result
	for seed := uint64(1); seed <= 12; seed++ {
		a, b := chaosRun(seed, false), chaosRun(seed, false)
		if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: %x/%d/%d vs %x/%d/%d", seed, a.Hash, a.RandCalls, a.Steps, b.Hash, b.RandCalls, b.Steps)
		}
		c := chaosRun(seed, true)
		if c.Hash != a.Hash || c.RandCalls != a.RandCalls || c.Steps != a.Steps || len(c.Log) == 0 {
			t.Fatalf("seed %d: logging changed the run (log lines %d)", seed, len(c.Log))
		}
		if !a.OK() {
			t.Fatalf("seed %d: %+v", seed, a.Failures)
		}
		if seed > 1 && a.Hash == prev.Hash {
			t.Fatalf("seeds %d and %d share a trace hash", seed-1, seed)
		}
		prev = a
	}
}

func TestChaosExercisesTheKernel(t *testing.T) {
	var crashes, exits int
	var faults uint64
	for seed := uint64(1); seed <= 12; seed++ {
		r := chaosRun(seed, false)
		crashes += r.Crashes
		exits += r.Exits
		faults += stat(r, "faults")
	}
	if crashes == 0 || exits == 0 || faults == 0 {
		t.Fatalf("crashes=%d exits=%d faults=%d", crashes, exits, faults)
	}
}

func TestRulesSeeFactsAndFailures(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		seen := 0
		h := newHarness(kernel.Config{Seed: seed, End: ms(2000)})
		h.spawn("b-0", ping, nil)
		var cur kernel.Cursor
		h.w.AddRule(&fnRule{
			observe: func(v kernel.View) { seen += len(v.Timeline().Since("ping", &cur)) },
			atEnd: func(v kernel.View) {
				v.Assert().Always(len(v.Timeline().Snapshot("ping")) > 0, sawPings)
				v.Assert().Always(false, alwaysBroken)
				v.Assert().Always(false, alwaysBroken)
				v.Assert().Sometimes(false, neverTrue)
				v.Assert().Sometimes(true, pingsHappen)
				v.Assert().Reachable(endReached)
			},
		})
		res := h.run()
		if seen == 0 {
			t.Fatal("Observe never saw a ping")
		}
		if len(res.Failures) != 1 || res.Failures[0].Kind != kernel.FailViolation ||
			res.Failures[0].Property != alwaysBroken.Name() || res.Failures[0].Repeats != 1 || res.Failures[0].Seed != seed {
			t.Fatalf("failures: %+v", res.Failures)
		}
		cov := res.Coverage
		if c := cov[neverTrue.Name()]; c.Kind != assert.KindSometimes || c.Checks != 1 || c.Passes != 0 {
			t.Fatalf("never true: %+v", c)
		}
		if c := cov[endReached.Name()]; c.Kind != assert.KindReachable || c.Passes != 1 || c.FirstStep != res.Steps {
			t.Fatalf("end reached: %+v (steps %d)", c, res.Steps)
		}
		if c := cov[sawPings.Name()]; c.Kind != assert.KindAlways || c.Passes != 1 {
			t.Fatalf("saw pings: %+v", c)
		}
	}
}

// A failure keeps the last Config.Tail step lines; a clean run keeps none.
func TestTailOnlyOnFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		h := newHarness(kernel.Config{Seed: 1, End: ms(200), Tail: 5})
		h.spawn("b-0", ping, nil)
		if fail {
			h.w.At(ms(100), kernel.NoLane, func() { h.w.Fail(kernel.FailViolation, "x", "y") })
		}
		res := h.run()
		if got := len(res.Tail); (fail && got != 5) || (!fail && got != 0) || len(res.Log) != 0 {
			t.Fatalf("fail=%v: tail %d log %d", fail, got, len(res.Log))
		}
	}
}

func TestFastWithRareSlowSample(t *testing.T) {
	r := rng.New(3)
	ts := kernel.FastWithRareSlow{FastMin: 10, FastMax: 20, SlowMin: 1000, SlowMax: 2000, SlowProb: 0.5}
	fast, slow := 0, 0
	for range 1000 {
		d := ts.Sample(r)
		switch {
		case d >= 10 && d <= 20:
			fast++
		case d >= 1000 && d <= 2000:
			slow++
		default:
			t.Fatalf("out of both ranges: %d", d)
		}
	}
	if fast < 300 || slow < 300 {
		t.Fatalf("fast=%d slow=%d", fast, slow)
	}
	c := r.Calls()
	if (kernel.FastWithRareSlow{FastMin: 5, FastMax: 5}).Sample(r) != 5 || r.Calls() != c {
		t.Fatal("a fixed delay must draw nothing")
	}
}

func TestCostParamsSample(t *testing.T) {
	r := rng.New(1)
	if (kernel.CostParams{}).Sample(r) != 0 || r.Calls() != 0 {
		t.Fatal("zero params: free and draw-free")
	}
	if got := (kernel.CostParams{PerEvent: 10}).Sample(r); got != 10 {
		t.Fatalf("cost = %d", got)
	}
	pause := kernel.CostParams{PerEvent: 1, PauseProb: 1, Pause: kernel.FastWithRareSlow{FastMin: 500, FastMax: 500}}
	if got := pause.Sample(r); got != 501 {
		t.Fatalf("pause cost = %d", got)
	}
}

// NewID is two Uint64 draws, little-endian: the same bytes as the old broker
// NewID for seed 1 (c15c0289...).
func TestEnvNewID(t *testing.T) {
	w := kernel.New(kernel.Config{Seed: 1})
	id := w.Env().NewID()
	if got := hex.EncodeToString(id[:]); got != "c15c0289ec2d0a9167ec8e65a18debbe" || w.Rand().Calls() != 2 {
		t.Fatalf("NewID = %s after %d draws", got, w.Rand().Calls())
	}
}

func TestTimeline(t *testing.T) {
	tl := kernel.NewTimeline()
	var cur, cur2 kernel.Cursor
	f := func(name string, v int64) machine.Fact {
		return machine.Fact{Name: name, Fields: []machine.Field{{Key: "v", Value: machine.Int64Value(v)}}}
	}
	tl.Append(machine.NodeInstance{}, 1, f("a", 1))
	tl.Append(machine.NodeInstance{}, 2, f("b", 2))
	tl.Append(machine.NodeInstance{}, 3, f("a", 3))
	got := tl.Since("a", &cur)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 3 || got[1].At != 3 {
		t.Fatalf("%+v", got)
	}
	if len(tl.Since("a", &cur)) != 0 {
		t.Fatal("the cursor must move")
	}
	tl.Append(machine.NodeInstance{}, 4, f("a", 4))
	if got := tl.Since("a", &cur); len(got) != 1 || got[0].Seq != 4 {
		t.Fatalf("%+v", got)
	}
	if len(tl.Since("a", &cur2)) != 3 || len(tl.Snapshot("b")) != 1 || len(tl.Snapshot("zz")) != 0 || tl.Len() != 4 {
		t.Fatal("independent cursors / snapshot / len")
	}
}

func TestBuggifyIsQuietOutsideTheFaultPhase(t *testing.T) {
	w := kernel.New(kernel.Config{Seed: 1})
	if !w.Env().Fault(testSite, 1) {
		t.Fatal("Fault(p=1) must fire in PhaseFaults")
	}
	w.SetPhase(kernel.PhaseRecovering)
	if w.Env().Fault(testSite, 1) {
		t.Fatal("Fault must not fire outside PhaseFaults")
	}
	w.Env().MaybeP(testSite, 1)
	if _, ok := w.Faults().Sites()[testSite.Name()]; !ok {
		t.Fatal("MaybeP must count the declared site by its name")
	}
}
