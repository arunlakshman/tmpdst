package campaign

import (
	"testing"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
)

type stubAssert struct{ sometimes []string }

func (a *stubAssert) Always(bool, core.Prop, ...core.Detail)              {}
func (a *stubAssert) AlwaysOrUnreachable(bool, core.Prop, ...core.Detail) {}
func (a *stubAssert) Sometimes(cond bool, p core.Prop) {
	if cond {
		a.sometimes = append(a.sometimes, string(p))
	}
}
func (a *stubAssert) Reachable(core.Prop)                   {}
func (a *stubAssert) Unreachable(core.Prop, ...core.Detail) {}

type stubTruth struct{}

func (stubTruth) Object(string) ([]byte, bool)    { return nil, false }
func (stubTruth) EachObject(func(string, []byte)) {}
func (stubTruth) Gen() uint64                     { return 0 }
func (stubTruth) Committed(core.TopicPartition) []core.BatchRange {
	return nil
}
func (stubTruth) HighWatermark(core.TopicPartition) core.Offset { return 0 }

type stubHist struct{}

func (stubHist) Invoke(core.NodeRef, core.ID, core.ID, any) {}
func (stubHist) Return(core.ID, any, sim.OpOutcome) bool    { return false }
func (stubHist) Ops() []sim.HistOp                          { return nil }

type stubView struct {
	tl sim.Timeline
	as core.Assertions
}

func (v stubView) Step() uint64             { return 0 }
func (v stubView) Now() sim.Time            { return 0 }
func (v stubView) Phase() sim.Phase         { return sim.PhaseChaos }
func (v stubView) Timeline() sim.Timeline   { return v.tl }
func (v stubView) History() sim.History     { return stubHist{} }
func (v stubView) Objects() sim.ObjectTruth { return stubTruth{} }
func (v stubView) CP() sim.CPTruth          { return stubTruth{} }
func (v stubView) Assert() core.Assertions  { return v.as }

func TestNewAndPhases(t *testing.T) {
	t.Parallel()
	p := New()
	if p.Phases != Phases() {
		t.Fatalf("New phases = %+v, want %+v", p.Phases, Phases())
	}
	if p.CheckDeterminism != 10 || p.QuiesceEvery != core.Second {
		t.Fatalf("New defaults CheckDeterminism=%d QuiesceEvery=%d", p.CheckDeterminism, p.QuiesceEvery)
	}
	if Millis(5) != 5*core.Millisecond {
		t.Fatalf("Millis(5) = %d", Millis(5))
	}
	if LivenessGrace != 6*core.Second {
		t.Fatalf("LivenessGrace = %d", LivenessGrace)
	}
}

func TestSwarm(t *testing.T) {
	t.Parallel()
	p := New()
	cfg := &sim.SeedConfig{
		ObjectStore:  p.ObjectStore,
		ControlPlane: p.ControlPlane,
		Network:      p.Network,
		Attrition:    p.Attrition,
	}
	called := false
	Swarm(sim.NewRand(1), cfg, func() { called = true })
	if !called {
		t.Fatal("afterKnobs not called")
	}
	Swarm(sim.NewRand(2), cfg, nil)
}

func TestPhaseStart(t *testing.T) {
	t.Parallel()
	tl := sim.NewTimeline()
	tl.Append(core.NodeRef{}, 10, core.Fact{
		Name:   "sim_phase",
		Fields: []core.Field{{Key: "phase", Value: core.U64(uint64(sim.PhaseSettling))}},
	})
	v := stubView{tl: tl, as: &stubAssert{}}
	at, ok := PhaseStart(v, sim.PhaseSettling)
	if !ok || at != 10 {
		t.Fatalf("PhaseStart Settling = %d, %v", at, ok)
	}
	if _, ok := PhaseStart(v, sim.PhaseSettle); ok {
		t.Fatal("PhaseStart Settle unexpectedly found")
	}
}

func TestFaultCoverage(t *testing.T) {
	t.Parallel()
	inv := FaultCoverage("echo:")
	if inv.Name() != "FaultCoverage" {
		t.Fatalf("Name = %q", inv.Name())
	}
	inv.Observe(stubView{tl: sim.NewTimeline(), as: &stubAssert{}})

	tl := sim.NewTimeline()
	tl.Append(core.NodeRef{}, 1, core.Fact{
		Name:   "sim_crashed",
		Fields: []core.Field{{Key: "cause", Value: core.Str("attrition")}},
	})
	tl.Append(core.NodeRef{}, 2, core.Fact{Name: "sim_exited"})
	as := &stubAssert{}
	inv.AtEnd(stubView{tl: tl, as: as})
	if len(as.sometimes) != 2 {
		t.Fatalf("Sometimes props = %v", as.sometimes)
	}
}
