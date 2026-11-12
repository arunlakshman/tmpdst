package campaign

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator"
	"github.com/manifesterio/mds/dst/machine"
)

type stubAssert struct{ sometimes []string }

func (a *stubAssert) Always(bool, core.PropertyID, ...core.ContextPair)              {}
func (a *stubAssert) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (a *stubAssert) Sometimes(cond bool, p core.PropertyID) {
	if cond {
		a.sometimes = append(a.sometimes, string(p))
	}
}
func (a *stubAssert) Reachable(core.PropertyID)                        {}
func (a *stubAssert) Unreachable(core.PropertyID, ...core.ContextPair) {}

type stubTruth struct{}

func (stubTruth) Object(string) ([]byte, bool)    { return nil, false }
func (stubTruth) EachObject(func(string, []byte)) {}
func (stubTruth) Gen() uint64                     { return 0 }
func (stubTruth) Committed(core.TopicPartition) []core.BatchRange {
	return nil
}
func (stubTruth) HighWatermark(core.TopicPartition) core.Offset { return 0 }

type stubHist struct{}

func (stubHist) Invoke(machine.NodeInstance, machine.ID, machine.ID, any) {}
func (stubHist) Return(machine.ID, any, simulator.OpOutcome) bool         { return false }
func (stubHist) Ops() []simulator.HistoryOperation                        { return nil }

type stubView struct {
	tl simulator.Timeline
	as core.Assertions
}

func (v stubView) Step() uint64                            { return 0 }
func (v stubView) Now() simulator.Time                     { return 0 }
func (v stubView) Phase() simulator.Phase                  { return simulator.PhaseFaults }
func (v stubView) Timeline() simulator.Timeline            { return v.tl }
func (v stubView) History() simulator.History              { return stubHist{} }
func (v stubView) Objects() simulator.ObjectStoreRealState { return stubTruth{} }
func (v stubView) CP() simulator.ControlPlaneRealState     { return stubTruth{} }
func (v stubView) Assert() core.Assertions                 { return v.as }

func TestNewAndPhases(t *testing.T) {
	t.Parallel()
	p := New()
	if p.Phases != Phases() {
		t.Fatalf("New phases = %+v, want %+v", p.Phases, Phases())
	}
	if p.CheckDeterminism != 10 || p.QuiesceEvery != machine.Second {
		t.Fatalf("New defaults CheckDeterminism=%d QuiesceEvery=%d", p.CheckDeterminism, p.QuiesceEvery)
	}
	if Millis(5) != 5*machine.Millisecond {
		t.Fatalf("Millis(5) = %d", Millis(5))
	}
	if ProgressGraceTime != 6*machine.Second {
		t.Fatalf("LivenessGrace = %d", ProgressGraceTime)
	}
}

func TestSwarm(t *testing.T) {
	t.Parallel()
	p := New()
	config := &simulator.SeedConfig{
		ObjectStore:  p.ObjectStore,
		ControlPlane: p.ControlPlane,
		Network:      p.Network,
		Attrition:    p.Attrition,
	}
	called := false
	Swarm(simulator.NewRand(1), config, func() { called = true })
	if !called {
		t.Fatal("afterKnobs not called")
	}
	Swarm(simulator.NewRand(2), config, nil)
}

func TestPhaseStart(t *testing.T) {
	t.Parallel()
	tl := simulator.NewTimeline()
	tl.Append(machine.NodeInstance{}, 10, machine.Fact{
		Name:   "sim_phase",
		Fields: []machine.Field{{Key: "phase", Value: machine.Uint64Value(uint64(simulator.PhaseRecovering))}},
	})
	v := stubView{tl: tl, as: &stubAssert{}}
	at, ok := PhaseStart(v, simulator.PhaseRecovering)
	if !ok || at != 10 {
		t.Fatalf("PhaseStart Settling = %d, %v", at, ok)
	}
	if _, ok := PhaseStart(v, simulator.PhaseRecover); ok {
		t.Fatal("PhaseStart Settle unexpectedly found")
	}
}

func TestFaultCoverage(t *testing.T) {
	t.Parallel()
	inv := FaultCoverageCheck("echo:")
	if inv.Name() != "FaultCoverage" {
		t.Fatalf("Name = %q", inv.Name())
	}
	inv.Observe(stubView{tl: simulator.NewTimeline(), as: &stubAssert{}})

	tl := simulator.NewTimeline()
	tl.Append(machine.NodeInstance{}, 1, machine.Fact{
		Name:   "sim_crashed",
		Fields: []machine.Field{{Key: "cause", Value: machine.StringValue("attrition")}},
	})
	tl.Append(machine.NodeInstance{}, 2, machine.Fact{Name: "sim_exited"})
	as := &stubAssert{}
	inv.AtEnd(stubView{tl: tl, as: as})
	if len(as.sometimes) != 2 {
		t.Fatalf("Sometimes props = %v", as.sometimes)
	}
}
