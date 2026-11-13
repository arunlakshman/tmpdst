package preset

import (
	"testing"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// stubAssert keeps the names of the Sometimes properties that held.
type stubAssert struct{ sometimes []string }

func (a *stubAssert) Always(bool, assert.Always, ...assert.Pair)                           {}
func (a *stubAssert) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (a *stubAssert) Sometimes(cond bool, p assert.Sometimes) {
	if cond {
		a.sometimes = append(a.sometimes, p.Name())
	}
}
func (a *stubAssert) Reachable(assert.Reachable)                     {}
func (a *stubAssert) Unreachable(assert.Unreachable, ...assert.Pair) {}

type stubView struct {
	tl *kernel.Timeline
	as machine.Assertions
}

func (v stubView) Step() uint64               { return 0 }
func (v stubView) Now() kernel.Time           { return 0 }
func (v stubView) Phase() kernel.Phase        { return kernel.PhaseFaults }
func (v stubView) Timeline() *kernel.Timeline { return v.tl }
func (v stubView) Assert() machine.Assertions { return v.as }

func TestNewAndPhases(t *testing.T) {
	t.Parallel()
	p := New()
	if p.Phases != Phases() {
		t.Fatalf("New phases = %+v, want %+v", p.Phases, Phases())
	}
	if p.QuiesceEvery != machine.Second {
		t.Fatalf("New defaults QuiesceEvery=%d", p.QuiesceEvery)
	}
	if ProgressGraceTime != 6*machine.Second {
		t.Fatalf("ProgressGraceTime = %d", ProgressGraceTime)
	}
}

func TestSwarm(t *testing.T) {
	t.Parallel()
	p := New()
	s := p.Settings
	called := false
	Swarm(rng.New(1), &s, func() { called = true })
	if !called {
		t.Fatal("afterKnobs not called")
	}
	Swarm(rng.New(2), &s, nil)
}

func TestPhaseStart(t *testing.T) {
	t.Parallel()
	tl := kernel.NewTimeline()
	tl.Append(machine.NodeInstance{}, 10, machine.Fact{
		Name:   "sim_phase",
		Fields: []machine.Field{{Key: "phase", Value: machine.Uint64Value(uint64(kernel.PhaseRecovering))}},
	})
	v := stubView{tl: tl, as: &stubAssert{}}
	at, ok := PhaseStart(v, kernel.PhaseRecovering)
	if !ok || at != 10 {
		t.Fatalf("PhaseStart Settling = %d, %v", at, ok)
	}
	if _, ok := PhaseStart(v, kernel.PhaseRecover); ok {
		t.Fatal("PhaseStart Settle unexpectedly found")
	}
}

func TestFaultCoverage(t *testing.T) {
	t.Parallel()
	rule := FaultCoverageCheck(EchoFaultCoverage)
	if rule.Name() != "FaultCoverage" {
		t.Fatalf("Name = %q", rule.Name())
	}
	rule.Observe(stubView{tl: kernel.NewTimeline(), as: &stubAssert{}})

	tl := kernel.NewTimeline()
	tl.Append(machine.NodeInstance{}, 1, machine.Fact{
		Name:   "sim_crashed",
		Fields: []machine.Field{{Key: "cause", Value: machine.StringValue("attrition")}},
	})
	tl.Append(machine.NodeInstance{}, 2, machine.Fact{Name: "sim_exited"})
	as := &stubAssert{}
	rule.AtEnd(stubView{tl: tl, as: as})
	if len(as.sometimes) != 2 {
		t.Fatalf("Sometimes props = %v", as.sometimes)
	}
}
