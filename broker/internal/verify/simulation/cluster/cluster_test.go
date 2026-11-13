package cluster

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/shared/objectstore"
)

// stepRecord is what a stub machine saw in one Step.
type stepRecord struct {
	now machine.Now
	in  core.EventData
}

// stubMachine is a core machine for tests. script gives the actions of a Step.
type stubMachine struct {
	seen   *[]stepRecord
	script func(in core.EventData) []core.Action
}

func (m *stubMachine) Step(ev core.Event) []core.Action {
	*m.seen = append(*m.seen, stepRecord{now: machine.Now{Mono: ev.At, Wall: ev.Wall}, in: ev.Body})
	if m.script == nil {
		return nil
	}
	return m.script(ev.Body)
}

func (m *stubMachine) StateHash() uint64 { return uint64(len(*m.seen)) }

// stubWorkload is a Workload that counts the phases it heard.
type stubWorkload struct {
	stubMachine
	phases    []kernel.Phase
	quiescent bool
}

func (w *stubWorkload) OnPhase(p kernel.Phase) []core.Action {
	w.phases = append(w.phases, p)
	return nil
}

func (w *stubWorkload) Quiescent() bool { return w.quiescent }

func quickPlan(seed uint64) Plan {
	return Plan{
		Seed:         seed,
		Phases:       kernel.Phases{Chaos: 2 * machine.Second, Settling: machine.Second, Settle: machine.Second},
		QuiesceEvery: 500 * machine.Millisecond,
		Settings:     Settings{RecoveryDelay: [2]machine.Duration{100 * machine.Millisecond, 200 * machine.Millisecond}},
	}
}

// The old Between left out the upper end, and with hi == lo+1 it still drew once.
// rng.Between(lo, hi-1) would draw nothing there, and every later draw would move.
func TestRestartDelayWithOneValueStillDrawsOnce(t *testing.T) {
	p := quickPlan(1)
	p.Settings.RecoveryDelay = [2]machine.Duration{5, 6}
	c := New(p)
	r := rng.New(9)
	before := r.Calls()
	d, ok := c.restart(machine.Exit, r)
	if !ok || d != 5 {
		t.Fatalf("restart delay %v ok=%v, want 5", d, ok)
	}
	if got := r.Calls() - before; got != 1 {
		t.Fatalf("the delay drew %d times, want 1", got)
	}
}

func TestMachineAdapterPassesTheTimeInTheEvent(t *testing.T) {
	var seen []stepRecord
	a := machineAdapter{&stubMachine{seen: &seen}}
	now := machine.Now{Mono: 5, Wall: 7}
	a.Step(now, core.NodeStarted{})
	if len(seen) != 1 || seen[0].now != now {
		t.Fatalf("saw %+v, want one Step at %+v", seen, now)
	}
	if a.StateHash() != 1 {
		t.Fatalf("StateHash = %d", a.StateHash())
	}
}

func TestAdaptFactoryKeepsNil(t *testing.T) {
	f := adaptFactory(func(core.Environment, machine.NodeInstance) core.Machine { return nil })
	if m := f(nil, machine.NodeInstance{}); m != nil {
		t.Fatalf("a nil machine became %v", m)
	}
}

func TestDriverAdapterRoutesPhaseChanges(t *testing.T) {
	w := &stubWorkload{stubMachine: stubMachine{seen: new([]stepRecord)}}
	d := driverAdapter{w}
	ev, ok := phaseEvent(kernel.PhaseRecover)
	if !ok {
		t.Fatal("phaseEvent gave no input")
	}
	d.Step(machine.Now{}, ev)
	d.Step(machine.Now{}, core.TimerFired{ID: 3, Gen: 1}) // a normal timer
	if !slices.Equal(w.phases, []kernel.Phase{kernel.PhaseRecover}) {
		t.Fatalf("phases = %v", w.phases)
	}
	if len(*w.seen) != 1 {
		t.Fatalf("the normal timer was not a Step: %d Steps", len(*w.seen))
	}
}

func TestRestartDelayIsInRecoveryDelay(t *testing.T) {
	c := New(quickPlan(1))
	r := rng.New(9)
	for range 200 {
		d, ok := c.restart(machine.Crash, r)
		if !ok || d < 100*machine.Millisecond || d >= 200*machine.Millisecond {
			t.Fatalf("restart delay %v ok=%v", d, ok)
		}
	}
}

func TestRunBootsBrokersThenWorkloadAndTellsPhases(t *testing.T) {
	var brokerSeen, workloadSeen []stepRecord
	c := New(quickPlan(3))
	c.AddBrokers(2, func(core.Environment, machine.NodeInstance) core.Machine {
		return &stubMachine{seen: &brokerSeen}
	})
	var live *stubWorkload
	c.AddWorkload(func(_ core.Environment, _ machine.NodeInstance, brokers []machine.NodeID) Workload {
		if !slices.Equal(brokers, []machine.NodeID{"broker-0", "broker-1"}) {
			t.Errorf("workload got brokers %v", brokers)
		}
		live = &stubWorkload{stubMachine: stubMachine{seen: &workloadSeen}, quiescent: true}
		return live
	})
	res := c.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if len(brokerSeen) != 2 {
		t.Errorf("brokers took %d Steps, want 2 boot Steps", len(brokerSeen))
	}
	if len(workloadSeen) != 1 {
		t.Errorf("workload took %d Steps, want 1 boot Step", len(workloadSeen))
	}
	if _, ok := workloadSeen[0].in.(core.NodeStarted); !ok {
		t.Errorf("the first workload input was %T", workloadSeen[0].in)
	}
	if want := []kernel.Phase{kernel.PhaseRecovering, kernel.PhaseRecover}; !slices.Equal(live.phases, want) {
		t.Errorf("workload heard %v, want %v", live.phases, want)
	}
	quiet := c.World.Timeline().Snapshot("sim_quiesce")
	if len(quiet) != 8 { // every 500 ms from 0.5 s to the end at 4 s
		t.Errorf("%d quiesce facts", len(quiet))
	}
	for _, q := range quiet {
		if v, _ := q.Fact.Field("quiescent").(machine.Bool); !bool(v) {
			t.Errorf("a quiesce fact says not quiescent: %+v", q)
		}
	}
}

func TestQuiesceFactSaysWhenAWorkloadIsBusy(t *testing.T) {
	c := New(quickPlan(4))
	c.AddBrokers(1, func(core.Environment, machine.NodeInstance) core.Machine {
		return &stubMachine{seen: new([]stepRecord)}
	})
	c.AddWorkload(func(core.Environment, machine.NodeInstance, []machine.NodeID) Workload {
		return &stubWorkload{stubMachine: stubMachine{seen: new([]stepRecord)}}
	})
	c.Run()
	for _, q := range c.World.Timeline().Snapshot("sim_quiesce") {
		if v, _ := q.Fact.Field("quiescent").(machine.Bool); bool(v) {
			t.Fatalf("a busy workload was reported quiescent: %+v", q)
		}
	}
}

func TestRouteSendsTimersAndFactsAndEndsTheNode(t *testing.T) {
	var seen []stepRecord
	fired := 0
	c := New(quickPlan(5))
	c.AddBrokers(1, func(core.Environment, machine.NodeInstance) core.Machine {
		return &stubMachine{seen: &seen, script: func(in core.EventData) []core.Action {
			switch x := in.(type) {
			case core.NodeStarted:
				return []core.Action{
					core.RecordFact{Fact: machine.Fact{Name: "hello"}},
					core.SetTimer{ID: 1, Gen: 1, After: 10 * machine.Millisecond},
					core.SetTimer{ID: 2, Gen: 1, After: 20 * machine.Millisecond},
					core.CancelTimer{ID: 2},
				}
			case core.TimerFired:
				fired++
				if x.ID == 1 && fired == 1 {
					return []core.Action{core.StopForRestart{Cause: "test"}}
				}
			}
			return nil
		}}
	})
	res := c.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if fired != 2 {
		t.Errorf("%d timers fired, want 2 (one for each boot; timer 2 was cancelled)", fired)
	}
	if res.Trips != 1 {
		t.Errorf("Trips = %d, want 1", res.Trips)
	}
	if n := len(c.World.Timeline().Snapshot("hello")); n != 2 { // the first boot and the restart
		t.Errorf("%d hello facts, want 2", n)
	}
	if n := len(c.World.Timeline().Snapshot("sim_restarted")); n != 1 {
		t.Errorf("%d restart facts, want 1", n)
	}
}

// Every action that goes to a service is routed to it, and the service answers.
func TestRouteSendsEveryServiceAction(t *testing.T) {
	var seen []stepRecord
	c := New(quickPlan(7))
	c.AddBrokers(2, func(_ core.Environment, self machine.NodeInstance) core.Machine {
		return &stubMachine{seen: &seen, script: func(in core.EventData) []core.Action {
			switch x := in.(type) {
			case core.NodeStarted:
				if self.Node != "broker-0" {
					return nil
				}
				return []core.Action{
					core.ObjectCall{Call: objectstore.Put{Req: 1, Key: "k", Data: []byte("v")}},
					core.ControlPlaneCall{Req: 2, Op: contract.CommitSegment{SegmentKey: "k"}},
					core.Connect{Req: 3, Addr: "broker-1"},
				}
			case core.ConnectDone:
				if x.Err != core.OK {
					return nil
				}
				return []core.Action{
					core.PauseReads{Conn: x.Conn},
					core.ResumeReads{Conn: x.Conn},
					core.Send{Req: 4, Conn: x.Conn, Data: []byte("hello")},
					core.CloseConn{Conn: x.Conn},
				}
			}
			return nil
		}}
	})
	res := c.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	got := map[string]bool{}
	for _, s := range seen {
		switch s.in.(type) {
		case core.ObjectDone:
			got["object"] = true
		case core.ControlPlaneDone:
			got["control plane"] = true
		case core.ConnectDone:
			got["connect"] = true
		case core.SendDone:
			got["send"] = true
		case core.BytesReceived:
			got["bytes"] = true
		}
	}
	for _, want := range []string{"object", "control plane", "connect", "send", "bytes"} {
		if !got[want] {
			t.Errorf("no %s answer was delivered; saw %v", want, got)
		}
	}
}

func TestRouteFailsAnActionThatHasNoService(t *testing.T) {
	c := New(quickPlan(6))
	c.AddBrokers(1, func(core.Environment, machine.NodeInstance) core.Machine {
		return &stubMachine{seen: new([]stepRecord), script: func(in core.EventData) []core.Action {
			if _, ok := in.(core.NodeStarted); ok {
				return []core.Action{core.ControlPlaneReply{}}
			}
			return nil
		}}
	})
	res := c.Run()
	if len(res.Failures) == 0 || res.Failures[0].Property != "unrouted_action" || res.Failures[0].Kind != kernel.FailProtocol {
		t.Fatalf("failures = %+v", res.Failures)
	}
}
