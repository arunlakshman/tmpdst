package simadapter_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/system/simadapter"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
)

// stepContext is the sim.Context of a test: only the time is used.
type stepContext struct{ now, wall sched.TimeNs }

func (c stepContext) Now() sched.TimeNs     { return c.now }
func (c stepContext) Wall() sched.TimeNs    { return c.wall }
func (stepContext) Rand() *rng.Rand         { return rng.New(1) }
func (stepContext) Faults() *fault.Injector { return fault.New(rng.New(1)) }
func (stepContext) Assert() *assert.Log     { return assert.New(nil) }

// testEnv is a machine.Env with no faults and no recorded assertions.
type testEnv struct{ n uint64 }

func (e *testEnv) Uint64() uint64 { e.n++; return e.n }
func (e *testEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}
func (e *testEnv) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(e.Intn(int(hi-lo+1)))
}
func (e *testEnv) Chance(float64) bool                                                { return false }
func (e *testEnv) NewID() machine.ID                                                  { return machine.ID{byte(e.Uint64())} }
func (*testEnv) Maybe(fault.NamedSite) bool                                           { return false }
func (*testEnv) MaybeP(fault.NamedSite, float64) bool                                 { return false }
func (*testEnv) Fault(fault.NamedSite, float64) bool                                  { return false }
func (*testEnv) Always(bool, assert.Always, ...assert.Pair)                           {}
func (*testEnv) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (*testEnv) Sometimes(bool, assert.Sometimes)                                     {}
func (*testEnv) Reachable(assert.Reachable)                                           {}
func (*testEnv) Unreachable(assert.Unreachable, ...assert.Pair)                       {}

func newReplica() *simadapter.Replica {
	cfg := replica.Config{CatchUpEvery: machine.Millisecond, Writer: "w", Rank: 1}
	m := replica.New(cfg)(&testEnv{}, machine.NodeInstance{Node: "cp-0"})
	return simadapter.NewReplica("cp-0", m)
}

// A boot gives a LogGetEnd call and a timer. The timer is a dst SetTimer whose body is a
// runtime.TimerFired, as the old replica returned it.
func TestReplicaTimersAndCallsPassAsDstValues(t *testing.T) {
	p := newReplica()
	if p.ID() != "cp-0" {
		t.Fatalf("ID = %q", p.ID())
	}
	out := p.Step(stepContext{now: 5}, runtime.NodeStarted{})
	var timers int
	for _, o := range out {
		if st, ok := o.(sim.SetTimer); ok {
			timers++
			fired, isFired := st.Body.(runtime.TimerFired)
			if !isFired || uint64(fired.ID) != st.ID || st.After != sched.TimeNs(machine.Millisecond) {
				t.Errorf("timer = %+v", st)
			}
		}
	}
	if timers == 0 {
		t.Fatalf("no timer in %#v", out)
	}
}

// A stop request gives sim.Exit, and nothing else after it.
func TestReplicaStopIsExit(t *testing.T) {
	p := newReplica()
	p.Step(stepContext{}, runtime.NodeStarted{})
	out := p.Step(stepContext{}, runtime.StopRequest{})
	if len(out) != 1 {
		t.Fatalf("out = %#v", out)
	}
	if _, ok := out[0].(sim.Exit); !ok {
		t.Fatalf("out[0] = %#v, want sim.Exit", out[0])
	}
	if got := p.Step(stepContext{}, runtime.NodeStarted{}); got != nil {
		t.Fatalf("a step after Exit gave %#v", got)
	}
}

// A value that is not an EventData is ignored.
func TestReplicaIgnoresOtherValues(t *testing.T) {
	if got := newReplica().Step(stepContext{}, "not an event"); got != nil {
		t.Fatalf("got %#v", got)
	}
}

// A role change makes the machine read the log: the SubmitRead passes as it is. A
// Join in Standby is refused with a Reply.
func TestRebalanceOutputsPass(t *testing.T) {
	m := rebalance.New(rebalance.Config{}, &testEnv{})
	p := simadapter.NewRebalance("rb-0", m)
	if p.ID() != "rb-0" {
		t.Fatalf("ID = %q", p.ID())
	}
	out := p.Step(stepContext{}, rebalance.List{Call: 7})
	if len(out) != 1 {
		t.Fatalf("out = %#v", out)
	}
	if r, ok := out[0].(rebalance.Reply); !ok || r.Call != 7 || r.Err != runtime.ErrUnavailable {
		t.Fatalf("out[0] = %#v", out[0])
	}
	out = p.Step(stepContext{}, rebalance.RoleChanged{Active: true})
	if len(out) != 1 {
		t.Fatalf("out = %#v", out)
	}
	if r, ok := out[0].(rebalance.SubmitRead); !ok || r.Op != (contract.ListGroups{}) {
		t.Fatalf("out[0] = %#v", out[0])
	}
}

// The timer of the machine goes out as a dst SetTimer, and the runtime.TimerFired that
// comes back reaches the machine as its own TimerFired.
func TestRebalanceTimerRoundTrip(t *testing.T) {
	m := rebalance.New(rebalance.Config{}, &testEnv{})
	p := simadapter.NewRebalance("rb-0", m)
	p.Step(stepContext{}, rebalance.RoleChanged{Active: true})
	// A refused load read arms the retry timer (rule 39).
	out := p.Step(stepContext{}, rebalance.ReplicaReply{Req: 1, Done: runtime.ControlPlaneDone{Req: 1, Err: runtime.ErrUnavailable}})
	if len(out) != 1 {
		t.Fatalf("out = %#v", out)
	}
	st, ok := out[0].(sim.SetTimer)
	if !ok {
		t.Fatalf("out[0] = %#v, want sim.SetTimer", out[0])
	}
	fired, ok := st.Body.(runtime.TimerFired)
	if !ok {
		t.Fatalf("body = %#v, want runtime.TimerFired", st.Body)
	}
	again := p.Step(stepContext{now: st.After}, fired)
	if len(again) != 1 {
		t.Fatalf("after the timer: %#v", again)
	}
	if _, ok := again[0].(rebalance.SubmitRead); !ok {
		t.Fatalf("after the timer: %#v, want a read sent again", again[0])
	}
}
