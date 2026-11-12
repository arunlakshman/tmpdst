package replica_test

import (
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

type alwaysHit struct {
	cond    bool
	prop    runtime.PropertyID
	details []runtime.ContextPair
}

type testEnv struct {
	n      uint64
	always []alwaysHit
}

func (e *testEnv) Uint64() uint64 {
	e.n++
	return e.n
}
func (e *testEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}
func (e *testEnv) NewID() runtime.ID {
	var id runtime.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (e *testEnv) Maybe(runtime.FaultPointID) bool           { return false }
func (e *testEnv) MaybeP(runtime.FaultPointID, float64) bool { return false }
func (e *testEnv) Fault(runtime.FaultPointID, float64) bool  { return false }
func (e *testEnv) Always(cond bool, p runtime.PropertyID, details ...runtime.ContextPair) {
	e.always = append(e.always, alwaysHit{cond: cond, prop: p, details: details})
}
func (e *testEnv) AlwaysOrUnreachable(bool, runtime.PropertyID, ...runtime.ContextPair) {}
func (e *testEnv) Sometimes(bool, runtime.PropertyID)                                   {}
func (e *testEnv) Reachable(runtime.PropertyID)                                         {}
func (e *testEnv) Unreachable(runtime.PropertyID, ...runtime.ContextPair)               {}

// timerView is what a test reads from a timer request: which timer, its
// generation, and when it fires.
type timerView struct {
	ID    runtime.TimerID
	Gen   runtime.TimerVersion
	After runtime.Duration
}

// asTimer reads a timer request.
func asTimer(a any) (timerView, bool) {
	st, ok := a.(sim.SetTimer)
	if !ok {
		return timerView{}, false
	}
	fired, _ := st.Body.(runtime.TimerFired)
	return timerView{ID: fired.ID, Gen: fired.Gen, After: runtime.Duration(st.After)}, true
}

func mustTimer(t *testing.T, a any) timerView {
	t.Helper()
	tv, ok := asTimer(a)
	if !ok {
		t.Fatalf("%T is not a timer request", a)
	}
	return tv
}

func newMachine(t *testing.T, config replica.Config) *replica.Replica {
	t.Helper()
	m, _ := newMachineEnv(t, config)
	return m
}

func newMachineEnv(t *testing.T, config replica.Config) (*replica.Replica, *testEnv) {
	t.Helper()
	env := &testEnv{}
	return replica.New(config)(env, runtime.NodeInstance{Node: "cp-0"}), env
}

// testContext is the sim.Context of a unit test. The replica only reads the time from it.
type testContext struct{ now sched.TimeNs }

func (c testContext) Now() sched.TimeNs     { return c.now }
func (testContext) Wall() sched.TimeNs      { return 0 }
func (testContext) Rand() *rng.Rand         { return rng.New(1) }
func (testContext) Faults() *fault.Injector { return fault.New(rng.New(1)) }
func (testContext) Assert() *assert.Log     { return assert.New(nil) }

// stepAt gives body to the replica at time at.
func stepAt(m *replica.Replica, at runtime.NodeTime, body runtime.EventData) []any {
	return m.Step(testContext{now: sched.TimeNs(at)}, body)
}

// step gives body to the replica at time zero.
func step(m *replica.Replica, body runtime.EventData) []any { return stepAt(m, 0, body) }
