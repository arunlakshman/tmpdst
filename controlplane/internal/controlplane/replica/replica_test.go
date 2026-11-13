package replica_test

import (
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

type alwaysHit struct {
	cond    bool
	prop    string
	details []assert.Pair
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
func (e *testEnv) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(e.Intn(int(hi-lo+1)))
}
func (e *testEnv) Chance(p float64) bool { return p > 0 && float64(e.Uint64()%1000)/1000 < p }
func (e *testEnv) NewID() machine.ID {
	var id machine.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (e *testEnv) Maybe(fault.NamedSite) bool           { return false }
func (e *testEnv) MaybeP(fault.NamedSite, float64) bool { return false }
func (e *testEnv) Fault(fault.NamedSite, float64) bool  { return false }
func (e *testEnv) Always(cond bool, p assert.Always, details ...assert.Pair) {
	e.always = append(e.always, alwaysHit{cond: cond, prop: p.Name(), details: details})
}
func (e *testEnv) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (e *testEnv) Sometimes(bool, assert.Sometimes)                                     {}
func (e *testEnv) Reachable(assert.Reachable)                                           {}
func (e *testEnv) Unreachable(assert.Unreachable, ...assert.Pair)                       {}

// timerView is what a test reads from a timer request: which timer, its
// generation, and when it fires.
type timerView struct {
	ID    machine.TimerID
	Gen   machine.TimerVersion
	After machine.Duration
}

// asTimer reads a timer request.
func asTimer(a runtime.Action) (timerView, bool) {
	st, ok := a.(runtime.SetTimer)
	if !ok {
		return timerView{}, false
	}
	return timerView{ID: st.ID, Gen: st.Gen, After: st.After}, true
}

func mustTimer(t *testing.T, a runtime.Action) timerView {
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
	return replica.New(config)(env, machine.NodeInstance{Node: "cp-0"}), env
}

// stepAt gives body to the replica at time at.
func stepAt(m *replica.Replica, at machine.NodeTime, body runtime.EventData) []runtime.Action {
	return m.Step(machine.Now{Mono: at}, body)
}

// step gives body to the replica at time zero.
func step(m *replica.Replica, body runtime.EventData) []runtime.Action { return stepAt(m, 0, body) }
