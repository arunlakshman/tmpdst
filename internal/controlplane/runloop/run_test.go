package runloop_test

import (
	"sync"
	"testing"
	"time"

	"mds/internal/controlplane/runloop"
	"mds/internal/core"
)

type fakeClock struct{}

func (fakeClock) Mono() core.NodeTime { return 7 }
func (fakeClock) Wall() core.RealTime { return 9 }

type fakeExec struct {
	mu  sync.Mutex
	got []core.IOCall
}

func (e *fakeExec) Execute(a core.IOCall) {
	e.mu.Lock()
	e.got = append(e.got, a)
	e.mu.Unlock()
}

type fakeMachine struct {
	mu     sync.Mutex
	events []core.Event
	script func(n int, b core.EventData) []core.Action
}

func (m *fakeMachine) Step(e core.Event) []core.Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.events)
	m.events = append(m.events, e)
	if m.script == nil {
		return nil
	}
	return m.script(n, e.Body)
}
func (m *fakeMachine) StateHash() uint64 { return 0 }

type testEnv struct{}

func (testEnv) Uint64() uint64                                                 { return 1 }
func (testEnv) Intn(n int) int                                                 { return 0 }
func (testEnv) NewID() core.ID                                                 { return core.ID{} }
func (testEnv) Maybe(core.FaultPointID) bool                                   { return false }
func (testEnv) MaybeP(core.FaultPointID, float64) bool                         { return false }
func (testEnv) Fault(core.FaultPointID, float64) bool                          { return false }
func (testEnv) Always(bool, core.PropertyID, ...core.ContextPair)              {}
func (testEnv) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (testEnv) Sometimes(bool, core.PropertyID)                                {}
func (testEnv) Reachable(core.PropertyID)                                      {}
func (testEnv) Unreachable(core.PropertyID, ...core.ContextPair)               {}

func TestStartedThenExit(t *testing.T) {
	m := &fakeMachine{}
	m.script = func(n int, _ core.EventData) []core.Action {
		if n == 0 {
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	b := runloop.Dependencies{Clock: fakeClock{}, Env: testEnv{}, Inbox: runloop.NewInbox(4)}
	f := func(core.Environment, core.NodeInstance) core.Machine { return m }
	if err := runloop.Run(t.Context(), b, &fakeExec{}, f, core.NodeInstance{Node: "cp"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.events[0].Body.(core.NodeStarted); !ok {
		t.Fatalf("first = %T", m.events[0].Body)
	}
}

func TestInjectRunsOnLoop(t *testing.T) {
	m := &fakeMachine{}
	inject := make(chan runloop.InjectFunc, 1)
	var saw bool
	m.script = func(n int, body core.EventData) []core.Action {
		if n == 1 {
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	b := runloop.Dependencies{
		Clock: fakeClock{}, Env: testEnv{}, Inbox: runloop.NewInbox(4), Inject: inject,
	}
	f := func(core.Environment, core.NodeInstance) core.Machine { return m }
	done := make(chan error, 1)
	go func() { done <- runloop.Run(t.Context(), b, &fakeExec{}, f, core.NodeInstance{}) }()
	inject <- func() []core.Action {
		saw = true
		b.Inbox.DeliverControl(core.TimerFired{ID: 1})
		return nil
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	if !saw {
		t.Fatal("inject did not run")
	}
}

func TestWaitIdleThenExit(t *testing.T) {
	m := &fakeMachine{}
	inject := make(chan runloop.InjectFunc)
	b := runloop.Dependencies{Clock: fakeClock{}, Env: testEnv{}, Inbox: runloop.NewInbox(4), Inject: inject}
	f := func(core.Environment, core.NodeInstance) core.Machine { return m }
	done := make(chan error, 1)
	go func() { done <- runloop.Run(t.Context(), b, &fakeExec{}, f, core.NodeInstance{}) }()
	time.Sleep(20 * time.Millisecond) // park in waitIdle
	inject <- func() []core.Action { return []core.Action{core.ShutdownDone{}} }
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestDeliverDataAndDispatch(t *testing.T) {
	in := runloop.NewInbox(2)
	in.DeliverData(core.TimerFired{ID: 9})
	if in.Data().Len() != 1 {
		t.Fatal("data not queued")
	}
	m := &fakeMachine{}
	m.script = func(n int, body core.EventData) []core.Action {
		switch n {
		case 0: // Started
			return []core.Action{
				core.SetTimer{ID: 1, Gen: 1, After: core.Duration(time.Hour)},
				core.CancelTimer{ID: 1},
				core.RecordFact{Fact: core.Fact{}},
				core.LogAppend{Req: 1},
			}
		case 1:
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	facts := 0
	b := runloop.Dependencies{
		Clock: fakeClock{}, Env: testEnv{}, Inbox: in,
		Facts: func(core.Fact) { facts++ },
	}
	ex := &fakeExec{}
	if err := runloop.Run(t.Context(), b, ex, func(core.Environment, core.NodeInstance) core.Machine { return m }, core.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if facts != 1 || len(ex.got) != 1 {
		t.Fatalf("facts=%d io=%d", facts, len(ex.got))
	}
}
