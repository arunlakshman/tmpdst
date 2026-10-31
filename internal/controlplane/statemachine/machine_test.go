package statemachine_test

import (
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

type alwaysHit struct {
	cond    bool
	prop    core.PropertyID
	details []core.ContextPair
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
func (e *testEnv) NewID() core.ID {
	var id core.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (e *testEnv) Maybe(core.FaultPointID) bool           { return false }
func (e *testEnv) MaybeP(core.FaultPointID, float64) bool { return false }
func (e *testEnv) Fault(core.FaultPointID, float64) bool  { return false }
func (e *testEnv) Always(cond bool, p core.PropertyID, details ...core.ContextPair) {
	e.always = append(e.always, alwaysHit{cond: cond, prop: p, details: details})
}
func (e *testEnv) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (e *testEnv) Sometimes(bool, core.PropertyID)                                {}
func (e *testEnv) Reachable(core.PropertyID)                                      {}
func (e *testEnv) Unreachable(core.PropertyID, ...core.ContextPair)               {}

func newMachine(t *testing.T, cfg statemachine.Config) *statemachine.Machine {
	t.Helper()
	m, _ := newMachineEnv(t, cfg)
	return m
}

func newMachineEnv(t *testing.T, cfg statemachine.Config) (*statemachine.Machine, *testEnv) {
	t.Helper()
	env := &testEnv{}
	m := statemachine.New(cfg)(env, core.NodeInstance{Node: "cp-0"})
	sm, ok := m.(*statemachine.Machine)
	if !ok {
		t.Fatalf("New returned %T, want *statemachine.Machine", m)
	}
	return sm, env
}

func step(m *statemachine.Machine, body core.EventData) []core.Action {
	return m.Step(core.Event{Body: body})
}

func noop(id string) []byte {
	return stateupdater.Encode(stateupdater.Entry{ID: id, Kind: stateupdater.KindNoop})
}

func TestStartedAndStop(t *testing.T) {
	m := newMachine(t, statemachine.Config{})
	if acts := step(m, core.NodeStarted{Self: core.NodeInstance{Node: "cp-0"}}); acts != nil {
		t.Fatalf("Started returned actions %v", acts)
	}
	beforeStop := m.StateHash()
	acts := step(m, core.StopRequest{})
	if len(acts) != 1 {
		t.Fatalf("Stop actions = %v, want [Exit]", acts)
	}
	if _, ok := acts[0].(core.ShutdownDone); !ok {
		t.Fatalf("Stop action = %T, want core.ShutdownDone", acts[0])
	}
	if m.StateHash() == beforeStop {
		t.Fatal("Stop left StateHash unchanged")
	}
	after := m.StateHash()
	if acts := step(m, core.StopRequest{}); acts != nil {
		t.Fatalf("second Stop after Exit returned %v", acts)
	}
	if m.StateHash() != after {
		t.Fatal("post-Exit Stop changed StateHash")
	}
}

func TestStateHashStableAcrossNoopIOResults(t *testing.T) {
	m := newMachine(t, statemachine.Config{})
	step(m, core.NodeStarted{})
	before := m.StateHash()
	noops := []core.EventData{
		core.ControlPlaneDone{Req: 999},
		core.ObjectPutDone{Req: 999},
		core.ObjectGetDone{Req: 999},
		core.LogAppendDone{Req: 999},
		core.LogReadDone{Req: 999},
		core.LogGetEndDone{Req: 999},
		core.LogDeleteOldDone{Req: 999},
		core.SendDone{Req: 1},
		core.TimerFired{ID: 77, Gen: 3},
		core.ConnOpened{Conn: 1},
	}
	for _, body := range noops {
		if acts := step(m, body); acts != nil {
			t.Fatalf("%T returned actions %v", body, acts)
		}
		if m.StateHash() != before {
			t.Fatalf("%T changed StateHash", body)
		}
	}
	step(m, core.NodeStarted{})
	if m.StateHash() != before {
		t.Fatal("repeat Started changed StateHash")
	}
}

func TestStateHashDeterministicAcrossBoots(t *testing.T) {
	hash := func() uint64 {
		m := newMachine(t, statemachine.Config{})
		step(m, core.NodeStarted{})
		return m.StateHash()
	}
	if a, b := hash(), hash(); a != b {
		t.Fatalf("fresh boots hashed differently: %d vs %d", a, b)
	}
}

func TestNewImplementsFactory(t *testing.T) {
	f := statemachine.New(statemachine.Config{})
	if f(&testEnv{}, core.NodeInstance{Node: "cp-1"}) == nil {
		t.Fatal("MachineFactory returned nil")
	}
}

func TestCatchUpAppliesContiguousReads(t *testing.T) {
	m := newMachine(t, statemachine.Config{CatchUpEvery: core.Millisecond, ReadLimit: 8})

	acts := step(m, core.NodeStarted{})
	if len(acts) != 1 {
		t.Fatalf("Started actions = %v, want [SetTimer]", acts)
	}
	timer, ok := acts[0].(core.SetTimer)
	if !ok || timer.ID == 0 {
		t.Fatalf("Started action = %T %#v, want SetTimer", acts[0], acts[0])
	}

	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	if len(acts) != 1 {
		t.Fatalf("TimerFired actions = %v, want [LogCheckTail]", acts)
	}
	tail, ok := acts[0].(core.LogGetEnd)
	if !ok || tail.Req == 0 {
		t.Fatalf("TimerFired action = %T %#v, want LogCheckTail", acts[0], acts[0])
	}

	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	if len(acts) != 1 {
		t.Fatalf("CheckTailDone actions = %v, want [LogRead]", acts)
	}
	read, ok := acts[0].(core.LogRead)
	if !ok || read.From != 0 || read.Limit != 8 {
		t.Fatalf("CheckTailDone action = %#v, want LogRead From=0 Limit=8", acts[0])
	}

	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{
			{Slot: 0, Data: noop("a")},
			{Slot: 1, Data: noop("b")},
		},
	})
	if len(acts) != 1 {
		t.Fatalf("ReadDone actions = %v, want [SetTimer]", acts)
	}
	if _, ok := acts[0].(core.SetTimer); !ok {
		t.Fatalf("ReadDone action = %T, want SetTimer", acts[0])
	}
	if m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", m.Applied())
	}

	before := m.StateHash()
	if acts := step(m, core.LogGetEndDone{Req: 999, Tail: 99, Err: core.OK, Outcome: core.Applied}); acts != nil {
		t.Fatalf("foreign CheckTailDone returned %v", acts)
	}
	if m.StateHash() != before {
		t.Fatal("foreign completion changed StateHash")
	}
}

func TestCatchUpDisabledByDefault(t *testing.T) {
	m := newMachine(t, statemachine.Config{})
	if acts := step(m, core.NodeStarted{}); acts != nil {
		t.Fatalf("default Started returned %v", acts)
	}
}
