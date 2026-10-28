package machine_test

import (
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/controlplane/machine"
	"manifester/internal/core"
)

type alwaysHit struct {
	cond    bool
	prop    core.Prop
	details []core.Detail
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
func (e *testEnv) Maybe(core.Site) bool           { return false }
func (e *testEnv) MaybeP(core.Site, float64) bool { return false }
func (e *testEnv) Fault(core.Site, float64) bool  { return false }
func (e *testEnv) Always(cond bool, p core.Prop, details ...core.Detail) {
	e.always = append(e.always, alwaysHit{cond: cond, prop: p, details: details})
}
func (e *testEnv) AlwaysOrUnreachable(bool, core.Prop, ...core.Detail) {}
func (e *testEnv) Sometimes(bool, core.Prop)                           {}
func (e *testEnv) Reachable(core.Prop)                                 {}
func (e *testEnv) Unreachable(core.Prop, ...core.Detail)               {}

func newMachine(t *testing.T, cfg machine.Config) *machine.Machine {
	t.Helper()
	m, _ := newMachineEnv(t, cfg)
	return m
}

func newMachineEnv(t *testing.T, cfg machine.Config) (*machine.Machine, *testEnv) {
	t.Helper()
	env := &testEnv{}
	m := machine.New(cfg)(env, core.NodeRef{Node: "cp-0"})
	sm, ok := m.(*machine.Machine)
	if !ok {
		t.Fatalf("New returned %T, want *machine.Machine", m)
	}
	return sm, env
}

func step(m *machine.Machine, body core.EventBody) []core.Action {
	return m.Step(core.Event{Body: body})
}

func noop(id string) []byte {
	return applier.Encode(applier.Entry{ID: id, Kind: applier.KindNoop})
}

func TestStartedAndStop(t *testing.T) {
	m := newMachine(t, machine.Config{})
	if acts := step(m, core.Started{Self: core.NodeRef{Node: "cp-0"}}); acts != nil {
		t.Fatalf("Started returned actions %v", acts)
	}
	beforeStop := m.StateHash()
	acts := step(m, core.Stop{})
	if len(acts) != 1 {
		t.Fatalf("Stop actions = %v, want [Exit]", acts)
	}
	if _, ok := acts[0].(core.Exit); !ok {
		t.Fatalf("Stop action = %T, want core.Exit", acts[0])
	}
	if m.StateHash() == beforeStop {
		t.Fatal("Stop left StateHash unchanged")
	}
	after := m.StateHash()
	if acts := step(m, core.Stop{}); acts != nil {
		t.Fatalf("second Stop after Exit returned %v", acts)
	}
	if m.StateHash() != after {
		t.Fatal("post-Exit Stop changed StateHash")
	}
}

func TestStateHashStableAcrossNoopCompletions(t *testing.T) {
	m := newMachine(t, machine.Config{})
	step(m, core.Started{})
	before := m.StateHash()
	noops := []core.EventBody{
		core.CPDone{Req: 999},
		core.ObjPutDone{Req: 999},
		core.ObjGetDone{Req: 999},
		core.LogAppendDone{Req: 999},
		core.LogReadDone{Req: 999},
		core.LogCheckTailDone{Req: 999},
		core.LogTrimDone{Req: 999},
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
	step(m, core.Started{})
	if m.StateHash() != before {
		t.Fatal("repeat Started changed StateHash")
	}
}

func TestStateHashDeterministicAcrossBoots(t *testing.T) {
	hash := func() uint64 {
		m := newMachine(t, machine.Config{})
		step(m, core.Started{})
		return m.StateHash()
	}
	if a, b := hash(), hash(); a != b {
		t.Fatalf("fresh boots hashed differently: %d vs %d", a, b)
	}
}

func TestNewImplementsFactory(t *testing.T) {
	f := machine.New(machine.Config{})
	if f(&testEnv{}, core.NodeRef{Node: "cp-1"}) == nil {
		t.Fatal("Factory returned nil")
	}
}

func TestCatchUpAppliesContiguousReads(t *testing.T) {
	m := newMachine(t, machine.Config{CatchUpEvery: core.Millisecond, ReadLimit: 8})

	acts := step(m, core.Started{})
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
	tail, ok := acts[0].(core.LogCheckTail)
	if !ok || tail.Req == 0 {
		t.Fatalf("TimerFired action = %T %#v, want LogCheckTail", acts[0], acts[0])
	}

	acts = step(m, core.LogCheckTailDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
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
	if acts := step(m, core.LogCheckTailDone{Req: 999, Tail: 99, Err: core.OK, Outcome: core.Applied}); acts != nil {
		t.Fatalf("foreign CheckTailDone returned %v", acts)
	}
	if m.StateHash() != before {
		t.Fatal("foreign completion changed StateHash")
	}
}

func TestCatchUpDisabledByDefault(t *testing.T) {
	m := newMachine(t, machine.Config{})
	if acts := step(m, core.Started{}); acts != nil {
		t.Fatalf("default Started returned %v", acts)
	}
}
