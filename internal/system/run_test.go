package system

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"manifester/internal/core"
)

type fakeClock struct{}

func (fakeClock) Mono() core.NodeTime { return 7 }
func (fakeClock) Wall() core.RealTime { return 9 }

type fakeConns struct {
	mu  sync.Mutex
	log []string
}

func (c *fakeConns) Write(core.ConnID, []byte) error { return nil }
func (c *fakeConns) Close(core.ConnID, bool) error   { c.add("close"); return nil }
func (c *fakeConns) PauseReads(core.ConnID)          { c.add("pause") }
func (c *fakeConns) ResumeReads(core.ConnID)         { c.add("resume") }
func (c *fakeConns) add(s string)                    { c.mu.Lock(); c.log = append(c.log, s); c.mu.Unlock() }

type fakeExec struct {
	mu  sync.Mutex
	got []core.IOCall
}

func (e *fakeExec) Execute(a core.IOCall) { e.mu.Lock(); e.got = append(e.got, a); e.mu.Unlock() }

// fakeMachine records events and replays script(event index, body).
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

func setup(m *fakeMachine) (Dependencies, *fakeExec, *fakeConns, core.MachineFactory) {
	cn := &fakeConns{}
	b := Dependencies{Clock: fakeClock{}, Connections: cn, Inbox: NewInbox(8)}
	return b, &fakeExec{}, cn, func(core.Environment, core.NodeInstance) core.Machine { return m }
}

func TestStartedFirstControlBeatsData(t *testing.T) {
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	// Preload both queues before Run: data first, then control.
	b.Inbox.DeliverData(core.ConnOpened{Conn: 1})
	b.Inbox.DeliverControl(core.TimerFired{ID: 1})
	m.script = func(n int, body core.EventData) []core.Action {
		if n == 2 {
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	if err := Run(t.Context(), b, ex, f, core.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.events[0].Body.(core.NodeStarted); !ok {
		t.Fatalf("first = %T", m.events[0].Body)
	}
	if _, ok := m.events[1].Body.(core.TimerFired); !ok {
		t.Fatalf("second = %T, control must beat data", m.events[1].Body)
	}
	if m.events[1].At != 7 || m.events[1].Wall != 9 {
		t.Fatalf("not stamped: %+v", m.events[1])
	}
}

func TestTripRunsEarlierActions(t *testing.T) {
	m := &fakeMachine{script: func(n int, _ core.EventData) []core.Action {
		return []core.Action{core.ObjectPut{Req: 1}, core.PauseReads{Conn: 2}, core.StopForRestart{Cause: "x"}, core.ObjectPut{Req: 2}}
	}}
	b, ex, cn, f := setup(m)
	err := Run(t.Context(), b, ex, f, core.NodeInstance{})
	if !errors.Is(err, ErrStoppedForRestart) {
		t.Fatalf("err = %v", err)
	}
	if len(ex.got) != 1 || ex.got[0].RequestID() != 1 || len(cn.log) != 1 {
		t.Fatalf("got %v %v", ex.got, cn.log)
	}
}

func TestExitReturnsNilAndLateDeliveryDropped(t *testing.T) {
	m := &fakeMachine{script: func(int, core.EventData) []core.Action { return []core.Action{core.ShutdownDone{}} }}
	b, ex, _, f := setup(m)
	if err := Run(t.Context(), b, ex, f, core.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		b.Inbox.DeliverControl(core.SendDone{Req: 1})
		for range 100 { // would block on a full data queue if not dropped
			b.Inbox.DeliverData(core.ConnOpened{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery after Run blocked")
	}
	select {
	case <-b.Inbox.Closed():
	default:
		t.Fatal("sink not closed")
	}
}

func TestCtxCancelStops(t *testing.T) {
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	ctx, cancel := context.WithCancel(t.Context())
	res := make(chan error, 1)
	go func() { res <- Run(ctx, b, ex, f, core.NodeInstance{}) }()
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestTimerFiresAndEmitFact(t *testing.T) {
	var facts []core.Fact
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	b.Facts = func(f core.Fact) { facts = append(facts, f) }
	m.script = func(n int, body core.EventData) []core.Action {
		switch n {
		case 0:
			return []core.Action{core.RecordFact{}, core.SetTimer{ID: 3, Gen: 4, After: core.Duration(time.Millisecond)}}
		default:
			if tf, ok := body.(core.TimerFired); ok && tf.ID == 3 && tf.Gen == 4 {
				return []core.Action{core.ShutdownDone{}}
			}
		}
		return nil
	}
	if err := Run(t.Context(), b, ex, f, core.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 {
		t.Fatalf("facts = %v", facts)
	}
}

func TestDispatchTimersAndConns(t *testing.T) {
	m := &fakeMachine{}
	b, ex, cn, f := setup(m)
	m.script = func(n int, _ core.EventData) []core.Action {
		if n != 0 {
			return nil
		}
		return []core.Action{
			core.SetTimer{ID: 1, After: core.Duration(time.Hour)},
			core.SetTimer{ID: 1, After: core.Duration(time.Hour)}, // replace
			core.CancelTimer{ID: 1},
			core.CancelTimer{ID: 99}, // missing
			core.CloseConn{Conn: 7, Abort: true},
			core.ResumeReads{Conn: 7},
			core.RecordFact{}, // Facts is nil
			core.ShutdownDone{},
		}
	}
	if err := Run(t.Context(), b, ex, f, core.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cn.log, ","); got != "close,resume" {
		t.Fatalf("conns = %v", cn.log)
	}
}

func stops(m *fakeMachine) (int, core.StopRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int
	var last core.StopRequest
	for _, e := range m.events {
		if s, ok := e.Body.(core.StopRequest); ok {
			n++
			last = s
		}
	}
	return n, last
}

func TestShutdownStopThenExit(t *testing.T) {
	m := &fakeMachine{}
	m.script = func(_ int, body core.EventData) []core.Action {
		if _, ok := body.(core.StopRequest); ok {
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	b, ex, _, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, core.Duration(time.Second)
	res := make(chan error, 1)
	go func() { res <- Run(t.Context(), b, ex, f, core.NodeInstance{}) }()
	close(sd)
	if err := <-res; err != nil {
		t.Fatal(err)
	}
	n, s := stops(m)
	if n != 1 || s.Deadline != 7+core.NodeTime(time.Second) {
		t.Fatalf("stops=%d deadline=%d", n, s.Deadline)
	}
}

func TestShutdownIgnoredKilled(t *testing.T) {
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, core.Duration(50*time.Millisecond)
	res := make(chan error, 1)
	go func() { res <- Run(t.Context(), b, ex, f, core.NodeInstance{}) }()
	close(sd)
	start := time.Now()
	if err := <-res; !errors.Is(err, ErrKilledByRuntime) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("killed before grace elapsed")
	}
	select {
	case <-b.Inbox.Closed():
	default:
		t.Fatal("inbox not closed")
	}
}

func TestShutdownStopDeliveredOnce(t *testing.T) {
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, core.Duration(100*time.Millisecond)
	close(sd)
	for i := range 5 {
		b.Inbox.DeliverControl(core.TimerFired{ID: core.TimerID(i)})
	}
	err := Run(t.Context(), b, ex, f, core.NodeInstance{})
	if !errors.Is(err, ErrKilledByRuntime) {
		t.Fatalf("err = %v", err)
	}
	if n, _ := stops(m); n != 1 {
		t.Fatalf("stops = %d", n)
	}
}
