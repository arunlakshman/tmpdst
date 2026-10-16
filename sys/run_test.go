package sys

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"manifester/core"
)

type fakeClock struct{}

func (fakeClock) Mono() core.Time     { return 7 }
func (fakeClock) Wall() core.WallTime { return 9 }

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
	got []core.IOAction
}

func (e *fakeExec) Execute(a core.IOAction) { e.mu.Lock(); e.got = append(e.got, a); e.mu.Unlock() }

// fakeMachine records events and replays script(event index, body).
type fakeMachine struct {
	mu     sync.Mutex
	events []core.Event
	script func(n int, b core.EventBody) []core.Action
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

func setup(m *fakeMachine) (Bundle, *fakeExec, *fakeConns, core.Factory) {
	cn := &fakeConns{}
	b := Bundle{Clock: fakeClock{}, Conns: cn, Inbox: NewInbox(8)}
	return b, &fakeExec{}, cn, func(core.Env, core.NodeRef) core.Machine { return m }
}

func TestStartedFirstControlBeatsData(t *testing.T) {
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	// Preload both queues before Run: data first, then control.
	b.Inbox.DeliverData(core.ConnOpened{Conn: 1})
	b.Inbox.DeliverControl(core.TimerFired{ID: 1})
	m.script = func(n int, body core.EventBody) []core.Action {
		if n == 2 {
			return []core.Action{core.Exit{}}
		}
		return nil
	}
	if err := Run(context.Background(), b, ex, f, core.NodeRef{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.events[0].Body.(core.Started); !ok {
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
	m := &fakeMachine{script: func(n int, _ core.EventBody) []core.Action {
		return []core.Action{core.ObjPut{Req: 1}, core.PauseReads{Conn: 2}, core.Trip{Cause: "x"}, core.ObjPut{Req: 2}}
	}}
	b, ex, cn, f := setup(m)
	err := Run(context.Background(), b, ex, f, core.NodeRef{})
	if !errors.Is(err, ErrTripped) {
		t.Fatalf("err = %v", err)
	}
	if len(ex.got) != 1 || ex.got[0].ReqID() != 1 || len(cn.log) != 1 {
		t.Fatalf("got %v %v", ex.got, cn.log)
	}
}

func TestExitReturnsNilAndLateDeliveryDropped(t *testing.T) {
	m := &fakeMachine{script: func(int, core.EventBody) []core.Action { return []core.Action{core.Exit{}} }}
	b, ex, _, f := setup(m)
	if err := Run(context.Background(), b, ex, f, core.NodeRef{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		b.Inbox.DeliverControl(core.SendDone{Req: 1})
		for i := 0; i < 100; i++ { // would block on a full data queue if not dropped
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
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- Run(ctx, b, ex, f, core.NodeRef{}) }()
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
	m.script = func(n int, body core.EventBody) []core.Action {
		switch n {
		case 0:
			return []core.Action{core.Emit{}, core.SetTimer{ID: 3, Gen: 4, After: core.Duration(time.Millisecond)}}
		default:
			if tf, ok := body.(core.TimerFired); ok && tf.ID == 3 && tf.Gen == 4 {
				return []core.Action{core.Exit{}}
			}
		}
		return nil
	}
	if err := Run(context.Background(), b, ex, f, core.NodeRef{}); err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 {
		t.Fatalf("facts = %v", facts)
	}
}

func stops(m *fakeMachine) (n int, last core.Stop) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		if s, ok := e.Body.(core.Stop); ok {
			n++
			last = s
		}
	}
	return
}

func TestShutdownStopThenExit(t *testing.T) {
	m := &fakeMachine{}
	m.script = func(_ int, body core.EventBody) []core.Action {
		if _, ok := body.(core.Stop); ok {
			return []core.Action{core.Exit{}}
		}
		return nil
	}
	b, ex, _, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, core.Duration(time.Second)
	res := make(chan error, 1)
	go func() { res <- Run(context.Background(), b, ex, f, core.NodeRef{}) }()
	close(sd)
	if err := <-res; err != nil {
		t.Fatal(err)
	}
	n, s := stops(m)
	if n != 1 || s.Deadline != 7+core.Time(time.Second) {
		t.Fatalf("stops=%d deadline=%d", n, s.Deadline)
	}
}

func TestShutdownIgnoredKilled(t *testing.T) {
	m := &fakeMachine{}
	b, ex, _, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, core.Duration(50*time.Millisecond)
	res := make(chan error, 1)
	go func() { res <- Run(context.Background(), b, ex, f, core.NodeRef{}) }()
	close(sd)
	start := time.Now()
	if err := <-res; !errors.Is(err, ErrKilled) {
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
	for i := 0; i < 5; i++ {
		b.Inbox.DeliverControl(core.TimerFired{ID: core.TimerID(i)})
	}
	err := Run(context.Background(), b, ex, f, core.NodeRef{})
	if !errors.Is(err, ErrKilled) {
		t.Fatalf("err = %v", err)
	}
	if n, _ := stops(m); n != 1 {
		t.Fatalf("stops = %d", n)
	}
}
