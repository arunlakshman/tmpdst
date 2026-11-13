package system_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// These tests run the broker loop as production does: dst/live with system.Route
// and the core inbox classes. They are the tests of the old system.Run, on the new loop.

type fakeClock struct{}

func (fakeClock) Mono() machine.NodeTime { return 7 }
func (fakeClock) Wall() machine.RealTime { return 9 }
func (fakeClock) AfterFunc(d machine.Duration, f func()) machine.Timer {
	return time.AfterFunc(time.Duration(d), f)
}

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

// rig is one loop with its fakes.
type rig struct {
	m     *fakeMachine
	ex    *fakeExec
	conns *fakeConns
	facts []machine.Fact
	cfg   live.Config[core.EventData, core.Action]
}

func newRig(m *fakeMachine) *rig {
	r := &rig{m: m, ex: &fakeExec{}, conns: &fakeConns{}}
	r.cfg = live.Config[core.EventData, core.Action]{
		New: core.ContractFactory(func(core.Environment, machine.NodeInstance) core.Machine { return m }),
		Boot: func(self machine.NodeInstance) core.EventData {
			return core.NodeStarted{Self: self}
		},
		StopMsg: func(deadline machine.NodeTime) core.EventData {
			return core.StopRequest{Deadline: deadline}
		},
		Clock: fakeClock{},
		Inbox: live.NewInbox[core.EventData](8),
		Class: core.InboxKindOf,
		Route: system.Route(system.Host{Exec: r.ex, Connections: r.conns, Facts: func(f machine.Fact) { r.facts = append(r.facts, f) }}),
	}
	return r
}

func (r *rig) run(ctx context.Context) error { return live.Run(ctx, r.cfg) }

func TestStartedFirstControlBeatsData(t *testing.T) {
	m := &fakeMachine{}
	r := newRig(m)
	// Preload both queues before Run: data first, then control.
	r.cfg.Inbox.Deliver(machine.Data, core.ConnOpened{Conn: 1})
	r.cfg.Inbox.Deliver(machine.Control, core.TimerFired{ID: 1})
	m.script = func(n int, body core.EventData) []core.Action {
		if n == 2 {
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	if err := r.run(t.Context()); err != nil {
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
		return []core.Action{core.ObjectCall{Call: objectstore.Put{Req: 1}}, core.PauseReads{Conn: 2}, core.StopForRestart{Cause: "x"}, core.ObjectCall{Call: objectstore.Put{Req: 2}}}
	}}
	r := newRig(m)
	err := r.run(t.Context())
	if !errors.Is(err, live.ErrStoppedForRestart) {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), "tripped: x") {
		t.Fatalf("the cause is lost: %v", err)
	}
	if len(r.ex.got) != 1 || r.ex.got[0].RequestID() != 1 || len(r.conns.log) != 1 {
		t.Fatalf("got %v %v", r.ex.got, r.conns.log)
	}
}

func TestExitReturnsNilAndLateDeliveryDropped(t *testing.T) {
	m := &fakeMachine{script: func(int, core.EventData) []core.Action { return []core.Action{core.ShutdownDone{}} }}
	r := newRig(m)
	if err := r.run(t.Context()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		r.cfg.Inbox.Deliver(machine.Control, core.SendDone{Req: 1})
		for range 100 { // would block on a full data queue if not dropped
			r.cfg.Inbox.Deliver(machine.Data, core.ConnOpened{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery after Run blocked")
	}
	select {
	case <-r.cfg.Inbox.Closed():
	default:
		t.Fatal("sink not closed")
	}
}

func TestCtxCancelStops(t *testing.T) {
	r := newRig(&fakeMachine{})
	ctx, cancel := context.WithCancel(t.Context())
	res := make(chan error, 1)
	go func() { res <- r.run(ctx) }()
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, live.ErrStopped) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestTimerFiresAndEmitFact(t *testing.T) {
	m := &fakeMachine{}
	r := newRig(m)
	m.script = func(n int, body core.EventData) []core.Action {
		switch n {
		case 0:
			return []core.Action{core.RecordFact{}, core.SetTimer{ID: 3, Gen: 4, After: machine.Duration(time.Millisecond)}}
		default:
			if tf, ok := body.(core.TimerFired); ok && tf.ID == 3 && tf.Gen == 4 {
				return []core.Action{core.ShutdownDone{}}
			}
		}
		return nil
	}
	if err := r.run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(r.facts) != 1 {
		t.Fatalf("facts = %v", r.facts)
	}
}

// A timer that fired goes to the Control queue, so it is taken before the Data that
// waits. The first Data step is slow, so the timer fires while it runs.
func TestTimerFiredIsControl(t *testing.T) {
	m := &fakeMachine{}
	r := newRig(m)
	for i := range 5 {
		r.cfg.Inbox.Deliver(machine.Data, core.ConnOpened{Conn: core.ConnID(i)})
	}
	m.script = func(n int, body core.EventData) []core.Action {
		switch body.(type) {
		case core.NodeStarted:
			return []core.Action{core.SetTimer{ID: 3, Gen: 4, After: machine.Duration(time.Millisecond)}}
		case core.ConnOpened:
			time.Sleep(30 * time.Millisecond)
		case core.TimerFired:
			return []core.Action{core.ShutdownDone{}}
		}
		return nil
	}
	if err := r.run(t.Context()); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, e := range m.events {
		switch e.Body.(type) {
		case core.NodeStarted:
			order = append(order, "boot")
		case core.ConnOpened:
			order = append(order, "data")
		case core.TimerFired:
			order = append(order, "timer")
		}
	}
	if got := strings.Join(order, ","); got != "boot,data,timer" {
		t.Fatalf("order = %s, want boot,data,timer", got)
	}
}

func TestDispatchTimersAndConns(t *testing.T) {
	m := &fakeMachine{}
	r := newRig(m)
	m.script = func(n int, _ core.EventData) []core.Action {
		if n != 0 {
			return nil
		}
		return []core.Action{
			core.SetTimer{ID: 1, After: machine.Duration(time.Hour)},
			core.SetTimer{ID: 1, After: machine.Duration(time.Hour)}, // replace
			core.CancelTimer{ID: 1},
			core.CancelTimer{ID: 99}, // missing
			core.CloseConn{Conn: 7, Abort: true},
			core.ResumeReads{Conn: 7},
			core.RecordFact{}, // Facts is nil
			core.ShutdownDone{},
		}
	}
	r.cfg.Route = system.Route(system.Host{Exec: r.ex, Connections: r.conns}) // Facts is nil
	if err := r.run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.conns.log, ","); got != "close,resume" {
		t.Fatalf("conns = %v", r.conns.log)
	}
}

// Every IOCall case goes to the executor, in output order.
func TestIOCallsGoToTheExecutor(t *testing.T) {
	m := &fakeMachine{script: func(int, core.EventData) []core.Action {
		return []core.Action{
			core.Send{Req: 1}, core.Connect{Req: 2}, core.ObjectCall{Call: objectstore.Get{Req: 3}},
			core.ControlPlaneCall{Req: 4}, core.ShutdownDone{},
		}
	}}
	r := newRig(m)
	if err := r.run(t.Context()); err != nil {
		t.Fatal(err)
	}
	var reqs []machine.RequestID
	for _, a := range r.ex.got {
		reqs = append(reqs, a.RequestID())
	}
	if len(reqs) != 4 || reqs[0] != 1 || reqs[1] != 2 || reqs[2] != 3 || reqs[3] != 4 {
		t.Fatalf("executor saw %v", reqs)
	}
}

// ControlPlaneReply is for the simulator. A production process refuses it.
func TestUnknownActionEndsTheRun(t *testing.T) {
	m := &fakeMachine{script: func(int, core.EventData) []core.Action {
		return []core.Action{core.ControlPlaneReply{}}
	}}
	r := newRig(m)
	err := r.run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "unknown action core.ControlPlaneReply") {
		t.Fatalf("err = %v", err)
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
	r := newRig(m)
	sd := make(chan struct{})
	r.cfg.Shutdown, r.cfg.StopGrace = sd, machine.Duration(time.Second)
	res := make(chan error, 1)
	go func() { res <- r.run(t.Context()) }()
	close(sd)
	if err := <-res; err != nil {
		t.Fatal(err)
	}
	n, s := stops(m)
	if n != 1 || s.Deadline != 7+machine.NodeTime(time.Second) {
		t.Fatalf("stops=%d deadline=%d", n, s.Deadline)
	}
}

func TestShutdownIgnoredKilled(t *testing.T) {
	r := newRig(&fakeMachine{})
	sd := make(chan struct{})
	r.cfg.Shutdown, r.cfg.StopGrace = sd, machine.Duration(50*time.Millisecond)
	res := make(chan error, 1)
	go func() { res <- r.run(t.Context()) }()
	close(sd)
	start := time.Now()
	if err := <-res; !errors.Is(err, live.ErrKilled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("killed before grace elapsed")
	}
	select {
	case <-r.cfg.Inbox.Closed():
	default:
		t.Fatal("inbox not closed")
	}
}

func TestShutdownStopDeliveredOnce(t *testing.T) {
	m := &fakeMachine{}
	r := newRig(m)
	sd := make(chan struct{})
	r.cfg.Shutdown, r.cfg.StopGrace = sd, machine.Duration(100*time.Millisecond)
	close(sd)
	for i := range 5 {
		r.cfg.Inbox.Deliver(machine.Control, core.TimerFired{ID: machine.TimerID(i)})
	}
	err := r.run(t.Context())
	if !errors.Is(err, live.ErrKilled) {
		t.Fatalf("err = %v", err)
	}
	if n, _ := stops(m); n != 1 {
		t.Fatalf("stops = %d", n)
	}
}
