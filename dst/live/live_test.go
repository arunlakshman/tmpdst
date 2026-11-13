package live_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/syncclock"
	"github.com/manifesterio/mds/dst/trace"
)

// in is the input type of the test machine. kind says what happened.
type in struct {
	kind     string // "boot", "stop", "fired", "data"
	n        int
	deadline machine.NodeTime
}

func (i in) Fold(h *trace.Hash) {
	h.String(i.kind)
	h.Int64(int64(i.n))
}

// out is the output type of the test machine. The test Route function has the one switch
// over kind.
type out struct {
	kind  string // "timer", "cancel", "send", "trip", "exit", "io", "bad"
	id    machine.TimerID
	after machine.Duration
	msg   in
	req   int
}

func (o out) Fold(h *trace.Hash) {
	h.String(o.kind)
	h.Int64(int64(o.req))
}

// manualClock is a clock that moves only when the test says so.
type manualClock struct{ *syncclock.Sync }

func newManualClock() manualClock {
	return manualClock{syncclock.NewSync(1_000_000_000, 1_000_000_000)}
}

func (c manualClock) Mono() machine.NodeTime { return machine.NodeTime(c.Sync.Mono()) }
func (c manualClock) Wall() machine.RealTime { return machine.RealTime(c.Sync.Wall()) }
func (c manualClock) AfterFunc(d machine.Duration, f func()) machine.Timer {
	return c.Sync.AfterFunc(sched.TimeNs(d), f)
}

// script is a machine whose Step is a function of the step number and the input.
type script struct {
	mu     sync.Mutex
	inputs []in
	script func(n int, msg in) []out
}

func (s *script) StateHash() uint64 { return 0 }
func (s *script) Step(_ machine.Now, msg in) []out {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.inputs)
	s.inputs = append(s.inputs, msg)
	if s.script == nil {
		return nil
	}
	return s.script(n, msg)
}

func (s *script) seen() []in {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]in(nil), s.inputs...)
}

// calls records the "io" outputs that Route got.
type calls struct {
	mu  sync.Mutex
	got []int
}

func (c *calls) list() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.got...)
}

func (c *calls) route(l *live.Loop[in], o out) error {
	switch o.kind {
	case "timer":
		l.SetTimer(o.id, o.after, o.msg)
	case "cancel":
		l.CancelTimer(o.id)
	case "send":
		l.SendSelf(o.msg)
	case "trip":
		l.End(machine.Trip, "guard X")
	case "crash":
		l.End(machine.Crash, "from inside")
	case "exit":
		l.End(machine.Exit, "")
	case "io":
		c.mu.Lock()
		c.got = append(c.got, o.req)
		c.mu.Unlock()
	case "bad":
		return errors.New("bad output")
	}
	return nil
}

func config(m *script, c *calls) live.Config[in, out] {
	return live.Config[in, out]{
		Self:  machine.NodeInstance{Node: "p"},
		New:   func(machine.Env, machine.NodeInstance) machine.Machine[in, out] { return m },
		Boot:  func(machine.NodeInstance) in { return in{kind: "boot"} },
		Clock: newManualClock(),
		Inbox: live.NewInbox[in](0),
		Route: c.route,
		StopMsg: func(d machine.NodeTime) in {
			return in{kind: "stop", deadline: d}
		},
	}
}

func within(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not end")
		return nil
	}
}

func exitAt(step int) func(n int, msg in) []out {
	return func(n int, _ in) []out {
		if n == step {
			return []out{{kind: "exit"}}
		}
		return nil
	}
}

func TestBootThenExit(t *testing.T) {
	m := &script{script: exitAt(0)}
	if err := live.Run(t.Context(), config(m, &calls{})); err != nil {
		t.Fatal(err)
	}
	if got := m.seen(); len(got) != 1 || got[0].kind != "boot" {
		t.Fatalf("seen = %v", got)
	}
}

func TestNeedsInboxNewRouteAndClock(t *testing.T) {
	for name, edit := range map[string]func(*live.Config[in, out]){
		"inbox": func(c *live.Config[in, out]) { c.Inbox = nil },
		"new":   func(c *live.Config[in, out]) { c.New = nil },
		"route": func(c *live.Config[in, out]) { c.Route = nil },
		"clock": func(c *live.Config[in, out]) { c.Clock = nil },
	} {
		cfg := config(&script{}, &calls{})
		edit(&cfg)
		if err := live.Run(t.Context(), cfg); err == nil {
			t.Errorf("%s: Run did not refuse a missing field", name)
		}
	}
}

func TestInjectRunsOnLoop(t *testing.T) {
	m := &script{script: exitAt(1)}
	cfg := config(m, &calls{})
	inject := make(chan func() []out, 1)
	cfg.Inject = inject
	var saw bool
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	inject <- func() []out {
		saw = true
		cfg.Inbox.Deliver(machine.Data, in{kind: "fired", n: 1})
		return nil
	}
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
	if !saw {
		t.Fatal("inject did not run")
	}
}

func TestWaitIdleThenInjectExit(t *testing.T) {
	cfg := config(&script{}, &calls{})
	inject := make(chan func() []out)
	cfg.Inject = inject
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	time.Sleep(20 * time.Millisecond) // the loop is idle
	inject <- func() []out { return []out{{kind: "exit"}} }
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestTimersAndCallsAreRouted(t *testing.T) {
	c := &calls{}
	m := &script{script: func(n int, msg in) []out {
		switch n {
		case 0:
			return []out{
				{kind: "timer", id: 1, after: machine.Duration(time.Hour), msg: in{kind: "fired", n: 1}},
				{kind: "timer", id: 1, after: machine.Duration(time.Hour), msg: in{kind: "fired", n: 1}}, // replaces
				{kind: "cancel", id: 1},
				{kind: "cancel", id: 99}, // missing: harmless
				{kind: "io", req: 1},
				{kind: "send", msg: in{kind: "fired", n: 2}},
			}
		case 1:
			if msg.kind != "fired" || msg.n != 2 {
				t.Errorf("second step got %v, want the SendSelf", msg)
			}
			return []out{{kind: "exit"}}
		}
		return nil
	}}
	if err := live.Run(t.Context(), config(m, c)); err != nil {
		t.Fatal(err)
	}
	if got := c.list(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("route got %v", got)
	}
}

func TestTimerFiresIntoInbox(t *testing.T) {
	clock := newManualClock()
	m := &script{script: func(n int, msg in) []out {
		switch {
		case n == 0:
			return []out{{kind: "timer", id: 3, after: machine.Duration(time.Second), msg: in{kind: "fired", n: 3}}}
		case msg.kind == "fired" && msg.n == 3:
			return []out{{kind: "exit"}}
		}
		return nil
	}}
	cfg := config(m, &calls{})
	cfg.Clock = clock
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	for clock.Pending() == 0 { // the loop armed the timer
		time.Sleep(time.Millisecond)
	}
	clock.Advance(2 * sched.TimeNs(time.Second))
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
}

// A replaced timer fires once, with the message of the last SetTimer.
func TestReplacedTimerFiresOnce(t *testing.T) {
	clock := newManualClock()
	var fired []int
	var mu sync.Mutex
	m := &script{script: func(n int, msg in) []out {
		switch {
		case n == 0:
			return []out{
				{kind: "timer", id: 4, after: machine.Duration(time.Second), msg: in{kind: "fired", n: 1}},
				{kind: "timer", id: 4, after: machine.Duration(2 * time.Second), msg: in{kind: "fired", n: 2}},
			}
		case msg.kind == "fired":
			mu.Lock()
			fired = append(fired, msg.n)
			mu.Unlock()
			return []out{{kind: "exit"}}
		}
		return nil
	}}
	cfg := config(m, &calls{})
	cfg.Clock = clock
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	for clock.Pending() == 0 {
		time.Sleep(time.Millisecond)
	}
	clock.Advance(3 * sched.TimeNs(time.Second))
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 1 || fired[0] != 2 {
		t.Fatalf("fired = %v, want [2]", fired)
	}
}

// A machine that ends with Trip (a tripped guard) ends the run with ErrStoppedForRestart
// and the cause, and nothing after the End is routed.
func TestTripIsStoppedForRestart(t *testing.T) {
	c := &calls{}
	m := &script{script: func(int, in) []out {
		return []out{{kind: "trip"}, {kind: "io", req: 7}}
	}}
	err := live.Run(t.Context(), config(m, c))
	if !errors.Is(err, live.ErrStoppedForRestart) || err.Error() != "tripped: guard X" {
		t.Fatalf("got %v", err)
	}
	if got := c.list(); len(got) != 0 {
		t.Fatalf("a call after End ran: %v", got)
	}
}

// End(Crash) is for the simulator: a machine can not use it in production.
func TestCrashIsRefused(t *testing.T) {
	m := &script{script: func(int, in) []out { return []out{{kind: "crash"}} }}
	err := live.Run(t.Context(), config(m, &calls{}))
	if err == nil || errors.Is(err, live.ErrStoppedForRestart) {
		t.Fatalf("got %v, want a plain error", err)
	}
}

// AfterStep runs after a step that ends the run, and after a step that does not.
func TestAfterStepRunsAlways(t *testing.T) {
	m := &script{script: exitAt(1)}
	cfg := config(m, &calls{})
	var mu sync.Mutex
	runs := 0
	cfg.AfterStep = func() { mu.Lock(); runs++; mu.Unlock() }
	cfg.Inbox.Deliver(machine.Data, in{kind: "data"})
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if runs != 2 {
		t.Fatalf("AfterStep ran %d times, want 2 (boot, then the step that exits)", runs)
	}
}

// Shutdown delivers the stop request with the deadline. A machine that answers
// with Exit ends the run cleanly.
func TestGracefulStop(t *testing.T) {
	m := &script{script: func(_ int, msg in) []out {
		if msg.kind == "stop" && msg.deadline > 0 {
			return []out{{kind: "exit"}}
		}
		return nil
	}}
	cfg := config(m, &calls{})
	shutdown := make(chan struct{})
	cfg.Shutdown, cfg.StopGrace = shutdown, machine.Duration(time.Second)
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	close(shutdown)
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
}

// A machine that ignores the stop request is killed when the grace time ends.
// The grace timer comes from the clock, so the test moves it by hand.
func TestStopGraceExpiryKills(t *testing.T) {
	clock := newManualClock()
	cfg := config(&script{}, &calls{})
	shutdown := make(chan struct{})
	cfg.Clock, cfg.Shutdown, cfg.StopGrace = clock, shutdown, machine.Duration(time.Second)
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	close(shutdown)
	for clock.Pending() == 0 { // the loop armed the grace timer
		time.Sleep(time.Millisecond)
	}
	clock.Advance(2 * sched.TimeNs(time.Second))
	if err := within(t, done); !errors.Is(err, live.ErrKilled) {
		t.Fatalf("got %v, want ErrKilled", err)
	}
}

// Control first: the stop request is taken before inputs that wait in the Data queue.
func TestStopRequestJumpsTheQueue(t *testing.T) {
	var order []string
	var mu sync.Mutex
	m := &script{script: func(_ int, msg in) []out {
		mu.Lock()
		defer mu.Unlock()
		switch msg.kind {
		case "stop":
			order = append(order, "stop")
			return []out{{kind: "exit"}}
		case "fired":
			order = append(order, "data")
		}
		return nil
	}}
	cfg := config(m, &calls{})
	for i := range 5 {
		cfg.Inbox.Deliver(machine.Data, in{kind: "fired", n: i})
	}
	shutdown := make(chan struct{})
	close(shutdown) // already asked when the loop starts
	cfg.Shutdown, cfg.StopGrace = shutdown, machine.Duration(time.Second)
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) == 0 || order[0] != "stop" {
		t.Fatalf("order = %v, want stop first", order)
	}
}

// Class picks the queue of an input that the loop delivers itself.
func TestClassPicksTheQueueOfSendSelf(t *testing.T) {
	var kinds []string
	m := &script{script: func(n int, msg in) []out {
		kinds = append(kinds, msg.kind)
		if n == 0 {
			return []out{
				{kind: "send", msg: in{kind: "data"}},
				{kind: "send", msg: in{kind: "urgent"}},
			}
		}
		if len(kinds) == 3 {
			return []out{{kind: "exit"}}
		}
		return nil
	}}
	cfg := config(m, &calls{})
	cfg.Class = func(msg in) machine.Class {
		if msg.kind == "urgent" {
			return machine.Control
		}
		return machine.Data
	}
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 3 || kinds[1] != "urgent" || kinds[2] != "data" {
		t.Fatalf("kinds = %v, want boot, urgent, data", kinds)
	}
}

func TestContextEndStopsTheRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- live.Run(ctx, config(&script{}, &calls{})) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := within(t, done); !errors.Is(err, live.ErrStopped) {
		t.Fatalf("got %v, want ErrStopped", err)
	}
}

func TestRouteErrorEndsTheRun(t *testing.T) {
	m := &script{script: func(int, in) []out { return []out{{kind: "bad"}} }}
	if err := live.Run(t.Context(), config(m, &calls{})); err == nil || err.Error() != "bad output" {
		t.Fatalf("got %v", err)
	}
}

func TestRunClosesTheInbox(t *testing.T) {
	cfg := config(&script{script: exitAt(0)}, &calls{})
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Inbox.Deliver(machine.Data, in{kind: "fired", n: 1}) // dropped, not queued
	if cfg.Inbox.Len() != 0 {
		t.Fatal("a closed inbox queued a value")
	}
	select {
	case <-cfg.Inbox.Closed():
	default:
		t.Fatal("inbox not closed")
	}
}

// The machine gets the env and the node instance that Config names.
func TestNewGetsEnvAndSelf(t *testing.T) {
	cfg := config(&script{script: exitAt(0)}, &calls{})
	var got machine.NodeInstance
	cfg.New = func(_ machine.Env, self machine.NodeInstance) machine.Machine[in, out] {
		got = self
		return &script{script: exitAt(0)}
	}
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if got.Node != "p" {
		t.Fatalf("self = %v", got)
	}
}
