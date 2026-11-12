package live_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/system/live"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/syncclock"
)

// manualClock is a clock that moves only when the test says so.
type manualClock struct{ *syncclock.Sync }

func newManualClock() manualClock {
	return manualClock{syncclock.NewSync(1_000_000_000, 1_000_000_000)}
}

func (c manualClock) AfterFunc(d sched.TimeNs, f func()) live.Timer { return c.Sync.AfterFunc(d, f) }

// script is a process whose Step is a function of the step number and the inbox value.
type script struct {
	mu     sync.Mutex
	inbox  []any
	script func(n int, inbox any) []any
}

func (s *script) ID() sim.ProcessID { return "p" }
func (s *script) StateHash() uint64 { return 0 }
func (s *script) Step(_ sim.Context, inbox any) []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.inbox)
	s.inbox = append(s.inbox, inbox)
	if s.script == nil {
		return nil
	}
	return s.script(n, inbox)
}

func (s *script) seen() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]any(nil), s.inbox...)
}

type (
	boot      struct{}
	stop      struct{ deadline sched.TimeNs }
	fired     struct{ id int }
	ioCall    struct{ req int }
	badOutput struct{}
)

type execs struct {
	mu  sync.Mutex
	got []any
	err error
}

func (e *execs) Exec(out any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, bad := out.(badOutput); bad {
		return errors.New("bad output")
	}
	e.got = append(e.got, out)
	return e.err
}

func config(p sim.Process, ex *execs) live.Config {
	return live.Config{
		Process: p, Boot: boot{}, Clock: newManualClock(), Inbox: live.NewInbox(), Exec: ex.Exec,
		Stop: func(d sched.TimeNs) any { return stop{deadline: d} },
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

func TestBootThenExit(t *testing.T) {
	p := &script{script: func(n int, _ any) []any {
		if n == 0 {
			return []any{sim.Exit{}}
		}
		return nil
	}}
	if err := live.Run(t.Context(), config(p, &execs{})); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.seen()[0].(boot); !ok {
		t.Fatalf("first = %T", p.seen()[0])
	}
}

func TestInjectRunsOnLoop(t *testing.T) {
	p := &script{script: func(n int, _ any) []any {
		if n == 1 {
			return []any{sim.Exit{}}
		}
		return nil
	}}
	cfg := config(p, &execs{})
	inject := make(chan live.InjectFunc, 1)
	cfg.Inject = inject
	var saw bool
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	inject <- func() []any {
		saw = true
		cfg.Inbox.Deliver(fired{1})
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
	cfg := config(&script{}, &execs{})
	inject := make(chan live.InjectFunc)
	cfg.Inject = inject
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	time.Sleep(20 * time.Millisecond) // the loop is idle
	inject <- func() []any { return []any{sim.Exit{}} }
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestTimersAndCallsAreDispatched(t *testing.T) {
	ex := &execs{}
	p := &script{script: func(n int, inbox any) []any {
		switch n {
		case 0:
			return []any{
				sim.SetTimer{ID: 1, After: sched.TimeNs(time.Hour), Body: fired{1}},
				sim.SetTimer{ID: 1, After: sched.TimeNs(time.Hour), Body: fired{1}}, // replaces
				sim.CancelTimer{ID: 1},
				sim.CancelTimer{ID: 99}, // missing: harmless
				ioCall{req: 1},
				sim.Send{To: "p", Body: fired{2}},
			}
		case 1:
			if f, ok := inbox.(fired); !ok || f.id != 2 {
				t.Errorf("second step got %v, want the self Send", inbox)
			}
			return []any{sim.Exit{}}
		}
		return nil
	}}
	if err := live.Run(t.Context(), config(p, ex)); err != nil {
		t.Fatal(err)
	}
	if len(ex.got) != 1 || ex.got[0] != (ioCall{req: 1}) {
		t.Fatalf("exec got %v", ex.got)
	}
}

func TestTimerFiresIntoInbox(t *testing.T) {
	clock := newManualClock()
	p := &script{script: func(n int, inbox any) []any {
		switch n {
		case 0:
			return []any{sim.SetTimer{ID: 3, After: sched.TimeNs(time.Second), Body: fired{3}}}
		default:
			if inbox == (fired{3}) {
				return []any{sim.Exit{}}
			}
		}
		return nil
	}}
	cfg := config(p, &execs{})
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

// A process that returns Crash (a tripped guard) ends the run with
// ErrStoppedForRestart and the cause, and nothing after the Crash is handled.
func TestCrashIsStoppedForRestart(t *testing.T) {
	ex := &execs{}
	p := &script{script: func(int, any) []any {
		return []any{sim.Crash{Cause: "guard X"}, ioCall{req: 7}}
	}}
	err := live.Run(t.Context(), config(p, ex))
	if !errors.Is(err, live.ErrStoppedForRestart) || err.Error() != "tripped: guard X" {
		t.Fatalf("got %v", err)
	}
	if len(ex.got) != 0 {
		t.Fatalf("a call after Crash ran: %v", ex.got)
	}
}

// Shutdown delivers the stop request with the deadline. A process that answers
// with Exit ends the run cleanly.
func TestGracefulStop(t *testing.T) {
	p := &script{script: func(_ int, inbox any) []any {
		if s, ok := inbox.(stop); ok && s.deadline > 0 {
			return []any{sim.Exit{}}
		}
		return nil
	}}
	cfg := config(p, &execs{})
	shutdown := make(chan struct{})
	cfg.Shutdown, cfg.StopGrace = shutdown, time.Second
	done := make(chan error, 1)
	go func() { done <- live.Run(t.Context(), cfg) }()
	close(shutdown)
	if err := within(t, done); err != nil {
		t.Fatal(err)
	}
}

// A process that ignores the stop request is killed when the grace time ends.
// The grace timer comes from the clock, so the test moves it by hand.
func TestStopGraceExpiryKills(t *testing.T) {
	clock := newManualClock()
	cfg := config(&script{}, &execs{})
	shutdown := make(chan struct{})
	cfg.Clock, cfg.Shutdown, cfg.StopGrace = clock, shutdown, time.Second
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

// Control first: the stop request is taken before values that wait in the queue.
func TestStopRequestJumpsTheQueue(t *testing.T) {
	var order []string
	var mu sync.Mutex
	p := &script{script: func(_ int, inbox any) []any {
		mu.Lock()
		defer mu.Unlock()
		switch inbox.(type) {
		case stop:
			order = append(order, "stop")
			return []any{sim.Exit{}}
		case fired:
			order = append(order, "data")
		}
		return nil
	}}
	cfg := config(p, &execs{})
	for i := range 5 {
		cfg.Inbox.Deliver(fired{i})
	}
	shutdown := make(chan struct{})
	close(shutdown) // already asked when the loop starts
	cfg.Shutdown, cfg.StopGrace = shutdown, time.Second
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) == 0 || order[0] != "stop" {
		t.Fatalf("order = %v, want stop first", order)
	}
}

func TestContextEndStopsTheRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- live.Run(ctx, config(&script{}, &execs{})) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := within(t, done); !errors.Is(err, live.ErrStopped) {
		t.Fatalf("got %v, want ErrStopped", err)
	}
}

func TestExecErrorEndsTheRun(t *testing.T) {
	p := &script{script: func(int, any) []any { return []any{badOutput{}} }}
	if err := live.Run(t.Context(), config(p, &execs{})); err == nil || err.Error() != "bad output" {
		t.Fatalf("got %v", err)
	}
}

func TestRunClosesTheInbox(t *testing.T) {
	cfg := config(&script{script: func(int, any) []any { return []any{sim.Exit{}} }}, &execs{})
	if err := live.Run(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Inbox.Deliver(fired{1}) // dropped, not queued
	if cfg.Inbox.Len() != 0 {
		t.Fatal("a closed inbox queued a value")
	}
	select {
	case <-cfg.Inbox.Closed():
	default:
		t.Fatal("inbox not closed")
	}
}
