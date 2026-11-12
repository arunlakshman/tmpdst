package live

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
)

var (
	// ErrStoppedForRestart is returned when the process returned sim.Crash: a
	// guard tripped and the memory of the process can not be trusted any more.
	ErrStoppedForRestart = errors.New("tripped")
	// ErrStopped is returned when the context ended.
	ErrStopped = errors.New("stopped")
	// ErrKilled is returned when a graceful stop missed its deadline.
	ErrKilled = errors.New("killed")
)

// Clock is the time source. The production adapters give the real time; a test gives its own.
type Clock interface {
	// Mono is the time since the clock started.
	Mono() sched.TimeNs
	// Wall is the UTC time in Unix nanoseconds.
	Wall() sched.TimeNs
	// AfterFunc calls f once, on its own goroutine, after d.
	AfterFunc(d sched.TimeNs, f func()) Timer
}

// Timer is a pending AfterFunc.
type Timer interface{ Stop() bool }

// InjectFunc runs on the loop goroutine, between steps, and returns outbound
// values that the loop then handles like the output of a step.
type InjectFunc func() []any

// Config is everything a run needs.
type Config struct {
	Process sim.Process
	// Boot is the first inbox value. Nil means none.
	Boot any
	// Stop makes the inbox value that asks the process to stop within the
	// grace time. It gets the deadline on the Clock.
	Stop      func(deadline sched.TimeNs) any
	StopGrace time.Duration
	Clock     Clock
	Inbox     *Inbox
	// Exec starts a value that is not a sim control value (a call to the log or
	// the object store). It must not block. An error ends the run.
	Exec func(out any) error
	// Shutdown, when closed, asks for a graceful stop (Stop, then the kill timer).
	Shutdown <-chan struct{}
	// Inject carries work that must run on the loop goroutine (gRPC requests).
	Inject <-chan InjectFunc
	// AfterStep runs on the loop goroutine after every step and every inject.
	AfterStep func()
	// Seed seeds the process's random stream. Zero takes it from the OS.
	Seed uint64
	// Assert receives the process's assertions. Nil keeps coverage only.
	Assert *assert.Log
}

// Run delivers Boot, then loops until the process ends, the context ends, or a
// stop misses its deadline. It closes the inbox on return.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Inbox == nil {
		return errors.New("live: Config.Inbox is nil")
	}
	l := &loop{cfg: cfg, timers: map[uint64]*pending{}}
	defer cfg.Inbox.Close()
	defer l.stopTimers()
	seed := cfg.Seed
	if seed == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return fmt.Errorf("live: seed: %w", err)
		}
		seed = binary.LittleEndian.Uint64(b[:]) | 1
	}
	r := rng.New(seed)
	faults := fault.New(r)
	faults.SetQuiet(true) // no fault injection in production
	asserts := cfg.Assert
	if asserts == nil {
		asserts = assert.New(nil)
	}
	l.cx = stepContext{clock: cfg.Clock, rand: r, faults: faults, asserts: asserts}

	if cfg.Boot != nil {
		if done, err := l.step(cfg.Boot); done {
			return err
		}
	}
	return l.run(ctx)
}

type loop struct {
	cfg    Config
	cx     stepContext
	mu     sync.Mutex
	timers map[uint64]*pending
	gen    uint64
}

// pending is one armed timer. gen tells a fired timer that was replaced.
type pending struct {
	t   Timer
	gen uint64
}

func (l *loop) run(ctx context.Context) error {
	in := l.cfg.Inbox
	shutdown := l.cfg.Shutdown
	var kill <-chan struct{}
	for {
		if ctx.Err() != nil {
			return ErrStopped
		}
		select {
		case <-shutdown:
			shutdown = nil
			deadline := l.cfg.Clock.Mono() + sched.TimeNs(l.cfg.StopGrace)
			if l.cfg.Stop != nil {
				in.DeliverUrgent(l.cfg.Stop(deadline))
			}
			killed := make(chan struct{})
			t := l.cfg.Clock.AfterFunc(sched.TimeNs(l.cfg.StopGrace), func() { close(killed) })
			defer t.Stop()
			kill = killed
		default:
		}
		select {
		case <-kill:
			return ErrKilled
		default:
		}
		if done, err := l.drainInject(); done {
			return err
		}
		body, ok := in.Pop()
		if !ok {
			if done, err := l.waitIdle(ctx, &shutdown, kill); done {
				return err
			}
			continue
		}
		if done, err := l.step(body); done {
			return err
		}
		if done, err := l.drainInject(); done {
			return err
		}
	}
}

func (l *loop) waitIdle(ctx context.Context, shutdown *<-chan struct{}, kill <-chan struct{}) (bool, error) {
	select {
	case <-ctx.Done():
		return true, ErrStopped
	case <-*shutdown:
		return false, nil
	case <-kill:
		return true, ErrKilled
	case fn := <-l.cfg.Inject:
		return l.inject(fn)
	case <-l.cfg.Inbox.wake:
		return false, nil
	}
}

func (l *loop) drainInject() (bool, error) {
	if l.cfg.Inject == nil {
		return false, nil
	}
	for {
		select {
		case fn := <-l.cfg.Inject:
			if done, err := l.inject(fn); done {
				return true, err
			}
		default:
			return false, nil
		}
	}
}

func (l *loop) inject(fn InjectFunc) (bool, error) {
	if fn == nil {
		return false, nil
	}
	return l.dispatch(fn())
}

func (l *loop) step(body any) (bool, error) {
	return l.dispatch(l.cfg.Process.Step(l.cx, body))
}

func (l *loop) after() {
	if l.cfg.AfterStep != nil {
		l.cfg.AfterStep()
	}
}

// dispatch handles outbound values in order. Crash and Exit end the run and
// nothing after them is handled.
func (l *loop) dispatch(out []any) (bool, error) {
	self := l.cfg.Process.ID()
	for _, v := range out {
		switch v := v.(type) {
		case sim.SetTimer:
			l.setTimer(v)
		case sim.CancelTimer:
			l.cancelTimer(v.ID)
		case sim.Send:
			if v.To != self {
				l.after()
				return true, fmt.Errorf("live: Send to %q: only the process itself can be reached", v.To)
			}
			l.cfg.Inbox.Deliver(v.Body)
		case sim.Crash:
			l.after()
			if v.Who != "" && v.Who != self {
				return true, fmt.Errorf("live: Crash of %q: only the process itself can be crashed", v.Who)
			}
			return true, fmt.Errorf("%w: %s", ErrStoppedForRestart, v.Cause)
		case sim.Exit:
			l.after()
			return true, nil
		case sim.Restart:
			l.after()
			return true, errors.New("live: Restart is for the simulator; a production restart starts a new process")
		default:
			if err := l.cfg.Exec(v); err != nil {
				l.after()
				return true, err
			}
		}
	}
	l.after()
	return false, nil
}

func (l *loop) setTimer(a sim.SetTimer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if old := l.timers[a.ID]; old != nil {
		old.t.Stop()
	}
	l.gen++
	p := &pending{gen: l.gen}
	id, body := a.ID, a.Body
	p.t = l.cfg.Clock.AfterFunc(a.After, func() {
		l.mu.Lock()
		cur := l.timers[id]
		live := cur != nil && cur.gen == p.gen
		if live {
			delete(l.timers, id)
		}
		l.mu.Unlock()
		if live {
			l.cfg.Inbox.Deliver(body)
		}
	})
	l.timers[id] = p
}

func (l *loop) cancelTimer(id uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p := l.timers[id]; p != nil {
		p.t.Stop()
		delete(l.timers, id)
	}
}

func (l *loop) stopTimers() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, p := range l.timers {
		p.t.Stop()
		delete(l.timers, id)
	}
}

// stepContext is the sim.Context of a live run.
type stepContext struct {
	clock   Clock
	rand    *rng.Rand
	faults  *fault.Injector
	asserts *assert.Log
}

func (c stepContext) Now() sched.TimeNs       { return c.clock.Mono() }
func (c stepContext) Wall() sched.TimeNs      { return c.clock.Wall() }
func (c stepContext) Rand() *rng.Rand         { return c.rand }
func (c stepContext) Faults() *fault.Injector { return c.faults }
func (c stepContext) Assert() *assert.Log     { return c.asserts }
