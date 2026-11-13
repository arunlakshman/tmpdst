package live

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/manifesterio/mds/dst/machine"
)

// The ways Run can end.
var (
	// ErrStoppedForRestart is returned when the machine asked for a restart (End with
	// machine.Trip): a guard tripped and the memory of the machine can not be trusted any
	// more. The error text is "tripped: " and the cause.
	ErrStoppedForRestart = errors.New("tripped")
	// ErrStopped is returned when the context ended.
	ErrStopped = errors.New("stopped")
	// ErrKilled is returned when a graceful stop missed its deadline.
	ErrKilled = errors.New("killed")
)

// Config is everything a run needs. In and Out are the input and output types of the
// machine. Out is the product's sealed sum; the product's Route function has the one
// switch over it.
type Config[In, Out machine.Message] struct {
	// Self names this incarnation of the node.
	Self machine.NodeInstance
	// New builds the machine. Run calls it once.
	New machine.Factory[In, Out]
	// Env gives the machine its random numbers, buggify sites and assertions. In
	// production no site fires.
	Env machine.Env
	// Boot makes the first input. Nil means there is none.
	Boot func(self machine.NodeInstance) In
	// StopMsg makes the input that asks the machine to stop within StopGrace. It gets the
	// deadline on the clock of the machine. The input goes in the Control queue. Nil: a
	// graceful stop sends no input, and only the kill timer ends the run.
	StopMsg   func(deadline machine.NodeTime) In
	StopGrace machine.Duration
	Clock     machine.Clock
	Inbox     *Inbox[In]
	// Class picks the queue of an input that the loop delivers itself (a timer that
	// fired, a SendSelf). Nil: every such input is Data.
	Class func(in In) machine.Class
	// Route runs one output of the machine. It runs on the loop goroutine, in output
	// order, and must not block. It can use l to set timers, send to the machine itself
	// and end the run. An error ends the run.
	Route func(l *Loop[In], out Out) error
	// Shutdown, when closed, asks for a graceful stop (StopMsg, then the kill timer).
	Shutdown <-chan struct{}
	// Inject carries work that must run on the loop goroutine (gRPC requests). The
	// function returns outputs that the loop then routes like the output of a step.
	Inject <-chan func() []Out
	// AfterStep runs on the loop goroutine after every step and every inject.
	AfterStep func()
}

// Loop is lent to the Route function. It is the only way a machine acts on itself: it
// has no way to send to another node. Use it on the loop goroutine only.
type Loop[In machine.Message] struct {
	self  machine.NodeInstance
	clock machine.Clock
	inbox *Inbox[In]
	class func(in In) machine.Class

	mu     sync.Mutex
	timers map[machine.TimerID]*pending
	gen    uint64

	ended bool
	how   machine.End
	cause string
}

// pending is one armed timer. gen tells a fired timer that it was replaced.
type pending struct {
	t   machine.Timer
	gen uint64
}

// Self names this incarnation of the node.
func (l *Loop[In]) Self() machine.NodeInstance { return l.self }

func (l *Loop[In]) classOf(msg In) machine.Class {
	if l.class == nil {
		return machine.Data
	}
	return l.class(msg)
}

// SetTimer delivers msg to the machine after `after` of node time. A pending timer with
// the same id is replaced: the old one never fires.
func (l *Loop[In]) SetTimer(id machine.TimerID, after machine.Duration, msg In) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if old := l.timers[id]; old != nil {
		old.t.Stop()
	}
	l.gen++
	p := &pending{gen: l.gen}
	p.t = l.clock.AfterFunc(after, func() {
		l.mu.Lock()
		cur := l.timers[id]
		live := cur != nil && cur.gen == p.gen
		if live {
			delete(l.timers, id)
		}
		l.mu.Unlock()
		if live {
			l.inbox.Deliver(l.classOf(msg), msg)
		}
	})
	l.timers[id] = p
}

// CancelTimer removes the pending timer id. A missing timer is harmless.
func (l *Loop[In]) CancelTimer(id machine.TimerID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p := l.timers[id]; p != nil {
		p.t.Stop()
		delete(l.timers, id)
	}
}

// SendSelf delivers msg to the machine as a later input.
func (l *Loop[In]) SendSelf(msg In) {
	l.inbox.Deliver(l.classOf(msg), msg)
}

// End ends the run after the current output. Nothing after it is routed. machine.Exit
// makes Run return nil. machine.Trip makes Run return ErrStoppedForRestart with the cause.
// machine.Crash is for the simulator: it ends the run with an error.
func (l *Loop[In]) End(how machine.End, cause string) {
	if l.ended {
		return
	}
	l.ended, l.how, l.cause = true, how, cause
}

// endError is the result of Run after End.
func (l *Loop[In]) endError() error {
	switch l.how {
	case machine.Exit:
		return nil
	case machine.Trip:
		return fmt.Errorf("%w: %s", ErrStoppedForRestart, l.cause)
	case machine.Crash:
		return fmt.Errorf("live: End(Crash) %q: a crash comes from outside, a machine ends with Trip or Exit", l.cause)
	}
	return fmt.Errorf("live: End(%v): unknown way to end", l.how)
}

func (l *Loop[In]) stopTimers() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, p := range l.timers {
		p.t.Stop()
		delete(l.timers, id)
	}
}

// runner is one run: the loop and the machine it drives.
type runner[In, Out machine.Message] struct {
	cfg Config[In, Out]
	l   *Loop[In]
	m   machine.Machine[In, Out]
}

// Run builds the machine, delivers Boot, then loops until the machine ends, the context
// ends, or a stop misses its deadline. It closes the inbox on return.
func Run[In, Out machine.Message](ctx context.Context, cfg Config[In, Out]) error {
	switch {
	case cfg.Inbox == nil:
		return errors.New("live: Config.Inbox is nil")
	case cfg.New == nil:
		return errors.New("live: Config.New is nil")
	case cfg.Route == nil:
		return errors.New("live: Config.Route is nil")
	case cfg.Clock == nil:
		return errors.New("live: Config.Clock is nil")
	}
	l := &Loop[In]{self: cfg.Self, clock: cfg.Clock, inbox: cfg.Inbox, class: cfg.Class, timers: map[machine.TimerID]*pending{}}
	defer cfg.Inbox.Close()
	defer l.stopTimers()
	r := &runner[In, Out]{cfg: cfg, l: l, m: cfg.New(cfg.Env, cfg.Self)}
	if cfg.Boot != nil {
		if done, err := r.step(cfg.Boot(cfg.Self)); done {
			return err
		}
	}
	return r.run(ctx)
}

func (r *runner[In, Out]) run(ctx context.Context) error {
	in := r.cfg.Inbox
	shutdown := r.cfg.Shutdown
	var kill <-chan struct{}
	for {
		if ctx.Err() != nil {
			return ErrStopped
		}
		select {
		case <-shutdown:
			shutdown = nil
			deadline := r.cfg.Clock.Mono() + machine.NodeTime(r.cfg.StopGrace)
			if r.cfg.StopMsg != nil {
				in.Deliver(machine.Control, r.cfg.StopMsg(deadline))
			}
			killed := make(chan struct{})
			t := r.cfg.Clock.AfterFunc(r.cfg.StopGrace, func() { close(killed) })
			defer t.Stop()
			kill = killed
		default:
		}
		select {
		case <-kill:
			return ErrKilled
		default:
		}
		if done, err := r.drainInject(); done {
			return err
		}
		msg, ok := in.Pop()
		if !ok {
			if done, err := r.waitIdle(ctx, &shutdown, kill); done {
				return err
			}
			continue
		}
		if done, err := r.step(msg); done {
			return err
		}
		if done, err := r.drainInject(); done {
			return err
		}
	}
}

func (r *runner[In, Out]) waitIdle(ctx context.Context, shutdown *<-chan struct{}, kill <-chan struct{}) (bool, error) {
	select {
	case <-ctx.Done():
		return true, ErrStopped
	case <-*shutdown:
		return false, nil
	case <-kill:
		return true, ErrKilled
	case fn := <-r.cfg.Inject:
		return r.inject(fn)
	case <-r.cfg.Inbox.wake:
		return false, nil
	}
}

func (r *runner[In, Out]) drainInject() (bool, error) {
	if r.cfg.Inject == nil {
		return false, nil
	}
	for {
		select {
		case fn := <-r.cfg.Inject:
			if done, err := r.inject(fn); done {
				return true, err
			}
		default:
			return false, nil
		}
	}
}

func (r *runner[In, Out]) inject(fn func() []Out) (bool, error) {
	if fn == nil {
		return false, nil
	}
	return r.dispatch(fn())
}

func (r *runner[In, Out]) step(msg In) (bool, error) {
	now := machine.Now{Mono: r.cfg.Clock.Mono(), Wall: r.cfg.Clock.Wall()}
	return r.dispatch(r.m.Step(now, msg))
}

func (r *runner[In, Out]) after() {
	if r.cfg.AfterStep != nil {
		r.cfg.AfterStep()
	}
}

// dispatch routes outputs in order. End and a Route error end the run, and nothing
// after them is routed.
func (r *runner[In, Out]) dispatch(outs []Out) (bool, error) {
	for _, out := range outs {
		if err := r.cfg.Route(r.l, out); err != nil {
			r.after()
			return true, err
		}
		if r.l.ended {
			r.after()
			return true, r.l.endError()
		}
	}
	r.after()
	return false, nil
}
