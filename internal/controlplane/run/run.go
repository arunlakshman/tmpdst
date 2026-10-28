package run

import (
	"context"
	"errors"
	"fmt"
	"time"

	"manifester/internal/core"
)

var (
	// ErrTripped is returned when the run was stopped by a tripped guard.
	ErrTripped = errors.New("tripped")
	// ErrStopped is returned when Run ends because its context was done.
	ErrStopped = errors.New("stopped")
	// ErrKilled is returned when a graceful stop missed its deadline.
	ErrKilled = errors.New("killed")
)

// Clock stamps Event.At/Wall at inbox dequeue.
type Clock interface {
	Mono() core.Time
	Wall() core.WallTime
}

// Executor runs I/O only. Exactly one Completion per IOAction, delivered to
// the Sink and never on the caller's goroutine. Execute MUST NOT block.
type Executor interface {
	Execute(a core.IOAction)
}

// InjectFunc runs on the loop goroutine and returns actions to dispatch
// (Offer, TakeAnswers bookkeeping). Optional on Bundle.Inject.
type InjectFunc func() []core.Action

// Bundle is everything nondeterministic a control-plane process needs.
type Bundle struct {
	Clock     Clock
	Env       core.Env
	Inbox     *Inbox
	Facts     func(core.Fact)
	Shutdown  <-chan struct{}
	StopGrace core.Duration
	// Inject, if non-nil, carries work that must run on the loop goroutine
	// (gRPC adapter Offer/reads). Checked every turn and while waiting.
	Inject <-chan InjectFunc
	// AfterStep runs on the loop goroutine after each Step or inject
	// dispatch. Used to drain TakeAnswers to waiting RPCs.
	AfterStep func(m core.Machine)
}

// Run delivers Started, then loops: ControlFirst picks, At/Wall stamped at
// dequeue, Step, actions dispatched in slice order. Returns ErrTripped
// (wrapped) on Trip, nil on Exit, ErrStopped on ctx done, ErrKilled when
// StopGrace expires after Shutdown. Closes b.Inbox on return.
func Run(ctx context.Context, b Bundle, exec Executor, f core.Factory, self core.NodeRef) error {
	in := b.Inbox
	if in == nil {
		return errors.New("controlplane/run: Bundle.Inbox is nil")
	}
	defer in.Close()
	m := f(b.Env, self)
	r := &runner{b: b, in: in, exec: exec, m: m, timers: map[core.TimerID]*time.Timer{}}
	defer func() {
		for _, t := range r.timers {
			t.Stop()
		}
	}()

	step := func(body core.EventBody) (bool, error) {
		acts := m.Step(core.Event{At: b.Clock.Mono(), Wall: b.Clock.Wall(), Body: body})
		return r.dispatch(acts)
	}
	if done, err := step(core.Started{Self: self}); done {
		return err
	}
	var policy core.InboxPolicy = core.ControlFirst{}
	ctrl, data := in.Control(), in.Data()
	shutdown := b.Shutdown
	var kill <-chan time.Time
	for {
		if ctx.Err() != nil {
			return ErrStopped
		}
		select {
		case <-shutdown:
			shutdown = nil
			in.DeliverControl(core.Stop{Deadline: b.Clock.Mono() + core.Time(b.StopGrace)})
			t := time.NewTimer(time.Duration(b.StopGrace))
			defer t.Stop()
			kill = t.C
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
		body, ok := policy.Next(ctrl, data)
		if !ok {
			if done, err := r.waitIdle(ctx, &shutdown, kill); done {
				return err
			}
			continue
		}
		if done, err := step(body); done {
			return err
		}
		if done, err := r.drainInject(); done {
			return err
		}
	}
}

func (r *runner) waitIdle(ctx context.Context, shutdown *<-chan struct{}, kill <-chan time.Time) (bool, error) {
	select {
	case <-ctx.Done():
		return true, ErrStopped
	case <-*shutdown:
		return false, nil
	case <-kill:
		return true, ErrKilled
	case fn := <-r.b.Inject:
		return r.runInject(fn)
	case <-r.in.wake:
		return false, nil
	}
}

type runner struct {
	b      Bundle
	in     *Inbox
	exec   Executor
	m      core.Machine
	timers map[core.TimerID]*time.Timer
}

func (r *runner) after() {
	if r.b.AfterStep != nil {
		r.b.AfterStep(r.m)
	}
}

func (r *runner) drainInject() (bool, error) {
	if r.b.Inject == nil {
		return false, nil
	}
	for {
		select {
		case fn := <-r.b.Inject:
			if done, err := r.runInject(fn); done {
				return true, err
			}
		default:
			return false, nil
		}
	}
}

func (r *runner) runInject(fn InjectFunc) (bool, error) {
	if fn == nil {
		return false, nil
	}
	return r.dispatch(fn())
}

func (r *runner) dispatch(acts []core.Action) (bool, error) {
	for _, a := range acts {
		switch a := a.(type) {
		case core.IOAction:
			r.exec.Execute(a)
		case core.Emit:
			if r.b.Facts != nil {
				r.b.Facts(a.Fact)
			}
		case core.SetTimer:
			r.setTimer(a)
		case core.CancelTimer:
			r.cancelTimer(a.ID)
		case core.Trip:
			r.after()
			return true, fmt.Errorf("%w: %s", ErrTripped, a.Cause)
		case core.Exit:
			r.after()
			return true, nil
		default:
			r.after()
			return true, fmt.Errorf("controlplane/run: unknown action %T", a)
		}
	}
	r.after()
	return false, nil
}

func (r *runner) setTimer(a core.SetTimer) {
	if old := r.timers[a.ID]; old != nil {
		old.Stop()
	}
	id, gen := a.ID, a.Gen
	r.timers[id] = time.AfterFunc(time.Duration(a.After), func() {
		r.in.DeliverControl(core.TimerFired{ID: id, Gen: gen})
	})
}

func (r *runner) cancelTimer(id core.TimerID) {
	if t := r.timers[id]; t != nil {
		t.Stop()
		delete(r.timers, id)
	}
}
