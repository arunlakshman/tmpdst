package runloop

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mds/internal/core"
)

var (
	// ErrStoppedForRestart is returned when the run was stopped by a tripped guard.
	ErrStoppedForRestart = errors.New("tripped")
	// ErrStopped is returned when Run ends because its context was done.
	ErrStopped = errors.New("stopped")
	// ErrKilledByRuntime is returned when a graceful stop missed its deadline.
	ErrKilledByRuntime = errors.New("killed")
)

// Clock stamps Event.At/Wall at inbox dequeue.
type Clock interface {
	Mono() core.NodeTime
	Wall() core.RealTime
}

// Executor runs I/O only. Exactly one IOResult per IOCall, delivered to
// the InboxWriter and never on the caller's goroutine. Execute MUST NOT block.
type Executor interface {
	Execute(a core.IOCall)
}

// InjectFunc runs on the loop goroutine and returns actions to dispatch
// (Offer, TakeAnswers bookkeeping). Optional on Dependencies.Inject.
type InjectFunc func() []core.Action

// Dependencies is everything nondeterministic a control-plane process needs.
type Dependencies struct {
	Clock     Clock
	Env       core.Environment
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
// dequeue, Step, actions dispatched in slice order. Returns ErrStoppedForRestart
// (wrapped) on Trip, nil on Exit, ErrStopped on ctx done, ErrKilledByRuntime when
// StopGrace expires after Shutdown. Closes b.Inbox on return.
func Run(ctx context.Context, b Dependencies, exec Executor, f core.MachineFactory, self core.NodeInstance) error {
	in := b.Inbox
	if in == nil {
		return errors.New("controlplane/runloop: Bundle.Inbox is nil")
	}
	defer in.Close()
	m := f(b.Env, self)
	r := &eventLoop{b: b, in: in, exec: exec, m: m, timers: map[core.TimerID]*time.Timer{}}
	defer func() {
		for _, t := range r.timers {
			t.Stop()
		}
	}()

	step := func(body core.EventData) (bool, error) {
		acts := m.Step(core.Event{At: b.Clock.Mono(), Wall: b.Clock.Wall(), Body: body})
		return r.dispatch(acts)
	}
	if done, err := step(core.NodeStarted{Self: self}); done {
		return err
	}
	var policy core.InboxPicker = core.ControlFirstPicker{}
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
			in.DeliverControl(core.StopRequest{Deadline: b.Clock.Mono() + core.NodeTime(b.StopGrace)})
			t := time.NewTimer(time.Duration(b.StopGrace))
			defer t.Stop()
			kill = t.C
		default:
		}
		select {
		case <-kill:
			return ErrKilledByRuntime
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

func (r *eventLoop) waitIdle(ctx context.Context, shutdown *<-chan struct{}, kill <-chan time.Time) (bool, error) {
	select {
	case <-ctx.Done():
		return true, ErrStopped
	case <-*shutdown:
		return false, nil
	case <-kill:
		return true, ErrKilledByRuntime
	case fn := <-r.b.Inject:
		return r.runInject(fn)
	case <-r.in.wake:
		return false, nil
	}
}

type eventLoop struct {
	b      Dependencies
	in     *Inbox
	exec   Executor
	m      core.Machine
	timers map[core.TimerID]*time.Timer
}

func (r *eventLoop) after() {
	if r.b.AfterStep != nil {
		r.b.AfterStep(r.m)
	}
}

func (r *eventLoop) drainInject() (bool, error) {
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

func (r *eventLoop) runInject(fn InjectFunc) (bool, error) {
	if fn == nil {
		return false, nil
	}
	return r.dispatch(fn())
}

func (r *eventLoop) dispatch(acts []core.Action) (bool, error) {
	for _, a := range acts {
		switch a := a.(type) {
		case core.IOCall:
			r.exec.Execute(a)
		case core.RecordFact:
			if r.b.Facts != nil {
				r.b.Facts(a.Fact)
			}
		case core.SetTimer:
			r.setTimer(a)
		case core.CancelTimer:
			r.cancelTimer(a.ID)
		case core.StopForRestart:
			r.after()
			return true, fmt.Errorf("%w: %s", ErrStoppedForRestart, a.Cause)
		case core.ShutdownDone:
			r.after()
			return true, nil
		default:
			r.after()
			return true, fmt.Errorf("controlplane/runloop: unknown action %T", a)
		}
	}
	r.after()
	return false, nil
}

func (r *eventLoop) setTimer(a core.SetTimer) {
	if old := r.timers[a.ID]; old != nil {
		old.Stop()
	}
	id, gen := a.ID, a.Gen
	r.timers[id] = time.AfterFunc(time.Duration(a.After), func() {
		r.in.DeliverControl(core.TimerFired{ID: id, Gen: gen})
	})
}

func (r *eventLoop) cancelTimer(id core.TimerID) {
	if t := r.timers[id]; t != nil {
		t.Stop()
		delete(r.timers, id)
	}
}
