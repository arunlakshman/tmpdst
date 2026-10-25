package sys

import (
	"context"
	"errors"
	"fmt"
	"time"

	"manifester/core"
)

var (
	// ErrTripped is returned when the run was stopped by a tripped guard.
	ErrTripped = errors.New("tripped")
	// ErrStopped is returned when Run ends because its context was done.
	ErrStopped = errors.New("stopped")
	// ErrKilled is returned when a graceful stop missed its deadline.
	ErrKilled = errors.New("killed") // graceful stop missed its deadline
)

// Run delivers Started, then loops: core.ControlFirst picks, At/Wall are
// stamped at dequeue, Step, actions dispatched in slice order. Returns
// ErrTripped (wrapped with the cause) on Trip, nil on Exit, ErrStopped on ctx
// done. When b.Shutdown fires, Run delivers core.Stop{Deadline: Mono+StopGrace}
// once; if the machine has not emitted Exit by then, Run returns ErrKilled and
// the supervisor kills the process. It closes b.Inbox on return so late
// completions are dropped. This goroutine alone owns the machine.
func Run(ctx context.Context, b Bundle, exec Executor, f core.Factory, self core.NodeRef) error {
	in := b.Inbox
	if in == nil {
		// A private inbox would be unreachable by the Executor and readers.
		return errors.New("sys.Run: Bundle.Inbox is nil")
	}
	defer in.Close()
	r := &runner{b: b, in: in, exec: exec, timers: map[core.TimerID]*time.Timer{}}
	defer func() {
		for _, t := range r.timers {
			t.Stop()
		}
	}()
	m := f(b.Env, self)

	step := func(body core.EventBody) (bool, error) {
		acts := m.Step(core.Event{At: b.Clock.Mono(), Wall: b.Clock.Wall(), Body: body})
		return r.dispatch(acts)
	}
	if done, err := step(core.Started{Self: self}); done {
		return err
	}
	var policy core.InboxPolicy = core.ControlFirst{}
	ctrl, data := in.Control(), in.Data()
	shutdown := b.Shutdown // nil once fired: Stop is delivered exactly once
	var kill <-chan time.Time
	for {
		if ctx.Err() != nil { // checked every turn so a busy inbox cannot starve cancel
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
		select { // checked every turn so a busy inbox cannot starve the deadline
		case <-kill:
			return ErrKilled
		default:
		}
		body, ok := policy.Next(ctrl, data)
		if !ok {
			select { // wake only; priority is the policy's
			case <-ctx.Done():
				return ErrStopped
			case <-shutdown:
			case <-kill:
				return ErrKilled
			case <-in.wake:
			}
			continue
		}
		if done, err := step(body); done {
			return err
		}
	}
}

type runner struct {
	b      Bundle
	in     *Inbox
	exec   Executor
	timers map[core.TimerID]*time.Timer // loop goroutine only
}

// dispatch runs one Step's actions in order; done reports Trip or Exit.
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
		case core.CloseConn:
			_ = r.b.Conns.Close(a.Conn, a.Abort)
		case core.PauseReads:
			r.b.Conns.PauseReads(a.Conn)
		case core.ResumeReads:
			r.b.Conns.ResumeReads(a.Conn)
		case core.Trip:
			return true, fmt.Errorf("%w: %s", ErrTripped, a.Cause)
		case core.Exit:
			return true, nil
		default:
			return true, fmt.Errorf("sys: unknown action %T", a)
		}
	}
	return false, nil
}

func (r *runner) setTimer(a core.SetTimer) {
	if old := r.timers[a.ID]; old != nil {
		old.Stop() // stale gen would be ignored anyway
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
