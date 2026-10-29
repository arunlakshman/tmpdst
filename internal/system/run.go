package system

import (
	"context"
	"errors"
	"fmt"
	"time"

	"manifester/internal/core"
)

var (
	// ErrStoppedForRestart is returned when the run was stopped by a tripped guard.
	ErrStoppedForRestart = errors.New("tripped")
	// ErrStopped is returned when Run ends because its context was done.
	ErrStopped = errors.New("stopped")
	// ErrKilledByRuntime is returned when a graceful stop missed its deadline.
	ErrKilledByRuntime = errors.New("killed") // graceful stop missed its deadline
)

// Run delivers NodeStarted, then loops: core.ControlFirstPicker picks, At/Wall are
// stamped at dequeue, Step, actions dispatched in slice order. Returns
// ErrStoppedForRestart (wrapped with the cause) on StopForRestart, nil on ShutdownDone, ErrStopped on ctx
// done. When b.Shutdown fires, Run delivers core.StopRequest{Deadline: Mono+StopGrace}
// once; if the machine has not emitted ShutdownDone by then, Run returns ErrKilledByRuntime and
// the supervisor kills the process. It closes b.Inbox on return so late
// completions are dropped. This goroutine alone owns the machine.
func Run(ctx context.Context, b Dependencies, exec Executor, f core.MachineFactory, self core.NodeInstance) error {
	in := b.Inbox
	if in == nil {
		// A private inbox would be unreachable by the Executor and readers.
		return errors.New("sys.Run: Bundle.Inbox is nil")
	}
	defer in.Close()
	r := &eventLoop{b: b, in: in, exec: exec, timers: map[core.TimerID]*time.Timer{}}
	defer func() {
		for _, t := range r.timers {
			t.Stop()
		}
	}()
	m := f(b.Env, self)

	step := func(body core.EventData) (bool, error) {
		acts := m.Step(core.Event{At: b.Clock.Mono(), Wall: b.Clock.Wall(), Body: body})
		return r.dispatch(acts)
	}
	if done, err := step(core.NodeStarted{Self: self}); done {
		return err
	}
	var policy core.InboxPicker = core.ControlFirstPicker{}
	ctrl, data := in.Control(), in.Data()
	shutdown := b.Shutdown // nil once fired: StopRequest is delivered exactly once
	var kill <-chan time.Time
	for {
		if ctx.Err() != nil { // checked every turn so a busy inbox cannot starve cancel
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
		select { // checked every turn so a busy inbox cannot starve the deadline
		case <-kill:
			return ErrKilledByRuntime
		default:
		}
		body, ok := policy.Next(ctrl, data)
		if !ok {
			select { // wake only; priority is the policy's
			case <-ctx.Done():
				return ErrStopped
			case <-shutdown:
			case <-kill:
				return ErrKilledByRuntime
			case <-in.wake:
			}
			continue
		}
		if done, err := step(body); done {
			return err
		}
	}
}

type eventLoop struct {
	b      Dependencies
	in     *Inbox
	exec   Executor
	timers map[core.TimerID]*time.Timer // loop goroutine only
}

// dispatch runs one Step's actions in order; done reports StopForRestart or ShutdownDone.
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
		case core.CloseConn:
			_ = r.b.Connections.Close(a.Conn, a.Abort)
		case core.PauseReads:
			r.b.Connections.PauseReads(a.Conn)
		case core.ResumeReads:
			r.b.Connections.ResumeReads(a.Conn)
		case core.StopForRestart:
			return true, fmt.Errorf("%w: %s", ErrStoppedForRestart, a.Cause)
		case core.ShutdownDone:
			return true, nil
		default:
			return true, fmt.Errorf("sys: unknown action %T", a)
		}
	}
	return false, nil
}

func (r *eventLoop) setTimer(a core.SetTimer) {
	if old := r.timers[a.ID]; old != nil {
		old.Stop() // stale gen would be ignored anyway
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
