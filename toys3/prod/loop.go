package prod

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"manifester/toys3/control"
)

// ErrHalted wraps the reason of a control.Halt returned by Run.
var ErrHalted = errors.New("toys3: server halted")

// ErrStopped is returned by Submit when the loop is not (or no longer) running.
var ErrStopped = errors.New("toys3: loop stopped")

// Loop owns one control.Machine and is the only goroutine that calls Step.
type Loop struct {
	cfg     control.Config
	factory control.Factory
	exec    Executor
	inbox   chan control.EventBody
	start   time.Time     // At = time.Since(start), monotonic
	stopped chan struct{} // closed when Run returns

	// Log receives facts and lifecycle lines; nil means log.Default().
	Log *log.Logger

	mu      sync.Mutex
	nextReq control.ClientReq
	waiters map[control.ClientReq]chan control.Reply // one per in-flight HTTP request
}

// NewLoop returns a loop for one process lifetime. inboxSize bounds the
// number of queued events (backpressure on HTTP handlers and completions).
func NewLoop(cfg control.Config, factory control.Factory, exec Executor, inboxSize int) *Loop {
	return &Loop{
		cfg:     cfg,
		factory: factory,
		exec:    exec,
		inbox:   make(chan control.EventBody, inboxSize),
		start:   time.Now(),
		stopped: make(chan struct{}),
		waiters: map[control.ClientReq]chan control.Reply{},
	}
}

// Run builds the machine, delivers Started, and then steps every inbox event
// in order, dispatching the actions:
//
//	Reply     → the waiter registered by Submit (dropped if it gave up)
//	FS*       → exec.Execute, whose done callback posts to the inbox
//	Emit      → structured log line
//	Halt      → return an error; the process exits and a supervisor restarts it
//
// It returns when ctx is cancelled or on Halt. Run may be called once.
func (l *Loop) Run(ctx context.Context) error {
	defer close(l.stopped)
	m := l.factory(Env{}, l.cfg)
	var ev control.EventBody = control.Started{}
	for {
		at := control.Time(time.Since(l.start)) // monotonic clock reading
		if err := l.dispatch(m.Step(control.Event{At: at, Body: ev})); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev = <-l.inbox:
		}
	}
}

// dispatch executes the actions of one Step in order.
func (l *Loop) dispatch(acts []control.Action) error {
	for _, a := range acts {
		switch a := a.(type) {
		case control.Reply:
			l.mu.Lock()
			ch := l.waiters[a.Client]
			delete(l.waiters, a.Client)
			l.mu.Unlock()
			if ch != nil {
				ch <- a // buffered(1); exactly one reply per request
			}
		case control.Emit:
			l.logf("toys3: fact %T %+v", a.Fact, a.Fact)
		case control.Halt:
			l.logf("toys3: HALT: %s", a.Reason)
			return fmt.Errorf("%w: %s", ErrHalted, a.Reason)
		default:
			l.exec.Execute(a, l.post)
		}
	}
	return nil
}

// post delivers a completion to the inbox, or drops it if Run has returned
// (a completion addressed to a dead incarnation).
func (l *Loop) post(b control.EventBody) {
	select {
	case l.inbox <- b:
	case <-l.stopped:
	}
}

func (l *Loop) logf(format string, args ...any) {
	lg := l.Log
	if lg == nil {
		lg = log.Default()
	}
	lg.Printf(format, args...)
}

// Submit sends op to the server as a control.Request with a fresh ClientReq and
// waits for its Reply. If ctx ends first the reply is discarded on arrival;
// the operation itself may still complete (the client just never learns).
func (l *Loop) Submit(ctx context.Context, op control.Op) (control.Reply, error) {
	ch := make(chan control.Reply, 1)
	l.mu.Lock()
	l.nextReq++
	id := l.nextReq
	l.waiters[id] = ch
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.waiters, id)
		l.mu.Unlock()
	}()

	select {
	case l.inbox <- control.Request{Client: id, Op: op}:
	case <-ctx.Done():
		return control.Reply{}, ctx.Err()
	case <-l.stopped:
		return control.Reply{}, ErrStopped
	}
	select {
	case rep := <-ch:
		return rep, nil
	case <-ctx.Done():
		return control.Reply{}, ctx.Err()
	case <-l.stopped:
		select { // a Reply dispatched just before a Halt still counts
		case rep := <-ch:
			return rep, nil
		default:
			return control.Reply{}, ErrStopped
		}
	}
}
