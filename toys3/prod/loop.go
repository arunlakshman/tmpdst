package prod

import (
	"context"
	"sync"
	"time"

	"manifester/toys3/core"
)

// Loop owns one core.Machine and is the only goroutine that calls Step.
type Loop struct {
	cfg     core.Config
	factory core.Factory
	exec    Executor
	inbox   chan core.EventBody
	start   time.Time // At = time.Since(start), monotonic

	mu      sync.Mutex
	nextReq core.ClientReq
	waiters map[core.ClientReq]chan core.Reply // one per in-flight HTTP request
}

// NewLoop returns a loop for one process lifetime. inboxSize bounds the
// number of queued events (backpressure on HTTP handlers and completions).
func NewLoop(cfg core.Config, factory core.Factory, exec Executor, inboxSize int) *Loop {
	return &Loop{
		cfg:     cfg,
		factory: factory,
		exec:    exec,
		inbox:   make(chan core.EventBody, inboxSize),
		waiters: map[core.ClientReq]chan core.Reply{},
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
// It returns when ctx is cancelled or on Halt.
func (l *Loop) Run(ctx context.Context) error {
	panic("toys3/prod: Loop.Run unimplemented (phase 2)")
}

// Submit sends op to the server as a core.Request with a fresh ClientReq and
// waits for its Reply. If ctx ends first the reply is discarded on arrival;
// the operation itself may still complete (the client just never learns).
func (l *Loop) Submit(ctx context.Context, op core.Op) (core.Reply, error) {
	panic("toys3/prod: Loop.Submit unimplemented (phase 2)")
}
