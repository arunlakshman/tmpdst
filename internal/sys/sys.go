// Package sys is the production seam: narrow interfaces over real I/O that the
// production Executor calls from goroutines. SDK and HTTP types never leak past
// an adapter. The simulator does NOT implement these; it implements sim.Engine
// over the same Action/Event vocabulary (docs/lessons.md, section 0).
package sys

import (
	"context"

	"manifester/internal/core"
)

// Clock is read only by the runtime, at inbox dequeue, to stamp Event.At/Wall.
type Clock interface {
	Mono() core.NodeTime
	Wall() core.RealTime
}

// ObjectStore is the S3 adapter.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, ref core.SegmentRef) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// ControlPlane is the HTTP client adapter for the control plane.
type ControlPlane interface {
	Call(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneResponse, error)
}

// Connections writes to and closes accepted connections. Writes on one connection
// are applied in call order (one writer goroutine per connection).
type Connections interface {
	Write(conn core.ConnID, data []byte) error
	Close(conn core.ConnID, abort bool) error
	// PauseReads parks the connection's reader goroutine; ResumeReads wakes it.
	PauseReads(conn core.ConnID)
	ResumeReads(conn core.ConnID)
}

// ErrorClassifier is the single place SDK/HTTP/net errors become plain data
// (lesson 1). Getting Outcome right here is what makes acks honest.
type ErrorClassifier interface {
	// Classify reduces err to plain data. It logs err; the core only sees the
	// code. The Outcome depends on which operation failed, not only on err.
	Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome)
}

// InboxWriter is the node's pair of inboxes (docs/flow-control.md sec. 3). Callers route
// with core.InboxKindOf. The loop picks with core.InboxPicker and stamps At/Wall
// when it dequeues.
type InboxWriter interface {
	// DeliverControl is used by timer callbacks and I/O completion goroutines.
	// It never blocks: the control queue is bounded by construction.
	DeliverControl(body core.EventData)
	// DeliverData is used by connection reader goroutines. It blocks when the
	// data queue is full (a safety net; real backpressure is PauseReads).
	DeliverData(body core.EventData)
	// Closed is closed when the incarnation ends. Later deliveries are dropped,
	// never block, and never reach the next incarnation.
	Closed() <-chan struct{}
}

// Executor runs I/O only. The loop handles RecordFact, timers, StopForRestart, ShutdownDone and
// returns an error on an unknown action. Exactly one IOResult per IOCall, with the
// same RequestID, delivered to the InboxWriter and never on the caller's goroutine.
// Execute MUST NOT block: it hands work to goroutines/queues.
type Executor interface {
	Execute(a core.IOCall)
}

// Dependencies is everything nondeterministic a production broker process needs.
type Dependencies struct {
	Clock        Clock
	Objects      ObjectStore
	ControlPlane ControlPlane
	Connections  Connections
	Classifier   ErrorClassifier
	Env          core.Environment // crypto-seeded Rand, inert FaultInjector, logging Assertions
	Inbox        *Inbox           // shared with the Executor and readers; Run closes it on return
	Facts        func(core.Fact)  // optional: receives RecordFact facts (tracing)
	Shutdown     <-chan struct{}  // optional: closing it asks Run for a graceful stop (see StopOnSignal)
	StopGrace    core.Duration    // time the machine gets after Shutdown to emit ShutdownDone
}
