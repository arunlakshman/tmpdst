// Package sys is the production seam: narrow interfaces over real I/O that the
// production Executor calls from goroutines. SDK and HTTP types never leak past
// an adapter. The simulator does NOT implement these; it implements sim.Engine
// over the same Action/Event vocabulary (docs/lessons.md, section 0).
package sys

import (
	"context"

	"manifester/core"
)

// Clock is read only by the runtime, at inbox dequeue, to stamp Event.At/Wall.
type Clock interface {
	Mono() core.Time
	Wall() core.WallTime
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
	Call(ctx context.Context, op core.CPOp) (core.CPResp, error)
}

// Conns writes to and closes accepted connections. Writes on one connection
// are applied in call order (one writer goroutine per connection).
type Conns interface {
	Write(conn core.ConnID, data []byte) error
	Close(conn core.ConnID, abort bool) error
	// PauseReads parks the connection's reader goroutine; ResumeReads wakes it.
	PauseReads(conn core.ConnID)
	ResumeReads(conn core.ConnID)
}

// Classifier is the single place SDK/HTTP/net errors become plain data
// (lesson 1). Getting Outcome right here is what makes acks honest.
type Classifier interface {
	Classify(err error) (core.ErrCode, core.Outcome)
}

// Sink is the node's pair of inboxes (docs/flow-control.md §3). Callers route
// with core.ClassOf. The loop picks with core.InboxPolicy and stamps At/Wall
// when it dequeues.
type Sink interface {
	// DeliverControl is used by timer callbacks and I/O completion goroutines.
	// It never blocks: the control queue is bounded by construction.
	DeliverControl(body core.EventBody)
	// DeliverData is used by connection reader goroutines. It blocks when the
	// data queue is full (a safety net; real backpressure is PauseReads).
	DeliverData(body core.EventBody)
}

// Executor turns Actions into I/O. Execute MUST NOT block the loop goroutine:
// it hands work to goroutines/queues whose completions go to the Sink.
type Executor interface {
	Execute(a core.Action)
}

// Bundle is everything nondeterministic a production broker process needs.
type Bundle struct {
	Clock        Clock
	Objects      ObjectStore
	ControlPlane ControlPlane
	Conns        Conns
	Classifier   Classifier
	Env          core.Env // crypto-seeded Rand, inert Buggify, logging Assertions
}
