// Package system is the production seam: narrow interfaces over real I/O that the
// production Executor calls from goroutines. SDK and HTTP types never leak past
// an adapter. The simulator does NOT implement these; it implements simulator.Engine
// over the same Action/Event vocabulary (docs/lessons.md, section 0). The production
// loop is dst/live; this package has its Route function.
package system

import (
	"context"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectStore is the S3 adapter. The interface is defined in shared/objectstore.
type ObjectStore = objectstore.Store

// ControlPlane is the HTTP client adapter for the control plane.
type ControlPlane interface {
	Call(ctx context.Context, operation contract.Operation) (contract.Response, error)
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

// InboxWriter is where completions and bytes go. It is the Deliver side of a
// live.Inbox[core.EventData]: the caller picks the queue (docs/flow-control.md sec. 3).
type InboxWriter interface {
	// Deliver queues body in the queue of class c. Timer callbacks and I/O
	// completion goroutines use machine.Control, which never blocks (the queue is
	// bounded by construction). Connection reader goroutines use machine.Data,
	// which blocks when the data queue is full (a safety net; real backpressure is
	// PauseReads).
	Deliver(c machine.Class, body core.EventData)
	// Closed is closed when the incarnation ends. Later deliveries are dropped,
	// never block, and never reach the next incarnation.
	Closed() <-chan struct{}
}

var _ InboxWriter = (*live.Inbox[core.EventData])(nil)

// Executor runs I/O only. The route function (Route) handles RecordFact, timers,
// StopForRestart, ShutdownDone and the connection actions, and returns an error on an
// unknown action. Exactly one IOResult per IOCall, with the same RequestID, delivered
// to the InboxWriter and never on the caller's goroutine. Execute MUST NOT block: it
// hands work to goroutines/queues.
type Executor interface {
	Execute(a core.IOCall)
}
