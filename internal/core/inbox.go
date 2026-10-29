package core

import "fmt"

// InboxKind selects which of a node's two inboxes an event goes to
// (docs/flow-control.md sec. 3.1). It is folded into the hash: append-only from
// here on, never reordered.
type InboxKind uint8

const (
	// Control is the class of things the node caused itself (completions of its own actions,
	// its own timers, boot). Strict priority; bounded by construction because
	// the state machine caps in-flight work.
	Control InboxKind = iota
	// Data is the class of everything a connection sends, one ordered stream per connection.
	// Bounded; real backpressure is PauseReads.
	Data
)

// InboxKindOf is the fixed classification table. Events that must stay ordered
// relative to each other share a queue: ConnClosed is Data so a client's EOF
// can never overtake its last request.
func InboxKindOf(b EventData) InboxKind {
	switch b.(type) {
	case NodeStarted, StopRequest, TimerFired, ConnectDone,
		ObjectPutDone, ObjectGetDone, ObjectDeleteDone, ObjectListDone,
		ControlPlaneDone, ControlPlaneDeliver,
		LogAppendDone, LogReadDone, LogGetEndDone, LogDeleteOldDone,
		SendDone:
		return Control
	case ConnOpened, BytesReceived, ConnClosed:
		return Data
	default:
		// A new EventData type without a row here is a programming error.
		panic(fmt.Sprintf("core.InboxKindOf: unclassified event %T", b))
	}
}

// Inbox is one inbox as seen by an InboxPicker. Production backs it with a
// channel (non-blocking receive); the simulator backs it with a slice.
type Inbox interface {
	Len() int
	// Pop removes and returns the head. ok=false when empty.
	Pop() (body EventData, ok bool)
}

// InboxPicker picks the next event for Step. It must be a pure function of the
// two queues' contents and its own counters, and is the SAME code in
// production and simulation (docs/flow-control.md sec. 3.3):
//
//  1. control non-empty -> take head of control (always);
//  2. else data non-empty -> take head of data;
//  3. else ok=false (the caller sleeps until either queue is non-empty).
//
// Go's select must never decide priority.
type InboxPicker interface {
	Next(control, data Inbox) (body EventData, ok bool)
}
