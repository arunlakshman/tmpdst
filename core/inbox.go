package core

import "fmt"

// Class selects which of a node's two inboxes an event goes to
// (docs/flow-control.md §3.1). It is folded into the hash: append-only from
// here on, never reordered.
type Class uint8

const (
	// Control: things the node caused itself (completions of its own actions,
	// its own timers, boot). Strict priority; bounded by construction because
	// the state machine caps in-flight work.
	Control Class = iota
	// Data: everything a connection sends, one ordered stream per connection.
	// Bounded; real backpressure is PauseReads.
	Data
)

// ClassOf is the fixed classification table. Events that must stay ordered
// relative to each other share a queue: ConnClosed is Data so a client's EOF
// can never overtake its last request.
func ClassOf(b EventBody) Class {
	switch b.(type) {
	case Started, Stop, TimerFired, DialDone,
		ObjPutDone, ObjGetDone, ObjDeleteDone, ObjListDone,
		CPDone, SendDone:
		return Control
	case ConnOpened, BytesReceived, ConnClosed:
		return Data
	default:
		// A new EventBody type without a row here is a programming error.
		panic(fmt.Sprintf("core.ClassOf: unclassified event %T", b))
	}
}

// Queue is one inbox as seen by an InboxPolicy. Production backs it with a
// channel (non-blocking receive); the simulator backs it with a slice.
type Queue interface {
	Len() int
	// Pop removes and returns the head. ok=false when empty.
	Pop() (body EventBody, ok bool)
}

// InboxPolicy picks the next event for Step. It must be a pure function of the
// two queues' contents and its own counters, and is the SAME code in
// production and simulation (docs/flow-control.md §3.3):
//
//  1. control non-empty → take head of control (always);
//  2. else data non-empty → take head of data;
//  3. else ok=false (the caller sleeps until either queue is non-empty).
//
// Go's select must never decide priority.
type InboxPolicy interface {
	Next(control, data Queue) (body EventBody, ok bool)
}
