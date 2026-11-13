package core

import (
	"fmt"

	"github.com/manifesterio/mds/dst/machine"
)

// The inbox class (machine.Class) selects which of a node's two inboxes an event goes to
// (docs/flow-control.md sec. 3.1). It is folded into the hash: append-only from here on,
// never reordered.
//
// Control is the class of things the node caused itself (completions of its own actions,
// its own timers, boot). Strict priority; bounded by construction because the state
// machine caps in-flight work.
//
// Data is the class of everything a connection sends, one ordered stream per connection.
// Bounded; real backpressure is PauseReads.
//
// The pick between the two queues is the rule of dst/live.Inbox.Pop in production and of
// dst/kernel in simulation: Control first, then Data, and a queue is first in, first out.

// InboxKindOf is the fixed classification table. Events that must stay ordered
// relative to each other share a queue: ConnClosed is Data so a client's EOF
// can never overtake its last request.
func InboxKindOf(b EventData) machine.Class {
	switch b.(type) {
	case NodeStarted, StopRequest, TimerFired, ConnectDone,
		ObjectDone,
		ControlPlaneDone, ControlPlaneDeliver,
		LiveBrokers, SendDone:
		return machine.Control
	case ConnOpened, BytesReceived, ConnClosed:
		return machine.Data
	default:
		// A new EventData type without a row here is a programming error.
		panic(fmt.Sprintf("core.InboxKindOf: unclassified event %T", b))
	}
}
