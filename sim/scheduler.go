package sim

import "manifester/core"

// Time is global simulation time. Nodes never see it directly; they see their
// own skewed core.Time on each Event.
type Time int64

// Lane is an ordering domain. Entries on one lane at the same Time pop in FIFO
// order; entries on different lanes at the same Time are ordered by a seeded
// random tie-break (decision C). One lane per connection direction, per engine
// operation stream, per node timer set.
type Lane uint64

// EntryID identifies a scheduled entry so it can be cancelled.
type EntryID uint64

// Target says who consumes an entry: a node (by incarnation) or an engine.
type Target struct {
	Node   core.NodeRef // set when delivering to a node's StateMachine
	Engine string       // set when delivering an internal event to an engine
}

// Entry is one item in the global queue.
type Entry struct {
	At     Time
	Lane   Lane
	Target Target
	Body   any // core.EventBody for nodes; engine-private plain data for engines
}

// Scheduler owns simulation time and the global order of everything.
type Scheduler interface {
	Now() Time
	// Schedule enqueues; scheduling into the past clamps to Now.
	Schedule(e Entry) EntryID
	Cancel(id EntryID)
	// DropNode removes every pending entry addressed to that incarnation (crash).
	DropNode(ref core.NodeRef)
	// Pop removes and returns the next entry, advancing Now. ok=false when empty.
	Pop() (e Entry, ok bool)
	Len() int
}
