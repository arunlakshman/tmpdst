package sim

import "manifester/internal/core"

// Time is global simulation time. Nodes never see it directly; they see their
// own skewed core.Time on each Event.
type Time int64

// Lane is an ordering domain. Entries on one lane at the same Time pop in FIFO
// order; entries on different lanes at the same Time are ordered by Tie, a
// seeded random tie-break (decision C). One lane per connection direction, per engine
// operation stream, per node timer set.
type Lane uint64

// EntryID identifies a scheduled entry so it can be cancelled.
type EntryID uint64

// Target says who consumes an entry: a node (by incarnation) or an engine.
type Target struct {
	Node   core.NodeInstance // set when delivering to a node's StateMachine
	Engine string            // set when delivering an internal event to an engine
}

// EntryKind says what an entry is. Append-only (hashed). Crash, Restart, Stop,
// Phase and Quiesce are addressed to the simulator itself.
type EntryKind uint8

// EntryKind values.
const (
	// EntryDeliver is an event to a node.
	EntryDeliver EntryKind = iota // event to a node
	// EntryEngineInternal is an engine-private body to an engine.
	EntryEngineInternal // engine-private body to an engine
	EntryCrash
	EntryRestart
	EntryStop
	EntryPhase
	EntryQuiesce
)

// Entry is one item in the global queue. Pop order is (At, Tie, Seq).
type Entry struct {
	At     Time
	Lane   Lane
	Kind   EntryKind
	Target Target
	Body   any    // core.EventData for nodes; engine-private plain data for engines
	Tie    uint64 // drawn by Schedule: exactly one draw per call
	Seq    uint64 // assigned by Schedule, in call order
}

// Scheduler owns simulation time and the global order of everything.
type Scheduler interface {
	Now() Time
	// Schedule enqueues; scheduling into the past clamps to Now. It draws Tie
	// exactly once, and entries on one Lane at the same At stay FIFO whatever
	// their Ties.
	Schedule(e Entry) EntryID
	Cancel(id EntryID)
	// DropNode removes every pending entry addressed to that incarnation (crash)
	// and returns how many. Engine-internal entries are not addressed to a node
	// and survive.
	DropNode(ref core.NodeInstance) int
	// Pop removes and returns the next entry, advancing Now. ok=false when empty.
	Pop() (e Entry, ok bool)
	Len() int
}
