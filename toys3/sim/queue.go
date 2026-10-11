package sim

import "manifester/toys3/core"

// Incarnation numbers server boots: 1 for the first, +1 after each crash.
// Every server-bound entry carries the incarnation it was scheduled for;
// entries for a dead incarnation are dropped on Pop (a crashed process never
// sees a completion meant for its previous life).
type Incarnation uint64

// EntryKind says what an Entry does when popped.
type EntryKind uint8

const (
	KindDeliver    EntryKind = iota // deliver Event to the server (Inc must be current)
	KindFSLand                      // a filesystem action takes effect in the FakeFS
	KindClientWake                  // an idle client issues its next request
	KindCrash                       // kill the server now
	KindRestart                     // boot a new incarnation and deliver Started
)

// Entry is one item in the global queue. Plain data, like events.
type Entry struct {
	At   core.Time
	Tie  uint64 // drawn from the run's Rand at Schedule time; orders same-At entries
	Seq  uint64 // assigned by Schedule; final tie-break, makes the order total
	Kind EntryKind
	Inc  Incarnation // for KindDeliver and KindFSLand; 0 otherwise

	Event  core.EventBody // KindDeliver
	Action core.Action    // KindFSLand: the FS action to apply
	Plan   FSPlan         // KindFSLand: fault and latency decided at issue time
	Client int            // KindClientWake
}

// Queue is the global priority queue ordered by (At, Tie, Seq): time first,
// then a seeded random tie-break so same-instant events are explored in
// different orders across seeds, then insertion order so the order is total.
// A binary heap over a slice is enough.
type Queue struct {
	items []Entry
	seq   uint64
}

// Schedule enqueues e, assigning Tie (from r) and Seq. Scheduling in the past
// clamps At to now.
func (q *Queue) Schedule(r *Rand, now core.Time, e Entry) {
	panic("toys3/sim: Queue.Schedule unimplemented (phase 2)")
}

// Pop removes and returns the earliest entry; ok is false when empty.
func (q *Queue) Pop() (e Entry, ok bool) {
	panic("toys3/sim: Queue.Pop unimplemented (phase 2)")
}

// DropIncarnation removes every entry addressed to inc (KindDeliver and
// KindFSLand). Called on crash: un-landed FS operations never happen and
// pending completions are lost.
func (q *Queue) DropIncarnation(inc Incarnation) {
	panic("toys3/sim: Queue.DropIncarnation unimplemented (phase 2)")
}

// Len is the number of pending entries.
func (q *Queue) Len() int { return len(q.items) }
