package sim

import "manifester/toys3/control"

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
	KindQuiesce                     // clients stop issuing until every request has replied
	KindQuiet                       // Chaos ends: no new faults or crashes (QuietTail begins)
	KindTailEnd                     // QuietTail ends: clients stop for good; the queue drains
)

func (k EntryKind) String() string {
	switch k {
	case KindDeliver:
		return "deliver"
	case KindFSLand:
		return "fs-land"
	case KindClientWake:
		return "client-wake"
	case KindCrash:
		return "crash"
	case KindRestart:
		return "restart"
	case KindQuiesce:
		return "quiesce"
	case KindQuiet:
		return "quiet"
	case KindTailEnd:
		return "tail-end"
	}
	return "kind(?)"
}

// Entry is one item in the global queue. Plain data, like events.
type Entry struct {
	At   control.Time
	Tie  uint64 // drawn from the run's Rand at Schedule time; orders same-At entries
	Seq  uint64 // assigned by Schedule; final tie-break, makes the order total
	Kind EntryKind
	Inc  Incarnation // for KindDeliver and KindFSLand; 0 otherwise

	Event  control.EventBody // KindDeliver
	Action control.Action    // KindFSLand: the FS action to apply
	Plan   FSPlan            // KindFSLand: fault and latency decided at issue time
	Client int               // KindClientWake
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
func (q *Queue) Schedule(r *Rand, now control.Time, e Entry) {
	if e.At < now {
		e.At = now
	}
	e.Tie = r.Uint64()
	q.seq++
	e.Seq = q.seq
	q.items = append(q.items, e)
	q.up(len(q.items) - 1)
}

// Pop removes and returns the earliest entry; ok is false when empty.
func (q *Queue) Pop() (e Entry, ok bool) {
	n := len(q.items)
	if n == 0 {
		return Entry{}, false
	}
	e = q.items[0]
	q.items[0] = q.items[n-1]
	q.items[n-1] = Entry{}
	q.items = q.items[:n-1]
	if len(q.items) > 0 {
		q.down(0)
	}
	return e, true
}

// DropIncarnation removes every entry addressed to inc (KindDeliver and
// KindFSLand). Called on crash: un-landed FS operations never happen and
// pending completions are lost. It returns how many entries were dropped.
func (q *Queue) DropIncarnation(inc Incarnation) int {
	kept := q.items[:0]
	dropped := 0
	for _, e := range q.items {
		if (e.Kind == KindDeliver || e.Kind == KindFSLand) && e.Inc == inc {
			dropped++
			continue
		}
		kept = append(kept, e)
	}
	for i := len(kept); i < len(q.items); i++ {
		q.items[i] = Entry{}
	}
	q.items = kept
	for i := len(q.items)/2 - 1; i >= 0; i-- {
		q.down(i)
	}
	return dropped
}

// Len is the number of pending entries.
func (q *Queue) Len() int { return len(q.items) }

func (q *Queue) less(i, j int) bool {
	a, b := &q.items[i], &q.items[j]
	if a.At != b.At {
		return a.At < b.At
	}
	if a.Tie != b.Tie {
		return a.Tie < b.Tie
	}
	return a.Seq < b.Seq
}

func (q *Queue) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !q.less(i, p) {
			return
		}
		q.items[i], q.items[p] = q.items[p], q.items[i]
		i = p
	}
}

func (q *Queue) down(i int) {
	n := len(q.items)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		m := l
		if r := l + 1; r < n && q.less(r, l) {
			m = r
		}
		if !q.less(m, i) {
			return
		}
		q.items[i], q.items[m] = q.items[m], q.items[i]
		i = m
	}
}
