package kernel

import (
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// entryKind says what a queue entry is. The number is folded into the trace
// hash, so the values are frozen (they are the numbers of the old broker
// kernel; 4 and 6 are no longer used).
type entryKind uint8

const (
	kindDeliver     entryKind = 0 // an input for a node, or a wake-up of a node
	kindServiceStep entryKind = 1 // a ServiceContext.Later step
	kindCrash       entryKind = 2 // the kill at the end of a graceful stop
	kindRestart     entryKind = 3 // the next boot of a node
	kindPhase       entryKind = 5 // a phase change from SchedulePhases
	kindHost        entryKind = 7 // a host task (World.At, World.After)
)

// laneKey names one (lane, time) FIFO group.
type laneKey struct {
	lane Lane
	at   Time
}

// laneGroup is the tie shared by the pending entries of one laneKey, and how
// many of them are pending. The group is forgotten at zero.
type laneGroup struct {
	tie uint64
	n   int
}

// queue is the one event queue of a run: a binary min-heap ordered by (at,
// tie, seq).
//
// add draws exactly one tie per entry, so the number of draws never depends
// on lane history. If a pending entry has the same lane (not NoLane) and the
// same time, the new entry takes that entry's tie instead. Equal (at, tie)
// falls through to seq, the order of adding. So one lane at one time is FIFO,
// and different lanes at one time are in seeded random order. NoLane entries
// never share a tie.
type queue struct {
	rand  *rng.Rand
	now   Time
	seq   uint64
	heap  []*entry
	byID  map[EntryID]*entry
	lanes map[laneKey]*laneGroup
	live  int
}

func newQueue(r *rng.Rand) *queue {
	return &queue{rand: r, byID: map[EntryID]*entry{}, lanes: map[laneKey]*laneGroup{}}
}

func earlier(a, b *entry) bool {
	if a.at != b.at {
		return a.at < b.at
	}
	if a.tie != b.tie {
		return a.tie < b.tie
	}
	return a.seq < b.seq
}

func (q *queue) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !earlier(q.heap[i], q.heap[p]) {
			return
		}
		q.heap[i], q.heap[p] = q.heap[p], q.heap[i]
		i = p
	}
}

func (q *queue) down(i int) {
	n := len(q.heap)
	for {
		l, m := 2*i+1, i
		if l < n && earlier(q.heap[l], q.heap[m]) {
			m = l
		}
		if r := l + 1; r < n && earlier(q.heap[r], q.heap[m]) {
			m = r
		}
		if m == i {
			return
		}
		q.heap[i], q.heap[m] = q.heap[m], q.heap[i]
		i = m
	}
}

// add queues e. A time in the past becomes now.
func (q *queue) add(e *entry) EntryID {
	e.at = max(e.at, q.now)
	q.seq++
	e.seq = q.seq
	e.tie = q.rand.Uint64()
	if e.lane != NoLane {
		k := laneKey{e.lane, e.at}
		if g, ok := q.lanes[k]; ok {
			e.tie = g.tie
			g.n++
		} else {
			q.lanes[k] = &laneGroup{tie: e.tie, n: 1}
		}
	}
	id := EntryID(e.seq)
	q.byID[id] = e
	q.heap = append(q.heap, e)
	q.up(len(q.heap) - 1)
	q.live++
	return id
}

// kill retires a live entry. It stays in the heap until it reaches the top.
func (q *queue) kill(e *entry) {
	e.dead = true
	q.live--
	if e.lane != NoLane {
		k := laneKey{e.lane, e.at}
		if g := q.lanes[k]; g != nil {
			if g.n--; g.n == 0 {
				delete(q.lanes, k)
			}
		}
	}
	delete(q.byID, EntryID(e.seq))
}

// cancel retires the entry id, if it is still pending.
func (q *queue) cancel(id EntryID) {
	if e, ok := q.byID[id]; ok {
		q.kill(e)
	}
}

// dropIncarnation retires every pending entry addressed to incarnation inc of
// node idx, and returns how many. Entries for whatever incarnation is up (inc
// zero) and entries for services are kept.
func (q *queue) dropIncarnation(idx int, inc machine.BootNumber) int {
	if inc == (machine.BootNumber{}) {
		return 0
	}
	n := 0
	for _, e := range q.heap { // slice order: the same every run
		if !e.dead && e.node == idx && e.inc == inc {
			q.kill(e)
			n++
		}
	}
	return n
}

// top throws away dead entries and returns the next live one, or nil.
func (q *queue) top() *entry {
	for len(q.heap) > 0 {
		e := q.heap[0]
		if !e.dead {
			return e
		}
		q.removeTop()
	}
	return nil
}

func (q *queue) removeTop() {
	last := len(q.heap) - 1
	q.heap[0] = q.heap[last]
	q.heap[last] = nil
	q.heap = q.heap[:last]
	if last > 0 {
		q.down(0)
	}
}

// peek reports the time of the next live entry, without moving time.
func (q *queue) peek() (Time, bool) {
	if e := q.top(); e != nil {
		return e.at, true
	}
	return 0, false
}

// pop removes and returns the next live entry, and moves time to it.
func (q *queue) pop() (*entry, bool) {
	e := q.top()
	if e == nil {
		return nil, false
	}
	q.removeTop()
	q.kill(e) // bookkeeping only; the entry is already out of the heap
	q.now = e.at
	return e, true
}

// size is the number of live entries.
func (q *queue) size() int { return q.live }
