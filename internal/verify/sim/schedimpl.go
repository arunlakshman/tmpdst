package sim

import "manifester/internal/core"

// heapItem is one pending entry. Cancel and DropNode mark it dead; dead items
// are discarded lazily when they reach the top of the heap.
type heapItem struct {
	e    Entry
	dead bool
}

// laneKey names one (lane, time) FIFO group.
type laneKey struct {
	lane Lane
	at   Time
}

// laneGroup is the tie shared by every pending entry of one laneKey, and how
// many such entries are pending (the group is forgotten at zero).
type laneGroup struct {
	tie uint64
	n   int
}

// sched is the Scheduler: a binary min-heap ordered by (At, Tie, Seq).
//
// FIFO within a lane: Schedule always draws a Tie (one draw per call, so the
// draw count never depends on lane history), but if an entry on the same Lane
// at the same At is already pending, the new entry takes that entry's Tie
// instead. Equal (At, Tie) falls through to Seq, which is call order. Across
// lanes the drawn Ties differ, so same-At order between lanes is seeded.
type sched struct {
	rng   CountedRand
	now   Time
	seq   uint64
	heap  []*heapItem
	byID  map[EntryID]*heapItem
	lanes map[laneKey]*laneGroup
	live  int
}

// NewScheduler returns a Scheduler that draws its ties from rng.
func NewScheduler(rng CountedRand) Scheduler { return newSched(rng) }

func newSched(rng CountedRand) *sched {
	return &sched{rng: rng, byID: map[EntryID]*heapItem{}, lanes: map[laneKey]*laneGroup{}}
}

func (s *sched) Now() Time { return s.now }
func (s *sched) Len() int  { return s.live }

func less(a, b *Entry) bool {
	if a.At != b.At {
		return a.At < b.At
	}
	if a.Tie != b.Tie {
		return a.Tie < b.Tie
	}
	return a.Seq < b.Seq
}

func (s *sched) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !less(&s.heap[i].e, &s.heap[p].e) {
			return
		}
		s.heap[i], s.heap[p] = s.heap[p], s.heap[i]
		i = p
	}
}

func (s *sched) down(i int) {
	n := len(s.heap)
	for {
		l, m := 2*i+1, i
		if l < n && less(&s.heap[l].e, &s.heap[m].e) {
			m = l
		}
		if r := l + 1; r < n && less(&s.heap[r].e, &s.heap[m].e) {
			m = r
		}
		if m == i {
			return
		}
		s.heap[i], s.heap[m] = s.heap[m], s.heap[i]
		i = m
	}
}

func (s *sched) Schedule(e Entry) EntryID {
	if e.At < s.now {
		e.At = s.now
	}
	s.seq++
	e.Seq = s.seq
	e.Tie = s.rng.Uint64()
	k := laneKey{e.Lane, e.At}
	if g, ok := s.lanes[k]; ok {
		e.Tie = g.tie
		g.n++
	} else {
		s.lanes[k] = &laneGroup{tie: e.Tie, n: 1}
	}
	it := &heapItem{e: e}
	id := EntryID(e.Seq)
	s.byID[id] = it
	s.heap = append(s.heap, it)
	s.up(len(s.heap) - 1)
	s.live++
	return id
}

// kill retires a live item (it stays in the heap until it surfaces).
func (s *sched) kill(it *heapItem) {
	it.dead = true
	s.live--
	k := laneKey{it.e.Lane, it.e.At}
	if g := s.lanes[k]; g != nil {
		if g.n--; g.n == 0 {
			delete(s.lanes, k)
		}
	}
	delete(s.byID, EntryID(it.e.Seq))
}

func (s *sched) Cancel(id EntryID) {
	if it, ok := s.byID[id]; ok {
		s.kill(it)
	}
}

func (s *sched) DropNode(ref core.NodeInstance) int {
	if ref == (core.NodeInstance{}) {
		return 0
	}
	n := 0
	for _, it := range s.heap { // slice order: deterministic
		if !it.dead && it.e.Target.Engine == "" && it.e.Target.Node == ref {
			s.kill(it)
			n++
		}
	}
	return n
}

// top discards dead items and returns the next live one, or nil.
func (s *sched) top() *heapItem {
	for len(s.heap) > 0 {
		it := s.heap[0]
		if !it.dead {
			return it
		}
		s.removeTop()
	}
	return nil
}

func (s *sched) removeTop() {
	last := len(s.heap) - 1
	s.heap[0] = s.heap[last]
	s.heap[last] = nil
	s.heap = s.heap[:last]
	if last > 0 {
		s.down(0)
	}
}

// Peek reports the time of the next live entry without advancing Now.
func (s *sched) Peek() (Time, bool) {
	if it := s.top(); it != nil {
		return it.e.At, true
	}
	return 0, false
}

func (s *sched) Pop() (Entry, bool) {
	it := s.top()
	if it == nil {
		return Entry{}, false
	}
	s.removeTop()
	s.kill(it) // bookkeeping only; the item is already out of the heap
	s.now = it.e.At
	return it.e, true
}

var _ Scheduler = (*sched)(nil)
