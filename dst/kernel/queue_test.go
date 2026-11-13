package kernel

import (
	"testing"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// Ports of the broker scheduler tests (sched_test.go). The queue is internal,
// so these tests are in the package.

// serviceEntry is a queue entry for a service.
func serviceEntry(at Time, lane Lane) *entry {
	return &entry{at: at, lane: lane, kind: kindServiceStep, node: -1, service: "e"}
}

// drainTags pops every entry and returns the tags given to addTagged, in order.
func drainTags(q *queue, tags map[*entry]int) []int {
	var out []int
	for {
		e, ok := q.pop()
		if !ok {
			return out
		}
		out = append(out, tags[e])
	}
}

func addTagged(q *queue, tags map[*entry]int, at Time, lane Lane, tag int) EntryID {
	e := serviceEntry(at, lane)
	tags[e] = tag
	return q.add(e)
}

func TestQueueOrderAndClamp(t *testing.T) {
	r := rng.New(1)
	q := newQueue(r)
	tags := map[*entry]int{}
	addTagged(q, tags, 30, 1, 3)
	addTagged(q, tags, 10, 2, 1)
	addTagged(q, tags, 20, 3, 2)
	if r.Calls() != 3 || q.size() != 3 {
		t.Fatalf("one draw per add: calls=%d size=%d", r.Calls(), q.size())
	}
	if got := drainTags(q, tags); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("order %v", got)
	}
	if q.now != 30 {
		t.Fatalf("now = %d", q.now)
	}
	id := addTagged(q, tags, 5, 1, 9) // in the past: becomes now
	e, _ := q.pop()
	if e.at != 30 || EntryID(e.seq) != id {
		t.Fatalf("clamp: %+v", e)
	}
}

func TestQueueFIFOWithinLaneAndSeededAcrossLanes(t *testing.T) {
	laneAFirst, laneBFirst := 0, 0
	for seed := range uint64(64) {
		q := newQueue(rng.New(seed))
		tags := map[*entry]int{}
		// Lanes 1 and 2 at one time: tags 10.. are lane 1, 20.. lane 2.
		for i := range 5 {
			addTagged(q, tags, 100, 1, 10+i)
			addTagged(q, tags, 100, 2, 20+i)
		}
		var a, b []int
		got := drainTags(q, tags)
		for _, v := range got {
			if v < 20 {
				a = append(a, v)
			} else {
				b = append(b, v)
			}
		}
		for i := range a {
			if a[i] != 10+i || b[i] != 20+i {
				t.Fatalf("seed %d: FIFO broken: %v %v", seed, a, b)
			}
		}
		if got[0] < 20 {
			laneAFirst++
		} else {
			laneBFirst++
		}
	}
	if laneAFirst == 0 || laneBFirst == 0 {
		t.Fatalf("the order across lanes must depend on the seed: %d/%d", laneAFirst, laneBFirst)
	}
}

// Entries on one lane at T, added around an entry at T+10, stay FIFO.
func TestQueueFIFOAfterLaterTime(t *testing.T) {
	for seed := range uint64(32) {
		q := newQueue(rng.New(seed))
		tags := map[*entry]int{}
		addTagged(q, tags, 10, 1, 1)
		addTagged(q, tags, 20, 1, 9)
		addTagged(q, tags, 10, 1, 2)
		addTagged(q, tags, 10, 2, 7)
		addTagged(q, tags, 10, 1, 3)
		var lane1 []int
		for _, v := range drainTags(q, tags) {
			if v != 7 && v != 9 {
				lane1 = append(lane1, v)
			}
		}
		if len(lane1) != 3 || lane1[0] != 1 || lane1[1] != 2 || lane1[2] != 3 {
			t.Fatalf("seed %d: %v", seed, lane1)
		}
	}
}

// NoLane entries never share a tie, so at one time their order is seeded,
// even when they are added one after the other.
func TestNoLaneEntriesNeverShareATie(t *testing.T) {
	inOrder, outOfOrder := 0, 0
	for seed := range uint64(64) {
		q := newQueue(rng.New(seed))
		tags := map[*entry]int{}
		addTagged(q, tags, 5, NoLane, 1)
		addTagged(q, tags, 5, NoLane, 2)
		if got := drainTags(q, tags); got[0] == 1 {
			inOrder++
		} else {
			outOfOrder++
		}
	}
	if inOrder == 0 || outOfOrder == 0 {
		t.Fatalf("NoLane order must depend on the seed: %d/%d", inOrder, outOfOrder)
	}
}

func TestQueueCancelAndDropIncarnation(t *testing.T) {
	q := newQueue(rng.New(1))
	inc := machine.BootNumber{1}
	other := machine.BootNumber{2}
	toNode := func(at Time, b machine.BootNumber) *entry {
		return &entry{at: at, lane: 5, kind: kindDeliver, node: 0, inc: b}
	}
	q.add(toNode(10, inc))
	keep := toNode(11, other)
	q.add(keep)
	q.add(toNode(12, inc))
	service := serviceEntry(13, 5) // a service entry is kept
	q.add(service)
	c := q.add(serviceEntry(14, 5))
	q.cancel(c)
	q.cancel(c)
	q.cancel(9999)
	if n := q.dropIncarnation(0, inc); n != 2 {
		t.Fatalf("dropIncarnation = %d", n)
	}
	if n := q.dropIncarnation(0, machine.BootNumber{}); n != 0 {
		t.Fatal("a zero boot number must drop nothing")
	}
	if q.size() != 2 {
		t.Fatalf("size = %d", q.size())
	}
	a, _ := q.pop()
	b, _ := q.pop()
	if a != keep || b != service {
		t.Fatalf("%+v %+v", a, b)
	}
	if _, ok := q.pop(); ok {
		t.Fatal("the queue must be empty")
	}
}

func TestNodeLanesAreAboveProductLanes(t *testing.T) {
	for _, l := range []Lane{nodeLane(0, 0), nodeLane(3, 1), nodeLane(1000, 1)} {
		if l <= HostLane {
			t.Fatalf("node lane %d is not above HostLane", l)
		}
	}
	if nodeLane(3, 0) == nodeLane(3, 1) || nodeLane(3, 1) == nodeLane(4, 0) {
		t.Fatal("node lanes collide")
	}
}
