package sim

import (
	"testing"

	"manifester/toys3/core"
)

func TestQueueOrdersByTimeThenTieThenSeq(t *testing.T) {
	r := NewRand(1)
	var q Queue
	times := []core.Time{50, 10, 30, 10, 20, 10, 40, 30}
	for i, at := range times {
		q.Schedule(r, 0, Entry{At: at, Client: i})
	}
	var prev Entry
	for i := 0; i < len(times); i++ {
		e, ok := q.Pop()
		if !ok {
			t.Fatal("queue ran dry")
		}
		if i > 0 {
			if e.At < prev.At || (e.At == prev.At && e.Tie < prev.Tie) {
				t.Fatalf("out of order: %+v after %+v", e, prev)
			}
		}
		prev = e
	}
	if _, ok := q.Pop(); ok || q.Len() != 0 {
		t.Fatal("queue not empty")
	}
}

func TestQueueTieBreakDependsOnSeed(t *testing.T) {
	order := func(seed uint64) []int {
		r := NewRand(seed)
		var q Queue
		for i := 0; i < 6; i++ {
			q.Schedule(r, 0, Entry{At: 7, Client: i})
		}
		var out []int
		for q.Len() > 0 {
			e, _ := q.Pop()
			out = append(out, e.Client)
		}
		return out
	}
	a, b := order(1), order(1)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed, different order: %v vs %v", a, b)
		}
	}
	differs := false
	for seed := uint64(2); seed < 20 && !differs; seed++ {
		c := order(seed)
		for i := range a {
			if a[i] != c[i] {
				differs = true
			}
		}
	}
	if !differs {
		t.Fatal("same-instant order never varies across seeds")
	}
}

func TestQueueClampsPastAndDropsIncarnation(t *testing.T) {
	r := NewRand(3)
	var q Queue
	q.Schedule(r, 100, Entry{At: 5, Kind: KindClientWake})
	q.Schedule(r, 100, Entry{At: 200, Kind: KindDeliver, Inc: 1})
	q.Schedule(r, 100, Entry{At: 150, Kind: KindFSLand, Inc: 1})
	q.Schedule(r, 100, Entry{At: 160, Kind: KindDeliver, Inc: 2})
	q.Schedule(r, 100, Entry{At: 170, Kind: KindRestart})
	if n := q.DropIncarnation(1); n != 2 {
		t.Fatalf("dropped %d, want 2", n)
	}
	var kinds []EntryKind
	var ats []core.Time
	for q.Len() > 0 {
		e, _ := q.Pop()
		kinds = append(kinds, e.Kind)
		ats = append(ats, e.At)
	}
	want := []EntryKind{KindClientWake, KindDeliver, KindRestart}
	if len(kinds) != 3 || kinds[0] != want[0] || kinds[1] != want[1] || kinds[2] != want[2] {
		t.Fatalf("got %v", kinds)
	}
	if ats[0] != 100 {
		t.Fatalf("past entry not clamped to now: %v", ats[0])
	}
}
