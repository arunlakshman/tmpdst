package simulator

import (
	"testing"

	"manifester/internal/core"
)

func entry(at Time, lane DeliveryLane, tag uint64) Entry {
	return Entry{At: at, Lane: lane, Kind: EntryFakeServiceStep, Target: Target{Engine: "e"}, Body: tag}
}

func drain(s Scheduler) []Entry {
	var out []Entry
	for {
		e, ok := s.Pop()
		if !ok {
			return out
		}
		out = append(out, e)
	}
}

func TestSchedulerOrderAndClamp(t *testing.T) {
	rng := NewRand(1)
	s := NewScheduler(rng)
	s.Schedule(entry(30, 1, 3))
	s.Schedule(entry(10, 2, 1))
	s.Schedule(entry(20, 3, 2))
	if rng.Calls() != 3 || s.Len() != 3 {
		t.Fatalf("one draw per Schedule: calls=%d len=%d", rng.Calls(), s.Len())
	}
	got := drain(s)
	for i, e := range got {
		if e.Body.(uint64) != uint64(i+1) {
			t.Fatalf("pop %d: %+v", i, e)
		}
	}
	if s.Now() != 30 {
		t.Fatalf("Now = %d", s.Now())
	}
	id := s.Schedule(entry(5, 1, 9)) // past: clamps to Now
	e, _ := s.Pop()
	if e.At != 30 || e.Seq != uint64(id) {
		t.Fatalf("clamp: %+v", e)
	}
}

func TestSchedulerFIFOWithinLaneAndSeededAcrossLanes(t *testing.T) {
	laneAFirst, laneBFirst := 0, 0
	for seed := range uint64(64) {
		s := NewScheduler(NewRand(seed))
		// Lane 1 and 2 interleaved at one instant: bodies 10.. are lane 1, 20.. lane 2.
		for i := range uint64(5) {
			s.Schedule(entry(100, 1, 10+i))
			s.Schedule(entry(100, 2, 20+i))
		}
		var a, b []uint64
		first := uint64(0)
		for i, e := range drain(s) {
			v := e.Body.(uint64)
			if i == 0 {
				first = v
			}
			if v < 20 {
				a = append(a, v)
			} else {
				b = append(b, v)
			}
		}
		for i := range a {
			if a[i] != 10+uint64(i) || b[i] != 20+uint64(i) {
				t.Fatalf("seed %d: FIFO broken: %v %v", seed, a, b)
			}
		}
		if first < 20 {
			laneAFirst++
		} else {
			laneBFirst++
		}
	}
	if laneAFirst == 0 || laneBFirst == 0 {
		t.Fatalf("cross-lane tie must depend on the seed: %d/%d", laneAFirst, laneBFirst)
	}
}

func TestSchedulerFIFOAfterLaterTime(t *testing.T) {
	// Entries on one lane at T scheduled around an entry at T+1 stay FIFO.
	for seed := range uint64(32) {
		s := NewScheduler(NewRand(seed))
		s.Schedule(entry(10, 1, 1))
		s.Schedule(entry(20, 1, 9))
		s.Schedule(entry(10, 1, 2))
		s.Schedule(entry(10, 2, 7))
		s.Schedule(entry(10, 1, 3))
		var lane1 []uint64
		for _, e := range drain(s) {
			if v := e.Body.(uint64); v != 7 && v != 9 {
				lane1 = append(lane1, v)
			}
		}
		if len(lane1) != 3 || lane1[0] != 1 || lane1[1] != 2 || lane1[2] != 3 {
			t.Fatalf("seed %d: %v", seed, lane1)
		}
	}
}

func TestSchedulerCancelAndDropNode(t *testing.T) {
	s := NewScheduler(NewRand(1))
	ref := core.NodeInstance{Node: "n", Inc: core.BootNumber{1}}
	other := core.NodeInstance{Node: "n", Inc: core.BootNumber{2}}
	toNode := func(at Time, r core.NodeInstance) Entry {
		return Entry{At: at, Lane: 5, Kind: EntryDeliver, Target: Target{Node: r}, Body: core.TimerFired{}}
	}
	s.Schedule(toNode(10, ref))
	s.Schedule(toNode(11, other))
	s.Schedule(toNode(12, ref))
	s.Schedule(entry(13, 5, 1)) // engine entry survives
	c := s.Schedule(entry(14, 5, 2))
	s.Cancel(c)
	s.Cancel(c)
	s.Cancel(9999)
	if n := s.DropNode(ref); n != 2 {
		t.Fatalf("DropNode = %d", n)
	}
	if n := s.DropNode(core.NodeInstance{}); n != 0 {
		t.Fatal("zero ref must drop nothing")
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d", s.Len())
	}
	got := drain(s)
	if len(got) != 2 || got[0].Target.Node != other || got[1].Target.Engine != "e" {
		t.Fatalf("%+v", got)
	}
}

func TestTimeline(t *testing.T) {
	tl := NewTimeline()
	var cur, cur2 Cursor
	f := func(name string, v int64) core.Fact {
		return core.Fact{Name: name, Fields: []core.Field{{Key: "v", Value: core.I64(v)}}}
	}
	tl.Append(core.NodeInstance{}, 1, f("a", 1))
	tl.Append(core.NodeInstance{}, 2, f("b", 2))
	tl.Append(core.NodeInstance{}, 3, f("a", 3))
	got := tl.Since("a", &cur)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 3 || got[1].At != 3 {
		t.Fatalf("%+v", got)
	}
	if len(tl.Since("a", &cur)) != 0 {
		t.Fatal("cursor must advance")
	}
	tl.Append(core.NodeInstance{}, 4, f("a", 4))
	if got := tl.Since("a", &cur); len(got) != 1 || got[0].Seq != 4 {
		t.Fatalf("%+v", got)
	}
	if len(tl.Since("a", &cur2)) != 3 || len(tl.Snapshot("b")) != 1 || len(tl.Snapshot("zz")) != 0 || tl.Len() != 4 {
		t.Fatal("independent cursors / snapshot / len")
	}
}

func TestNodeInboxAndCost(t *testing.T) {
	in := NewNodeInbox().(*nodeInbox)
	in.Push(core.ConnOpened{Conn: 1})
	in.Push(core.BytesReceived{Conn: 1, Data: make([]byte, 100)})
	in.Push(core.TimerFired{ID: 1})
	if in.Control().Len() != 1 || in.Data().Len() != 2 {
		t.Fatal("InboxKindOf routing")
	}
	var p core.InboxPicker = core.ControlFirstPicker{}
	b, _ := p.Next(in.Control(), in.Data())
	if _, ok := b.(core.TimerFired); !ok {
		t.Fatalf("control must win: %T", b)
	}

	rng := NewRand(1)
	zero := NewCostModel(CostParams{})
	if zero.Sample(core.BytesReceived{Data: make([]byte, 9)}, rng) != 0 || rng.Calls() != 0 {
		t.Fatal("zero params: free and draw-free")
	}
	m := NewCostModel(CostParams{PerEvent: 10, PerByte: 2})
	if got := m.Sample(core.BytesReceived{Data: make([]byte, 5)}, rng); got != 20 {
		t.Fatalf("cost = %d", got)
	}
	if got := m.Sample(core.TimerFired{}, rng); got != 10 {
		t.Fatalf("cost = %d", got)
	}
	pause := NewCostModel(CostParams{PerEvent: 1, PauseProb: 1, Pause: FastWithRareSlow{FastMin: 500, FastMax: 500}})
	if got := pause.Sample(core.TimerFired{}, rng); got != 501 {
		t.Fatalf("pause cost = %d", got)
	}
}
