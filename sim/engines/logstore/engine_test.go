package logstore_test

import (
	"slices"
	"testing"

	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/internal/enginetest"
	"manifester/sim/engines/logstore"
)

const ms = core.Duration(1_000_000)

func base() sim.LogStoreFaults {
	ts := sim.TwoSpeed{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return sim.LogStoreFaults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

func setup(f sim.LogStoreFaults, seed uint64) (*logstore.Engine, *enginetest.Ctx, core.NodeRef) {
	e := logstore.New(f)
	return e, enginetest.New(e, seed), enginetest.Ref("a")
}

func only[T core.EventBody](t *testing.T, c *enginetest.Ctx) T {
	t.Helper()
	if len(c.Delivered) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(c.Delivered))
	}
	return c.Delivered[0].Body.(T)
}

func TestIfAbsentPut(t *testing.T) {
	e, c, a := setup(base(), 1)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 3, Data: []byte("first")})
	c.Run()
	d := only[core.LogAppendDone](t, c)
	if d.Err != core.OK || d.Outcome != core.Applied || d.Taken {
		t.Fatal(d)
	}
	e.Execute(c, a, core.LogAppend{Req: 2, Slot: 3, Data: []byte("second")})
	c.Run()
	d2 := c.Find(2).(core.LogAppendDone)
	if d2.Err != core.OK || d2.Outcome != core.Applied || !d2.Taken || string(d2.Existing) != "first" {
		t.Fatal(d2)
	}
	if v, ok := e.Entry(3); !ok || string(v) != "first" {
		t.Fatal(v, ok)
	}
	if e.Handles(core.CPCall{}) || !e.Handles(core.LogAppend{}) {
		t.Fatal("Handles")
	}
}

func TestLostAppendMaybeApplied(t *testing.T) {
	f := base()
	f.LostResponse = 1
	var saw bool
	for seed := uint64(1); seed <= 5; seed++ {
		e, c, a := setup(f, seed)
		e.Execute(c, a, core.LogAppend{Req: 1, Slot: 0, Data: []byte("x")})
		c.Run()
		d := only[core.LogAppendDone](t, c)
		if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
			t.Fatalf("seed %d: %v", seed, d)
		}
		if v, ok := e.Entry(0); !ok || string(v) != "x" {
			t.Fatalf("seed %d: lost answer must still land", seed)
		}
		if !slices.Contains(c.Faults, "log_lost_response") {
			t.Fatalf("seed %d: want log_lost_response, got %v", seed, c.Faults)
		}
		saw = true
	}
	if !saw {
		t.Fatal("no seeds")
	}
}

func TestOrderedLandings(t *testing.T) {
	e, c, a := setup(base(), 2)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 2, Data: []byte("c")})
	e.Execute(c, a, core.LogAppend{Req: 2, Slot: 0, Data: []byte("a")})
	e.Execute(c, a, core.LogAppend{Req: 3, Slot: 1, Data: []byte("b")})
	c.Run()
	for _, req := range []core.ReqID{1, 2, 3} {
		if d := c.Find(req).(core.LogAppendDone); d.Err != core.OK || d.Taken {
			t.Fatal(req, d)
		}
	}
	e.Execute(c, a, core.LogRead{Req: 4, From: 0, Limit: 10})
	c.Run()
	r := c.Find(4).(core.LogReadDone)
	if r.Err != core.OK || len(r.Entries) != 3 {
		t.Fatal(r)
	}
	want := []core.LogSlot{0, 1, 2}
	for i, ent := range r.Entries {
		if ent.Slot != want[i] {
			t.Fatalf("entry %d slot %d, want %d", i, ent.Slot, want[i])
		}
	}
	e.Execute(c, a, core.LogCheckTail{Req: 5})
	c.Run()
	if d := c.Find(5).(core.LogCheckTailDone); d.Tail != 3 || d.Err != core.OK {
		t.Fatal(d)
	}
}

func TestTrimBlocksRewrite(t *testing.T) {
	e, c, a := setup(base(), 3)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 1, Data: []byte("x")})
	c.Run()
	e.Execute(c, a, core.LogTrim{Req: 2, Through: 1})
	c.Run()
	if d := c.Find(2).(core.LogTrimDone); d.Err != core.OK {
		t.Fatal(d)
	}
	e.Execute(c, a, core.LogAppend{Req: 3, Slot: 1, Data: []byte("y")})
	c.Run()
	d := c.Find(3).(core.LogAppendDone)
	if d.Err != core.ErrConflict || d.Outcome != core.NotApplied || !d.Taken {
		t.Fatal(d)
	}
	if _, ok := e.Entry(1); ok {
		t.Fatal("tombstoned entry still readable")
	}
}

func TestTimeoutMaybeApplied(t *testing.T) {
	f := base()
	f.Timeout = 1
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		e, c, a := setup(f, seed)
		e.Execute(c, a, core.LogAppend{Req: 1, Slot: 0, Data: []byte("x")})
		c.Run()
		d := only[core.LogAppendDone](t, c)
		if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
			t.Fatal(d)
		}
		if _, ok := e.Entry(0); ok {
			applied++
		} else {
			notApplied++
		}
	}
	if applied == 0 || notApplied == 0 {
		t.Fatal(applied, notApplied)
	}
}

func TestDeterminism(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	f.Outage = sim.Outage{Prob: 0.05, Dur: [2]core.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, c, a := setup(f, seed)
		for i := range 40 {
			c.T += sim.Time(ms)
			slot := core.LogSlot(i % 5)
			switch i % 4 {
			case 0:
				e.Execute(c, a, core.LogAppend{Req: core.ReqID(i), Slot: slot, Data: []byte{byte(i)}})
			case 1:
				e.Execute(c, a, core.LogRead{Req: core.ReqID(i), From: 0, Limit: 8})
			case 2:
				e.Execute(c, a, core.LogCheckTail{Req: core.ReqID(i)})
			default:
				e.Execute(c, a, core.LogTrim{Req: core.ReqID(i), Through: 0})
			}
		}
		mid := e.StateHash()
		c.Run()
		return c.Trace(), e.StateHash(), mid
	}
	t1, h1, m1 := run(7)
	t2, h2, m2 := run(7)
	if t1 != t2 || h1 != h2 || m1 != m2 {
		t.Fatal("same seed diverged")
	}
	t3, _, _ := run(8)
	if t1 == t3 {
		t.Fatal("different seeds gave one trace")
	}
}
