package fakelogstore_test

import (
	"slices"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/fakeservices/fakelogstore"
	"mds/internal/verify/simulator/fakeservices/internal/fakeservicetest"
)

const ms = core.Duration(1_000_000)

func base() simulator.LogStoreFaults {
	ts := simulator.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return simulator.LogStoreFaults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

func setup(f simulator.LogStoreFaults, seed uint64) (*fakelogstore.Engine, *fakeservicetest.Ctx, core.NodeInstance) {
	e := fakelogstore.New(f)
	return e, fakeservicetest.New(e, seed), fakeservicetest.Ref("a")
}

func only[T core.EventData](t *testing.T, c *fakeservicetest.Ctx) T {
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
	if e.Handles(core.ControlPlaneCall{}) || !e.Handles(core.LogAppend{}) {
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
	for _, request := range []core.RequestID{1, 2, 3} {
		if d := c.Find(request).(core.LogAppendDone); d.Err != core.OK || d.Taken {
			t.Fatal(request, d)
		}
	}
	e.Execute(c, a, core.LogRead{Req: 4, From: 0, Limit: 10})
	c.Run()
	r := c.Find(4).(core.LogReadDone)
	if r.Err != core.OK || len(r.Entries) != 3 {
		t.Fatal(r)
	}
	want := []core.LogPosition{0, 1, 2}
	for i, ent := range r.Entries {
		if ent.Slot != want[i] {
			t.Fatalf("entry %d slot %d, want %d", i, ent.Slot, want[i])
		}
	}
	e.Execute(c, a, core.LogGetEnd{Req: 5})
	c.Run()
	if d := c.Find(5).(core.LogGetEndDone); d.Tail != 3 || d.Err != core.OK {
		t.Fatal(d)
	}
}

func TestTrimBlocksRewrite(t *testing.T) {
	e, c, a := setup(base(), 3)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 1, Data: []byte("x")})
	c.Run()
	e.Execute(c, a, core.LogDeleteOld{Req: 2, Through: 1})
	c.Run()
	if d := c.Find(2).(core.LogDeleteOldDone); d.Err != core.OK {
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
	f.Outage = simulator.ServiceOutage{Prob: 0.05, Dur: [2]core.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, c, a := setup(f, seed)
		for i := range 40 {
			c.T += simulator.Time(ms)
			slot := core.LogPosition(i % 5)
			switch i % 4 {
			case 0:
				e.Execute(c, a, core.LogAppend{Req: core.RequestID(i), Slot: slot, Data: []byte{byte(i)}})
			case 1:
				e.Execute(c, a, core.LogRead{Req: core.RequestID(i), From: 0, Limit: 8})
			case 2:
				e.Execute(c, a, core.LogGetEnd{Req: core.RequestID(i)})
			default:
				e.Execute(c, a, core.LogDeleteOld{Req: core.RequestID(i), Through: 0})
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
