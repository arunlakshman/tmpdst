package objstore_test

import (
	"bytes"
	"slices"
	"testing"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/internal/enginetest"
	"manifester/internal/verify/sim/engines/objstore"
)

const ms = core.Duration(1_000_000)

func base() sim.ObjectStoreFaults {
	ts := sim.TwoSpeed{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return sim.ObjectStoreFaults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

func setup(f sim.ObjectStoreFaults, seed uint64) (*objstore.Engine, *enginetest.Ctx, core.NodeInstance) {
	e := objstore.New(f)
	return e, enginetest.New(e, seed), enginetest.Ref("a")
}

func put(e *objstore.Engine, c *enginetest.Ctx, from core.NodeInstance, req core.RequestID, key, data string) {
	e.Execute(c, from, core.ObjectPut{Req: req, Key: key, Data: []byte(data)})
}

func only[T core.EventData](t *testing.T, c *enginetest.Ctx) T {
	t.Helper()
	if len(c.Delivered) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(c.Delivered))
	}
	return c.Delivered[0].Body.(T)
}

func TestNormalPutGet(t *testing.T) {
	e, c, a := setup(base(), 1)
	put(e, c, a, 1, "k", "hello")
	c.Run()
	d := only[core.ObjectPutDone](t, c)
	if d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatal(d)
	}
	e.Execute(c, a, core.ObjectGet{Req: 2, Ref: core.SegmentRef{Key: "k"}})
	e.Execute(c, a, core.ObjectGet{Req: 3, Ref: core.SegmentRef{Key: "nope"}})
	c.Run()
	if g := c.Find(2).(core.ObjectGetDone); string(g.Data) != "hello" {
		t.Fatal(g)
	}
	if g := c.Find(3).(core.ObjectGetDone); g.Err != core.ErrNotFound {
		t.Fatal(g)
	}
	if !e.Handles(core.ObjectList{}) || e.Handles(core.ControlPlaneCall{}) {
		t.Fatal("Handles")
	}
}

func Test503(t *testing.T) {
	f := base()
	f.Unavailable503 = 1
	e, c, a := setup(f, 1)
	put(e, c, a, 1, "k", "x")
	c.Run()
	d := only[core.ObjectPutDone](t, c)
	if d.Err != core.ErrUnavailable || d.Outcome != core.NotApplied {
		t.Fatal(d)
	}
	if _, ok := e.Object("k"); ok || e.Gen() != 0 {
		t.Fatal("applied")
	}
	if !slices.Contains(c.Faults, "obj_503") {
		t.Fatal(c.Faults)
	}
}

func TestOutageWindow(t *testing.T) {
	f := base()
	f.Outage = sim.Outage{Prob: 1, Dur: [2]core.Duration{100 * ms, 200 * ms}}
	e, c, a := setup(f, 1)
	put(e, c, a, 1, "k", "x")
	c.T = 50 * sim.Time(ms) // still inside
	put(e, c, a, 2, "k", "x")
	c.Run()
	for _, d := range c.Delivered {
		if p := d.Body.(core.ObjectPutDone); p.Err != core.ErrUnavailable || p.Outcome != core.NotApplied {
			t.Fatal(p)
		}
	}
	if n := slices.Index(c.Faults, "obj_outage"); n < 0 || slices.Index(c.Faults[n+1:], "obj_outage") >= 0 {
		t.Fatal("want exactly one window", c.Faults)
	}
	e.EnterRecovery()
	c.T = 10_000 * sim.Time(ms)
	put(e, c, a, 3, "k", "x")
	c.Run()
	if p := c.Delivered[2].Body.(core.ObjectPutDone); p.Err != core.OK {
		t.Fatal(p)
	}
}

func TestTimeoutAppliesOrNot(t *testing.T) {
	f := base()
	f.Timeout = 1
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		e, c, a := setup(f, seed)
		put(e, c, a, 1, "k", "x")
		c.Run()
		d := only[core.ObjectPutDone](t, c)
		if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied || c.Delivered[0].At != sim.Time(f.CallerTimeout) {
			t.Fatal(d, c.Delivered[0].At)
		}
		if _, ok := e.Object("k"); ok {
			applied++
		} else {
			notApplied++
		}
	}
	if applied == 0 || notApplied == 0 {
		t.Fatal(applied, notApplied)
	}
}

func TestLostResponseStillApplies(t *testing.T) {
	f := base()
	f.LostResponse = 1
	e, c, a := setup(f, 1)
	put(e, c, a, 1, "k", "x")
	c.Run()
	d := only[core.ObjectPutDone](t, c)
	if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
		t.Fatal(d)
	}
	if v, ok := e.Object("k"); !ok || string(v) != "x" {
		t.Fatal("not applied")
	}
	// A lost read still answers exactly once.
	e.Execute(c, a, core.ObjectGet{Req: 2, Ref: core.SegmentRef{Key: "k"}})
	c.Run()
	if g := c.Delivered[1].Body.(core.ObjectGetDone); g.Err != core.ErrTimeout {
		t.Fatal(g)
	}
}

func TestLandsAfterCrash(t *testing.T) {
	e, c, a := setup(base(), 1)
	put(e, c, a, 1, "k", "x")
	c.Crash("a")
	c.Run()
	if len(c.Delivered) != 0 {
		t.Fatal("delivered to dead node")
	}
	if v, ok := e.Object("k"); !ok || string(v) != "x" {
		t.Fatal("did not land")
	}
	// Crash between landing and response: effect stands, no delivery.
	e2, c2, a2 := setup(base(), 1)
	put(e2, c2, a2, 1, "k", "x")
	c2.Down["a"] = true
	c2.Run()
	if len(c2.Delivered) != 0 {
		t.Fatal("delivered to down node")
	}
	if _, ok := e2.Object("k"); !ok {
		t.Fatal("did not land")
	}
}

func TestRecoveryStopsFaults(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse, f.GetCorruptProb = 1, 1, 1, 1
	f.Outage = sim.Outage{Prob: 1, Dur: [2]core.Duration{ms, 2 * ms}}
	e, c, a := setup(f, 1)
	put(e, c, a, 1, "k", "x") // faulted, in flight
	e.EnterRecovery()
	put(e, c, a, 2, "j", "y")
	c.Run()
	if p := c.Find(1).(core.ObjectPutDone); p.Err != core.ErrUnavailable {
		t.Fatal(p)
	}
	if p := c.Find(2).(core.ObjectPutDone); p.Err != core.OK || p.Outcome != core.Applied {
		t.Fatal(p)
	}
	e.Execute(c, a, core.ObjectGet{Req: 3, Ref: core.SegmentRef{Key: "j"}})
	c.Run()
	if g := c.Find(3).(core.ObjectGetDone); g.Err != core.OK || string(g.Data) != "y" {
		t.Fatal(g)
	}
}

func TestGetCorrupt(t *testing.T) {
	f := base()
	f.GetCorruptProb = 1
	e, c, a := setup(f, 1)
	put(e, c, a, 1, "k", "hello world")
	c.Run()
	e.Execute(c, a, core.ObjectGet{Req: 2, Ref: core.SegmentRef{Key: "k"}})
	c.Run()
	g := c.Delivered[1].Body.(core.ObjectGetDone)
	if g.Err != core.OK || bytes.Equal(g.Data, []byte("hello world")) || len(g.Data) != 11 {
		t.Fatal(g)
	}
	diff := 0
	for i, b := range g.Data {
		x := b ^ "hello world"[i]
		for ; x != 0; x &= x - 1 {
			diff++
		}
	}
	if diff != 1 {
		t.Fatal("want one flipped bit, got", diff)
	}
	if v, _ := e.Object("k"); string(v) != "hello world" {
		t.Fatal("stored object damaged")
	}
	// Mutating the returned slice never touches the store.
	g.Data[0] = 0
	if v, _ := e.Object("k"); string(v) != "hello world" {
		t.Fatal("aliasing")
	}
}

func TestTruthAndGen(t *testing.T) {
	e, c, a := setup(base(), 1)
	put(e, c, a, 1, "b", "2")
	put(e, c, a, 2, "a", "1")
	c.Run()
	g := e.Gen()
	if g != 2 {
		t.Fatal(g)
	}
	var keys []string
	e.EachObject(func(k string, _ []byte) { keys = append(keys, k) })
	if !slices.Equal(keys, []string{"a", "b"}) {
		t.Fatal(keys)
	}
	e.Execute(c, a, core.ObjectGet{Req: 3, Ref: core.SegmentRef{Key: "a"}})
	c.Run()
	if e.Gen() != g {
		t.Fatal("read bumped Gen")
	}
	e.Execute(c, a, core.ObjectDelete{Req: 4, Key: "a"})
	c.Run()
	if e.Gen() != g+1 {
		t.Fatal("delete did not bump Gen")
	}
}

func TestDeterminism(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse, f.GetCorruptProb = 0.2, 0.2, 0.2, 0.3
	f.Outage = sim.Outage{Prob: 0.05, Dur: [2]core.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, c, a := setup(f, seed)
		for i := range 60 {
			c.T += sim.Time(ms)
			k := string(rune('a' + i%5))
			switch i % 3 {
			case 0:
				put(e, c, a, core.RequestID(i), k, "data-"+k)
			case 1:
				e.Execute(c, a, core.ObjectGet{Req: core.RequestID(i), Ref: core.SegmentRef{Key: k}})
			default:
				e.Execute(c, a, core.ObjectDelete{Req: core.RequestID(i), Key: k})
			}
		}
		mid := e.StateHash() // pending plans folded in
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
	if m1 == h1 {
		t.Fatal("pending plans not hashed")
	}
}
