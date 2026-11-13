package fakeservices_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

const ms = machine.Duration(1_000_000)

func objectBase() fakeservices.ObjectStoreFaults {
	ts := kernel.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return fakeservices.ObjectStoreFaults{Faults: faulty.Faults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}}
}

// objectSetup makes a world with one caller "a" and the fake object store.
func objectSetup(t *testing.T, f fakeservices.ObjectStoreFaults, seed uint64) (*fakeservices.ObjectStore, *servicetest.Harness) {
	t.Helper()
	h := servicetest.New(t, seed, "a")
	e := fakeservices.NewObjectStore(h.W, f)
	svc := kernel.AddService[core.ObjectCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		if oc, ok := out.(core.ObjectCall); ok {
			kernel.Call(r, svc, oc)
		}
	}
	return e, h
}

func put(h *servicetest.Harness, request machine.RequestID, key, data string) {
	h.Do("a", core.ObjectCall{Call: objectstore.Put{Req: request, Key: key, Data: []byte(data)}})
}

func get(h *servicetest.Harness, request machine.RequestID, key string) {
	h.Do("a", core.ObjectCall{Call: objectstore.Get{Req: request, Ref: core.SegmentRef{Key: key}}})
}

// only returns the one result that was delivered, as the object store result type T.
func only[T objectstore.Result](t *testing.T, h *servicetest.Harness) T {
	t.Helper()
	if len(h.Delivered) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(h.Delivered))
	}
	return h.Delivered[0].Body.(core.ObjectDone).Result.(T)
}

func TestObjectStoreNormalPutGet(t *testing.T) {
	e, h := objectSetup(t, objectBase(), 1)
	put(h, 1, "k", "hello")
	h.Run()
	d := only[objectstore.PutDone](t, h)
	if d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatal(d)
	}
	get(h, 2, "k")
	get(h, 3, "nope")
	h.Run()
	if g := h.Find(2).(core.ObjectDone).Result.(objectstore.GetDone); string(g.Data) != "hello" {
		t.Fatal(g)
	}
	if g := h.Find(3).(core.ObjectDone).Result.(objectstore.GetDone); g.Err != core.ErrNotFound {
		t.Fatal(g)
	}
	if e.Name() != "fakeobjectstore" {
		t.Fatal(e.Name())
	}
}

func TestObjectStoreRefusedAppliesNothing(t *testing.T) {
	f := objectBase()
	f.Unavailable503 = 1
	e, h := objectSetup(t, f, 1)
	put(h, 1, "k", "x")
	h.Run()
	d := only[objectstore.PutDone](t, h)
	if d.Err != core.ErrUnavailable || d.Outcome != core.NotApplied {
		t.Fatal(d)
	}
	if _, ok := e.Object("k"); ok || e.Gen() != 0 {
		t.Fatal("applied")
	}
	if !slices.Contains(h.Faults(), "obj_503") {
		t.Fatal(h.Faults())
	}
}

func TestObjectStoreOutageWindow(t *testing.T) {
	f := objectBase()
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]machine.Duration{100 * ms, 200 * ms}}
	_, h := objectSetup(t, f, 1)
	put(h, 1, "k", "x")
	h.RunTo(50 * kernel.Time(ms)) // still inside
	put(h, 2, "k", "x")
	h.Run()
	for _, d := range h.Delivered {
		if p := d.Body.(core.ObjectDone).Result.(objectstore.PutDone); p.Err != core.ErrUnavailable || p.Outcome != core.NotApplied {
			t.Fatal(p)
		}
	}
	faults := h.Faults()
	if n := slices.Index(faults, "obj_outage"); n < 0 || slices.Index(faults[n+1:], "obj_outage") >= 0 {
		t.Fatal("want exactly one window", faults)
	}
	h.Recover()
	h.RunTo(10_000 * kernel.Time(ms))
	put(h, 3, "k", "x")
	h.Run()
	if p := h.Delivered[2].Body.(core.ObjectDone).Result.(objectstore.PutDone); p.Err != core.OK {
		t.Fatal(p)
	}
}

func TestObjectStoreTimeoutAppliesOrNot(t *testing.T) {
	f := objectBase()
	f.Timeout = 1
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		e, h := objectSetup(t, f, seed)
		put(h, 1, "k", "x")
		h.Run()
		d := only[objectstore.PutDone](t, h)
		// The caller sees the timeout one CallerTimeout after the call. The call is
		// issued at the time of the command, which is time zero.
		if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied || h.Delivered[0].At != kernel.Time(f.CallerTimeout) {
			t.Fatal(d, h.Delivered[0].At)
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

func TestObjectStoreLostResponseStillApplies(t *testing.T) {
	f := objectBase()
	f.LostResponse = 1
	e, h := objectSetup(t, f, 1)
	put(h, 1, "k", "x")
	h.Run()
	d := only[objectstore.PutDone](t, h)
	if d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
		t.Fatal(d)
	}
	if v, ok := e.Object("k"); !ok || string(v) != "x" {
		t.Fatal("not applied")
	}
	// A lost read still answers exactly once.
	get(h, 2, "k")
	h.Run()
	if g := h.Delivered[1].Body.(core.ObjectDone).Result.(objectstore.GetDone); g.Err != core.ErrTimeout {
		t.Fatal(g)
	}
}

// The caller dies after the call was issued: the effect lands, and nothing is
// delivered to the dead caller.
func TestObjectStoreLandsAfterCrash(t *testing.T) {
	e, h := objectSetup(t, objectBase(), 1)
	put(h, 1, "k", "x")
	h.Step() // the caller issues the call
	h.Crash("a")
	h.Run()
	if len(h.Delivered) != 0 {
		t.Fatal("delivered to dead node")
	}
	if v, ok := e.Object("k"); !ok || string(v) != "x" {
		t.Fatal("did not land")
	}
}

// The caller dies between the landing and the response: the effect stands, and
// the response is not delivered to the dead incarnation, nor to the next one.
func TestObjectStoreCrashBetweenLandingAndResponse(t *testing.T) {
	e, h := objectSetup(t, objectBase(), 2)
	put(h, 1, "k", "x")
	h.Step()
	for _, ok := e.Object("k"); !ok; _, ok = e.Object("k") {
		if !h.Step() {
			t.Fatal("the put never landed")
		}
	}
	h.Crash("a")
	h.Boot("a") // a new incarnation must not get the answer for the old one
	h.Run()
	if len(h.Delivered) != 0 {
		t.Fatal("delivered to another incarnation")
	}
	if _, ok := e.Object("k"); !ok {
		t.Fatal("did not land")
	}
}

func TestObjectStoreRecoveryStopsFaults(t *testing.T) {
	f := objectBase()
	f.Unavailable503, f.Timeout, f.LostResponse, f.GetCorruptProb = 1, 1, 1, 1
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]machine.Duration{ms, 2 * ms}}
	_, h := objectSetup(t, f, 1)
	put(h, 1, "k", "x") // faulted, in flight
	h.Run()
	h.Recover()
	put(h, 2, "j", "y")
	h.Run()
	if p := h.Find(1).(core.ObjectDone).Result.(objectstore.PutDone); p.Err != core.ErrUnavailable {
		t.Fatal(p)
	}
	if p := h.Find(2).(core.ObjectDone).Result.(objectstore.PutDone); p.Err != core.OK || p.Outcome != core.Applied {
		t.Fatal(p)
	}
	get(h, 3, "j")
	h.Run()
	if g := h.Find(3).(core.ObjectDone).Result.(objectstore.GetDone); g.Err != core.OK || string(g.Data) != "y" {
		t.Fatal(g)
	}
}

// A plan made before EnterRecovery stands (it is not redrawn).
func TestObjectStoreInFlightPlanStandsAfterRecovery(t *testing.T) {
	f := objectBase()
	f.Unavailable503 = 1
	_, h := objectSetup(t, f, 1)
	put(h, 1, "k", "x")
	h.Step() // issued: the plan is a 503
	h.Recover()
	put(h, 2, "j", "y")
	h.Run()
	if p := h.Find(1).(core.ObjectDone).Result.(objectstore.PutDone); p.Err != core.ErrUnavailable {
		t.Fatal("in-flight plan must stand", p)
	}
	if p := h.Find(2).(core.ObjectDone).Result.(objectstore.PutDone); p.Err != core.OK {
		t.Fatal(p)
	}
}

func TestObjectStoreGetCorrupt(t *testing.T) {
	f := objectBase()
	f.GetCorruptProb = 1
	e, h := objectSetup(t, f, 1)
	put(h, 1, "k", "hello world")
	h.Run()
	get(h, 2, "k")
	h.Run()
	g := h.Delivered[1].Body.(core.ObjectDone).Result.(objectstore.GetDone)
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
	if !h.Hit("obj get corrupted") {
		t.Fatal("the corruption was not recorded")
	}
	// Mutating the returned slice never touches the store.
	g.Data[0] = 0
	if v, _ := e.Object("k"); string(v) != "hello world" {
		t.Fatal("aliasing")
	}
}

func TestObjectStoreTruthAndGen(t *testing.T) {
	e, h := objectSetup(t, objectBase(), 1)
	put(h, 1, "b", "2")
	put(h, 2, "a", "1")
	h.Run()
	g := e.Gen()
	if g != 2 {
		t.Fatal(g)
	}
	var keys []string
	e.EachObject(func(k string, _ []byte) { keys = append(keys, k) })
	if !slices.Equal(keys, []string{"a", "b"}) {
		t.Fatal(keys)
	}
	get(h, 3, "a")
	h.Run()
	if e.Gen() != g {
		t.Fatal("read bumped Gen")
	}
	h.Do("a", core.ObjectCall{Call: objectstore.Delete{Req: 4, Key: "a"}})
	h.Run()
	if e.Gen() != g+1 {
		t.Fatal("delete did not bump Gen")
	}
}

func TestObjectStoreDeterminism(t *testing.T) {
	f := objectBase()
	f.Unavailable503, f.Timeout, f.LostResponse, f.GetCorruptProb = 0.2, 0.2, 0.2, 0.3
	f.Outage = faulty.Outage{Prob: 0.05, Dur: [2]machine.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, h := objectSetup(t, f, seed)
		for i := range 60 {
			after := machine.Duration(i) * ms
			k := string(rune('a' + i%5))
			switch i % 3 {
			case 0:
				h.DoAfter(after, "a", core.ObjectCall{Call: objectstore.Put{Req: machine.RequestID(i), Key: k, Data: []byte("data-" + k)}})
			case 1:
				h.DoAfter(after, "a", core.ObjectCall{Call: objectstore.Get{Req: machine.RequestID(i), Ref: core.SegmentRef{Key: k}}})
			default:
				h.DoAfter(after, "a", core.ObjectCall{Call: objectstore.Delete{Req: machine.RequestID(i), Key: k}})
			}
		}
		h.RunTo(30 * kernel.Time(ms))
		mid := e.StateHash() // pending plans folded in
		h.Run()
		return h.Trace(), e.StateHash(), mid
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
