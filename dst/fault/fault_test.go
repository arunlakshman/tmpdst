package fault_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/rng"
)

func TestQuietSilencesFaultOnly(t *testing.T) {
	r := rng.New(42)
	inj := fault.New(r)
	inj.SetQuiet(true)
	if !inj.Quiet() {
		t.Fatal("Quiet not set")
	}
	before := r.Calls()
	if inj.Fault("drop", 1.0) {
		t.Fatal("Fault fired while quiet")
	}
	if r.Calls() != before {
		t.Fatal("quiet Fault must not draw")
	}
	// Maybe still draws (activate 0.5, then fire 0.05).
	_ = inj.Maybe("site")
	if r.Calls() <= before {
		t.Fatal("Maybe drew nothing while quiet")
	}
}

func TestMaybeLatchesPerSite(t *testing.T) {
	// First call draws Chance(0.5) to latch, then Chance(0.05) only if active.
	// Find a seed where site A activates (2 draws), then confirm a second call draws once.
	for seed := range uint64(10_000) {
		r := rng.New(seed)
		inj := fault.New(r)
		c0 := r.Calls()
		_ = inj.Maybe("A")
		if r.Calls()-c0 != 2 {
			continue
		}
		c1 := r.Calls()
		_ = inj.Maybe("A")
		if r.Calls()-c1 != 1 {
			t.Fatalf("seed %d: second Maybe drew %d, want 1 (latch)", seed, r.Calls()-c1)
		}
		c2 := r.Calls()
		_ = inj.Maybe("B")
		delta := r.Calls() - c2
		if delta != 1 && delta != 2 {
			t.Fatalf("seed %d: new site drew %d", seed, delta)
		}
		return
	}
	t.Fatal("no seed found for latch check")
}

func TestMaybePIgnoresQuiet(t *testing.T) {
	r := rng.New(11)
	inj := fault.New(r)
	inj.SetQuiet(true)
	before := r.Calls()
	_ = inj.MaybeP("x", 0.5)
	if r.Calls() == before {
		t.Fatal("MaybeP must draw while quiet")
	}
}

func TestSitesCountCallsAndFires(t *testing.T) {
	inj := fault.New(rng.New(5))
	if len(inj.Sites()) != 0 {
		t.Fatal("a new injector has sites")
	}
	var fired uint64
	for range 400 {
		if inj.MaybeP("a", 0.5) {
			fired++
		}
	}
	got := inj.Sites()["a"]
	if got.Calls != 400 || got.Fired != fired {
		t.Fatalf("counts %+v, want 400 calls and %d fired", got, fired)
	}
	if !got.On && got.Fired != 0 {
		t.Fatalf("an off site fired: %+v", got)
	}
	// Fault is not a site.
	_ = inj.Fault("drop", 1.0)
	if _, ok := inj.Sites()["drop"]; ok {
		t.Fatal("Fault must not be counted as a site")
	}
	// The result is a copy.
	s := inj.Sites()
	s["a"] = fault.SiteCount{}
	if inj.Sites()["a"].Calls != 400 {
		t.Fatal("Sites returned the live map")
	}
}

// TestSiteCountsDoNotMoveTheStream: counting draws nothing, so the same seed
// gives the same answers whether or not anyone reads Sites.
func TestSiteCountsDoNotMoveTheStream(t *testing.T) {
	a, b := fault.New(rng.New(9)), fault.New(rng.New(9))
	for range 100 {
		if a.Maybe("x") != b.Maybe("x") {
			t.Fatal("same seed, different answers")
		}
		_ = b.Sites()
	}
}

func TestMaybePLatchesPerSite(t *testing.T) {
	for seed := range uint64(10_000) {
		r := rng.New(seed)
		inj := fault.New(r)
		c0 := r.Calls()
		_ = inj.MaybeP("A", 0.05)
		if r.Calls()-c0 != 2 {
			continue
		}
		c1 := r.Calls()
		_ = inj.MaybeP("A", 0.05)
		if r.Calls()-c1 != 1 {
			t.Fatalf("seed %d: second MaybeP drew %d, want 1 (latch)", seed, r.Calls()-c1)
		}
		return
	}
	t.Fatal("no seed found for MaybeP latch check")
}
