package simulator

import (
	"testing"

	"manifester/internal/verify/speccheck/checker"
)

// checker.Rand is a copy of the sim stream, so that the speccheck tests do
// not import sim. This test fails if the two copies ever give different values.
func TestVeritastestRandMatchesSimStream(t *testing.T) {
	for _, seed := range []uint64{0, 1, 7, 42, 1 << 40, ^uint64(0)} {
		simStream := NewRand(seed)
		specStream := checker.NewRand(seed)
		for i := range 1000 {
			if s, v := simStream.Uint64(), specStream.Uint64(); s != v {
				t.Fatalf("seed %d, draw %d: the sim stream gave %#x, checker.Rand gave %#x.", seed, i, s, v)
			}
		}
	}
}

func TestSplitMixReference(t *testing.T) {
	r := NewRand(0)
	for i, want := range []uint64{0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4, 0x06c45d188009454f} {
		if got := r.Uint64(); got != want {
			t.Fatalf("draw %d: got %#x want %#x", i, got, want)
		}
	}
	if r.Calls() != 3 {
		t.Fatalf("calls = %d", r.Calls())
	}
}

func TestRandBasics(t *testing.T) {
	r := NewRand(42)
	for range 2000 {
		if v := r.Intn(7); v < 0 || v >= 7 {
			t.Fatalf("Intn out of range: %d", v)
		}
		if f := r.Float64(); f < 0 || f >= 1 {
			t.Fatalf("Float64 out of range: %v", f)
		}
		if b := r.Between(-5, 5); b < -5 || b >= 5 {
			t.Fatalf("Between out of range: %d", b)
		}
	}
	c := r.Calls()
	if r.Chance(0) || r.Chance(-1) || r.Between(3, 3) != 3 || r.Between(9, 3) != 9 {
		t.Fatal("degenerate cases wrong")
	}
	if r.Calls() != c {
		t.Fatal("Chance(<=0) and empty Between must not draw")
	}
	if !r.Chance(1) || r.Calls() != c+1 {
		t.Fatal("Chance(1) must be true and draw once")
	}
	r.NewID()
	if r.Calls() != c+3 {
		t.Fatal("NewID must draw twice")
	}
	fp := r.Fingerprint()
	if r.Fingerprint() != fp || r.Calls() != c+3 {
		t.Fatal("Fingerprint must not draw")
	}
	r.Uint64()
	if r.Fingerprint() == fp {
		t.Fatal("Fingerprint must move with the stream")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Intn(0) must panic")
		}
	}()
	r.Intn(0)
}

func TestIntnUnbiasedish(t *testing.T) {
	r := NewRand(7)
	var buckets [3]int
	for range 30000 {
		buckets[r.Intn(3)]++
	}
	for _, b := range buckets {
		if b < 9000 || b > 11000 {
			t.Fatalf("skewed: %v", buckets)
		}
	}
}

func TestDeriveIndependence(t *testing.T) {
	r := NewRand(99)
	before := r.Calls()
	a, b, a2 := r.Derive("swarm"), r.Derive("buggify"), r.Derive("swarm")
	if r.Calls() != before {
		t.Fatal("Derive must not draw from the parent")
	}
	if a.Uint64() == b.Uint64() {
		t.Fatal("different labels must give different streams")
	}
	a3 := NewRand(99).Derive("swarm")
	a2.Uint64()
	if a2.Uint64() != func() uint64 { a3.Uint64(); return a3.Uint64() }() {
		t.Fatal("same (seed, label) must replay")
	}
	// Drawing from the parent never moves a derived stream.
	p := NewRand(5)
	d1 := p.Derive("x").Uint64()
	for range 10 {
		p.Uint64()
	}
	if p.Derive("x").Uint64() != d1 {
		t.Fatal("derived stream depends on parent position")
	}
	if NewRand(5).Derive("x").Uint64() == NewRand(6).Derive("x").Uint64() {
		t.Fatal("derived stream ignores the seed")
	}
}

func TestReseed(t *testing.T) {
	r := NewRand(1)
	r.Uint64()
	r.Reseed(0)
	if r.Uint64() != 0xe220a8397b1dcdaf || r.Calls() != 2 {
		t.Fatal("Reseed must restart the sequence and keep counting")
	}
	if r.Derive("q").Uint64() != NewRand(0).Derive("q").Uint64() {
		t.Fatal("Derive must follow Reseed")
	}
}

func TestTwoSpeedSample(t *testing.T) {
	r := NewRand(3)
	ts := FastWithRareSlow{FastMin: 10, FastMax: 20, SlowMin: 1000, SlowMax: 2000, SlowProb: 0.5}
	fast, slow := 0, 0
	for range 1000 {
		d := ts.Sample(r)
		switch {
		case d >= 10 && d <= 20:
			fast++
		case d >= 1000 && d <= 2000:
			slow++
		default:
			t.Fatalf("out of both ranges: %d", d)
		}
	}
	if fast < 300 || slow < 300 {
		t.Fatalf("fast=%d slow=%d", fast, slow)
	}
	c := r.Calls()
	if (FastWithRareSlow{FastMin: 5, FastMax: 5}).Sample(r) != 5 || r.Calls() != c {
		t.Fatal("a degenerate TwoSpeed must draw nothing")
	}
}
