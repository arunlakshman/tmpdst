package rng_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/rng"
)

func TestSeed42Stream(t *testing.T) {
	want := []uint64{
		0xbdd732262feb6e95,
		0x28efe333b266f103,
		0x47526757130f9f52,
		0x581ce1ff0e4ae394,
		0x09bc585a244823f2,
		0xde4431fa3c80db06,
		0x37e9671c45376d5d,
		0xccf635ee9e9e2fa4,
	}
	r := rng.New(42)
	for i, w := range want {
		if got := r.Uint64(); got != w {
			t.Fatalf("draw %d: got %016x want %016x", i, got, w)
		}
	}
	if r.Calls() != 8 {
		t.Fatalf("Calls = %d, want 8", r.Calls())
	}
}

func TestNoDrawEdges(t *testing.T) {
	r := rng.New(42)
	_ = r.Float64()
	_ = r.Intn(10)
	before := r.Calls()
	if r.Chance(0) {
		t.Fatal("Chance(0) true")
	}
	if r.Calls() != before {
		t.Fatal("Chance(0) drew")
	}
	if r.Intn(0) != 0 || r.Calls() != before {
		t.Fatal("Intn(0) drew or wrong")
	}
	if got := r.Between(5, 5); got != 5 || r.Calls() != before {
		t.Fatal("Between(5,5) drew or wrong")
	}
}

// The values below were made by the old Derive of the broker kernel
// (CountedRand.Derive). The formula must not change.
func TestDeriveMatchesTheOldFormula(t *testing.T) {
	cases := []struct {
		seed  uint64
		label string
		want  uint64
	}{
		{99, "swarm", 0xf59e9e20f6e82e0b},
		{5, "x", 0x8b0268362b66c315},
		{0, "", 0x7261e9031e12a9d7},
	}
	for _, c := range cases {
		if got := rng.New(c.seed).Derive(c.label).Uint64(); got != c.want {
			t.Fatalf("seed %d label %q: got %#x want %#x", c.seed, c.label, got, c.want)
		}
	}
}

func TestDeriveIndependence(t *testing.T) {
	r := rng.New(99)
	before := r.Calls()
	a, b := r.Derive("swarm"), r.Derive("buggify")
	if r.Calls() != before {
		t.Fatal("Derive must not draw from the parent")
	}
	if a.Uint64() == b.Uint64() {
		t.Fatal("different labels must give different streams")
	}
	// Drawing from the parent never moves a derived stream.
	p := rng.New(5)
	d1 := p.Derive("x").Uint64()
	for range 10 {
		p.Uint64()
	}
	if p.Derive("x").Uint64() != d1 {
		t.Fatal("the derived stream depends on the position of the parent")
	}
	if rng.New(5).Derive("x").Uint64() == rng.New(6).Derive("x").Uint64() {
		t.Fatal("the derived stream ignores the seed")
	}
}

func TestSeedZeroReference(t *testing.T) {
	r := rng.New(0)
	for i, want := range []uint64{0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4, 0x06c45d188009454f} {
		if got := r.Uint64(); got != want {
			t.Fatalf("draw %d: got %#x want %#x", i, got, want)
		}
	}
}

func TestRangesAndIntnSpread(t *testing.T) {
	r := rng.New(42)
	for range 2000 {
		if v := r.Intn(7); v < 0 || v >= 7 {
			t.Fatalf("Intn out of range: %d", v)
		}
		if f := r.Float64(); f < 0 || f >= 1 {
			t.Fatalf("Float64 out of range: %v", f)
		}
		// Between includes both ends.
		if b := r.Between(-5, 5); b < -5 || b > 5 {
			t.Fatalf("Between out of range: %d", b)
		}
	}
	c := r.Calls()
	if !r.Chance(1) || r.Calls() != c+1 {
		t.Fatal("Chance(1) must be true and draw once")
	}
	if r.Between(9, 3) != 9 || r.Calls() != c+1 {
		t.Fatal("Between with hi < lo must return lo and draw nothing")
	}
	var buckets [3]int
	for range 30000 {
		buckets[r.Intn(3)]++
	}
	for _, b := range buckets {
		if b < 9000 || b > 11000 {
			t.Fatalf("Intn is skewed: %v", buckets)
		}
	}
}

func TestBytes(t *testing.T) {
	a, b := rng.New(7), rng.New(7)
	var buf [20]byte
	a.Bytes(buf[:])
	u0, u1, u2 := b.Uint64(), b.Uint64(), b.Uint64()
	for i := range 8 {
		if buf[i] != byte(u0>>(8*i)) {
			t.Fatalf("byte %d", i)
		}
		if buf[8+i] != byte(u1>>(8*i)) {
			t.Fatalf("byte %d", 8+i)
		}
	}
	for i := range 4 {
		if buf[16+i] != byte(u2>>(8*i)) {
			t.Fatalf("byte %d", 16+i)
		}
	}
}
