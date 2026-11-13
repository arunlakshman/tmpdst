package span_test

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/span"
	"github.com/manifesterio/mds/dst/rng"
)

// Between must leave out the upper end.
func TestBetweenIsHalfOpen(t *testing.T) {
	r := rng.New(1)
	for range 1000 {
		if v := span.Between(r, 5, 9); v < 5 || v > 8 {
			t.Fatalf("out of [5,9): %d", v)
		}
	}
}

// Between must draw once whenever hi > lo, and never when hi <= lo.
func TestBetweenDrawCounts(t *testing.T) {
	cases := []struct {
		lo, hi int64
		draws  uint64
	}{
		{5, 5, 0}, // empty range: no draw
		{9, 5, 0}, // reversed range: no draw
		{5, 6, 1}, // one value, but still one draw (the old rule)
		{0, 100, 1},
	}
	for _, c := range cases {
		r := rng.New(7)
		got := span.Between(r, c.lo, c.hi)
		if r.Calls() != c.draws {
			t.Fatalf("[%d,%d): %d draws, want %d", c.lo, c.hi, r.Calls(), c.draws)
		}
		if c.hi <= c.lo+1 && got != c.lo {
			t.Fatalf("[%d,%d): got %d, want lo", c.lo, c.hi, got)
		}
	}
}

// The old formula: lo + Intn(hi-lo), with Intn from the same stream.
func TestBetweenMatchesOldFormula(t *testing.T) {
	a, b := rng.New(42), rng.New(42)
	for range 200 {
		want := int64(10) + int64(b.Intn(90))
		if got := span.Between(a, 10, 100); got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	}
}
