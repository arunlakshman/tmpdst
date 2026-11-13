package conformance

import (
	"math/bits"

	"pgregory.net/rapid"
)

// Rand is where the generators get their random whole numbers. A test run
// uses a drawer, which draws from rapid. The helper Batches uses a plain
// seeded stream (dst/rng), because its answer must depend only on its
// arguments.
type Rand interface {
	// Intn returns a whole number from 0 to n-1, and every value has the same
	// chance. It needs n >= 1.
	Intn(n int) int
}

// bit draws one fair bit. rapid.Bool is a fair coin; the integer generators of
// rapid are not fair on purpose (they favor small and edge values).
var bit = rapid.Bool()

// drawer gives the generators numbers from one rapid.T.
type drawer struct{ t *rapid.T }

// Intn draws as many fair bits as the number n-1 has, and draws again when the
// result is n or more, so every value in [0, n) has the same chance. The
// profile weights are chances, and a biased draw would change them. Rapid
// shrinks the bits toward 0, so a shrunk case has small numbers.
func (d drawer) Intn(n int) int {
	if n < 1 {
		panic("conformance: Intn needs n >= 1")
	}
	width := bits.Len(uint(n - 1))
	for {
		v := 0
		for i := range width {
			if bit.Draw(d.t, "bit") {
				v |= 1 << i
			}
		}
		if v < n {
			return v
		}
	}
}

// chooseByWeight returns the index of one weight, chosen with a chance in
// proportion to the weights. The weights must not all be zero. Rapid shrinks
// toward index 0, so put the simplest kind first.
func chooseByWeight(r Rand, weights ...int) int {
	total := 0
	for _, w := range weights {
		total += w
	}
	n := r.Intn(total)
	for i, w := range weights {
		if n < w {
			return i
		}
		n -= w
	}
	return len(weights) - 1
}

// percent returns true with a chance of p out of 100. The rare answer is the
// high end of the draw, so that rapid shrinks toward false (the plain case).
func percent(r Rand, p int) bool { return r.Intn(100) >= 100-p }

// pick returns one element of items.
func pick[T any](r Rand, items []T) T { return items[r.Intn(len(items))] }

// Custom makes a rapid generator from a function that gets numbers from a
// drawer.
func Custom[V any](gen func(r Rand) V) *rapid.Generator[V] {
	return rapid.Custom(func(t *rapid.T) V { return gen(drawer{t}) })
}
