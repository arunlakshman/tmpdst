// Package rng is the single seeded random stream (SplitMix64) of one run.
package rng

import "math/bits"

// Rand is the single seeded stream of one run (SplitMix64).
// Invariant: same seed and same call sequence produce the same values.
// Not safe for concurrent use. One Rand per run, owned by the caller.
type Rand struct {
	state uint64
	calls uint64
}

// New returns a stream for seed. Seed 0 is a valid seed.
func New(seed uint64) *Rand { return &Rand{state: seed} }

func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Uint64 returns the next value.
func (r *Rand) Uint64() uint64 {
	r.calls++
	r.state += 0x9e3779b97f4a7c15
	return mix64(r.state)
}

// Intn returns a value in [0, n). It returns 0 and draws nothing when n <= 0.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	bound := uint64(n)
	hi, lo := bits.Mul64(r.Uint64(), bound)
	if lo < bound {
		threshold := -bound % bound
		for lo < threshold {
			hi, lo = bits.Mul64(r.Uint64(), bound)
		}
	}
	return int(hi)
}

// Float64 returns a value in [0, 1).
func (r *Rand) Float64() float64 { return float64(r.Uint64()>>11) / (1 << 53) }

// Chance is true with probability p. It draws nothing when p <= 0.
func (r *Rand) Chance(p float64) bool { return p > 0 && r.Float64() < p }

// Between returns a value in [lo, hi]. It returns lo, and draws nothing, when hi <= lo.
func (r *Rand) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo+1)))
}

// Calls is the number of draws so far.
func (r *Rand) Calls() uint64 { return r.calls }

// Bytes fills b from the stream (two Uint64 per 16 bytes, partial last block).
func (r *Rand) Bytes(b []byte) {
	for len(b) > 0 {
		v := r.Uint64()
		n := min(8, len(b))
		for i := range n {
			b[i] = byte(v >> (8 * i))
		}
		b = b[n:]
	}
}
