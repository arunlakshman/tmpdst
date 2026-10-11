package sim

import "math/bits"

// Rand is the run's single seeded random stream (SplitMix64). Every random
// choice in a run (latencies, fault coins, tie-breaks, workload ops, the
// server's Env) draws from one Rand, so a seed fully determines the run.
// math/rand is banned by detcheck because its global state and algorithm are
// outside our control.
type Rand struct {
	state uint64
	calls uint64
}

// NewRand returns a stream seeded with seed.
func NewRand(seed uint64) *Rand { return &Rand{state: seed} }

// Uint64 returns the next 64 random bits.
func (r *Rand) Uint64() uint64 {
	r.calls++
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a uniform value in [0, n) without modulo bias (Lemire).
// It panics if n <= 0.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic("sim: Rand.Intn: n <= 0")
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

// Float64 returns a uniform value in [0, 1).
func (r *Rand) Float64() float64 { return float64(r.Uint64()>>11) / (1 << 53) }

// Chance reports true with probability p.
func (r *Rand) Chance(p float64) bool { return p > 0 && r.Float64() < p }

// Between returns a uniform duration in [lo, hi] (lo if hi <= lo).
func (r *Rand) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo+1)))
}

// Calls is the number of Uint64 draws so far: a cheap coordinate for
// debugging ("the runs diverged after draw 1234").
func (r *Rand) Calls() uint64 { return r.calls }
