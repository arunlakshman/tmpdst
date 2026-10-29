package simulator

import (
	"math/bits"

	"manifester/internal/core"
)

// splitMix is the run's counted stream: SplitMix64 with a draw counter. One
// stream per run; Derive gives independent side streams (swarm knobs, buggify
// activation) so editing a menu never reshuffles the run stream (decision 17).
type splitMix struct {
	seed  uint64 // what Derive mixes with; the seed given to NewRand or Reseed
	state uint64
	calls uint64
}

// NewRand returns a counted SplitMix64 stream seeded with seed.
func NewRand(seed uint64) CountedRand { return &splitMix{seed: seed, state: seed} }

// mixBits64 is the SplitMix64 output finalizer (a bijection on uint64).
func mixBits64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *splitMix) Uint64() uint64 {
	r.calls++
	r.state += 0x9e3779b97f4a7c15
	return mixBits64(r.state)
}

// below returns a uniform value in [0, bound) without modulo bias (Lemire).
func (r *splitMix) below(bound uint64) uint64 {
	hi, lo := bits.Mul64(r.Uint64(), bound)
	if lo < bound {
		threshold := -bound % bound
		for lo < threshold {
			hi, lo = bits.Mul64(r.Uint64(), bound)
		}
	}
	return hi
}

func (r *splitMix) Intn(n int) int {
	if n <= 0 {
		panic("sim: Intn: n <= 0")
	}
	return int(r.below(uint64(n)))
}

func (r *splitMix) Float64() float64 { return float64(r.Uint64()>>11) / (1 << 53) }

func (r *splitMix) Chance(p float64) bool { return p > 0 && r.Float64() < p }

// Between returns lo when hi <= lo, without drawing.
func (r *splitMix) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.below(uint64(hi-lo)))
}

func (r *splitMix) NewID() core.ID {
	var id core.ID
	a, b := r.Uint64(), r.Uint64()
	for i := range 8 {
		id[i] = byte(a >> (8 * i))
		id[8+i] = byte(b >> (8 * i))
	}
	return id
}

// Derive mixes this stream's seed with FNV-1a(label). It reads no state and
// draws nothing.
func (r *splitMix) Derive(label string) CountedRand {
	return NewRand(mixBits64(r.seed ^ mixBits64(fnv1aHash(label))))
}

func (r *splitMix) Calls() uint64 { return r.calls }

// Reseed restarts the sequence from seed; Calls keeps counting.
func (r *splitMix) Reseed(seed uint64) { r.seed, r.state = seed, seed }

func (r *splitMix) Fingerprint() uint64 { return mixBits64(r.state ^ mixBits64(r.calls)) }

// Sample draws one delay: a coin for the slow tail, then a uniform value in
// the chosen inclusive range. Ranges with Max <= Min draw nothing.
func (t FastWithRareSlow) Sample(r CountedRand) core.Duration {
	lo, hi := t.FastMin, t.FastMax
	if r.Chance(t.SlowProb) {
		lo, hi = t.SlowMin, t.SlowMax
	}
	if hi <= lo {
		return lo
	}
	return core.Duration(r.Between(int64(lo), int64(hi)+1))
}

var (
	_ CountedRand = (*splitMix)(nil)
	_ Latency     = FastWithRareSlow{}
)
