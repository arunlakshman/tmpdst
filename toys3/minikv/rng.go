package minikv

// RNG is the run's only source of randomness: SplitMix64, seeded once.
// math/rand is the wrong tool here. Its global generator is shared with
// the rest of the process, and its algorithm is not ours to freeze.
type RNG struct {
	state uint64
	calls uint64
}

// NewRNG returns a stream seeded with seed. Seed 0 is a fine seed;
// the first step adds a constant, so the state does not stay zero.
func NewRNG(seed uint64) *RNG { return &RNG{state: seed} }

// Uint64 returns the next 64 bits. This is SplitMix64.
func (r *RNG) Uint64() uint64 {
	r.calls++
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a value in [0, n). The modulo is slightly biased when n is
// not a power of two. For latencies of a few hundred that bias does not
// matter; a production harness should use an unbiased method.
func (r *RNG) Intn(n int) int {
	if n <= 0 {
		panic("minikv: Intn n <= 0")
	}
	return int(r.Uint64() % uint64(n))
}

// Calls is how many Uint64 draws this stream has made. When two runs of
// one seed diverge, this number says how far they got together.
func (r *RNG) Calls() uint64 { return r.calls }
