package sim

import "manifester/core"

// CountedRand is the single seeded stream (lesson 3). Every draw in a run,
// including engine latencies, fault coins, tie-breaks and swarm masks, comes
// from it.
type CountedRand interface {
	core.Rand
	// Float64 returns a value in [0, 1).
	Float64() float64
	// Calls is the number of draws so far; the coordinate for debugging and exploration.
	Calls() uint64
	// Reseed switches to a new seed from this point on (exploration recipes).
	Reseed(seed uint64)
	// Fingerprint is a probe of the stream state taken without consuming a draw;
	// the determinism canary compares these between two runs of one seed.
	Fingerprint() uint64
}
