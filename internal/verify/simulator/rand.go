package simulator

import "mds/internal/core"

// CountedRand is the single seeded stream (lesson 3). Every draw in a run,
// including engine latencies, fault coins and tie-breaks, comes from it. RandomSettingsPicker
// knobs and per-site buggify activation use Derive, never the run stream, so
// editing a menu or adding a site does not reshuffle every seed.
type CountedRand interface {
	core.Rand
	// Float64 returns a value in [0, 1).
	Float64() float64
	// Chance reports true with probability p. It draws nothing when p <= 0.
	Chance(p float64) bool
	// Between returns a value in [lo, hi).
	Between(lo, hi int64) int64
	// Derive returns an independent stream from (seed, label). It does not
	// consume a draw from this stream.
	Derive(label string) CountedRand
	// Calls is the number of draws so far; the coordinate for debugging and exploration.
	Calls() uint64
	// Reseed switches to a new seed from this point on (exploration recipes).
	Reseed(seed uint64)
	// Fingerprint is a probe of the stream state taken without consuming a draw;
	// the determinism canary compares these between two runs of one seed.
	Fingerprint() uint64
}
