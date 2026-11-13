// Package span draws a number from a half-open range, like the Between of the old
// broker random stream.
//
// dst/rng.Between includes both ends. The old broker Between left out the upper end.
// The fake services must draw the same values in the same number of draws as before,
// so they use Between of this package where the old code used Between.
package span

import "github.com/manifesterio/mds/dst/rng"

// Between returns a value in [lo, hi). If hi <= lo it returns lo and draws
// nothing. If hi == lo+1 it still draws once (the value is lo), as the old Between
// did. Plain rng.Between(lo, hi-1) would draw nothing in that case.
func Between(r *rng.Rand, lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo)))
}
