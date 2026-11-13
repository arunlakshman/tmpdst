package checker

import (
	"testing"

	"github.com/manifesterio/mds/dst/rng"
)

// checker.Rand is a copy of the dst stream, so that the speccheck tests do not import the
// simulation. This test fails if the two copies ever give different values. checker.Rand
// has no Derive method, so only the plain stream is compared.
func TestRandMatchesDstStream(t *testing.T) {
	for _, seed := range []uint64{0, 1, 7, 42, 1 << 40, ^uint64(0)} {
		dst := rng.New(seed)
		spec := NewRand(seed)
		for i := range 3000 {
			if d, s := dst.Uint64(), spec.Uint64(); d != s {
				t.Fatalf("seed %d, draw %d: dst/rng gave %#x, checker.Rand gave %#x.", seed, i, d, s)
			}
		}
	}
}
