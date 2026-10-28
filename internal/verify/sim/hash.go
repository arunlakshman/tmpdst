package sim

import "manifester/internal/core"

// hasher is the 64-bit FNV-1a core.Hasher plus the tag-ged folds of tags.go.
// The trace hash and Derive's label hash are built from it.
type hasher struct{ core.Hasher }

func newHasher() hasher { return hasher{core.NewHasher()} }

// fnv1a is the FNV-1a hash of s, without a length prefix.
func fnv1a(s string) uint64 {
	h := core.NewHasher()
	for _, c := range []byte(s) {
		h.Byte(c)
	}
	return h.Sum()
}
