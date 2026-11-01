package simulator

import "mds/internal/core"

// stateHasher is the 64-bit FNV-1a core.Hasher plus the tag-ged folds of tags.go.
// The trace hash and Derive's label hash are built from it.
type stateHasher struct{ core.Hasher }

func newStateHasher() stateHasher { return stateHasher{core.NewHasher()} }

// fnv1aHash is the FNV-1a hash of s, without a length prefix.
func fnv1aHash(s string) uint64 {
	h := core.NewHasher()
	for _, c := range []byte(s) {
		h.Byte(c)
	}
	return h.Sum()
}
