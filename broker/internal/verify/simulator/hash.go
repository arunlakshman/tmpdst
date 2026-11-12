package simulator

import (
	"github.com/manifesterio/mds/dst/trace"
)

// stateHasher is the 64-bit FNV-1a trace.Hash plus the tag-ged folds of tags.go.
// The trace hash and Derive's label hash are built from it.
type stateHasher struct{ trace.Hash }

func newStateHasher() stateHasher { return stateHasher{trace.New()} }

// fnv1aHash is the FNV-1a hash of s, without a length prefix.
func fnv1aHash(s string) uint64 {
	h := trace.New()
	for _, c := range []byte(s) {
		h.Byte(c)
	}
	return h.Sum()
}
