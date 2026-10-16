package sim

// hasher is 64-bit FNV-1a written out by hand (hash/fnv is off the allow-list).
// The trace hash and Derive's label hash are built from it. Integers are folded
// as 8 little-endian bytes; strings and byte slices are length-prefixed so
// adjacent fields cannot run together.
type hasher struct{ sum uint64 }

const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

func newHasher() hasher { return hasher{sum: fnvOffset} }

func (h *hasher) byte(b byte) {
	h.sum ^= uint64(b)
	h.sum *= fnvPrime
}

func (h *hasher) u64(x uint64) {
	for i := 0; i < 8; i++ {
		h.byte(byte(x >> (8 * i)))
	}
}

func (h *hasher) i64(x int64) { h.u64(uint64(x)) }

func (h *hasher) bool(b bool) {
	if b {
		h.byte(1)
	} else {
		h.byte(0)
	}
}

func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}

func (h *hasher) bytes(b []byte) {
	h.u64(uint64(len(b)))
	for _, c := range b {
		h.byte(c)
	}
}

// fnv1a is the FNV-1a hash of s.
func fnv1a(s string) uint64 {
	h := newHasher()
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
	return h.sum
}
