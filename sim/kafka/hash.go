package kafka

import "math/bits"

// hasher is FNV-1a; hash/fnv is off the deterministic allow-list.
type hasher uint64

func newHasher() hasher { return 14695981039346656037 }

func (h *hasher) byte(b byte)  { *h = (*h ^ hasher(b)) * 1099511628211 }
func (h *hasher) u64(v uint64) { *h = hasher(fnvU64(uint64(*h), v)) }
func (h *hasher) i64(v int64)  { h.u64(uint64(v)) }
func (h *hasher) bool(b bool) {
	if b {
		h.u64(1)
	} else {
		h.u64(0)
	}
}
func (h *hasher) bytes(b []byte) {
	h.u64(uint64(len(b)))
	for _, c := range b {
		h.byte(c)
	}
}
func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}

// fnvP^k mod 2^64, for the zero high bytes of a little-endian u64.
const (
	fnvP1 = 1099511628211
	fnvP2 = (1099511628211 * 1099511628211) % (1 << 64)
	fnvP3 = (fnvP2 * 1099511628211) % (1 << 64)
	fnvP4 = (fnvP3 * 1099511628211) % (1 << 64)
	fnvP5 = (fnvP4 * 1099511628211) % (1 << 64)
	fnvP6 = (fnvP5 * 1099511628211) % (1 << 64)
	fnvP7 = (fnvP6 * 1099511628211) % (1 << 64)
	fnvP8 = (fnvP7 * 1099511628211) % (1 << 64)
)

// fnvU64 folds v as 8 little-endian bytes. A zero byte only multiplies, so the
// leading zero bytes collapse into one multiply by fnvP^k: same value, ~4x
// fewer dependent multiplies for the small integers that dominate.
func fnvU64(x, v uint64) uint64 {
	n := (bits.Len64(v) + 7) / 8
	for i := 0; i < n; i++ {
		x = (x ^ (v & 0xff)) * fnvP1
		v >>= 8
	}
	switch 8 - n {
	case 1:
		return x * fnvP1
	case 2:
		return x * fnvP2
	case 3:
		return x * fnvP3
	case 4:
		return x * fnvP4
	case 5:
		return x * fnvP5
	case 6:
		return x * fnvP6
	case 7:
		return x * fnvP7
	case 8:
		return x * fnvP8
	}
	return x
}
