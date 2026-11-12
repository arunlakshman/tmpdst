package core

import "math/bits"

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

// Fnv64Hash folds v as 8 little-endian FNV-1a bytes into x. A zero byte only
// multiplies, so leading zero bytes collapse into one multiply by fnvP^k:
// same value, ~4x fewer dependent multiplies for the small integers that dominate.
func Fnv64Hash(x, v uint64) uint64 {
	n := (bits.Len64(v) + 7) / 8
	for range n {
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

// Hasher is a running 64-bit FNV-1a hash (hash/fnv is off the deterministic
// allow-list). Integers fold as 8 little-endian bytes; strings and byte
// slices are length-prefixed so adjacent fields cannot run together.
type Hasher uint64

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = fnvP1
)

// NewHasher returns the FNV-1a offset basis.
func NewHasher() Hasher { return fnvOffset }

// Sum is the hash so far.
func (h Hasher) Sum() uint64 { return uint64(h) }

// Byte folds one byte.
func (h *Hasher) Byte(b byte) { *h = (*h ^ Hasher(b)) * fnvPrime }

// Uint64Value folds v as 8 little-endian bytes.
func (h *Hasher) Uint64Value(v uint64) { *h = Hasher(Fnv64Hash(uint64(*h), v)) }

// Int64Value folds v as Uint64Value does.
func (h *Hasher) Int64Value(v int64) { h.Uint64Value(uint64(v)) }

// Bool folds 1 or 0 as Uint64Value does.
func (h *Hasher) Bool(b bool) {
	if b {
		h.Uint64Value(1)
	} else {
		h.Uint64Value(0)
	}
}

// StringValue folds the length of s, then its bytes.
func (h *Hasher) StringValue(s string) {
	h.Uint64Value(uint64(len(s)))
	for _, c := range []byte(s) {
		h.Byte(c)
	}
}

// Bytes folds the length of b, then its bytes.
func (h *Hasher) Bytes(b []byte) {
	h.Uint64Value(uint64(len(b)))
	for _, c := range b {
		h.Byte(c)
	}
}
