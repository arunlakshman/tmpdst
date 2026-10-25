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

// FnvU64 folds v as 8 little-endian FNV-1a bytes into x. A zero byte only
// multiplies, so leading zero bytes collapse into one multiply by fnvP^k:
// same value, ~4x fewer dependent multiplies for the small integers that dominate.
func FnvU64(x, v uint64) uint64 {
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
