package core

import "testing"

// TestFnvU64MatchesBytewise: the zero-byte shortcut folds exactly the 8
// little-endian bytes one at a time.
func TestFnvU64MatchesBytewise(t *testing.T) {
	slow := func(x, v uint64) uint64 {
		for i := range 8 {
			x = (x ^ (v >> (8 * i) & 0xff)) * 1099511628211
		}
		return x
	}
	x := uint64(14695981039346656037)
	for i := range 20000 {
		v := uint64(i) * 0x9e3779b97f4a7c15 >> (uint(i) % 64) // every byte width
		if i%7 == 0 {
			v = uint64(i)
		}
		if got, want := FnvU64(x, v), slow(x, v); got != want {
			t.Fatalf("v=%#x: %#x != %#x", v, got, want)
		}
		x = slow(x, v)
	}
	for _, v := range []uint64{0, 1, 255, 256, 1<<64 - 1, 1 << 63} {
		if FnvU64(x, v) != slow(x, v) {
			t.Fatalf("v=%#x", v)
		}
	}
}
