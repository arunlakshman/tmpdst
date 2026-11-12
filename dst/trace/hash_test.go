package trace_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/trace"
)

func TestNewOffset(t *testing.T) {
	if got, want := trace.New().Sum(), uint64(0xcbf29ce484222325); got != want {
		t.Fatalf("offset %#x want %#x", got, want)
	}
}

func TestGoldenFolds(t *testing.T) {
	cases := []struct {
		name string
		fold func(*trace.Hash)
		want uint64
	}{
		{"Uint64(0)", func(h *trace.Hash) { h.Uint64(0) }, 0xa8c7f832281a39c5},
		{"Uint64(1)", func(h *trace.Hash) { h.Uint64(1) }, 0x89cd31291d2aefa4},
		{"Uint64(255)", func(h *trace.Hash) { h.Uint64(255) }, 0x9016b196e349a31a},
		{"Uint64(256)", func(h *trace.Hash) { h.Uint64(256) }, 0xe3757ca7d64666ea},
		{"Byte(a)", func(h *trace.Hash) { h.Byte('a') }, 0xaf63dc4c8601ec8c},
		{"String(ab)", func(h *trace.Hash) { h.String("ab") }, 0x9c6001d328a94690},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := trace.New()
			tc.fold(&h)
			if got := h.Sum(); got != tc.want {
				t.Fatalf("got %016x want %016x", got, tc.want)
			}
		})
	}
}

func TestStringIsLengthPrefixed(t *testing.T) {
	a, b := trace.New(), trace.New()
	a.String("xy")
	b.Uint64(2)
	b.Byte('x')
	b.Byte('y')
	if a != b {
		t.Fatal("String is not length prefix + bytes")
	}
	c := trace.New()
	c.Bytes([]byte("xy"))
	if c != a {
		t.Fatal("Bytes differs from String")
	}
}

func TestBoolAsUint64(t *testing.T) {
	d, e := trace.New(), trace.New()
	d.Bool(true)
	e.Uint64(1)
	if d != e {
		t.Fatal("Bool(true) is not Uint64(1)")
	}
}

func TestFnvMatchesBytewise(t *testing.T) {
	slow := func(x, v uint64) uint64 {
		for i := range 8 {
			x = (x ^ (v>>(8*i))&0xff) * 1099511628211
		}
		return x
	}
	x := uint64(14695981039346656037)
	for i := range 20000 {
		v := uint64(i) * 0x9e3779b97f4a7c15 >> (uint(i) % 64)
		if i%7 == 0 {
			v = uint64(i)
		}
		h := trace.Hash(x)
		h.Uint64(v)
		if got, want := h.Sum(), slow(x, v); got != want {
			t.Fatalf("v=%#x: %#x != %#x", v, got, want)
		}
		x = slow(x, v)
	}
}

func TestRingTail(t *testing.T) {
	r := trace.NewRing[int](3)
	for i := 1; i <= 5; i++ {
		r.Add(i)
	}
	got := r.Tail()
	want := []int{3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("len %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
