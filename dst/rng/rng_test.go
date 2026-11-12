package rng_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/rng"
)

func TestSeed42Stream(t *testing.T) {
	want := []uint64{
		0xbdd732262feb6e95,
		0x28efe333b266f103,
		0x47526757130f9f52,
		0x581ce1ff0e4ae394,
		0x09bc585a244823f2,
		0xde4431fa3c80db06,
		0x37e9671c45376d5d,
		0xccf635ee9e9e2fa4,
	}
	r := rng.New(42)
	for i, w := range want {
		if got := r.Uint64(); got != w {
			t.Fatalf("draw %d: got %016x want %016x", i, got, w)
		}
	}
	if r.Calls() != 8 {
		t.Fatalf("Calls = %d, want 8", r.Calls())
	}
}

func TestNoDrawEdges(t *testing.T) {
	r := rng.New(42)
	_ = r.Float64()
	_ = r.Intn(10)
	before := r.Calls()
	if r.Chance(0) {
		t.Fatal("Chance(0) true")
	}
	if r.Calls() != before {
		t.Fatal("Chance(0) drew")
	}
	if r.Intn(0) != 0 || r.Calls() != before {
		t.Fatal("Intn(0) drew or wrong")
	}
	if got := r.Between(5, 5); got != 5 || r.Calls() != before {
		t.Fatal("Between(5,5) drew or wrong")
	}
}

func TestBytes(t *testing.T) {
	a, b := rng.New(7), rng.New(7)
	var buf [20]byte
	a.Bytes(buf[:])
	u0, u1, u2 := b.Uint64(), b.Uint64(), b.Uint64()
	for i := range 8 {
		if buf[i] != byte(u0>>(8*i)) {
			t.Fatalf("byte %d", i)
		}
		if buf[8+i] != byte(u1>>(8*i)) {
			t.Fatalf("byte %d", 8+i)
		}
	}
	for i := range 4 {
		if buf[16+i] != byte(u2>>(8*i)) {
			t.Fatalf("byte %d", 16+i)
		}
	}
}
