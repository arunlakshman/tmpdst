package minikv

import (
	"slices"
	"testing"
)

func TestDeterminism(t *testing.T) {
	a := Run(1)
	b := Run(1)
	if a.Hash != b.Hash || a.Calls != b.Calls || !slices.Equal(a.Log, b.Log) {
		t.Fatalf("seed 1 diverged:\n  hash %x vs %x\n  calls %d vs %d", a.Hash, b.Hash, a.Calls, b.Calls)
	}
	c := Run(2)
	if a.Hash == c.Hash {
		t.Fatal("seeds 1 and 2 produced the same trace hash")
	}

	got := map[uint64]Reply{}
	for _, r := range a.Replies {
		got[r.Client] = r
	}
	want := []Reply{
		{Client: 1, Status: 200},
		{Client: 2, Status: 200, Body: []byte("one")},
		{Client: 3, Status: 200},
		{Client: 4, Status: 200, Body: []byte("one")},
		{Client: 5, Status: 404},
	}
	for _, w := range want {
		r, ok := got[w.Client]
		if !ok {
			t.Fatalf("missing reply for c%d", w.Client)
		}
		if r.Status != w.Status || !slices.Equal(r.Body, w.Body) {
			t.Fatalf("c%d: got status %d body %q, want status %d body %q", w.Client, r.Status, r.Body, w.Status, w.Body)
		}
	}

	t.Logf("seed 1 hash %016x  steps %d  rng calls %d", a.Hash, a.Steps, a.Calls)
	for _, line := range a.Log {
		t.Log(line)
	}
}

// hashMap is the bug, written on purpose. Ranging a Go map follows an order
// the runtime picks afresh on every range, so two calls need not agree.
func hashMap(m map[string]int) uint64 {
	h := uint64(14695981039346656037)
	for k, v := range m {
		h = hashStr(h, k)
		h = fold(h, uint64(v))
	}
	return h
}

func TestMapRangeLeaks(t *testing.T) {
	m := map[string]int{}
	for i := 0; i < 16; i++ {
		m[string(rune('a'+i))] = i
	}
	seen := map[uint64]struct{}{}
	for i := 0; i < 40; i++ {
		seen[hashMap(m)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("map iteration did not vary; the leak demo failed to leak")
	}
}
