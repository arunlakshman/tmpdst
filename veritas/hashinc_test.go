package veritas

import (
	"fmt"
	"testing"

	"manifester/core"
)

// lcg is a tiny deterministic generator for op sequences.
type lcg uint64

func (r *lcg) n(m int) int {
	*r = *r*6364136223846793005 + 1442695040888963407
	return int(uint64(*r)>>33) % m
}

// TestIncrementalStateHashMatchesFull drives random op sequences and checks
// the prefix-cached StateHash against a from-scratch fold, hashing at random
// points so several changes can pile up between two hashes.
func TestIncrementalStateHashMatchesFull(t *testing.T) {
	for seed := 1; seed <= 50; seed++ {
		r := lcg(seed)
		s, c := NewObjectStore(), NewControlPlane()
		next := map[int32]core.Offset{}
		for step := 0; step < 300; step++ {
			switch r.n(5) {
			case 0, 1:
				put(s, fmt.Sprintf("seg/%d", r.n(40)), fmt.Sprint(r.n(1000)))
			case 2:
				s.Exec(core.ObjDelete{Req: 3, Key: fmt.Sprintf("seg/%d", r.n(40))})
			default:
				// Fresh keys at the right base, plus stale ones that must fail without a trace.
				p := int32(r.n(3))
				n := int64(1 + r.n(5))
				base := next[p]
				if r.n(4) == 0 && base > 0 {
					base--
				}
				if d := commit(c, fmt.Sprintf("k/%d", r.n(40)), pb("t", p, base, n)); d.Err == core.OK {
					next[p] = base + core.Offset(n)
				}
			}
			if r.n(3) == 0 {
				if got, want := s.StateHash(), s.fullStateHash(); got != want {
					t.Fatalf("seed %d step %d: object store %x != full %x", seed, step, got, want)
				}
				if got, want := c.StateHash(), c.fullStateHash(); got != want {
					t.Fatalf("seed %d step %d: control plane %x != full %x", seed, step, got, want)
				}
			}
		}
	}
}
