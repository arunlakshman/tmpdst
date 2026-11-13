package spec

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
)

// lcg is a tiny deterministic generator for op sequences.
type lcg uint64

func (r *lcg) n(m int) int {
	*r = *r*6364136223846793005 + 1442695040888963407
	return int(uint64(*r)>>33) % m
}

// TestIncrementalStateHashMatchesFull drives random commits and checks the
// prefix-cached StateHash against a from-scratch fold, hashing at random
// points so several changes can pile up between two hashes.
func TestIncrementalStateHashMatchesFull(t *testing.T) {
	for seed := 1; seed <= 50; seed++ {
		r := lcg(seed)
		m := New()
		for step := range 300 {
			commit(m, fmt.Sprintf("k/%d", r.n(40)), batch("t", int32(r.n(3)), int64(1+r.n(5))))
			if r.n(3) == 0 {
				if got, want := m.StateHash(), m.fullStateHash(); got != want {
					t.Fatalf("seed %d step %d: hash %x != full %x", seed, step, got, want)
				}
			}
		}
	}
}

// TestStateHashSeesSegmentsAndTopics checks that a new segment and a new
// topic each move the hash.
func TestStateHashSeesSegmentsAndTopics(t *testing.T) {
	m := New()
	empty := m.StateHash()
	commit(m, "s1", batch("t", 0, 2))
	afterCommit := m.StateHash()
	if afterCommit == empty {
		t.Fatal("a commit did not move the hash")
	}
	m.Apply(contract.CreateTopic{Name: "u", Partitions: 2})
	if m.StateHash() == afterCommit {
		t.Fatal("a new topic did not move the hash")
	}
}
