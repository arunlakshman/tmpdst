package simulator

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/golden"
)

// goldenHashes pins named scenario/seed traces. An accidental determinism
// change fails this test in review; bump a pin only after intentional review.
var goldenHashes = map[string]uint64{
	"basic/17": 0x337ae38013d6ceff,
	"chaos/42": 0x7d8e4e6f2cc72b2f,
}

// goldenWithoutGroups are the pins of the same seeds with the rebalance machines
// and the consumers left out (the run of before the consumer groups were added,
// T6.5). They moved once, with the others, when the host code moved to the dst
// kernel (T1.11c).
var goldenWithoutGroups = map[string]uint64{
	"basic/17": 0x798ef51af00c8c5f,
	"chaos/42": 0xef9a9a341d46a6ca,
}

// TestGoldenWithoutGroups pins the run with the consumer groups left out.
func TestGoldenWithoutGroups(t *testing.T) {
	for _, c := range []struct {
		scenario string
		seed     uint64
	}{{"basic", 17}, {"chaos", 42}} {
		name := fmt.Sprintf("%s/%d", c.scenario, c.seed)
		r := mustRun(t, Config{Scenario: c.scenario, Seed: c.seed, noGroups: true})
		if !r.OK() {
			t.Fatal(Report(r))
		}
		golden.Assert(t, name, r.Hash, goldenWithoutGroups)
	}
}

// TestGoldenTraceHashes pins a few seeds so a silent determinism drift fails CI.
// TestSameSeedSameHash only compares a seed with itself.
func TestGoldenTraceHashes(t *testing.T) {
	cases := []struct {
		scenario string
		seed     uint64
	}{
		{"basic", 17},
		{"chaos", 42},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%s/%d", c.scenario, c.seed)
		r := mustRun(t, Config{Scenario: c.scenario, Seed: c.seed})
		if !r.OK() {
			t.Fatal(Report(r))
		}
		golden.Assert(t, name, r.Hash, goldenHashes)
	}
}
