package simulator

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/golden"
)

// goldenHashes pins named scenario/seed traces. An accidental determinism
// change fails this test in review; bump a pin only after intentional review.
var goldenHashes = map[string]uint64{
	"basic/17": 0xb8160450304fcc49,
	"chaos/42": 0xfe0e8c6763239d00,
}

// goldenWithoutGroups are the pins of the same seeds from before the consumer
// groups ran in the simulator (T6.5). With the rebalance machines and the
// consumers left out, a run is the old run step for step, so these do not move.
var goldenWithoutGroups = map[string]uint64{
	"basic/17": 0x08bfa86385d8e8ae,
	"chaos/42": 0x45d210abda0e2bed,
}

// TestGoldenWithoutGroups shows that the consumer groups are the only change
// to the trace: with them left out, the old hashes come back.
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
