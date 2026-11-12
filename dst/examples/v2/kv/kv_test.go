package kv

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/golden"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

func TestDeterministic(t *testing.T) {
	a, b := Run(42), Run(42)
	if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
		t.Fatalf("seed 42 drifted: %x/%d/%d vs %x/%d/%d", a.Hash, a.RandCalls, a.Steps, b.Hash, b.RandCalls, b.Steps)
	}
	if a.Steps == 0 || !a.OK() {
		t.Fatalf("steps=%d failures=%+v", a.Steps, a.Failures)
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	a, b := Run(1), Run(2)
	if a.Hash == b.Hash && a.RandCalls == b.RandCalls {
		t.Fatal("two seeds gave the same result")
	}
}

// TestCampaign runs many seeds, each one twice. It fails only on a failure or
// on a seed that does not repeat. Two seeds with the same hash are not a
// problem.
func TestCampaign(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		a := Run(seed)
		if !a.OK() || a.Steps == 0 {
			t.Fatalf("seed %d: steps=%d failures=%+v", seed, a.Steps, a.Failures)
		}
		if c := a.Coverage[readMatches.Name()]; c.Checks != 1 || c.Passes != 1 {
			t.Fatalf("seed %d: read check %+v", seed, c)
		}
		b := Run(seed)
		if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d does not repeat", seed)
		}
	}
}

// goldenHashes pin the trace hash of seeds 1 to 3. A change here means the
// kernel or the example changed behavior.
var goldenHashes = map[string]uint64{
	"kv/1": 0x54fe7c2c349cf68f,
	"kv/2": 0x89317de789cbb955,
	"kv/3": 0x1ecfe9e5fbf7d1b9,
}

func TestGoldenHashes(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		golden.Assert(t, fmt.Sprintf("kv/%d", seed), Run(seed).Hash, goldenHashes)
	}
}

func TestTagsAreUnique(t *testing.T) {
	msgs := []interface {
		machine.Message
		Tag() uint64
	}{
		begin{}, nextOp{}, getResponse{}, sendPut{}, sendGet{}, waitNext{},
		putRequest{}, getRequest{}, getDone{}, storePut{}, storeGet{}, reply{},
	}
	seen := map[uint64]bool{}
	for _, m := range msgs {
		if seen[m.Tag()] {
			t.Fatalf("tag %d is used twice", m.Tag())
		}
		seen[m.Tag()] = true
		h := trace.New()
		m.Fold(&h)
		if m.Tag() == 0 || h == trace.New() {
			t.Fatalf("%T does not fold its tag", m)
		}
	}
}

func ExampleRun() {
	res := Run(42)
	fmt.Println(res.OK(), res.Steps > 0)
	// Output:
	// true true
}
