package kvchaos_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/examples/kvchaos"
)

func TestDeterministic(t *testing.T) {
	a, err := kvchaos.Run(42)
	if err != nil {
		t.Fatal(err)
	}
	b, err := kvchaos.Run(42)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != b.Hash || a.Calls != b.Calls || a.Steps != b.Steps {
		t.Fatalf("drift: %+v vs %+v", a, b)
	}
	if len(a.Failures) != 0 {
		t.Fatalf("failures: %+v", a.Failures)
	}
}

func TestCampaign(t *testing.T) {
	const seeds = 50
	var crashed int
	for seed := uint64(1); seed <= seeds; seed++ {
		a, err := kvchaos.Run(seed)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if len(a.Failures) != 0 {
			t.Fatalf("seed %d failures: %+v", seed, a.Failures)
		}
		if a.Steps == 0 {
			t.Fatalf("seed %d: no steps", seed)
		}
		if !a.Done {
			t.Fatalf("seed %d: client did not confirm all goals", seed)
		}
		b, err := kvchaos.Run(seed)
		if err != nil {
			t.Fatalf("seed %d replay: %v", seed, err)
		}
		if a.Hash != b.Hash || a.Calls != b.Calls || a.Steps != b.Steps {
			t.Fatalf("seed %d not deterministic: %+v vs %+v", seed, a, b)
		}
		if a.Crashed {
			crashed++
		}
	}
	if crashed == 0 {
		t.Fatal("campaign never scheduled Crash+Restart")
	}
	t.Logf("campaign: %d seeds, %d with Crash+Restart", seeds, crashed)
}
