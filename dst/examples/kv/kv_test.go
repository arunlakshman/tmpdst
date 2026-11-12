package kv_test

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/examples/kv"
)

func TestDeterministic(t *testing.T) {
	a, err := kv.Run(42)
	if err != nil {
		t.Fatal(err)
	}
	b, err := kv.Run(42)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != b.Hash {
		t.Fatalf("hash %016x != %016x", a.Hash, b.Hash)
	}
	if a.Calls != b.Calls {
		t.Fatalf("Calls %d != %d", a.Calls, b.Calls)
	}
	if a.Steps != b.Steps {
		t.Fatalf("Steps %d != %d", a.Steps, b.Steps)
	}
	if a.Steps == 0 {
		t.Fatal("no steps")
	}
	if len(a.Failures) != 0 {
		t.Fatalf("failures: %+v", a.Failures)
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	a, err := kv.Run(1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := kv.Run(2)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash == b.Hash && a.Calls == b.Calls {
		t.Fatal("distinct seeds produced identical Result")
	}
}

// TestCampaign is the DST loop: many seeds, each replayed once for hash parity.
// Fail only on protocol errors or assertion failures. Cross-seed hash collisions
// are not campaign signal.
func TestCampaign(t *testing.T) {
	const seeds = 100
	for seed := uint64(1); seed <= seeds; seed++ {
		a, err := kv.Run(seed)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if len(a.Failures) != 0 {
			t.Fatalf("seed %d failures: %+v", seed, a.Failures)
		}
		if a.Steps == 0 {
			t.Fatalf("seed %d: no steps", seed)
		}
		b, err := kv.Run(seed)
		if err != nil {
			t.Fatalf("seed %d replay: %v", seed, err)
		}
		if a.Hash != b.Hash || a.Calls != b.Calls || a.Steps != b.Steps {
			t.Fatalf("seed %d not deterministic: %+v vs %+v", seed, a, b)
		}
	}
}

func ExampleRun() {
	res, err := kv.Run(42)
	if err != nil || len(res.Failures) != 0 {
		fmt.Println("fail")
		return
	}
	if res.Steps == 0 {
		fmt.Println("fail")
		return
	}
	fmt.Println("ok")
	// Output:
	// ok
}
