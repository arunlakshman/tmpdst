package kvchaos

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
	if !a.OK() {
		t.Fatalf("failures: %+v", a.Failures)
	}
}

func TestCampaign(t *testing.T) {
	const seeds = 50
	crashed := 0
	var dropped, uncertain uint64
	for seed := uint64(1); seed <= seeds; seed++ {
		a := Run(seed)
		if !a.OK() || a.Steps == 0 {
			t.Fatalf("seed %d: steps=%d failures=%+v", seed, a.Steps, a.Failures)
		}
		if !a.Done {
			t.Fatalf("seed %d: the client did not confirm every goal", seed)
		}
		b := Run(seed)
		if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d does not repeat", seed)
		}
		if a.Crashed {
			crashed++
			if a.Crashes != 1 {
				t.Fatalf("seed %d: the supervisor ran but crashes=%d", seed, a.Crashes)
			}
		}
		dropped += a.Coverage[writeDropped.Name()].Passes
		uncertain += a.Coverage[putUncertain.Name()].Passes
	}
	if crashed == 0 || crashed == seeds {
		t.Fatalf("the supervisor crashed the node on %d of %d seeds; want some, not all", crashed, seeds)
	}
	if dropped == 0 || uncertain == 0 {
		t.Fatalf("dropped=%d uncertain=%d: the faults never happened", dropped, uncertain)
	}
	t.Logf("campaign: %d seeds, %d with a crash and a restart", seeds, crashed)
}

// goldenHashes pin the trace hash of seeds 1 to 3. A change here means the
// kernel or the example changed behavior.
var goldenHashes = map[string]uint64{
	"kvchaos/1": 0xd6a2d56e6c88a026,
	"kvchaos/2": 0xa426daa41d8d221c,
	"kvchaos/3": 0xf0df766fb40afee6,
}

func TestGoldenHashes(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		golden.Assert(t, fmt.Sprintf("kvchaos/%d", seed), Run(seed).Hash, goldenHashes)
	}
}

func TestTagsAreUnique(t *testing.T) {
	msgs := []interface {
		machine.Message
		Tag() uint64
	}{
		begin{}, retry{}, getProbe{}, getResponse{}, sendPut{}, sendGet{}, arm{Msg: retry{}},
		putRequest{}, getRequest{}, getDone{}, storePut{}, storeGet{}, reply{},
	}
	seen := map[uint64]bool{}
	for _, m := range msgs {
		if m.Tag() == 0 || seen[m.Tag()] {
			t.Fatalf("tag %d is zero or used twice", m.Tag())
		}
		seen[m.Tag()] = true
		h := trace.New()
		m.Fold(&h)
		if h == trace.New() {
			t.Fatalf("%T does not fold its tag", m)
		}
	}
}
