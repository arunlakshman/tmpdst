package campaign_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/campaign"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/trace"
)

func TestRunFoldsHashesInSeedOrder(t *testing.T) {
	const first, seeds = 10, 5
	want := trace.New()
	for i := range seeds {
		seed := first + uint64(i)
		want.Uint64(seed * 1000)
	}

	got := campaign.Run(first, seeds, 4, func(seed uint64) campaign.Result {
		// Finish later seeds first so a buggy fold would scramble order.
		time.Sleep(time.Duration(seeds-int(seed-first)) * time.Millisecond)
		return campaign.Result{Hash: seed * 1000, OK: true}
	})

	if got.Hash != want.Sum() {
		t.Fatalf("fold order: got %#x want %#x", got.Hash, want.Sum())
	}
	if got.Counts != (campaign.Counts{Seeds: seeds, OK: seeds}) {
		t.Fatalf("counts: %+v", got.Counts)
	}
	if len(got.Failed) != 0 {
		t.Fatalf("failed: %+v", got.Failed)
	}
}

func TestRunCollectsFailuresAndMergesCoverage(t *testing.T) {
	got := campaign.Run(1, 4, 2, func(seed uint64) campaign.Result {
		ok := seed%2 == 0
		return campaign.Result{
			Hash: seed,
			OK:   ok,
			Coverage: map[string]campaign.Coverage{
				"path": {Kind: "reachable", Checks: 1, Passes: uint64(seed)},
			},
			Reproduce: "repro",
		}
	})

	if got.Counts.Failed != 2 || got.Counts.OK != 2 {
		t.Fatalf("counts: %+v", got.Counts)
	}
	if len(got.Failed) != 2 {
		t.Fatalf("failed len=%d", len(got.Failed))
	}
	if got.Failed[0].Seed != 1 || got.Failed[1].Seed != 3 {
		t.Fatalf("failed seed order: %+v", got.Failed)
	}
	c := got.Coverage["path"]
	if c.Kind != "reachable" || c.Checks != 4 || c.Passes != 1+2+3+4 {
		t.Fatalf("coverage: %+v", c)
	}
}

func TestRunFoldsSitesAndNeverFired(t *testing.T) {
	run := func(first uint64) campaign.Summary {
		return campaign.Run(first, 4, 2, func(seed uint64) campaign.Result {
			sites := map[string]fault.SiteCount{
				"fires": {On: seed%2 == 0, Calls: 10},
				"quiet": {On: true, Calls: 5},
			}
			if seed == 2 {
				sites["fires"] = fault.SiteCount{On: true, Calls: 10, Fired: 3}
			}
			return campaign.Result{OK: true, Sites: sites}
		})
	}
	var total campaign.Summary
	total.Merge(run(1))
	total.Merge(run(1))
	f := total.Sites["fires"]
	if f != (campaign.SiteTotal{Reached: 8, On: 4, Calls: 80, Fired: 6}) {
		t.Fatalf("fires: %+v", f)
	}
	if q := total.Sites["quiet"]; q != (campaign.SiteTotal{Reached: 8, On: 8, Calls: 40}) {
		t.Fatalf("quiet: %+v", q)
	}
	got := campaign.NeverFired(total.Sites, []string{"gone", "fires", "quiet"})
	if len(got) != 2 || got[0] != "gone" || got[1] != "quiet" {
		t.Fatalf("never fired: %v", got)
	}
}

func TestRunZeroSeeds(t *testing.T) {
	var calls atomic.Int32
	got := campaign.Run(1, 0, 2, func(uint64) campaign.Result {
		calls.Add(1)
		return campaign.Result{OK: true}
	})
	if calls.Load() != 0 {
		t.Fatalf("ran %d times", calls.Load())
	}
	if got.Counts.Seeds != 0 || got.Hash != 0 {
		t.Fatalf("summary: %+v", got)
	}
}

func TestRunStampsSeedOnFailedResults(t *testing.T) {
	got := campaign.Run(7, 3, 1, func(seed uint64) campaign.Result {
		return campaign.Result{Hash: seed, OK: seed != 8}
	})
	if len(got.Failed) != 1 || got.Failed[0].Seed != 8 {
		t.Fatalf("seed stamp: %+v", got.Failed)
	}
}

func TestRunSumsCountersAndMergeAndMissing(t *testing.T) {
	run := func(first uint64) campaign.Summary {
		return campaign.Run(first, 3, 2, func(seed uint64) campaign.Result {
			return campaign.Result{
				OK:       true,
				Counters: map[string]uint64{"steps": 10},
				Coverage: map[string]campaign.Coverage{"a": {Kind: "reachable", Checks: 1, Passes: 1}},
			}
		})
	}
	var total campaign.Summary
	total.Merge(run(1))
	total.Merge(run(10))
	if total.Counters["steps"] != 60 || total.Coverage["a"].Passes != 6 {
		t.Fatalf("merge: %+v %+v", total.Counters, total.Coverage)
	}
	got := campaign.Missing(total.Coverage, []string{"z", "a", "b"})
	if len(got) != 2 || got[0] != "b" || got[1] != "z" {
		t.Fatalf("missing: %v", got)
	}
}
