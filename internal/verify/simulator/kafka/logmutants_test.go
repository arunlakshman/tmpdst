package kafka

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/internal/broker"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/simulator"
	"github.com/manifesterio/mds/internal/verify/simulator/fakeservices/fakelogservice"
)

func TestLogCampaign(t *testing.T) {
	n := 40 // the coverage list needs rare faults (replica stalled); 20 seeds hit it only by luck
	if testing.Short() {
		n = 8
	}
	rep, err := simulator.NewRunner().Run(ConfigLog(seeds(1, n)))
	if err != nil {
		t.Fatal(err)
	}
	if testing.Short() { // coverage needs a full campaign
		rep.Coverage = nil
	}
	report(t, rep)
}

func TestLogDeterminismCanary(t *testing.T) {
	config := ConfigLog(nil)
	for seed := uint64(1); seed <= 3; seed++ {
		a, b := simulator.RunSeed(config, seed), simulator.RunSeed(config, seed)
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged", seed)
		}
		if len(a.Failures) != 0 {
			t.Fatalf("seed %d: %+v", seed, a.Failures)
		}
	}
}

// Each log engine mutant is caught within a few seeds by the property that
// names the bug (decision 21: the properties, not a second implementation of
// the log, catch it).
func TestLogMutantsAreCaught(t *testing.T) {
	const budget, soon = 20, 6
	cases := []struct {
		name   string
		bug    uint8
		window int
		tune   func(*simulator.Config) // makes the failure frequent; nil keeps the defaults
		prop   core.PropertyID
	}{
		// Truth is log replay (correct assign); the writer stamps missing-slot bases only
		// into its fold/answers, so acks disagree with ControlPlaneRealState.
		{"offsets computed by the writer, not by apply", fakelogservice.InjectedBugWriterOffsets, 0, nil, "kafka: acked range is committed in the control plane"},
		{"each replica forgets keys on its own pace", fakelogservice.InjectedBugPerReplicaWindow, 40, nil, "logcp-sim: a replica's state equals the replay of the log at its slot"},
		// A put that the log store applies seconds after it was sent: by then the
		// other node has taken over and trimmed past the slot, so the put lands in a
		// deleted slot. A short takeover, a short keep window and a slow tail on
		// the store make that happen often.
		{"put at or below the floor answered as success", fakelogservice.InjectedBugNoFloorCheck, 0, slowPutsAfterTakeover, "kafka: a consumer reads the committed value at each offset"},
		// The same mutant, found by freezing a control plane node: it comes back
		// behind the floor of the log, and its put must be refused there.
		{"put at or below the floor answered as success (frozen writer)", fakelogservice.InjectedBugNoFloorCheck, 0, withFreezes, "kafka: acked range is committed in the control plane"},
	}
	for _, c := range cases {
		config := configLog(nil, broker.InjectedBugNone, c.bug)
		if c.window > 0 {
			config.ControlPlane.Log.WindowSlots = c.window
		}
		if c.tune != nil {
			c.tune(&config)
		}
		var props []core.PropertyID
		caught := uint64(0)
		for s := uint64(1); s <= budget && caught == 0; s++ {
			if props = violated(config, s); len(props) > 0 {
				caught = s
			}
		}
		switch {
		case caught == 0:
			t.Errorf("mutant %q not caught in %d seeds", c.name, budget)
		case caught > soon:
			t.Errorf("mutant %q caught only at seed %d (want <= %d)", c.name, caught, soon)
		}
		if caught > 0 && !slices.Contains(props, c.prop) {
			t.Errorf("mutant %q: %q not among %v", c.name, c.prop, props)
		}
	}
}

// slowPutsAfterTakeover makes a log put land seconds after it was sent. By then
// the other node has taken over and trimmed past the slot. A short takeover
// wait, a short keep window and a slow tail on the log store make that happen
// often.
func slowPutsAfterTakeover(c *simulator.Config) {
	l := &c.ControlPlane.Log
	l.TakeoverAfter = 300 * core.Millisecond
	l.CheckpointEvery, l.KeepSlots, l.TrimStride = 8, 4, 4
	c.LogStore.ApplyLatency.SlowProb = 0.005
	c.LogStore.ApplyLatency.SlowMin, c.LogStore.ApplyLatency.SlowMax = 2000*core.Millisecond, 4000*core.Millisecond
}

// The real protocol (epoch fence and floor) keeps every property under the same
// slow-put pressure that the no-floor-check mutant fails under.
func TestLogSlowPutsAfterTakeoverStayClean(t *testing.T) {
	config := configLog(nil, broker.InjectedBugNone, fakelogservice.InjectedBugNone)
	slowPutsAfterTakeover(&config)
	n := uint64(40)
	if testing.Short() {
		n = 10
	}
	for s := uint64(1); s <= n; s++ {
		if props := violated(config, s); len(props) > 0 {
			t.Fatalf("seed %d violated %v with the real protocol", s, props)
		}
	}
}
