package kafka

import (
	"testing"

	"manifester/internal/broker"
	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/logengine"
)

func TestLogCampaign(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 8
	}
	rep, err := sim.NewRunner().Run(ConfigLog(seeds(1, n)))
	if err != nil {
		t.Fatal(err)
	}
	if testing.Short() { // coverage needs a full campaign
		rep.Coverage = nil
	}
	report(t, rep)
}

func TestLogDeterminismCanary(t *testing.T) {
	cfg := ConfigLog(nil)
	for seed := uint64(1); seed <= 3; seed++ {
		a, b := sim.RunSeed(cfg, seed), sim.RunSeed(cfg, seed)
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
		prop   core.PropertyID
	}{
		// Truth is log replay (correct assign); the writer stamps gap bases only
		// into its fold/answers, so acks disagree with CPTruth.
		{"offsets computed by the writer, not by apply", logengine.BugWriterOffsets, 0, "kafka: acked range is committed in the control plane"},
		{"trimmed slots deleted instead of tombstoned", logengine.BugTrimDelete, 0, "logcp-sim: a Put never wins a slot below the trim floor"},
		{"each replica forgets keys on its own pace", logengine.BugPerReplicaWindow, 40, "logcp-sim: a replica's state equals the replay of the log at its slot"},
		{"a replica passes an empty slot without a no-op", logengine.BugHoleSkip, 0, "logcp-sim: a replica's state equals the replay of the log at its slot"},
	}
	for _, c := range cases {
		cfg := configLog(nil, broker.InjectedBugNone, c.bug)
		if c.window > 0 {
			cfg.ControlPlane.Log.WindowSlots = c.window
		}
		var props []core.PropertyID
		caught := uint64(0)
		for s := uint64(1); s <= budget && caught == 0; s++ {
			if props = violated(cfg, s); len(props) > 0 {
				caught = s
			}
		}
		switch {
		case caught == 0:
			t.Errorf("mutant %q not caught in %d seeds", c.name, budget)
		case caught > soon:
			t.Errorf("mutant %q caught only at seed %d (want <= %d)", c.name, caught, soon)
		}
		found := false
		for _, p := range props {
			found = found || p == c.prop
		}
		if caught > 0 && !found {
			t.Errorf("mutant %q: %q not among %v", c.name, c.prop, props)
		}
	}
}
