package kafka

import (
	"testing"

	"manifester/broker"
	"manifester/core"
	"manifester/sim"
)

func seeds(from uint64, n int) []uint64 {
	s := make([]uint64, n)
	for i := range s {
		s[i] = from + uint64(i)
	}
	return s
}

func report(t *testing.T, rep sim.Report) {
	t.Helper()
	for _, a := range rep.Always {
		t.Errorf("violated %q kind=%s first seed %d step %d (%d seeds)", a.Prop, a.Kind, a.FirstSeed, a.FirstStep, len(a.Seeds))
	}
	for _, a := range rep.Coverage {
		t.Errorf("never satisfied: %q (%s)", a.Prop, a.Kind)
	}
	for _, s := range rep.Seeds {
		for _, f := range s.Failures {
			t.Errorf("seed %d: kind=%d %s step=%d at=%d: %s", s.Seed, f.Kind, f.Prop, f.Step, f.At, f.Msg)
		}
	}
	if len(rep.Nondeterminism) > 0 {
		t.Errorf("nondeterministic seeds: %v", rep.Nondeterminism)
	}
}

func TestKafkaCampaign(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 8
	}
	rep, err := sim.NewRunner().Run(Config(seeds(1, n)))
	if err != nil {
		t.Fatal(err)
	}
	var crashes, exits, trips int
	for _, s := range rep.Seeds {
		crashes, exits, trips = crashes+s.Crashes, exits+s.Exits, trips+s.Trips
	}
	t.Logf("%d seeds: crashes=%d exits=%d trips=%d", n, crashes, exits, trips)
	if testing.Short() { // coverage needs a full campaign
		rep.Coverage = nil
	}
	report(t, rep)
}

func TestDeterminismCanary(t *testing.T) {
	cfg := Config(nil)
	for seed := uint64(1); seed <= 4; seed++ {
		a, b := sim.RunSeed(cfg, seed), sim.RunSeed(cfg, seed)
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: %x/%d/%d vs %x/%d/%d", seed, a.TraceHash, a.RandCalls, a.Steps, b.TraceHash, b.RandCalls, b.Steps)
		}
		logged := cfg
		logged.Log = true
		if c := sim.RunSeed(logged, seed); c.TraceHash != a.TraceHash || c.RandCalls != a.RandCalls || c.Steps != a.Steps {
			t.Fatalf("seed %d: logging changed the run", seed)
		}
		if len(a.Failures) != 0 {
			t.Fatalf("seed %d: %+v", seed, a.Failures)
		}
	}
}

// violated returns the violated props of seed, in first-failure order.
func violated(cfg sim.Config, seed uint64) []core.Prop {
	var out []core.Prop
	seen := map[core.Prop]bool{}
	for _, f := range sim.RunSeed(cfg, seed).Failures {
		if f.Kind == sim.FailViolation && !seen[f.Prop] {
			seen[f.Prop] = true
			out = append(out, f.Prop)
		}
	}
	return out
}

// Each broker mutant must be caught within a few seeds, by the invariant that
// names the bug (it may be caught by others as well).
func TestMutantsAreCaught(t *testing.T) {
	const budget, soon = 20, 5
	cases := []struct {
		name string
		bug  broker.Bug
		prop core.Prop
	}{
		{"BugEarlyAck: ack before CommitSegment", broker.BugEarlyAck, "kafka: acked range is committed in the control plane"},
		{"BugNoIdempotence: retries are not deduplicated", broker.BugNoIdempotence, "kafka: no record is committed twice"},
		{"BugWrongOffset: fetch patches base offset + 1", broker.BugWrongOffset, "kafka: a consumer sees every offset in order, none skipped"},
		{"BugServeCorrupt: corrupt GET reaches the client", broker.BugServeCorrupt, "kafka: a consumer never receives a corrupt batch"},
	}
	for _, c := range cases {
		cfg := config(nil, c.bug)
		caught := uint64(0)
		var props []core.Prop
		for s := uint64(1); s <= budget && caught == 0; s++ {
			if props = violated(cfg, s); len(props) > 0 {
				caught = s
			}
		}
		if caught == 0 {
			t.Errorf("mutant %q not caught in %d seeds", c.name, budget)
			continue
		}
		t.Logf("mutant %q first caught by seed %d: %v", c.name, caught, props)
		if caught > soon {
			t.Errorf("mutant %q caught only at seed %d (want <= %d)", c.name, caught, soon)
		}
		found := false
		for _, p := range props {
			found = found || p == c.prop
		}
		if !found {
			t.Errorf("mutant %q: %q not among %v", c.name, c.prop, props)
		}
	}
}

// The real broker must pass every invariant on the same seeds.
func TestRealBrokerIsClean(t *testing.T) {
	for s := uint64(1); s <= 5; s++ {
		if p := violated(Config(nil), s); len(p) > 0 {
			t.Errorf("seed %d violated %v", s, p)
		}
	}
}

type tally struct {
	batches, definite, indet, abandoned, reads, readRecs, retried, dupAcks, ackFacts int
}

type tallyInv struct{ t *tally }

func (*tallyInv) Name() string     { return "tally" }
func (*tallyInv) Observe(sim.View) {}
func (i *tallyInv) AtEnd(v sim.View) {
	for _, tf := range v.Timeline().Snapshot("produce_acked") {
		i.t.ackFacts++
		if d, _ := fieldOf(tf.Fact, "dup").(core.Bool); bool(d) {
			i.t.dupAcks++
		}
	}
	for _, op := range v.History().Ops() {
		switch r := op.Result.(type) {
		case ProduceRes:
			i.t.batches++
			switch op.Outcome {
			case sim.OpDefinite:
				i.t.definite++
			case sim.OpIndeterminate:
				i.t.indet++
			case sim.OpAbandoned:
				i.t.abandoned++
			}
			if r.Attempts > 1 {
				i.t.retried++
			}
		case ReadRes:
			i.t.reads++
			for _, p := range r.Parts {
				i.t.readRecs += len(p.Recs)
			}
		}
	}
}

// The campaign must actually exercise the system: acks, retried batches,
// duplicate answers, ambiguous and rejected outcomes, and reads.
func TestCampaignIsNotVacuous(t *testing.T) {
	cfg := Config(nil)
	var tl tally
	cfg.Invariants = append(cfg.Invariants, func() sim.Invariant { return &tallyInv{&tl} })
	for s := uint64(1); s <= 20; s++ {
		sim.RunSeed(cfg, s)
	}
	t.Logf("20 seeds: %+v", tl)
	if tl.definite < 2000 || tl.indet == 0 || tl.retried == 0 || tl.dupAcks == 0 || tl.readRecs < 2000 {
		t.Fatalf("campaign too weak: %+v", tl)
	}
}
