package echo

import (
	"testing"

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

func TestEchoCampaign(t *testing.T) {
	n := 200
	if testing.Short() {
		n = 30
	}
	rep, err := sim.NewRunner().Run(Config(seeds(1, n)))
	if err != nil {
		t.Fatal(err)
	}
	report(t, rep)
	var crashes, exits, trips int
	for _, s := range rep.Seeds {
		crashes, exits, trips = crashes+s.Crashes, exits+s.Exits, trips+s.Trips
	}
	t.Logf("%d seeds: crashes=%d exits=%d trips=%d", n, crashes, exits, trips)
}

func TestDeterminismCanary(t *testing.T) {
	cfg := Config(nil)
	for seed := uint64(1); seed <= 8; seed++ {
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

// firstCatch runs seeds from 1 and returns the first seed where prop is
// violated, or 0.
func firstCatch(cfg sim.Config, n int, prop core.Prop) uint64 {
	for s := uint64(1); s <= uint64(n); s++ {
		for _, f := range sim.RunSeed(cfg, s).Failures {
			if f.Kind == sim.FailViolation && f.Prop == prop {
				return s
			}
		}
	}
	return 0
}

func mutantFactory(m mutation) core.Factory {
	return func(env core.Env, self core.NodeRef) core.Machine {
		return &Broker{env: env, self: self, mut: m}
	}
}

func TestMutantsAreCaught(t *testing.T) {
	const budget = 50
	cases := []struct {
		name string
		m    mutation
		prop core.Prop
	}{
		{"ack before CommitSegment", mutEarlyAck, "echo: acked offset is committed"},
		{"ack when a commit times out", mutAckOnMaybe, "echo: acked offset is committed"},
		{"GET returns a different payload", mutBadRead, "echo: GET of an acked offset returns the acked payload"},
	}
	for _, c := range cases {
		s := firstCatch(config(nil, mutantFactory(c.m)), budget, c.prop)
		if s == 0 {
			t.Errorf("mutant %q not caught in %d seeds", c.name, budget)
			continue
		}
		t.Logf("mutant %q first caught by seed %d", c.name, s)
	}
	if s := firstCatch(config(nil, mutantFactory(mutNone)), 10, "echo: acked offset is committed"); s != 0 {
		t.Errorf("real broker violated AckedIsCommitted at seed %d", s)
	}
}

type tally struct {
	puts, gets, acks, vals, window, indet, errs, corrupt int
}

type tallyInv struct {
	ph sim.Phases
	t  *tally
}

func (*tallyInv) Name() string     { return "tally" }
func (*tallyInv) Observe(sim.View) {}
func (i *tallyInv) AtEnd(v sim.View) {
	i.t.acks += len(v.Timeline().Snapshot("echo_acked"))
	for _, op := range v.History().Ops() {
		res, _ := op.Result.(string)
		switch op.Op.(type) {
		case PutOp:
			i.t.puts++
		case GetOp:
			i.t.gets++
			if len(res) > 3 && res[:4] == "VAL " {
				i.t.vals++
			}
		}
		if res == "ERR corrupt" {
			i.t.corrupt++
		}
		if op.Outcome == sim.OpIndeterminate {
			i.t.indet++
		}
		if len(res) >= 3 && res[:3] == "ERR" {
			i.t.errs++
		}
		if op.Invoke >= sim.Time(i.ph.Chaos+livenessGrace) {
			i.t.window++
		}
	}
}

// The campaign must actually exercise the system: acks, reads, ambiguity,
// errors and requests inside the liveness window.
func TestCampaignIsNotVacuous(t *testing.T) {
	cfg := Config(nil)
	var tl tally
	cfg.Invariants = append(cfg.Invariants, func() sim.Invariant { return &tallyInv{ph: cfg.Phases, t: &tl} })
	for s := uint64(1); s <= 20; s++ {
		sim.RunSeed(cfg, s)
	}
	t.Logf("20 seeds: %+v", tl)
	if tl.acks < 500 || tl.vals < 100 || tl.indet == 0 || tl.errs == 0 || tl.corrupt == 0 || tl.window < 200 {
		t.Fatalf("campaign too weak: %+v", tl)
	}
}

func TestFramerReassemblesSplitFrames(t *testing.T) {
	var wire []byte
	for _, m := range []string{"PUT a", "GET 7", "PUT " + string(make([]byte, 300))} {
		wire = appendFrame(wire, m)
	}
	for step := 1; step <= 9; step++ {
		var f framer
		var got []string
		for i := 0; i < len(wire); i += step {
			fr, bad := f.push(wire[i:min(i+step, len(wire))])
			if bad {
				t.Fatal("bad stream")
			}
			got = append(got, fr...)
		}
		if len(got) != 3 || got[0] != "PUT a" || got[1] != "GET 7" || len(got[2]) != 304 || len(f.buf) != 0 {
			t.Fatalf("step %d: %d frames, %d buffered", step, len(got), len(f.buf))
		}
	}
	var f framer
	if _, bad := f.push([]byte{0xff, 0xff, 0xff, 0xff}); !bad {
		t.Fatal("oversized frame must be rejected")
	}
}

// The fail-stop path (ErrFenced -> Trip) must be exercised, and a fenced
// incarnation must stop acking (TrippedNeverAcks runs in the preset).
func TestFencingTripsAreExercised(t *testing.T) {
	cfg := Config(nil)
	var fenced, trips int
	cfg.Invariants = append(cfg.Invariants, func() sim.Invariant {
		return &tripCount{fenced: &fenced}
	})
	for s := uint64(1); s <= 40; s++ {
		res := sim.RunSeed(cfg, s)
		trips += res.Trips
		if len(res.Failures) != 0 {
			t.Fatalf("seed %d: %+v", s, res.Failures)
		}
	}
	if trips == 0 || fenced != trips {
		t.Fatalf("trips=%d fenced facts=%d", trips, fenced)
	}
}

type tripCount struct{ fenced *int }

func (*tripCount) Name() string     { return "tripCount" }
func (*tripCount) Observe(sim.View) {}
func (i *tripCount) AtEnd(v sim.View) {
	*i.fenced += len(v.Timeline().Snapshot("echo_fenced"))
}

// TestCachedStateHashMatchesFull recomputes every cached StateHash each step;
// a stale cache fails the seed.
func TestCachedStateHashMatchesFull(t *testing.T) {
	n := 60
	if testing.Short() {
		n = 10
	}
	cfg := Config(seeds(1, n))
	cfg.VerifyHashes = true
	rep, err := sim.NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if testing.Short() {
		rep.Coverage = nil
	}
	report(t, rep)
}
