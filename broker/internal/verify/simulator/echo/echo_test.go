package echo

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/preset"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

func seeds(from uint64, n int) []uint64 {
	s := make([]uint64, n)
	for i := range s {
		s[i] = from + uint64(i)
	}
	return s
}

// expectedReachable is the coverage contract of the campaign: every fault family
// and every recovery path must be reached by some seed.
var expectedReachable = []string{
	"obj 503 injected", "obj timeout injected", "obj lost response injected", "obj outage started", "obj get corrupted",
	"cp 503 injected", "cp timeout injected", "cp lost response injected", "cp outage started",
	"net: partition injected", "net: clog injected", "net: random_close injected", "net: black_hole injected",
	"echo: commit retried after MaybeApplied",
	"echo: corruption detected on read",
}

// runSeeds runs the seeds one after another. Like the campaign, it runs every 10th
// seed a second time with Log flipped, and the two runs must give the same hash,
// steps and random calls.
func runSeeds(t *testing.T, ss []uint64, cfgFor func(seed uint64) Config) []kernel.Result {
	t.Helper()
	var out []kernel.Result
	for i, seed := range ss {
		cfg := cfgFor(seed)
		res := Run(cfg)
		if i%10 == 0 {
			cfg.Log = !cfg.Log
			again := Run(cfg)
			if again.Hash != res.Hash || again.RandCalls != res.RandCalls || again.Steps != res.Steps {
				t.Errorf("seed %d: the second run of the seed diverged", seed)
			}
		}
		out = append(out, res)
	}
	return out
}

// report fails the test for each failure of a seed, and, when coverage is true, for each
// path that no seed reached.
func report(t *testing.T, results []kernel.Result, coverage bool) {
	t.Helper()
	total := map[string]assert.Coverage{}
	for _, r := range results {
		for _, f := range r.Failures {
			t.Errorf("seed %d: kind=%d %s step=%d at=%d: %s", r.Seed, f.Kind, f.Property, f.Step, f.At, f.Detail)
		}
		for name, c := range r.Coverage {
			x := total[name]
			x.Kind, x.Checks, x.Passes = c.Kind, x.Checks+c.Checks, x.Passes+c.Passes
			total[name] = x
		}
	}
	if !coverage {
		return
	}
	for name, c := range total {
		if (c.Kind == assert.KindSometimes || c.Kind == assert.KindReachable) && c.Passes == 0 {
			t.Errorf("never satisfied: %q (%s)", name, c.Kind)
		}
	}
	for _, name := range expectedReachable {
		if total[name].Passes == 0 {
			t.Errorf("never reached: %q", name)
		}
	}
}

func TestEchoCampaign(t *testing.T) {
	n := 200
	if testing.Short() {
		n = 30
	}
	results := runSeeds(t, seeds(1, n), func(seed uint64) Config { return Config{Seed: seed} })
	report(t, results, true)
	var crashes, exits, trips int
	for _, r := range results {
		crashes, exits, trips = crashes+r.Crashes, exits+r.Exits, trips+r.Trips
	}
	t.Logf("%d seeds: crashes=%d exits=%d trips=%d", n, crashes, exits, trips)
}

func TestDeterminismCanary(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		a, b := Run(Config{Seed: seed}), Run(Config{Seed: seed})
		if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: %x/%d/%d vs %x/%d/%d", seed, a.Hash, a.RandCalls, a.Steps, b.Hash, b.RandCalls, b.Steps)
		}
		if c := Run(Config{Seed: seed, Log: true}); c.Hash != a.Hash || c.RandCalls != a.RandCalls || c.Steps != a.Steps {
			t.Fatalf("seed %d: logging changed the run", seed)
		}
		if len(a.Failures) != 0 {
			t.Fatalf("seed %d: %+v", seed, a.Failures)
		}
	}
}

// firstCatch runs seeds from 1 and returns the first seed where prop is
// violated, or 0.
func firstCatch(brokers core.MachineFactory, n int, prop string) uint64 {
	for s := uint64(1); s <= uint64(n); s++ {
		for _, f := range Run(Config{Seed: s, Brokers: brokers}).Failures {
			if f.Kind == kernel.FailViolation && f.Property == prop {
				return s
			}
		}
	}
	return 0
}

func mutantFactory(m mutation) core.MachineFactory {
	return func(env core.Environment, self machine.NodeInstance) core.Machine {
		return &Broker{env: env, self: self, mut: m}
	}
}

func TestMutantsAreCaught(t *testing.T) {
	const budget = 50
	cases := []struct {
		name string
		m    mutation
		prop string
	}{
		{"ack before CommitSegment", bugEarlyAck, "echo: acked offset is committed"},
		{"ack when a commit times out", bugAckOnMaybe, "echo: acked offset is committed"},
		{"GET returns a different payload", bugBadRead, "echo: GET of an acked offset returns the acked payload"},
	}
	for _, c := range cases {
		s := firstCatch(mutantFactory(c.m), budget, c.prop)
		if s == 0 {
			t.Errorf("mutant %q not caught in %d seeds", c.name, budget)
			continue
		}
		t.Logf("mutant %q first caught by seed %d", c.name, s)
	}
	if s := firstCatch(mutantFactory(bugNone), 10, "echo: acked offset is committed"); s != 0 {
		t.Errorf("real broker violated AckedIsCommitted at seed %d", s)
	}
}

type tally struct {
	puts, gets, acks, vals, window, indet, errs, corrupt int
}

type tallyRule struct {
	ph   kernel.Phases
	t    *tally
	hist history.Reader[Op, string]
}

func (*tallyRule) Name() string        { return "tally" }
func (*tallyRule) Observe(kernel.View) {}
func (i *tallyRule) AtEnd(v kernel.View) {
	i.t.acks += len(v.Timeline().Snapshot("echo_acked"))
	for _, operation := range i.hist.Ops() {
		res := operation.Result
		switch operation.Op.(type) {
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
		if operation.Outcome == history.ResultUnknown {
			i.t.indet++
		}
		if len(res) >= 3 && res[:3] == "ERR" {
			i.t.errs++
		}
		if operation.Invoke >= kernel.Time(i.ph.Chaos+preset.ProgressGraceTime) {
			i.t.window++
		}
	}
}

// The campaign must actually exercise the system: acks, reads, ambiguity,
// errors and requests inside the liveness window.
func TestCampaignIsNotVacuous(t *testing.T) {
	var tl tally
	for s := uint64(1); s <= 20; s++ {
		Run(Config{Seed: s, ExtraRules: func(_ *cluster.Cluster, h *History) []kernel.Rule {
			return []kernel.Rule{&tallyRule{ph: preset.Phases(), t: &tl, hist: h}}
		}})
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

// Brokers are stateless (decision 44): nothing fences or trips an echo broker.
func TestNoTripsWithoutFencing(t *testing.T) {
	for s := uint64(1); s <= 40; s++ {
		res := Run(Config{Seed: s})
		if res.Trips != 0 || len(res.Failures) != 0 {
			t.Fatalf("seed %d: trips=%d failures=%+v", s, res.Trips, res.Failures)
		}
	}
}

// TestCachedStateHashMatchesFull recomputes every cached StateHash each step;
// a stale cache fails the seed.
func TestCachedStateHashMatchesFull(t *testing.T) {
	n := 60
	if testing.Short() {
		n = 10
	}
	results := runSeeds(t, seeds(1, n), func(seed uint64) Config { return Config{Seed: seed, VerifyHashes: true} })
	report(t, results, !testing.Short())
}
