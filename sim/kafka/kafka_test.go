package kafka

import (
	"github.com/twmb/franz-go/pkg/kmsg"
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
	for seed := uint64(1); seed <= 3; seed++ {
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
		{"BugNoBatchCap: MaxSegmentBatches ignored", broker.BugNoBatchCap, "kafka: a sealed segment holds at most MaxSegmentBatches batches"},
		{"BugNoItemCap: MaxSegmentItems ignored", broker.BugNoItemCap, "kafka: a sealed segment's commit cost stays within MaxSegmentItems"},
		{"BugTimerFromLastBatch: flush window restarts on every batch", broker.BugTimerFromLastBatch, "kafka: a segment is sealed within the window of its first batch"},
		{"BugSealOffByOne: size rule applied one batch late", broker.BugSealOffByOne, "kafka: a sealed segment overshoots MaxSegmentBytes by at most its last batch"},
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

// fakeEnv is the minimum Env for driving the workload by hand.
type fakeEnv struct{ n uint64 }

func (e *fakeEnv) Uint64() uint64 { e.n++; return e.n }
func (e *fakeEnv) Intn(n int) int { e.n++; return int(e.n % uint64(n)) }
func (e *fakeEnv) NewID() (id core.ID) {
	e.n++
	id[0], id[1] = byte(e.n), byte(e.n>>8)
	return id
}
func (*fakeEnv) Maybe(core.Site) bool                                { return false }
func (*fakeEnv) MaybeP(core.Site, float64) bool                      { return false }
func (*fakeEnv) Fault(core.Site, float64) bool                       { return false }
func (*fakeEnv) Always(bool, core.Prop, ...core.Detail)              {}
func (*fakeEnv) AlwaysOrUnreachable(bool, core.Prop, ...core.Detail) {}
func (*fakeEnv) Sometimes(bool, core.Prop)                           {}
func (*fakeEnv) Reachable(core.Prop)                                 {}
func (*fakeEnv) Unreachable(core.Prop, ...core.Detail)               {}

type fakeHist struct{ ops []sim.HistOp }

func (h *fakeHist) Invoke(c core.NodeRef, id, v core.ID, op any) {
	h.ops = append(h.ops, sim.HistOp{ID: id, Value: v, Client: c, Op: op})
}
func (h *fakeHist) Return(id core.ID, res any, o sim.OpOutcome) bool {
	for i := range h.ops {
		if h.ops[i].ID == id {
			h.ops[i].Result, h.ops[i].Outcome = res, o
			return true
		}
	}
	return false
}
func (h *fakeHist) Ops() []sim.HistOp { return h.ops }

// The real broker never answers OUT_OF_ORDER_SEQUENCE to this workload, so the
// give-up path is driven by hand: a definite rejection abandons the batch, an
// earlier ambiguous attempt makes it indeterminate, and either way the
// producer asks for a fresh id.
func TestFatalProduceErrorReinitsProducer(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		w := NewWorkload(&fakeEnv{}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
		h := &fakeHist{}
		w.SetHistory(h)
		c := w.clients[0]
		c.state, c.stage, c.p.pid = csUp, stRun, 1000
		w.newBatches(c)
		if len(h.ops) == 0 || len(c.p.cur) != len(h.ops) {
			t.Fatalf("no batches built: %d ops", len(h.ops))
		}
		for _, b := range c.p.cur {
			b.ambiguous = ambiguous
		}
		resp := kmsg.NewPtrProduceResponse()
		for _, b := range c.p.cur {
			tp := w.tps[b.tpIdx]
			resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{Topic: tp.Topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: tp.Partition, ErrorCode: 45, BaseOffset: -1}}})
		}
		w.onProduce(c, resp)
		want := sim.OpAbandoned
		if ambiguous {
			want = sim.OpIndeterminate
		}
		for i, op := range h.ops {
			if op.Outcome != want {
				t.Errorf("ambiguous=%v op %d: outcome %d, want %d", ambiguous, i, op.Outcome, want)
			}
		}
		if c.p.pid != 0 || c.stage != stInit || len(c.p.cur) != 0 {
			t.Errorf("ambiguous=%v: producer not reset: pid=%d stage=%d cur=%d", ambiguous, c.p.pid, c.stage, len(c.p.cur))
		}
	}
}

// TestCachedStateHashMatchesFull recomputes every cached StateHash each step
// (a stale cache fails the seed) and checks the trace is unchanged by it.
func TestCachedStateHashMatchesFull(t *testing.T) {
	n := 40
	if testing.Short() {
		n = 6
	}
	ss := seeds(1, n)
	cfg := Config(ss)
	cfg.VerifyHashes = true
	rep, err := sim.NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if testing.Short() {
		rep.Coverage = nil
	}
	report(t, rep)
	plain, err := sim.NewRunner().Run(Config(ss))
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range rep.Seeds {
		if p := plain.Seeds[i]; s.TraceHash != p.TraceHash || s.Steps != p.Steps || s.RandCalls != p.RandCalls {
			t.Errorf("seed %d: verify mode changed the trace", s.Seed)
		}
	}
}

// A broker that ignores its batch cap is refused by the control plane's commit
// limit too (tight on half the seeds), not only caught by the cap invariant.
func TestCommitLimitRefusesUncappedSegment(t *testing.T) {
	cfg := config(nil, broker.BugNoBatchCap)
	for s := uint64(1); s <= 40; s++ {
		for _, p := range violated(cfg, s) {
			if p == "broker: segment exceeds the control plane's commit size" {
				return
			}
		}
	}
	t.Fatal("no seed in 40 had the control plane refuse an uncapped segment")
}

// The production caps (24 partitions, 256 batches, 96 commit items) against
// DynamoDB's 100-item commit: 48 producers keep enough batches in flight to hit
// the partition or item cap, the control plane refuses anything over 100 items
// (2 + 2 x partitions + producer states), and the broker's defaults (16 MiB,
// 200 ms) are untouched.
func TestProductionCapsFitDynamoLimit(t *testing.T) {
	opts := Opts{Producers: 48, Consumers: 1, Topics: []string{"a", "b", "c", "d"}, Partitions: 8}
	cfg := configWith(seeds(1, 3), broker.BugNone, opts)
	prod := broker.DefaultConfig()
	prod.MaxBufferedBytes, prod.ResumeBufferedBytes = 64<<20, 32<<20
	cfg.Params = prod
	inner := cfg.Swarm
	cfg.Swarm = func(seed uint64, r sim.CountedRand, c *sim.SeedConfig) {
		inner(seed, r, c)
		c.Params, c.ControlPlane.MaxCommitItems = prod, sim.DynamoMaxCommitItems
	}
	rep, err := sim.NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep.Coverage = nil // a short campaign does not reach every fault
	report(t, rep)
	hit := uint64(0)
	for _, a := range rep.Assertions {
		if a.Prop == "broker: segment sealed by commit item cap" || a.Prop == "broker: segment sealed by partition cap" {
			t.Logf("%s: %d", a.Prop, a.Checks)
			hit += a.Checks
		}
	}
	if hit == 0 {
		t.Error("the production caps never bound: the test proves nothing")
	}
}
