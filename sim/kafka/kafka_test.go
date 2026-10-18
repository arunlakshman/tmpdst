package kafka

import (
	"github.com/twmb/franz-go/pkg/kmsg"
	"sync/atomic"
	"testing"

	"manifester/broker"
	"manifester/core"
	"manifester/kwire"
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
	const budget, soon = 20, 8
	cases := []struct {
		name string
		bug  broker.Bug
		prop core.Prop
	}{
		{"BugEarlyAck: ack before CommitSegment", broker.BugEarlyAck, "kafka: acked range is committed in the control plane"},
		{"BugFreshKeyOnRetry: a lost commit is retried under a fresh key", broker.BugFreshKeyOnRetry, "kafka: a stored batch is committed at most once"},
		{"BugNoPartitionCap: MaxSegmentPartitions ignored", broker.BugNoPartitionCap, "kafka: a sealed segment spans at most MaxSegmentPartitions partitions"},
		{"BugWrongOffset: fetch patches base offset + 1", broker.BugWrongOffset, "kafka: a consumer sees every offset in order, none skipped"},
		{"BugServeCorrupt: corrupt GET reaches the client", broker.BugServeCorrupt, "kafka: a consumer never receives a corrupt batch"},
		{"BugNoBatchCap: MaxSegmentBatches ignored", broker.BugNoBatchCap, "kafka: a sealed segment holds at most MaxSegmentBatches batches"},
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
	batches, definite, indet, abandoned, rejected, reads, readRecs, retried, ackFacts int
}

type tallyInv struct{ t *tally }

func (*tallyInv) Name() string     { return "tally" }
func (*tallyInv) Observe(sim.View) {}
func (i *tallyInv) AtEnd(v sim.View) {
	for _, tf := range v.Timeline().Snapshot("produce_acked") {
		i.t.ackFacts++
		_ = tf
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
				if r.Why == "rejected" {
					i.t.rejected++
				}
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
// ambiguous outcomes, deliberately idempotent batches rejected by the broker,
// and reads.
func TestCampaignIsNotVacuous(t *testing.T) {
	cfg := Config(nil)
	var tl tally
	cfg.Invariants = append(cfg.Invariants, func() sim.Invariant { return &tallyInv{&tl} })
	for s := uint64(1); s <= 20; s++ {
		sim.RunSeed(cfg, s)
	}
	t.Logf("20 seeds: %+v", tl)
	if tl.definite < 2000 || tl.indet == 0 || tl.retried == 0 || tl.rejected == 0 || tl.readRecs < 2000 {
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

// The real broker never answers a fatal code to a plain batch, so the give-up
// path is driven by hand: a definite rejection abandons the batch, an earlier
// ambiguous attempt makes it indeterminate.
func TestFatalProduceErrorGivesUp(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		w := NewWorkload(&fakeEnv{}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
		h := &fakeHist{}
		w.SetHistory(h)
		c := w.clients[0]
		c.state, c.stage = csUp, stRun
		w.newBatches(c)
		if len(h.ops) == 0 || len(c.p.cur) != len(h.ops) {
			t.Fatalf("no batches built: %d ops", len(h.ops))
		}
		for _, b := range c.p.cur {
			b.ambiguous, b.idem = ambiguous, false
		}
		w.onProduce(c, produceResp(w, c, 45))
		want := sim.OpAbandoned
		if ambiguous {
			want = sim.OpIndeterminate
		}
		for i, op := range h.ops {
			if op.Outcome != want {
				t.Errorf("ambiguous=%v op %d: outcome %d, want %d", ambiguous, i, op.Outcome, want)
			}
		}
		if len(c.p.cur) != 0 {
			t.Errorf("ambiguous=%v: %d batches still unresolved", ambiguous, len(c.p.cur))
		}
	}
}

func produceResp(w *Workload, c *client, code int16) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	for _, b := range c.p.cur {
		tp := w.tps[b.tpIdx]
		resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{Topic: tp.Topic,
			Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: tp.Partition, ErrorCode: code, BaseOffset: -1}}})
	}
	return resp
}

// A batch that claims a producer id is resolved as rejected on INVALID_REQUEST
// (even after an ambiguous earlier attempt) and the producer carries on.
func TestIdemBatchRejectedIsAbandoned(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		w := NewWorkload(&fakeEnv{}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
		h := &fakeHist{}
		w.SetHistory(h)
		c := w.clients[0]
		c.state, c.stage = csUp, stRun
		w.newBatches(c)
		for _, b := range c.p.cur {
			b.ambiguous, b.idem = ambiguous, true
		}
		acts := w.onProduce(c, produceResp(w, c, codeInvalidRequest))
		for i, op := range h.ops {
			res, _ := op.Result.(ProduceRes)
			if op.Outcome != sim.OpAbandoned || res.Why != "rejected" || res.Base != -1 {
				t.Errorf("ambiguous=%v op %d: %d %+v", ambiguous, i, op.Outcome, res)
			}
		}
		if len(c.p.cur) != 0 || len(acts) == 0 {
			t.Errorf("ambiguous=%v: %d unresolved, %d actions", ambiguous, len(c.p.cur), len(acts))
		}
	}
}

// Only the batches built with idem set carry a producer id on the wire.
func TestBatchesCarryProducerIDOnlyWhenIdem(t *testing.T) {
	w := NewWorkload(&fakeEnv{}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
	w.SetHistory(&fakeHist{})
	c := w.clients[0]
	idem, plain := 0, 0
	for i := 0; i < 200; i++ {
		c.p.cur = nil
		w.newBatches(c)
		for _, b := range c.p.cur {
			bt, err := kwire.ParseBatch(b.data)
			if err != nil {
				t.Fatal(err)
			}
			if b.idem {
				idem++
				if bt.ProducerID != idemPID {
					t.Fatalf("idem batch has producer id %d", bt.ProducerID)
				}
			} else {
				plain++
				if bt.ProducerID != -1 || bt.ProducerEpoch != -1 || bt.BaseSequence != -1 {
					t.Fatalf("plain batch is idempotent: %d/%d/%d", bt.ProducerID, bt.ProducerEpoch, bt.BaseSequence)
				}
			}
		}
	}
	if idem == 0 || plain == 0 {
		t.Fatalf("idem %d plain %d", idem, plain)
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

// A broker that ignores its partition cap is refused by the control plane's
// commit limit too (tight on half the seeds: 1 + the cap), not only caught by
// the cap invariants.
func TestCommitLimitRefusesUncappedSegment(t *testing.T) {
	cfg := config(nil, broker.BugNoPartitionCap)
	for s := uint64(1); s <= 40; s++ {
		for _, p := range violated(cfg, s) {
			if p == "broker: segment exceeds the control plane's commit size" {
				return
			}
		}
	}
	t.Fatal("no seed in 40 had the control plane refuse an uncapped segment")
}

// The production caps (96 partitions, 256 batches) against DynamoDB's 100-item
// commit: a commit costs 1 + distinct partitions (+ grown topics), so the cap
// leaves 3 items spare. 128 producers over 128 partitions keep enough batches in
// flight for segments to span many partitions; the control plane refuses
// anything over 100 items, and the broker's defaults (16 MiB, 200 ms) are
// untouched. The widest committed segment must be over half of the cap, or the
// test proves nothing.
func TestProductionCapsFitDynamoLimit(t *testing.T) {
	prod := broker.DefaultConfig()
	if 1+prod.MaxSegmentPartitions > sim.DynamoMaxCommitItems || sim.DynamoMaxCommitItems-1-prod.MaxSegmentPartitions < 3 {
		t.Fatalf("1 + %d partitions leaves no spare items under %d", prod.MaxSegmentPartitions, sim.DynamoMaxCommitItems)
	}
	opts := Opts{Producers: 128, Consumers: 1, Topics: []string{"a", "b"}, Partitions: 64}
	cfg := configWith(seeds(1, 2), broker.BugNone, opts)
	prod.MaxBufferedBytes, prod.ResumeBufferedBytes = 64<<20, 32<<20
	cfg.Params = prod
	inner := cfg.Swarm
	cfg.Swarm = func(seed uint64, r sim.CountedRand, c *sim.SeedConfig) {
		inner(seed, r, c)
		c.Params, c.ControlPlane.MaxCommitItems = prod, sim.DynamoMaxCommitItems
	}
	var widest atomic.Int64
	cfg.Invariants = append(cfg.Invariants, func() sim.Invariant { return &widestInv{&widest, opts.tps()} })
	rep, err := sim.NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep.Coverage = nil // a short campaign does not reach every fault
	report(t, rep)
	t.Logf("widest committed segment: %d partitions (cap %d)", widest.Load(), prod.MaxSegmentPartitions)
	if int(widest.Load()) <= prod.MaxSegmentPartitions/2 {
		t.Errorf("segments spanned at most %d partitions: the production cap was never approached", widest.Load())
	}
}

// widestInv records the most distinct partitions in one committed segment.
type widestInv struct {
	widest *atomic.Int64
	tps    []core.TopicPartition
}

func (*widestInv) Name() string     { return "widest" }
func (*widestInv) Observe(sim.View) {}
func (i *widestInv) AtEnd(v sim.View) {
	parts := map[string]map[core.TopicPartition]bool{}
	for _, tp := range i.tps {
		for _, r := range v.CP().Committed(tp) {
			if parts[r.Loc.Key] == nil {
				parts[r.Loc.Key] = map[core.TopicPartition]bool{}
			}
			parts[r.Loc.Key][tp] = true
		}
	}
	for _, m := range parts {
		for n := int64(len(m)); ; {
			old := i.widest.Load()
			if n <= old || i.widest.CompareAndSwap(old, n) {
				break
			}
		}
	}
}
