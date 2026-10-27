package kafka

import (
	"slices"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/broker"
	"manifester/core"
	"manifester/kwire"
	"manifester/sim"
	"manifester/sim/internal/campaign"
	"manifester/sim/internal/session"
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
		{"BugSkipProduceCRC: produce admits a corrupted batch", broker.BugSkipProduceCRC, "kafka: committed batch is intact"},
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
func (e *fakeEnv) NewID() core.ID {
	e.n++
	var id core.ID
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
		c := w.loop.Clients[0]
		c.State, c.stage = session.Up, stRun
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
		c := w.loop.Clients[0]
		c.State, c.stage = session.Up, stRun
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

// rngEnv is a fakeEnv whose Intn is a seeded xorshift, for tests that need
// rare events (1 in 40) to occur at unaligned draws.
type rngEnv struct {
	fakeEnv
	x uint64
}

func (e *rngEnv) Intn(n int) int {
	e.x ^= e.x << 13
	e.x ^= e.x >> 7
	e.x ^= e.x << 17
	return int(e.x % uint64(n))
}

// Only the batches built with idem set carry a producer id on the wire.
func TestBatchesCarryProducerIDOnlyWhenIdem(t *testing.T) {
	w := NewWorkload(&rngEnv{x: 88172645463325252}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
	w.SetHistory(&fakeHist{})
	c := w.loop.Clients[0]
	idem, plain := 0, 0
	for range 400 {
		c.p.cur = nil
		w.newBatches(c)
		for _, b := range c.p.cur {
			if b.corrupt {
				continue
			}
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
		if slices.Contains(violated(cfg, s), "broker: segment exceeds the control plane's commit size") {
			return
		}
	}
	t.Fatal("no seed in 40 had the control plane refuse an uncapped segment")
}

// wideCampaign runs 128 producers over 128 partitions (enough batches in flight
// for segments to span many partitions; 128 per topic in the log case, so more than the old cap can be reached) with the production broker defaults
// except for the partition cap, against the veritas-backed store limited to
// DynamoDB's 100-item commit (log false) or the log store (log true). It
// returns the widest committed segment, in partitions.
func wideCampaign(t *testing.T, capParts, batches int, log bool) int64 {
	t.Helper()
	prod := broker.DefaultConfig()
	prod.MaxSegmentPartitions, prod.MaxSegmentBatches = capParts, batches
	prod.MaxBufferedBytes, prod.ResumeBufferedBytes = 64<<20, 32<<20
	opts := Opts{Producers: 128, Consumers: 1, Topics: []string{"a", "b"}, Partitions: 64}
	cfg := configWith(seeds(1, 2), broker.BugNone, opts)
	items := sim.DynamoMaxCommitItems
	if log {
		opts.Partitions = 128
		cfg = configLogWith(seeds(1, 2), broker.BugNone, 0, opts)
		items = 0
		cfg.ControlPlane.CallerTimeout = 800 * core.Millisecond
		cfg.LogStore.CallerTimeout = 800 * core.Millisecond
		cfg.Phases = campaign.Phases()
	}
	cfg.Params = prod
	inner := cfg.Swarm
	cfg.Swarm = func(seed uint64, r sim.CountedRand, c *sim.SeedConfig) {
		inner(seed, r, c)
		c.Params, c.ControlPlane.MaxCommitItems = prod, items
		if log {
			c.ControlPlane.Unavailable503 = 0
			c.ControlPlane.Timeout = 0
			c.ControlPlane.LostResponse = 0
			c.ControlPlane.Outage.Prob = 0
			c.LogStore.Unavailable503 = 0
			c.LogStore.LostResponse = 0
			c.ControlPlane.Log.PutFail = 0
			c.ControlPlane.Log.PutLost = 0
			c.LogStore.CallerTimeout = c.ControlPlane.CallerTimeout
		}
	}
	var widest atomic.Int64
	cfg.Invariants = append(cfg.Invariants, func() sim.Invariant { return &widestInv{&widest, opts.tps()} })
	rep, err := sim.NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep.Coverage = nil
	report(t, rep)
	return widest.Load()
}

// The old store (decisions 39, 41, 44): a commit costs 1 + distinct partitions
// (+ grown topics) of the 100 items of a transaction, so the broker must run with
// -max-segment-partitions 96 (3 spare items). At that cap the control plane
// refuses nothing; the widest committed segment must be over half of the cap,
// or the test proves nothing.
func TestProductionCapsFitDynamoLimit(t *testing.T) {
	const capParts = 96
	if 1+capParts > sim.DynamoMaxCommitItems || sim.DynamoMaxCommitItems-1-capParts < 3 {
		t.Fatalf("1 + %d partitions leaves no spare items under %d", capParts, sim.DynamoMaxCommitItems)
	}
	w := wideCampaign(t, capParts, 256, false)
	t.Logf("old store: widest committed segment %d partitions (cap %d)", w, capParts)
	if int(w) <= capParts/2 {
		t.Errorf("segments spanned at most %d partitions: the cap was never approached", w)
	}
}

// The log store (decision 52): the production default cap of 4096 and no item
// limit. With 128 partitions a segment can span more than the old store's 96,
// and nothing is refused.
func TestLogStoreTakesTheDefaultCap(t *testing.T) {
	const old = 96
	if got := broker.DefaultConfig().MaxSegmentPartitions; got != 4096 {
		t.Fatalf("default cap %d, want 4096", got)
	}
	w := wideCampaign(t, broker.DefaultConfig().MaxSegmentPartitions, 1024, true)
	t.Logf("log store: widest committed segment %d partitions (cap 4096)", w)
	if int(w) <= old {
		t.Errorf("widest segment %d partitions: not past the old store's %d, the cap change was never exercised", w, old)
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

// The workload sends every codec id (compressed payloads are opaque to the
// broker but decodable by the simulated consumer), plus damaged batches.
func TestWorkloadBuildsEveryCodecAndCorruptBatches(t *testing.T) {
	w := NewWorkload(&rngEnv{x: 88172645463325252}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
	h := &fakeHist{}
	w.SetHistory(h)
	c := w.loop.Clients[0]
	seen := map[int]int{}
	corrupt := 0
	for range 400 {
		c.p.cur = nil
		w.newBatches(c)
		for _, b := range c.p.cur {
			if b.corrupt {
				corrupt++
				if _, err := kwire.ParseBatch(b.data); err == nil {
					t.Fatal("corrupt batch parses")
				}
				continue
			}
			bt, err := kwire.ParseBatch(b.data)
			if err != nil {
				t.Fatal(err)
			}
			recs, err := batchRecords(bt)
			if err != nil || len(recs) != len(b.vals) {
				t.Fatalf("codec %d: %v, %d records for %d values", bt.Codec(), err, len(recs), len(b.vals))
			}
			for j, r := range recs {
				if string(r.Value) != b.vals[j] {
					t.Fatalf("codec %d record %d: %q != %q", bt.Codec(), j, r.Value, b.vals[j])
				}
			}
			seen[bt.Codec()]++
		}
	}
	for codec := kwire.CodecNone; codec <= kwire.CodecZstd; codec++ {
		if seen[codec] == 0 {
			t.Errorf("codec %d never built: %v", codec, seen)
		}
	}
	if corrupt == 0 {
		t.Error("no corrupt batch built")
	}
}

// A damaged batch answered CORRUPT_MESSAGE is abandoned as rejected; the same
// code for a good batch stays fatal.
func TestCorruptBatchRejectedIsAbandoned(t *testing.T) {
	w := NewWorkload(&fakeEnv{}, core.NodeRef{Node: "w"}, []core.NodeRef{{Node: "b"}}, DefaultOpts())
	h := &fakeHist{}
	w.SetHistory(h)
	c := w.loop.Clients[0]
	c.State, c.stage = session.Up, stRun
	w.newBatches(c)
	for _, b := range c.p.cur {
		b.idem, b.corrupt = false, true
	}
	w.onProduce(c, produceResp(w, c, codeCorruptMessage))
	for i, op := range h.ops {
		res, _ := op.Result.(ProduceRes)
		if op.Outcome != sim.OpAbandoned || res.Why != "rejected" {
			t.Errorf("op %d: %d %+v", i, op.Outcome, res)
		}
	}
	if len(c.p.cur) != 0 {
		t.Fatal("rejected batch kept")
	}
}
