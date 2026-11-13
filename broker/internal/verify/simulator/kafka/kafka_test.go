package kafka

import (
	"slices"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/session"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/truth"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
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
// and every recovery path must be reached by some seed. The fakes and the broker
// declare these paths where the event happens. The list is here because only a
// campaign knows what it must see.
var expectedReachable = []string{
	"obj 503 injected", "obj timeout injected", "obj lost response injected", "obj outage started", "obj get corrupted",
	"cp 503 injected", "cp timeout injected", "cp lost response injected", "cp outage started",
	"net: partition injected", "net: clog injected", "net: random_close injected", "net: black_hole injected",
	"broker: commit retried", "broker: commit succeeded after the old retry budget",
	"broker: corrupt GET detected",
	"broker: fetch parked for a poll",
	"broker: reads paused by backpressure",
	"broker: reads resumed after backpressure",
	"broker: segment sealed by size", "broker: segment sealed by timer",
	"broker: segment sealed by batch cap", "broker: segment sealed by partition cap",
	"broker: oversize batch sealed alone",
	"broker: idempotent batch rejected",
	"broker: corrupt produce batch rejected",
	"broker: gzip batch admitted", "broker: snappy batch admitted",
	"broker: lz4 batch admitted", "broker: zstd batch admitted",
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
// path that no seed reached (a Sometimes or a Reachable with no pass, or a path
// of expectedReachable).
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

func TestKafkaCampaign(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 8
	}
	results := runSeeds(t, seeds(1, n), func(seed uint64) Config { return Config{Seed: seed} })
	var crashes, exits, trips int
	for _, r := range results {
		crashes, exits, trips = crashes+r.Crashes, exits+r.Exits, trips+r.Trips
	}
	t.Logf("%d seeds: crashes=%d exits=%d trips=%d", n, crashes, exits, trips)
	report(t, results, !testing.Short()) // coverage needs a full campaign
}

func TestDeterminismCanary(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
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

// violated returns the violated props of seed, in first-failure order.
func violated(cfg Config) []string {
	var out []string
	seen := map[string]bool{}
	for _, f := range Run(cfg).Failures {
		if f.Kind == kernel.FailViolation && !seen[f.Property] {
			seen[f.Property] = true
			out = append(out, f.Property)
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
		bug  broker.InjectedBug
		prop string
	}{
		{"BugEarlyAck: ack before CommitSegment", broker.InjectedBugEarlyAck, "kafka: acked range is committed in the control plane"},
		{"BugFreshKeyOnRetry: a lost commit is retried under a fresh key", broker.InjectedBugFreshKeyOnRetry, "kafka: a stored batch is committed at most once"},
		{"BugNoPartitionCap: MaxSegmentPartitions ignored", broker.InjectedBugNoPartitionCap, "kafka: a sealed segment spans at most MaxSegmentPartitions partitions"},
		{"BugWrongOffset: fetch patches base offset + 1", broker.InjectedBugWrongOffset, "kafka: a consumer sees every offset in order, none skipped"},
		{"BugServeCorrupt: corrupt GET reaches the client", broker.InjectedBugServeCorrupt, "kafka: a consumer never receives a corrupt batch"},
		{"BugNoBatchCap: MaxSegmentBatches ignored", broker.InjectedBugNoBatchCap, "kafka: a sealed segment holds at most MaxSegmentBatches batches"},
		{"BugTimerFromLastBatch: flush window restarts on every batch", broker.InjectedBugTimerFromLastBatch, "kafka: a segment is sealed within the window of its first batch"},
		{"BugSkipProduceCRC: produce admits a corrupted batch", broker.InjectedBugSkipProduceCRC, "kafka: committed batch is intact"},
		{"BugSealOffByOne: size rule applied one batch late", broker.InjectedBugSealOffByOne, "kafka: a sealed segment overshoots MaxSegmentBytes by at most its last batch"},
	}
	for _, c := range cases {
		caught := uint64(0)
		var props []string
		for s := uint64(1); s <= budget && caught == 0; s++ {
			if props = violated(Config{Seed: s, Bug: c.bug}); len(props) > 0 {
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
		if !slices.Contains(props, c.prop) {
			t.Errorf("mutant %q: %q not among %v", c.name, c.prop, props)
		}
	}
}

// The real broker must pass every invariant on the same seeds.
func TestRealBrokerIsClean(t *testing.T) {
	for s := uint64(1); s <= 5; s++ {
		if p := violated(Config{Seed: s}); len(p) > 0 {
			t.Errorf("seed %d violated %v", s, p)
		}
	}
}

type tally struct {
	batches, definite, indet, abandoned, rejected, reads, readRecs, retried, ackFacts int
}

type tallyRule struct {
	t    *tally
	hist history.Reader[Op, Res]
}

func (*tallyRule) Name() string        { return "tally" }
func (*tallyRule) Observe(kernel.View) {}
func (i *tallyRule) AtEnd(v kernel.View) {
	i.t.ackFacts += len(v.Timeline().Snapshot("produce_acked"))
	for _, operation := range i.hist.Ops() {
		switch r := operation.Result.(type) {
		case ProduceRes:
			i.t.batches++
			switch operation.Outcome {
			case history.ResultKnown:
				i.t.definite++
			case history.ResultUnknown:
				i.t.indet++
			case history.GivenUp:
				i.t.abandoned++
				if r.Why == "rejected" {
					i.t.rejected++
				}
			case history.Pending:
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
	var tl tally
	for s := uint64(1); s <= 20; s++ {
		Run(Config{Seed: s, ExtraRules: func(_ *cluster.Cluster, h *History) []kernel.Rule {
			return []kernel.Rule{&tallyRule{t: &tl, hist: h}}
		}})
	}
	t.Logf("20 seeds: %+v", tl)
	if tl.definite < 2000 || tl.indet == 0 || tl.retried == 0 || tl.rejected == 0 || tl.readRecs < 2000 {
		t.Fatalf("campaign too weak: %+v", tl)
	}
}

// fakeEnv is the minimum Environment for driving the workload by hand.
type fakeEnv struct{ n uint64 }

func (e *fakeEnv) Uint64() uint64 { e.n++; return e.n }
func (e *fakeEnv) Intn(n int) int { e.n++; return int(e.n % uint64(n)) }
func (e *fakeEnv) NewID() machine.ID {
	e.n++
	var id machine.ID
	id[0], id[1] = byte(e.n), byte(e.n>>8)
	return id
}
func (*fakeEnv) Maybe(fault.NamedSite) bool                                           { return false }
func (*fakeEnv) MaybeP(fault.NamedSite, float64) bool                                 { return false }
func (*fakeEnv) Fault(fault.NamedSite, float64) bool                                  { return false }
func (*fakeEnv) Always(bool, assert.Always, ...assert.Pair)                           {}
func (*fakeEnv) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (*fakeEnv) Sometimes(bool, assert.Sometimes)                                     {}
func (*fakeEnv) Reachable(assert.Reachable)                                           {}
func (*fakeEnv) Unreachable(assert.Unreachable, ...assert.Pair)                       {}

// newTestHistory is a history for a workload that is driven by hand.
func newTestHistory() *History {
	return history.New[Op, Res](func() kernel.Time { return 0 }, func(string) {})
}

// The real broker never answers a fatal code to a plain batch, so the give-up
// path is driven by hand: a definite rejection abandons the batch, an earlier
// ambiguous attempt makes it indeterminate.
func TestFatalProduceErrorGivesUp(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		h := newTestHistory()
		w := NewWorkload(&fakeEnv{}, machine.NodeInstance{Node: "w"}, []machine.NodeID{"b"}, h, DefaultOpts())
		c := w.loop.Clients[0]
		c.State, c.stage = session.Connected, stateRunning
		w.newBatches(c)
		if len(h.Ops()) == 0 || len(c.p.cur) != len(h.Ops()) {
			t.Fatalf("no batches built: %d ops", len(h.Ops()))
		}
		for _, b := range c.p.cur {
			b.ambiguous, b.idem = ambiguous, false
		}
		w.onProduce(c, produceResp(w, c, 45))
		want := history.GivenUp
		if ambiguous {
			want = history.ResultUnknown
		}
		for i, operation := range h.Ops() {
			if operation.Outcome != want {
				t.Errorf("ambiguous=%v op %d: outcome %d, want %d", ambiguous, i, operation.Outcome, want)
			}
		}
		if len(c.p.cur) != 0 {
			t.Errorf("ambiguous=%v: %d batches still unresolved", ambiguous, len(c.p.cur))
		}
	}
}

func produceResp(w *Workload, c *client, code int16) *kmsg.ProduceResponse {
	response := kmsg.NewPtrProduceResponse()
	for _, b := range c.p.cur {
		topicPartition := w.tps[b.tpIdx]
		response.Topics = append(response.Topics, kmsg.ProduceResponseTopic{Topic: topicPartition.Topic,
			Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: topicPartition.Partition, ErrorCode: code, BaseOffset: -1}}})
	}
	return response
}

// A batch that claims a producer id is resolved as rejected on INVALID_REQUEST
// (even after an ambiguous earlier attempt) and the producer carries on.
func TestIdemBatchRejectedIsAbandoned(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		h := newTestHistory()
		w := NewWorkload(&fakeEnv{}, machine.NodeInstance{Node: "w"}, []machine.NodeID{"b"}, h, DefaultOpts())
		c := w.loop.Clients[0]
		c.State, c.stage = session.Connected, stateRunning
		w.newBatches(c)
		for _, b := range c.p.cur {
			b.ambiguous, b.idem = ambiguous, true
		}
		acts := w.onProduce(c, produceResp(w, c, codeInvalidRequest))
		for i, operation := range h.Ops() {
			res, _ := operation.Result.(ProduceRes)
			if operation.Outcome != history.GivenUp || res.Why != "rejected" || res.Base != -1 {
				t.Errorf("ambiguous=%v op %d: %d %+v", ambiguous, i, operation.Outcome, res)
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
	w := NewWorkload(&rngEnv{x: 88172645463325252}, machine.NodeInstance{Node: "w"}, []machine.NodeID{"b"}, newTestHistory(), DefaultOpts())
	c := w.loop.Clients[0]
	idem, plain := 0, 0
	for range 400 {
		c.p.cur = nil
		w.newBatches(c)
		for _, b := range c.p.cur {
			if b.corrupt {
				continue
			}
			bt, err := kafkawire.ParseBatch(b.data)
			if err != nil {
				t.Fatal(err)
			}
			if b.idem {
				idem++
				if bt.ProducerID != duplicateCheckProducerID {
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
	verified := runSeeds(t, ss, func(seed uint64) Config { return Config{Seed: seed, VerifyHashes: true} })
	report(t, verified, !testing.Short())
	for i, seed := range ss {
		plain := Run(Config{Seed: seed})
		if v := verified[i]; v.Hash != plain.Hash || v.Steps != plain.Steps || v.RandCalls != plain.RandCalls {
			t.Errorf("seed %d: verify mode changed the trace", seed)
		}
	}
}

// A broker that ignores its partition cap is refused by the control plane's
// commit limit too (tight on half the seeds: 1 + the cap), not only caught by
// the cap invariants.
func TestCommitLimitRefusesUncappedSegment(t *testing.T) {
	for s := uint64(1); s <= 40; s++ {
		if slices.Contains(violated(Config{Seed: s, Bug: broker.InjectedBugNoPartitionCap}), "broker: segment exceeds the control plane's commit size") {
			return
		}
	}
	t.Fatal("no seed in 40 had the control plane refuse an uncapped segment")
}

// wideCampaign runs 128 producers over many partitions with the production
// broker defaults except for the partition cap, against the speccheck-backed
// store limited to DynamoDB's 100-item commit. It returns the widest committed
// segment, in partitions. (Log-store variant deferred: docs/cp-extraction.md.)
func wideCampaign(t *testing.T, capParts, batches int) int64 {
	t.Helper()
	prod := broker.DefaultConfig()
	prod.MaxSegmentPartitions, prod.MaxSegmentBatches = capParts, batches
	prod.MaxBufferedBytes, prod.ResumeBufferedBytes = 64<<20, 32<<20
	opts := Opts{Producers: 128, Consumers: 1, Topics: []string{"a", "b"}, Partitions: 64}
	items := fakeservices.DynamoMaxCommitItems
	var widest atomic.Int64
	results := runSeeds(t, seeds(1, 2), func(seed uint64) Config {
		return Config{
			Seed: seed, Opts: opts,
			Tune: func(s *Settings) { s.Broker, s.ControlPlane.MaxCommitItems = prod, items },
			ExtraRules: func(c *cluster.Cluster, _ *History) []kernel.Rule {
				return []kernel.Rule{&widestRule{widest: &widest, tps: opts.topicPartitions(), cp: c.ControlPlane}}
			},
		}
	})
	report(t, results, false)
	return widest.Load()
}

// The old store (decisions 39, 41, 44): a commit costs 1 + distinct partitions
// (+ grown topics) of the 100 items of a transaction, so the broker must run with
// -max-segment-partitions 96 (3 spare items). At that cap the control plane
// refuses nothing; the widest committed segment must be over half of the cap,
// or the test proves nothing.
func TestProductionCapsFitDynamoLimit(t *testing.T) {
	const capParts = 96
	if 1+capParts > fakeservices.DynamoMaxCommitItems || fakeservices.DynamoMaxCommitItems-1-capParts < 3 {
		t.Fatalf("1 + %d partitions leaves no spare items under %d", capParts, fakeservices.DynamoMaxCommitItems)
	}
	w := wideCampaign(t, capParts, 256)
	t.Logf("old store: widest committed segment %d partitions (cap %d)", w, capParts)
	if int(w) <= capParts/2 {
		t.Errorf("segments spanned at most %d partitions: the cap was never approached", w)
	}
}

// The log store (decision 52) campaign needs replica + fakelogstore in
// mds-controlplane; skip until reconnect.
func TestLogStoreTakesTheDefaultCap(t *testing.T) {
	t.Skip("control plane extracted to mds-controlplane; reconnect later")
}

// widestRule records the most distinct partitions in one committed segment.
type widestRule struct {
	widest *atomic.Int64
	tps    []core.TopicPartition
	cp     truth.ControlPlane
}

func (*widestRule) Name() string        { return "widest" }
func (*widestRule) Observe(kernel.View) {}
func (i *widestRule) AtEnd(kernel.View) {
	parts := map[string]map[core.TopicPartition]bool{}
	for _, topicPartition := range i.tps {
		for _, r := range i.cp.Committed(topicPartition) {
			if parts[r.Loc.Key] == nil {
				parts[r.Loc.Key] = map[core.TopicPartition]bool{}
			}
			parts[r.Loc.Key][topicPartition] = true
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
	h := newTestHistory()
	w := NewWorkload(&rngEnv{x: 88172645463325252}, machine.NodeInstance{Node: "w"}, []machine.NodeID{"b"}, h, DefaultOpts())
	c := w.loop.Clients[0]
	seen := map[int]int{}
	corrupt := 0
	for range 400 {
		c.p.cur = nil
		w.newBatches(c)
		for _, b := range c.p.cur {
			if b.corrupt {
				corrupt++
				if _, err := kafkawire.ParseBatch(b.data); err == nil {
					t.Fatal("corrupt batch parses")
				}
				continue
			}
			bt, err := kafkawire.ParseBatch(b.data)
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
	for codec := kafkawire.CodecNone; codec <= kafkawire.CodecZstd; codec++ {
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
	h := newTestHistory()
	w := NewWorkload(&fakeEnv{}, machine.NodeInstance{Node: "w"}, []machine.NodeID{"b"}, h, DefaultOpts())
	c := w.loop.Clients[0]
	c.State, c.stage = session.Connected, stateRunning
	w.newBatches(c)
	for _, b := range c.p.cur {
		b.idem, b.corrupt = false, true
	}
	w.onProduce(c, produceResp(w, c, codeCorruptMessage))
	for i, operation := range h.Ops() {
		res, _ := operation.Result.(ProduceRes)
		if operation.Outcome != history.GivenUp || res.Why != "rejected" {
			t.Errorf("op %d: %d %+v", i, operation.Outcome, res)
		}
	}
	if len(c.p.cur) != 0 {
		t.Fatal("rejected batch kept")
	}
}
