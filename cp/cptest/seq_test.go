package cptest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/dynamo"
	"manifester/cp/dynamo/fakedb"
	"manifester/veritas"
)

// The sequencer is a cp.Store: the same lockstep suite as for the plain store,
// then concurrent committers whose answers are checked against the spec by
// linearisation (below).

func seqOver(t testing.TB, st *dynamo.Store, o dynamo.SeqOptions) *dynamo.Sequencer {
	s := dynamo.NewSequencer(st, o)
	t.Cleanup(s.Close)
	return s
}

func TestConformanceSeqFake(t *testing.T) {
	for _, o := range []dynamo.SeqOptions{{}, {CacheKeys: 2, Pipeline: 1}, {CacheKeys: -1, Pipeline: 8}} {
		run(t, func() impl {
			st, _ := fakeStore(t)
			return storeImpl{seqOver(t, st, o)}
		}, 120, 25)
	}
}

func TestConformanceSeqFakeHTTP(t *testing.T) {
	run(t, func() impl { st, _ := fakeStore(t); return httpOver(t, seqOver(t, st, dynamo.SeqOptions{})) }, 100, 10)
}

func TestConformanceSeqDynamo(t *testing.T) {
	needDynamo(t)
	run(t, func() impl { return storeImpl{seqOver(t, dynamoStore(t), dynamo.SeqOptions{})} }, 80, 12)
}

// ---- concurrent committers, checked against the spec ----

// done is one accepted commit: what was asked, what was answered.
type done struct {
	op   core.CommitSegment
	resp core.CommitSegmentResp
	lat  time.Duration
}

// loadSegment is a segment of nparts distinct partitions of parts (one batch each).
func loadSegment(r *rand.Rand, key string, parts, nparts int) core.CommitSegment {
	var bs []core.PendingBatch
	for i, p := range r.Perm(parts)[:nparts] {
		bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "load", Partition: int32(p)},
			Records: int64(1 + r.IntN(5)), Loc: core.SegmentRef{Key: key, Off: int64(i) * 100, Len: 100}})
	}
	return core.CommitSegment{SegmentKey: key, Batches: bs}
}

// runLoad has workers goroutines ("brokers"), worker w using stores[w%len],
// commit per segments each (a 503 is resent under the same key, as a broker
// does; every answer is replayed once through another store).
func runLoad(t testing.TB, prefix string, stores []cp.Store, workers, per, parts, nparts int) []done {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var mu sync.Mutex
	var out []done
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 99))
			for i := 0; i < per; i++ {
				op := loadSegment(r, fmt.Sprintf("%sb%d-%d", prefix, w, i), parts, nparts)
				t0 := time.Now()
				resp := commitRetrying(ctx, t.(*testing.T), stores[w%len(stores)], op)
				lat := time.Since(t0)
				if again := commitRetrying(ctx, t.(*testing.T), stores[(w+1)%len(stores)], op); fmt.Sprint(again) != fmt.Sprint(resp) {
					t.Errorf("replay of %s: %v, want %v", op.SegmentKey, again, resp)
				}
				mu.Lock()
				out = append(out, done{op, resp, lat})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return out
}

// linearize orders the commits so that, for every partition, they follow the
// offsets they were given (Kahn's algorithm over "directly before" edges). The
// transactions are atomic and per-partition ordered, so a cycle means a bug.
func linearize(t testing.TB, ds []done) []done {
	t.Helper()
	byKey := map[string]int{}
	for i, d := range ds {
		byKey[d.op.SegmentKey] = i
	}
	type at struct {
		base int64
		idx  int
	}
	perPart := map[core.TopicPartition][]at{}
	for i, d := range ds {
		for _, r := range d.resp.Assigned {
			if !slices.ContainsFunc(perPart[r.TP], func(a at) bool { return a.idx == i }) {
				perPart[r.TP] = append(perPart[r.TP], at{int64(r.Base), i})
			}
		}
	}
	succ, indeg := map[int][]int{}, make([]int, len(ds))
	for _, list := range perPart {
		slices.SortFunc(list, func(a, b at) int { return int(a.base - b.base) })
		for j := 1; j < len(list); j++ {
			succ[list[j-1].idx] = append(succ[list[j-1].idx], list[j].idx)
			indeg[list[j].idx]++
		}
	}
	var queue, order []int
	for i := range ds {
		if indeg[i] == 0 {
			queue = append(queue, i)
		}
	}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		order = append(order, i)
		for _, j := range succ[i] {
			if indeg[j]--; indeg[j] == 0 {
				queue = append(queue, j)
			}
		}
	}
	if len(order) != len(ds) {
		t.Fatalf("no serial order: %d of %d commits ordered (a cycle between partitions)", len(order), len(ds))
	}
	out := make([]done, len(ds))
	for k, i := range order {
		out[k] = ds[i]
	}
	return out
}

// lockstepOrder runs the commits through the spec in a serial order that
// respects the offsets and compares every answer. It also checks the table:
// contiguous, non-overlapping, ends equal the spec's.
func lockstepOrder(t testing.TB, ds []done, st cp.Store) {
	t.Helper()
	spec := veritas.NewControlPlane()
	for _, d := range linearize(t, ds) {
		got := spec.Exec(core.CPCall{Op: d.op}).(core.CPDone)
		if show(d.resp, core.OK) != show(got.Resp, got.Err) {
			t.Fatalf("%s:\n store: %s\n spec:  %s", d.op.SegmentKey, show(d.resp, core.OK), show(got.Resp, got.Err))
		}
	}
	perPart := map[core.TopicPartition][][2]int64{}
	for _, d := range ds {
		for _, r := range d.resp.Assigned {
			perPart[r.TP] = append(perPart[r.TP], [2]int64{int64(r.Base), r.Count})
		}
	}
	for tp, list := range perPart {
		total := checkContiguousT(t, tp, list)
		l, _, err := st.Lookup(context.Background(), core.Lookup{TP: tp})
		if err != nil || int64(l.End) != total || int64(spec.End(tp)) != total || len(l.Ranges) != len(list) {
			t.Fatalf("%v: end %d (spec %d), ranges %d, want %d / %d: %v", tp, l.End, spec.End(tp), len(l.Ranges), total, len(list), err)
		}
	}
}

func checkContiguousT(t testing.TB, tp core.TopicPartition, list [][2]int64) int64 {
	t.Helper()
	return checkContiguous(t.(*testing.T), tp, list)
}

func lowLatency(f *fakedb.Fake) {
	f.Latency = func(call string) time.Duration {
		if call == "TransactWriteItems" {
			return 3 * time.Millisecond
		}
		return time.Millisecond
	}
}

// Six brokers, one sequencer: 288 partitions, 33 per segment. No transaction
// is cancelled, offsets are contiguous and equal the spec's.
func TestSeqSixBrokersOneSequencer(t *testing.T) {
	st, f := fakeStore(t)
	lowLatency(f)
	s := seqOver(t, st, dynamo.SeqOptions{})
	ds := runLoad(t, "", []cp.Store{s}, 6, 25, 288, 33)
	f.Latency = nil
	lockstepOrder(t, ds, s)
	if sts := s.Stats(); sts.Canceled != 0 || sts.Unknown != 0 || sts.Segments != 150 || sts.Txs >= 150 {
		t.Fatalf("%+v", sts) // grouping must have merged some commits
	}
}

// Split brain: two sequencers on one table. Correctness holds and the lost
// races show up as retries inside the sequencers.
func TestSeqTwoSequencersSplitBrain(t *testing.T) {
	st, f := fakeStore(t)
	lowLatency(f)
	a := seqOver(t, st, dynamo.SeqOptions{})
	b := seqOver(t, dynamo.New(f, st.Prefix()), dynamo.SeqOptions{})
	ds := runLoad(t, "", []cp.Store{a, b}, 6, 20, 288, 33)
	f.Latency = nil
	lockstepOrder(t, ds, a)
	if c := a.Stats().Canceled + b.Stats().Canceled; c == 0 {
		t.Fatal("two writers never collided: the test proves nothing")
	}
	// An old-style store beside a sequencer is equally safe.
	old := dynamo.New(f, st.Prefix())
	ds = append(ds, runLoad(t, "old-", []cp.Store{a, old}, 4, 10, 288, 33)...)
	lockstepOrder(t, ds, a)
}

// Restart: a new sequencer on an existing table loads ends lazily and goes on.
func TestSeqRestartReloadsLazily(t *testing.T) {
	st, f := fakeStore(t)
	first := seqOver(t, st, dynamo.SeqOptions{})
	ds := runLoad(t, "", []cp.Store{first}, 3, 10, 288, 33)
	first.Close()
	second := seqOver(t, dynamo.New(f, st.Prefix()), dynamo.SeqOptions{})
	op := loadSegment(rand.New(rand.NewPCG(5, 5)), "after", 288, 2)
	resp, code, err := second.CommitSegment(context.Background(), op)
	if err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	if sts := second.Stats(); sts.EndMisses != 2 || sts.Txs != 1 {
		t.Fatalf("lazy load: %+v", sts) // only the two touched partitions were read
	}
	ds = append(ds, done{op: op, resp: resp})
	ds = append(ds, runLoad(t, "2-", []cp.Store{second}, 3, 10, 288, 33)...)
	// first's keys replay through the new sequencer: G# is read, same answer
	again, code, err := second.CommitSegment(context.Background(), ds[0].op)
	if err != nil || code != core.OK || fmt.Sprint(again) != fmt.Sprint(ds[0].resp) {
		t.Fatalf("replay after restart: %v %v %+v", code, err, again)
	}
	lockstepOrder(t, ds, second)
}

// Throttling, errors and lost answers (the transaction applied, the caller
// was told it failed) under load from six brokers: still exactly once.
func TestSeqChaosSixBrokers(t *testing.T) {
	st, f := fakeStore(t)
	lowLatency(f)
	var mu sync.Mutex
	r := rand.New(rand.NewPCG(1, 2))
	roll := func(n int) int { mu.Lock(); defer mu.Unlock(); return r.IntN(n) }
	f.Fail = func(call string) error {
		n := map[string]int{"TransactWriteItems": 15, "Query": 80, "BatchGetItem": 20}[call]
		if n == 0 {
			return nil
		}
		switch roll(n) {
		case 0:
			return &types.ThrottlingException{Message: aws.String("slow")}
		case 1:
			return errors.New("connection reset")
		}
		return nil
	}
	f.AfterApply = func() error {
		if roll(8) == 0 {
			return &types.InternalServerError{Message: aws.String("answer lost")}
		}
		return nil
	}
	s := seqOver(t, st, dynamo.SeqOptions{MaxAttempts: 500, MaxTime: time.Minute})
	ds := runLoad(t, "", []cp.Store{s}, 6, 15, 288, 33)
	f.Fail, f.AfterApply, f.Latency = nil, nil, nil
	lockstepOrder(t, ds, s)
	if sts := s.Stats(); sts.Unknown == 0 || sts.NotApplied == 0 {
		t.Fatalf("no injected failure was seen: %+v", sts)
	}
}
