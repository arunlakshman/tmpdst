package cptest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpclient"
	"manifester/veritas"
)

// lockstep runs ops on impl and a fresh spec.
func lockstep(t *testing.T, im impl, ops []op) {
	t.Helper()
	p := pair{im, veritas.NewControlPlane()}
	for i, o := range ops {
		if err := step(p, o); err != nil {
			t.Fatalf("op %d (%+v): %v", i, o, err)
		}
	}
}

// windowOps drives one producer past the 5-entry window, then retries an
// evicted and a live sequence, and a sequence gap.
func windowOps() []op {
	ops := []op{{Kind: "register", Node: "a"}, {Kind: "init"}}
	tp := core.TopicPartition{Topic: "w", Partition: 0}
	commit := func(key string, seq int32, recs int64) op {
		return op{Kind: "commit", Node: "a", Name: key, Epoch: 1, Batches: []core.PendingBatch{
			{TP: tp, Records: recs, Loc: core.SegmentRef{Key: key, Len: 10}, Producer: 1000, BaseSeq: seq}}}
	}
	for i := 0; i < 8; i++ {
		ops = append(ops, commit(fmt.Sprintf("s%d", i), int32(i*2), 2))
	}
	ops = append(ops,
		commit("r-old", 0, 2),   // evicted from the window: out of order
		commit("r-live", 6, 2),  // oldest in the window: dup
		commit("r-size", 6, 3),  // same seq, other size: out of order
		commit("r-gap", 40, 1),  // gap
		commit("r-next", 16, 1), // next expected
		op{Kind: "lookup", TP: tp},
		op{Kind: "lookup", TP: tp, From: 5, MaxBytes: 25},
		op{Kind: "describe"},
	)
	return ops
}

func TestWindowEvictionMemstore(t *testing.T) { lockstep(t, storeImpl{newMem()}, windowOps()) }
func TestWindowEvictionDynamo(t *testing.T)   { lockstep(t, storeImpl{dynamoStore(t)}, windowOps()) }

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newMem()
	srv := httptest.NewServer(cp.NewHandler(st))
	defer srv.Close()
	c := cpclient.New(srv.URL, srv.Client())

	if resp, err := http.Get(srv.URL + "/healthz"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v", err)
	}
	self := core.NodeRef{Node: "n", Inc: core.Incarnation{1, 2, 3}}
	r, err := c.Call(ctx, core.Register{Self: self})
	if err != nil || r.(core.RegisterResp).Epoch != 1 {
		t.Fatalf("register: %v %v", r, err)
	}
	p, err := c.Call(ctx, core.InitProducer{})
	if err != nil || p.(core.InitProducerResp).Producer != 1000 {
		t.Fatalf("init: %v %v", p, err)
	}
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	commit := core.CommitSegment{SegmentKey: "k", Owner: self, Epoch: 1, Batches: []core.PendingBatch{
		{TP: core.TopicPartition{Topic: "t", Partition: 1}, Records: 4, Loc: core.SegmentRef{Key: "k", Off: 8, Len: 40}, Producer: 1000, BaseSeq: 0},
	}}
	cr, err := c.Call(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got := cr.(core.CommitSegmentResp).Assigned; len(got) != 1 || got[0].Base != 0 || got[0].Count != 4 || got[0].Loc != commit.Batches[0].Loc {
		t.Fatalf("commit: %+v", got)
	}
	l, err := c.Call(ctx, core.Lookup{TP: commit.Batches[0].TP})
	if err != nil || l.(core.LookupResp).HighWatermark != 4 || len(l.(core.LookupResp).Ranges) != 1 {
		t.Fatalf("lookup: %+v %v", l, err)
	}
	d, err := c.Call(ctx, core.DescribeTopics{})
	if err != nil || d.(core.DescribeTopicsResp).Topics[0] != (core.TopicInfo{Name: "t", Partitions: 3}) {
		t.Fatalf("describe: %+v %v", d, err)
	}
	// Business errors come back as typed errors.
	commit.SegmentKey, commit.Epoch = "k2", 9
	_, err = c.Call(ctx, commit)
	var be *cpclient.Error
	if !errors.As(err, &be) || be.Code != core.ErrFenced {
		t.Fatalf("want ErrFenced, got %v", err)
	}
	if code, out := (cpclient.Classifier{}).Classify(core.CPCall{Op: commit}, err); code != core.ErrFenced || out != core.NotApplied {
		t.Fatalf("classify: %v %v", code, out)
	}
	// Malformed body: ErrInvalid at 200.
	resp, _ := http.Post(srv.URL+"/v1/lookup", "application/json", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("malformed: %d", resp.StatusCode)
	}
}

type failStore struct {
	cp.Store
	err error
}

func (f failStore) Register(context.Context, core.Register) (core.RegisterResp, error) {
	return core.RegisterResp{}, f.err
}

func TestHandlerStatus(t *testing.T) {
	for _, c := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("busy: %w", cp.ErrUnavailable), 503},
		{context.DeadlineExceeded, 503},
		{errors.New("boom"), 500},
	} {
		srv := httptest.NewServer(cp.NewHandler(failStore{newMem(), c.err}))
		resp, err := http.Post(srv.URL+"/v1/register", "application/json", stringsReader(`{"self":{"node":"n","inc":"00000000000000000000000000000000"}}`))
		srv.Close()
		if err != nil || resp.StatusCode != c.want {
			t.Fatalf("%v: status %v err %v", c.err, resp, err)
		}
	}
}

// Concurrency on DynamoDB: several replicas (Store values) over one table.

func TestDynamoConcurrentCommits(t *testing.T) {
	first := dynamoStore(t)
	replicas := []cp.Store{first}
	for i := 0; i < 3; i++ {
		replicas = append(replicas, newReplica(t, first))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const workers, perWorker = 12, 8
	tps := []core.TopicPartition{{Topic: "c", Partition: 0}, {Topic: "c", Partition: 1}}

	var wg sync.WaitGroup
	var mu sync.Mutex
	pids := map[core.ProducerID]bool{}
	assigned := map[core.TopicPartition][][2]int64{} // (base, count)
	dups := 0
	var unavailable atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := replicas[w%len(replicas)]
			self := core.NodeRef{Node: core.NodeID(fmt.Sprintf("n%d", w))}
			reg, err := st.Register(ctx, core.Register{Self: self})
			if err != nil {
				t.Error(err)
				return
			}
			ip, err := st.InitProducer(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			pids[ip.Producer] = true
			mu.Unlock()
			for i := 0; i < perWorker; i++ {
				batches := []core.PendingBatch{
					{TP: tps[0], Records: 2, Loc: core.SegmentRef{Key: "x", Len: 5}},
					{TP: tps[1], Records: 3, Loc: core.SegmentRef{Key: "x", Len: 5}, Producer: ip.Producer, BaseSeq: int32(3 * i)},
				}
				// The same batches under two keys: the second is a producer retry.
				for _, key := range []string{fmt.Sprintf("w%d-%d", w, i), fmt.Sprintf("w%d-%d-retry", w, i)} {
					var resp core.CommitSegmentResp
					var code core.ErrCode
					// A 503 is NotApplied and the commit idempotent: retry.
					for try := 0; try < 5; try++ {
						resp, code, err = st.CommitSegment(ctx, core.CommitSegment{SegmentKey: key, Owner: self, Epoch: reg.Epoch, Batches: batches})
						if !errors.Is(err, cp.ErrUnavailable) {
							break
						}
						unavailable.Add(1)
					}
					if err != nil || code != core.OK {
						t.Errorf("commit %s: %v %v", key, code, err)
						return
					}
					mu.Lock()
					for _, r := range resp.Assigned {
						if r.Dup {
							dups++
						} else if r.Err == core.OK {
							assigned[r.TP] = append(assigned[r.TP], [2]int64{int64(r.Base), r.Count})
						}
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("503s retried by workers: %d", unavailable.Load())
	if t.Failed() {
		return
	}
	if len(pids) != workers {
		t.Errorf("producer ids not unique: %d", len(pids))
	}
	// Partition 0 batches are not idempotent: every commit (original and
	// retry) assigns. Partition 1 retries are dups: one assignment per i.
	want := map[core.TopicPartition]int{tps[0]: workers * perWorker * 2, tps[1]: workers * perWorker}
	if dups != workers*perWorker {
		t.Errorf("dups %d want %d", dups, workers*perWorker)
	}
	for tp, list := range assigned {
		if len(list) != want[tp] {
			t.Errorf("%v: %d assignments, want %d", tp, len(list), want[tp])
		}
		seen := map[int64]bool{}
		var total int64
		for _, a := range list {
			if seen[a[0]] {
				t.Errorf("%v: duplicate base %d", tp, a[0])
			}
			seen[a[0]] = true
			total += a[1]
		}
		// Contiguous: bases cover exactly [0, total).
		var next int64
		for next < total {
			var size int64
			for _, a := range list {
				if a[0] == next {
					size = a[1]
				}
			}
			if size == 0 {
				t.Errorf("%v: gap at %d (total %d)", tp, next, total)
				break
			}
			next += size
		}
		resp, _, err := first.Lookup(ctx, core.Lookup{TP: tp})
		if err != nil || int64(resp.HighWatermark) != total || len(resp.Ranges) != len(list) {
			t.Errorf("%v lookup: hwm %d ranges %d, want %d / %d (%v)", tp, resp.HighWatermark, len(resp.Ranges), total, len(list), err)
		}
	}
}

// The same segment key committed by many goroutines at once: one winner, all
// see the same ranges.
func TestDynamoSameKeyRace(t *testing.T) {
	st := dynamoStore(t)
	ctx := context.Background()
	self := core.NodeRef{Node: "n"}
	reg, _ := st.Register(ctx, core.Register{Self: self})
	op := core.CommitSegment{SegmentKey: "same", Owner: self, Epoch: reg.Epoch, Batches: []core.PendingBatch{
		{TP: core.TopicPartition{Topic: "r"}, Records: 5, Loc: core.SegmentRef{Key: "same", Len: 9}}}}
	var wg sync.WaitGroup
	res := make([]core.CommitSegmentResp, 10)
	for i := range res {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, code, err := st.CommitSegment(ctx, op)
			if err != nil || code != core.OK {
				t.Errorf("%v %v", code, err)
			}
			res[i] = r
		}()
	}
	wg.Wait()
	for _, r := range res {
		if fmt.Sprint(r) != fmt.Sprint(res[0]) || r.Assigned[0].Base != 0 {
			t.Fatalf("diverged: %+v vs %+v", r, res[0])
		}
	}
	if l, _, _ := st.Lookup(ctx, core.Lookup{TP: op.Batches[0].TP}); l.HighWatermark != 5 {
		t.Fatalf("hwm %d", l.HighWatermark)
	}
}

func TestDynamoTooLarge(t *testing.T) {
	st := dynamoStore(t)
	ctx := context.Background()
	self := core.NodeRef{Node: "n"}
	reg, _ := st.Register(ctx, core.Register{Self: self})
	var bs []core.PendingBatch
	for i := 0; i < 60; i++ {
		bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "big", Partition: int32(i)}, Records: 1, Loc: core.SegmentRef{Key: "b", Len: 1}})
	}
	_, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: "b", Owner: self, Epoch: reg.Epoch, Batches: bs})
	if err != nil || code != core.ErrTooLarge {
		t.Fatalf("120 items: %v %v", code, err)
	}
	_, code, err = st.CommitSegment(ctx, core.CommitSegment{SegmentKey: "b2", Owner: self, Epoch: reg.Epoch, Batches: bs[:40]})
	if err != nil || code != core.OK {
		t.Fatalf("82 items: %v %v", code, err)
	}
}

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }
