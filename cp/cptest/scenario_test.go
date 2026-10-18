package cptest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
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

// replayOps commits a three-batch, two-partition segment, replays it after the
// ends moved, sends a different request under its key, an invalid request, and
// a retry of the same batches under a fresh key (new offsets).
func replayOps() []op {
	a, b := core.TopicPartition{Topic: "w", Partition: 0}, core.TopicPartition{Topic: "w", Partition: 1}
	batches := func(key string) []core.PendingBatch {
		return []core.PendingBatch{
			{TP: a, Records: 2, Loc: core.SegmentRef{Key: key, Off: 0, Len: 20}},
			{TP: b, Records: 5, Loc: core.SegmentRef{Key: key, Off: 20, Len: 50}},
			{TP: a, Records: 3, Loc: core.SegmentRef{Key: key, Off: 70, Len: 30}},
		}
	}
	commit := func(key string, bs []core.PendingBatch) op { return op{Kind: "commit", Name: key, Batches: bs} }
	return []op{
		commit("s1", batches("s1")),
		commit("s2", batches("s2")),
		commit("s1", batches("s1")),                // replay: the original ranges
		commit("s1", batches("s2")),                // other request under s1: conflict
		commit("s1", nil),                          // malformed under a committed key: conflict
		commit("s3", []core.PendingBatch{{TP: a}}), // invalid: no records
		commit("s3", batches("s3")),                // the failed key is reusable
		commit("re", batches("s3")),                // the same batches, a fresh key: new offsets
		{Kind: "lookup", TP: a}, {Kind: "lookup", TP: b, From: 6, MaxBytes: 25},
		{Kind: "describe"},
	}
}

func TestReplayMemstore(t *testing.T) { lockstep(t, storeImpl{newMem()}, replayOps()) }
func TestReplayDynamo(t *testing.T)   { lockstep(t, storeImpl{dynamoStore(t)}, replayOps()) }

// manyBatchesOps commits segments of many small batches (one DynamoDB range
// item per partition and segment), with a replay
// of a segment, a segment spanning two runs of one partition (two commits), and
// a Lookup from every offset with and without a byte limit.
func manyBatchesOps() []op {
	var ops []op
	a := core.TopicPartition{Topic: "m", Partition: 0}
	b := core.TopicPartition{Topic: "m", Partition: 1}
	var s1, s2 []core.PendingBatch
	for i := 0; i < 200; i++ { // 200 batches for a, interleaved with b
		for _, tp := range []core.TopicPartition{a, b} {
			if tp == b && i%4 != 0 {
				continue
			}
			s1 = append(s1, core.PendingBatch{TP: tp, Records: int64(1 + i%3),
				Loc: core.SegmentRef{Key: "big1", Off: int64(i) * 70, Len: int64(10 + i%5)}})
		}
	}
	// Several batches of one partition around one of another.
	s2 = []core.PendingBatch{
		{TP: a, Records: 2, Loc: core.SegmentRef{Key: "big2", Off: 0, Len: 9}},
		{TP: a, Records: 2, Loc: core.SegmentRef{Key: "big2", Off: 9, Len: 9}},
		{TP: a, Records: 3, Loc: core.SegmentRef{Key: "big2", Off: 18, Len: 9}},
		{TP: b, Records: 1, Loc: core.SegmentRef{Key: "big2", Off: 27, Len: 9}},
		{TP: a, Records: 1, Loc: core.SegmentRef{Key: "big2", Off: 36, Len: 9}},
	}
	commit := func(key string, bs []core.PendingBatch) op {
		return op{Kind: "commit", Name: key, Batches: bs}
	}
	ops = append(ops, commit("big1", s1), commit("big1", s1), commit("big2", s2), commit("big2", s2),
		commit("big3", s1), // a second run on each partition, right after the first
		commit("big1", s2)) // same key, other batches: conflict
	for _, tp := range []core.TopicPartition{a, b, {Topic: "m", Partition: 2}} {
		for from := int64(0); from <= 520; from += 7 {
			for _, mb := range []int32{0, 1, 25, 300} {
				ops = append(ops, op{Kind: "lookup", TP: tp, From: from, MaxBytes: mb})
			}
		}
	}
	return append(ops, op{Kind: "describe"})
}

func TestManyBatchesMemstore(t *testing.T) { lockstep(t, storeImpl{newMem()}, manyBatchesOps()) }
func TestManyBatchesDynamo(t *testing.T)   { lockstep(t, storeImpl{dynamoStore(t)}, manyBatchesOps()) }

// Another replica replays a committed many-batch segment: the same answer.
func TestManyBatchesReplayOtherReplica(t *testing.T) {
	st := dynamoStore(t)
	ctx := context.Background()
	var bs []core.PendingBatch
	for i := 0; i < 200; i++ {
		bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "r", Partition: 0}, Records: 1,
			Loc: core.SegmentRef{Key: "s", Off: int64(i), Len: 1}})
	}
	op := core.CommitSegment{SegmentKey: "s", Batches: bs}
	first, code, err := st.CommitSegment(ctx, op)
	if err != nil || code != core.OK || len(first.Assigned) != 200 || first.Assigned[199].Base != 199 {
		t.Fatalf("first: %v %v %d", code, err, len(first.Assigned))
	}
	again, code, err := newReplica(t, st).CommitSegment(ctx, op)
	if err != nil || code != core.OK || fmt.Sprint(again) != fmt.Sprint(first) {
		t.Fatalf("replay: %v %v", code, err)
	}
	l, _, _ := st.Lookup(ctx, core.Lookup{TP: bs[0].TP, From: 150})
	if len(l.Ranges) != 50 || l.Ranges[0].Base != 150 || l.End != 200 {
		t.Fatalf("lookup: %d ranges end %d", len(l.Ranges), l.End)
	}
}

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newMem()
	srv := httptest.NewServer(cp.NewHandler(st))
	defer srv.Close()
	c := cpclient.New(srv.URL, srv.Client())

	if resp, err := http.Get(srv.URL + "/healthz"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v", err)
	}
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	commit := core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{
		{TP: core.TopicPartition{Topic: "t", Partition: 1}, Records: 4, Loc: core.SegmentRef{Key: "k", Off: 8, Len: 40}},
	}}
	cr, err := c.Call(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got := cr.(core.CommitSegmentResp).Assigned; len(got) != 1 || got[0].Base != 0 || got[0].Count != 4 || got[0].Loc != commit.Batches[0].Loc {
		t.Fatalf("commit: %+v", got)
	}
	l, err := c.Call(ctx, core.Lookup{TP: commit.Batches[0].TP})
	if err != nil || l.(core.LookupResp).End != 4 || len(l.(core.LookupResp).Ranges) != 1 {
		t.Fatalf("lookup: %+v %v", l, err)
	}
	d, err := c.Call(ctx, core.DescribeTopics{})
	if err != nil || d.(core.DescribeTopicsResp).Topics[0] != (core.TopicInfo{Name: "t", Partitions: 3}) {
		t.Fatalf("describe: %+v %v", d, err)
	}
	// Business errors come back as typed errors.
	commit.Batches[0].Records = 9 // the same key, another request
	_, err = c.Call(ctx, commit)
	var be *cpclient.Error
	if !errors.As(err, &be) || be.Code != core.ErrConflict {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if code, out := (cpclient.Classifier{}).Classify(core.CPCall{Op: commit}, err); code != core.ErrConflict || out != core.NotApplied {
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

func (f failStore) CommitSegment(context.Context, core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	return core.CommitSegmentResp{}, core.OK, f.err
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
		resp, err := http.Post(srv.URL+"/v1/commit-segment", "application/json", stringsReader(`{"segment_key":"k"}`))
		srv.Close()
		if err != nil || resp.StatusCode != c.want {
			t.Fatalf("%v: status %v err %v", c.err, resp, err)
		}
	}
}

// concurrentCommits has several replicas (Store values over one table) commit
// to the same two partitions from many goroutines: every batch of every
// accepted commit must land in a contiguous, non-overlapping offset range.
// Each commit is also retried under the same key (a replay: same answer).
func concurrentCommits(t *testing.T, replicas []cp.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const workers, perWorker = 12, 8
	tps := []core.TopicPartition{{Topic: "c", Partition: 0}, {Topic: "c", Partition: 1}}

	var wg sync.WaitGroup
	var mu sync.Mutex
	assigned := map[core.TopicPartition][][2]int64{} // (base, count)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := replicas[w%len(replicas)]
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d-%d", w, i)
				batches := []core.PendingBatch{
					{TP: tps[0], Records: 2, Loc: core.SegmentRef{Key: key, Len: 5}},
					{TP: tps[1], Records: 3, Loc: core.SegmentRef{Key: key, Len: 5}},
					{TP: tps[0], Records: int64(1 + i%3), Loc: core.SegmentRef{Key: key, Off: 5, Len: 5}},
				}
				first := commitRetrying(ctx, t, st, core.CommitSegment{SegmentKey: key, Batches: batches})
				again := commitRetrying(ctx, t, replicas[(w+1)%len(replicas)], core.CommitSegment{SegmentKey: key, Batches: batches})
				if fmt.Sprint(first) != fmt.Sprint(again) {
					t.Errorf("replay of %s: %v, want %v", key, again, first)
				}
				mu.Lock()
				for _, r := range first.Assigned {
					assigned[r.TP] = append(assigned[r.TP], [2]int64{int64(r.Base), r.Count})
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for tp, list := range assigned {
		total := checkContiguous(t, tp, list)
		resp, _, err := replicas[0].Lookup(ctx, core.Lookup{TP: tp})
		if err != nil || int64(resp.End) != total || len(resp.Ranges) != len(list) {
			t.Errorf("%v lookup: end %d ranges %d, want %d / %d (%v)", tp, resp.End, len(resp.Ranges), total, len(list), err)
		}
	}
}

// commitRetrying commits, retrying a 503 (NotApplied, and the commit is
// idempotent per key).
func commitRetrying(ctx context.Context, t *testing.T, st cp.Store, op core.CommitSegment) core.CommitSegmentResp {
	for try := 0; try < 5; try++ {
		resp, code, err := st.CommitSegment(ctx, op)
		if errors.Is(err, cp.ErrUnavailable) {
			continue
		}
		if err != nil || code != core.OK {
			t.Errorf("commit %s: %v %v", op.SegmentKey, code, err)
		}
		return resp
	}
	t.Errorf("commit %s: unavailable", op.SegmentKey)
	return core.CommitSegmentResp{}
}

// checkContiguous: the (base, count) ranges tile [0, total) exactly once.
func checkContiguous(t *testing.T, tp core.TopicPartition, list [][2]int64) (total int64) {
	t.Helper()
	slices.SortFunc(list, func(a, b [2]int64) int { return cmp.Compare(a[0], b[0]) })
	for _, a := range list {
		if a[0] != total {
			t.Errorf("%v: range at %d, expected %d (gap or overlap)", tp, a[0], total)
			return total
		}
		total += a[1]
	}
	return total
}

func TestDynamoConcurrentCommits(t *testing.T) {
	first := dynamoStore(t)
	replicas := []cp.Store{first}
	for i := 0; i < 3; i++ {
		replicas = append(replicas, newReplica(t, first))
	}
	concurrentCommits(t, replicas)
}

// The same segment key committed by many goroutines at once: one winner, all
// see the same ranges.
func TestDynamoSameKeyRace(t *testing.T) {
	sameKeyRace(t, dynamoStore(t))
}

func sameKeyRace(t *testing.T, st cp.Store) {
	t.Helper()
	ctx := context.Background()
	op := core.CommitSegment{SegmentKey: "same", Batches: []core.PendingBatch{
		{TP: core.TopicPartition{Topic: "r"}, Records: 5, Loc: core.SegmentRef{Key: "same", Len: 9}}}}
	var wg sync.WaitGroup
	res := make([]core.CommitSegmentResp, 10)
	for i := range res {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i] = commitRetrying(ctx, t, st, op)
		}()
	}
	wg.Wait()
	for _, r := range res {
		if len(r.Assigned) != 1 || fmt.Sprint(r) != fmt.Sprint(res[0]) || r.Assigned[0].Base != 0 {
			t.Fatalf("diverged: %+v vs %+v", r, res[0])
		}
	}
	if l, _, _ := st.Lookup(ctx, core.Lookup{TP: op.Batches[0].TP}); l.End != 5 {
		t.Fatalf("end %d", l.End)
	}
}

func TestDynamoTooLarge(t *testing.T) { tooLarge(t, dynamoStore(t)) }

// 1 segment item + one run per partition + the topic must fit in 100 items.
func tooLarge(t *testing.T, st cp.Store) {
	t.Helper()
	ctx := context.Background()
	var bs []core.PendingBatch
	for i := 0; i < 100; i++ {
		bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "big", Partition: int32(i)}, Records: 1, Loc: core.SegmentRef{Key: "b", Len: 1}})
	}
	_, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: "b", Batches: bs})
	if err != nil || code != core.ErrTooLarge {
		t.Fatalf("102 items: %v %v", code, err)
	}
	_, code, err = st.CommitSegment(ctx, core.CommitSegment{SegmentKey: "b2", Batches: bs[:98]})
	if err != nil || code != core.OK {
		t.Fatalf("100 items: %v %v", code, err)
	}
}

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }
