package cptest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"manifester/core"
	"manifester/cp/dynamo"
	"manifester/cp/dynamo/fakedb"
)

// The same lockstep suite against the DynamoDB store over fakedb (no Docker):
// it validates the fake, and the store's logic, against the spec the same way
// ExtendDB is validated.

func fakeStore(t testing.TB) (*dynamo.Store, *fakedb.Fake) {
	t.Helper()
	f := fakedb.New()
	f.PageSize = 3 // page often
	st := dynamo.New(f, "f_")
	if err := st.CreateTables(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st, f
}

func fakeImpl(t testing.TB) impl {
	st, _ := fakeStore(t)
	return storeImpl{st}
}

func TestConformanceFake(t *testing.T) {
	run(t, func() impl { return fakeImpl(t) }, 200, 100)
}

func TestConformanceFakeHTTP(t *testing.T) {
	run(t, func() impl { st, _ := fakeStore(t); return httpOver(t, st) }, 100, 10)
}

func TestWindowEvictionFake(t *testing.T) { lockstep(t, fakeImpl(t), windowOps()) }
func TestManyBatchesFake(t *testing.T)    { lockstep(t, fakeImpl(t), manyBatchesOps()) }

func TestFakeTooLarge(t *testing.T) {
	st, _ := fakeStore(t)
	ctx := context.Background()
	self := core.NodeRef{Node: "n"}
	reg, _ := st.Register(ctx, core.Register{Self: self})
	var bs []core.PendingBatch
	for i := 0; i < 60; i++ {
		bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "big", Partition: int32(i)}, Records: 1, Loc: core.SegmentRef{Key: "b", Len: 1}})
	}
	if _, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: "b", Owner: self, Epoch: reg.Epoch, Batches: bs}); err != nil || code != core.ErrTooLarge {
		t.Fatalf("120 items: %v %v", code, err)
	}
	if _, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: "b2", Owner: self, Epoch: reg.Epoch, Batches: bs[:40]}); err != nil || code != core.OK {
		t.Fatalf("82 items: %v %v", code, err)
	}
}

// Several replicas (Store values) over one fake table, many goroutines, one
// segment key: one winner, every caller sees the same ranges.
func TestFakeSameKeyRace(t *testing.T) {
	st, f := fakeStore(t)
	replicas := []*dynamo.Store{st, dynamo.New(f, st.Prefix()), dynamo.New(f, st.Prefix())}
	ctx := context.Background()
	self := core.NodeRef{Node: "n"}
	reg, _ := st.Register(ctx, core.Register{Self: self})
	op := core.CommitSegment{SegmentKey: "same", Owner: self, Epoch: reg.Epoch, Batches: []core.PendingBatch{
		{TP: core.TopicPartition{Topic: "r"}, Records: 5, Loc: core.SegmentRef{Key: "same", Len: 9}}}}
	var wg sync.WaitGroup
	res := make([]core.CommitSegmentResp, 12)
	for i := range res {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, code, err := replicas[i%3].CommitSegment(ctx, op)
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
