package cptest

import (
	"context"
	"testing"

	"manifester/cp"
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

func TestReplayFake(t *testing.T)      { lockstep(t, fakeImpl(t), replayOps()) }
func TestManyBatchesFake(t *testing.T) { lockstep(t, fakeImpl(t), manyBatchesOps()) }

func TestFakeTooLarge(t *testing.T) {
	st, _ := fakeStore(t)
	tooLarge(t, st)
}

// Several replicas (Store values) over one fake table, many goroutines, one
// segment key: one winner, every caller sees the same ranges.
func TestFakeSameKeyRace(t *testing.T) {
	st, _ := fakeStore(t)
	sameKeyRace(t, st)
}

// Several replicas committing the same partitions concurrently over the fake:
// contiguous, non-overlapping offsets (no Docker).
func TestFakeConcurrentCommits(t *testing.T) {
	st, f := fakeStore(t)
	concurrentCommits(t, []cp.Store{st, dynamo.New(f, st.Prefix()), dynamo.New(f, st.Prefix()), dynamo.New(f, st.Prefix())})
}
