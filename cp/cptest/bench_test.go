package cptest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/dynamo"
	"manifester/cp/dynamo/fakedb"
)

// TestSequencerBench is the measurement behind docs/benchmarks.md "Sequencer":
// six committers hammer one table with 33-of-288-partition segments, first
// through six independent Stores (every commit reads and compares-and-sets on
// its own: the pre-decision-46 path), then through one Sequencer.
//
//	MANIFESTER_SEQ_BENCH=fake  go test ./cp/cptest -run SequencerBench -v   (fakedb, 30 ms per transaction, 5 ms per read)
//	MANIFESTER_SEQ_BENCH=extenddb MANIFESTER_DYNAMO_ENDPOINT=... go test ...
//
// MANIFESTER_SEQ_BENCH_SECS (default 8) is the length of each run.
func TestSequencerBench(t *testing.T) {
	mode := os.Getenv("MANIFESTER_SEQ_BENCH")
	if mode == "" {
		t.Skip("set MANIFESTER_SEQ_BENCH=fake or extenddb")
	}
	secs, _ := strconv.Atoi(os.Getenv("MANIFESTER_SEQ_BENCH_SECS"))
	if secs <= 0 {
		secs = 8
	}
	fmt.Printf("%-34s %8s %9s %9s %9s %6s %7s %7s %8s\n", "case ("+mode+")", "commit/s", "avg ms", "p50 ms", "p99 ms", "503", "cancel", "txs", "items/tx")
	cases := []struct {
		name     string
		disjoint bool // each committer owns 48 partitions (a broker per partition range)
		mk       func(db dynamo.DB, prefix string) []cp.Store
	}{
		{"before: 6 independent stores", false, func(db dynamo.DB, p string) []cp.Store { return independent(db, p, 6) }},
		{"after: sequencer, pipeline 1", false, func(db dynamo.DB, p string) []cp.Store { return sequenced(t, db, p, 1) }},
		{"after: sequencer, pipeline 4", false, func(db dynamo.DB, p string) []cp.Store { return sequenced(t, db, p, 4) }},
		{"disjoint sets: sequencer, pipeline 1", true, func(db dynamo.DB, p string) []cp.Store { return sequenced(t, db, p, 1) }},
		{"disjoint sets: sequencer, pipeline 4", true, func(db dynamo.DB, p string) []cp.Store { return sequenced(t, db, p, 4) }},
	}
	for _, c := range cases {
		db, prefix, cleanup := benchDB(t, mode)
		cdb := &countingDB{DB: db}
		stores := c.mk(cdb, prefix)
		benchRun(t, c.name, c.disjoint, stores, cdb, time.Duration(secs)*time.Second)
		cleanup()
	}
}

func independent(db dynamo.DB, prefix string, n int) []cp.Store {
	var out []cp.Store
	for i := 0; i < n; i++ {
		out = append(out, dynamo.New(db, prefix))
	}
	return out
}

func sequenced(t *testing.T, db dynamo.DB, prefix string, pipeline int) []cp.Store {
	s := dynamo.NewSequencer(dynamo.New(db, prefix), dynamo.SeqOptions{Pipeline: pipeline})
	t.Cleanup(s.Close)
	return []cp.Store{s}
}

// benchDB is a fresh table on the fake (with latency) or on ExtendDB.
func benchDB(t *testing.T, mode string) (dynamo.DB, string, func()) {
	ctx := context.Background()
	if mode == "fake" {
		f := fakedb.New()
		f.Latency = func(call string) time.Duration {
			if call == "TransactWriteItems" {
				return 30 * time.Millisecond
			}
			return 5 * time.Millisecond
		}
		st := dynamo.New(f, "bench_")
		if err := st.CreateTables(ctx); err != nil {
			t.Fatal(err)
		}
		return f, "bench_", func() {}
	}
	st := dynamoStore(t)
	return dynamoClient(needDynamo(t)), st.Prefix(), func() { st.DeleteTables(ctx) }
}

// countingDB counts what the stores send.
type countingDB struct {
	dynamo.DB
	txs, cancels, items, queries atomic.Int64
}

func (c *countingDB) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, o ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	out, err := c.DB.TransactWriteItems(ctx, in, o...)
	c.txs.Add(1)
	var tc *types.TransactionCanceledException
	if errors.As(err, &tc) {
		c.cancels.Add(1)
	} else if err == nil {
		c.items.Add(int64(len(in.TransactItems)))
	}
	return out, err
}

func (c *countingDB) Query(ctx context.Context, in *dynamodb.QueryInput, o ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	c.queries.Add(1)
	return c.DB.Query(ctx, in, o...)
}

// benchRun runs six committers for d and prints one result line.
func benchRun(t *testing.T, name string, disjoint bool, stores []cp.Store, db *countingDB, d time.Duration) {
	var mu sync.Mutex
	var lats []time.Duration
	var unavailable atomic.Int64
	stop := time.Now().Add(d)
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for i := 0; time.Now().Before(stop); i++ {
				op := loadSegment(r, fmt.Sprintf("w%d-%d", w, i), 288, 33)
				if disjoint {
					op = ownedSegment(r, fmt.Sprintf("w%d-%d", w, i), w*48, 48, 33)
				}
				t0 := time.Now()
				for {
					_, code, err := stores[w%len(stores)].CommitSegment(context.Background(), op)
					if errors.Is(err, cp.ErrUnavailable) {
						unavailable.Add(1)
						continue
					}
					if err != nil || code != core.OK {
						t.Errorf("commit: %v %v", code, err)
					}
					break
				}
				mu.Lock()
				lats = append(lats, time.Since(t0))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	slices.Sort(lats)
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	n := float64(len(lats))
	ms := func(x time.Duration) float64 { return float64(x.Microseconds()) / 1000 }
	applied := float64(db.txs.Load() - db.cancels.Load())
	fmt.Printf("%-34s %8.1f %9.1f %9.1f %9.1f %6d %7d %7d %8.1f\n", name, n/d.Seconds(), ms(sum)/n,
		ms(lats[len(lats)/2]), ms(lats[len(lats)*99/100]), unavailable.Load(), db.cancels.Load(), db.txs.Load(), float64(db.items.Load())/max(applied, 1))
}

// ownedSegment is loadSegment over the partitions [lo, lo+n).
func ownedSegment(r *rand.Rand, key string, lo, n, k int) core.CommitSegment {
	op := loadSegment(r, key, n, k)
	for i := range op.Batches {
		op.Batches[i].TP.Partition += int32(lo)
	}
	return op
}
