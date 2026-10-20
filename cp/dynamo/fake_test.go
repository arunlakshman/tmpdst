package dynamo

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpobs"
	"manifester/cp/dynamo/fakedb"
	"manifester/cp/memstore"
)

// These tests run the Store over fakedb, so the CRAP gate and plain
// `go test` do not need Docker. The fake is validated against the same oracle
// as ExtendDB: TestAgainstMemstore here and the lockstep suite in cp/cptest.

var bg = context.Background()

func fakeStore(t testing.TB) (*Store, *fakedb.Fake) {
	t.Helper()
	f := fakedb.New()
	st := New(f, "x")
	if err := st.CreateTables(bg); err != nil {
		t.Fatal(err)
	}
	return st, f
}

func throttle(call string) func(string) error {
	return func(c string) error {
		if c == call {
			return &types.ThrottlingException{Message: aws.String("slow down")}
		}
		return nil
	}
}

func isUnavailable(err error) bool { return errors.Is(err, cp.ErrUnavailable) }

func tpOf(topic string, p int32) core.TopicPartition {
	return core.TopicPartition{Topic: topic, Partition: p}
}

func one(tp core.TopicPartition, key string, recs int64) core.PendingBatch {
	return core.PendingBatch{TP: tp, Records: recs, Loc: core.SegmentRef{Key: key, Len: 10 * recs}}
}

// ---- randomized lockstep against memstore (the spec) ----

func randBatches(r *rand.Rand, key string) []core.PendingBatch {
	n := r.IntN(5)
	if r.IntN(8) == 0 {
		n = 8 + r.IntN(40)
	}
	var out []core.PendingBatch
	for i := 0; i < n; i++ {
		b := one(tpOf([]string{"t1", "t2"}[r.IntN(2)], int32(r.IntN(2))), key, int64(r.IntN(3)))
		if b.Records == 0 && r.IntN(10) != 0 {
			b.Records, b.Loc.Len = 1, 10
		}
		out = append(out, b)
	}
	return out
}

func randOp(r *rand.Rand, i int) any {
	switch k := r.IntN(32); {
	case k < 4:
		return [2]any{[]string{"", "t1", "t2", "t3"}[r.IntN(4)], int32(r.IntN(5)) - 1}
	case k < 20:
		key := fmt.Sprintf("seg%d", r.IntN(6))
		bs := randBatches(rand.New(rand.NewPCG(uint64(len(key)), uint64(key[len(key)-1]))), key) // a key repeats its batches: replay
		if r.IntN(5) == 0 {
			bs = randBatches(r, key) // or not: conflict
		}
		return core.CommitSegment{SegmentKey: key, Batches: bs}
	case k < 28:
		return core.Lookup{TP: tpOf([]string{"t1", "t2"}[r.IntN(2)], int32(r.IntN(2))), From: core.Offset(r.IntN(30) - 1), MaxBytes: int32(r.IntN(3)) * 50}
	}
	var topics []string
	for j := r.IntN(3); j > 0; j-- {
		topics = append(topics, []string{"t1", "t2", "nope"}[r.IntN(3)])
	}
	return core.DescribeTopics{Topics: topics}
}

func exec(st cp.Store, op any) string {
	var resp any
	var code core.ErrCode
	var err error
	switch op := op.(type) {
	case [2]any:
		code, err = st.CreateTopic(bg, op[0].(string), op[1].(int32))
	case core.CommitSegment:
		resp, code, err = st.CommitSegment(bg, op)
	case core.Lookup:
		resp, code, err = st.Lookup(bg, op)
	case core.DescribeTopics:
		resp, err = st.DescribeTopics(bg, op)
	}
	if err != nil {
		return "error: " + err.Error()
	}
	if code != core.OK {
		return code.String()
	}
	return fmt.Sprintf("%+v", resp)
}

func TestAgainstMemstore(t *testing.T) {
	for seed := uint64(1); seed <= 60; seed++ {
		st, f := fakeStore(t)
		f.PageSize = 1 + int(seed%3) // exercise Query paging
		spec := memstore.New()
		r := rand.New(rand.NewPCG(seed, 7))
		for i := 0; i < 120; i++ {
			op := randOp(r, i)
			if got, want := exec(st, op), exec(spec, op); got != want {
				t.Fatalf("seed %d op %d %+v\n store: %s\n  spec: %s", seed, i, op, got, want)
			}
		}
	}
}

// ---- targeted behaviour ----

func TestInfraMapping(t *testing.T) {
	for _, e := range []error{
		&types.ProvisionedThroughputExceededException{}, &types.RequestLimitExceeded{}, &types.ThrottlingException{},
		&types.TransactionInProgressException{}, &types.TransactionConflictException{}, &types.InternalServerError{},
		fmt.Errorf("wrapped: %w", &types.ThrottlingException{}),
	} {
		if !isUnavailable(infra(e)) {
			t.Errorf("%T not unavailable", e)
		}
	}
	plain := errors.New("plain")
	if infra(nil) != nil || infra(plain) != plain {
		t.Error("non-retryable errors must pass through")
	}
}

func TestCallsReportUnavailable(t *testing.T) {
	st, f := fakeStore(t)
	commit := core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{one(tpOf("t", 0), "s", 1)}}
	for _, c := range []struct {
		call string
		run  func() error
	}{
		{"UpdateItem", func() error { _, err := st.CreateTopic(bg, "t", 1); return err }},
		{"Query", func() error { _, _, err := st.Lookup(bg, core.Lookup{TP: tpOf("t", 0)}); return err }},
		{"GetItem", func() error { _, err := st.DescribeTopics(bg, core.DescribeTopics{Topics: []string{"t"}}); return err }},
		{"Query", func() error { _, err := st.DescribeTopics(bg, core.DescribeTopics{}); return err }},
		{"BatchGetItem", func() error { _, _, err := st.CommitSegment(bg, commit); return err }},
		{"Query", func() error { _, _, err := st.CommitSegment(bg, commit); return err }},
		{"GetItem", func() error { _, _, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "z"}); return err }},
	} {
		f.Fail = throttle(c.call)
		if err := c.run(); !isUnavailable(err) {
			t.Errorf("%s: %v", c.call, err)
		}
	}
	f.Fail = nil
	// A Lookup whose range query fails after the end read.
	if _, _, err := st.CommitSegment(bg, commit); err != nil {
		t.Fatal(err)
	}
	var queries atomic.Int32
	f.Fail = func(call string) error { // the end read succeeds, the next Query fails
		if call == "Query" {
			if queries.Add(1) > 1 {
				return &types.ThrottlingException{Message: aws.String("slow down")}
			}
		}
		return nil
	}
	if _, _, err := st.Lookup(bg, core.Lookup{TP: tpOf("t", 0)}); !isUnavailable(err) {
		t.Errorf("lookup run start: %v", err)
	}
	f.Fail = nil
	if _, _, err := st.Lookup(bg, core.Lookup{TP: tpOf("t", 0), From: -1}); err != nil {
		t.Errorf("negative From: %v", err)
	}
}

func TestCreateTopicErrors(t *testing.T) {
	st, f := fakeStore(t)
	boom := errors.New("boom")
	f.Fail = func(call string) error { return boom }
	if code, err := st.CreateTopic(bg, "t", 1); !errors.Is(err, boom) || code != core.OK {
		t.Error(code, err)
	}
	if code, _ := st.CreateTopic(bg, "", 1); code != core.ErrInvalid {
		t.Error("empty topic name")
	}
}

func TestBatchGetUnprocessed(t *testing.T) {
	st, f := fakeStore(t)
	for i := 0; i < 3; i++ {
		f.Put(st.Table(), key(fmt.Sprint("p", i), "-"))
	}
	keys := []item{key("p0", "-"), key("p1", "-"), key("p2", "-"), key("nope", "-")}
	f.Unprocess = 3 // three calls leave a key behind; the fourth completes it
	got, err := st.batchGet(bg, keys)
	if err != nil || len(got) != 3 || f.Unprocess != 0 {
		t.Fatalf("got %d items, %v", len(got), err)
	}
	if lookupKey(got, key("p1", "-")) == nil || lookupKey(got, key("nope", "-")) != nil {
		t.Fatal("wrong items")
	}
	// 250 keys: three chunks of 100/100/50.
	var many []item
	for i := 0; i < 250; i++ {
		many = append(many, key("p0", fmt.Sprint(i)))
	}
	if got, err := st.batchGet(bg, many); err != nil || len(got) != 0 {
		t.Fatalf("many: %d, %v", len(got), err)
	}
	// A context that ends while backing off.
	f.Unprocess = 100
	ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer cancel()
	if _, err := st.batchGet(ctx, keys); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestBatchGetGivesUp(t *testing.T) {
	if testing.Short() {
		t.Skip("backs off for about a second")
	}
	st, f := fakeStore(t)
	f.Unprocess = 1000
	if _, err := st.batchGet(bg, []item{key("a", "b")}); !isUnavailable(err) {
		t.Fatalf("want unavailable, got %v", err)
	}
}

func runKey(tp core.TopicPartition, base int64) item { return key(partKey("R#", tp), offSK(base)) }

// Another replica takes the base between this commit's read and its write: the
// transaction is cancelled and only that partition is read again.
func TestCommitRetriesLostRace(t *testing.T) {
	st, f := fakeStore(t)
	st.Obs = cpobs.New()
	tp, other := tpOf("t", 0), tpOf("t", 1)
	if _, _, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "first", Batches: []core.PendingBatch{one(tp, "first", 2)}}); err != nil {
		t.Fatal(err)
	}
	op := core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{one(tp, "s", 3), one(other, "s", 1)}}
	races := 0
	var queries atomic.Int32
	f.BeforeTransact = func() {
		if races++; races == 1 {
			f.Put(st.Table(), merge(runKey(tp, 2), item{"base": num(2), "cnt": num(4), "runs": blob(encodeRun([]core.BatchRange{{TP: tp, Base: 2, Count: 4, Loc: core.SegmentRef{Key: "x"}}}))}))
		}
	}
	f.Fail = func(call string) error {
		if call == "Query" {
			queries.Add(1)
		}
		return nil
	}
	resp, code, err := st.CommitSegment(bg, op)
	if err != nil || code != core.OK || races != 2 || resp.Assigned[0].Base != 6 || resp.Assigned[1].Base != 0 {
		t.Fatalf("resp %+v code %v err %v after %d attempts", resp, code, err, races)
	}
	if queries.Load() != 3 { // two partitions, then the lost one again
		t.Fatalf("%d queries, want 3", queries.Load())
	}
}

// A replica commits the same key between the read and the write: the retry
// finds it and replays its answer.
func TestCommitLostRaceOnSameKeyReplays(t *testing.T) {
	st, f := fakeStore(t)
	other := New(f, st.Prefix())
	op := core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{one(tpOf("t", 0), "s", 3)}}
	races := 0
	f.BeforeTransact = func() {
		if races++; races == 1 {
			if _, _, err := other.CommitSegment(bg, op); err != nil {
				t.Error(err)
			}
		}
	}
	resp, code, err := st.CommitSegment(bg, op)
	if err != nil || code != core.OK || races != 2 || len(resp.Assigned) != 1 || resp.Assigned[0].Base != 0 {
		t.Fatalf("resp %+v code %v err %v after %d attempts", resp, code, err, races)
	}
	if l, _, _ := st.Lookup(bg, core.Lookup{TP: tpOf("t", 0)}); l.End != 3 {
		t.Fatalf("end %d: committed twice", l.End)
	}
}

func TestCommitGivesUpAfterMaxAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("backs off for about a second")
	}
	st, f := fakeStore(t)
	tp := tpOf("t", 0)
	n := int64(0)
	f.BeforeTransact = func() { // always lose: take the base this attempt read
		f.Put(st.Table(), merge(runKey(tp, n), item{"base": num(n), "cnt": num(1), "runs": blob(encodeRun([]core.BatchRange{{TP: tp, Base: core.Offset(n), Count: 1, Loc: core.SegmentRef{Key: "x"}}}))}))
		n++
	}
	if _, _, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{one(tp, "s", 1)}}); !isUnavailable(err) {
		t.Fatalf("got %v", err)
	}
}

func TestCommitUnavailableRetriesUntilContextEnds(t *testing.T) {
	st, f := fakeStore(t)
	st.Obs = cpobs.New()
	op := core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{one(tpOf("t", 0), "s", 1)}}
	f.Fail = throttle("TransactWriteItems")
	ctx, cancel := context.WithTimeout(bg, 60*time.Millisecond)
	defer cancel()
	if _, _, err := st.CommitSegment(ctx, op); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	// A failure that is not a retry condition is returned as is.
	boom := errors.New("boom")
	f.Fail = func(call string) error {
		if call == "TransactWriteItems" {
			return boom
		}
		return nil
	}
	if _, _, err := st.CommitSegment(bg, op); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	// A cancelled context is not retried either.
	f.Fail = func(call string) error {
		if call == "TransactWriteItems" {
			return fmt.Errorf("op: %w", context.Canceled)
		}
		return nil
	}
	if _, _, err := st.CommitSegment(bg, op); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestCommitCorruptItems(t *testing.T) {
	st, f := fakeStore(t)
	tp := tpOf("t", 0)
	f.Put(st.Table(), merge(segKey("bad"), item{"blob": blob([]byte("{"))}))
	bad := core.CommitSegment{SegmentKey: "bad", Batches: []core.PendingBatch{one(tp, "bad", 1)}}
	if _, _, err := st.CommitSegment(bg, bad); err == nil || !strings.Contains(err.Error(), "corrupt segment") {
		t.Fatalf("segment: %v", err)
	}
	bad.Batches = nil // the malformed path replays too
	if _, _, err := st.CommitSegment(bg, bad); err == nil || !strings.Contains(err.Error(), "corrupt segment") {
		t.Fatalf("malformed: %v", err)
	}
}

func TestLookupCorruptRun(t *testing.T) {
	st, f := fakeStore(t)
	tp := tpOf("t", 0)
	f.Put(st.Table(), item{"pk": str("R#t#0"), "sk": str(offSK(0)), "base": num(0), "cnt": num(3), "runs": blob([]byte{9})})
	if _, _, err := st.Lookup(bg, core.Lookup{TP: tp}); err == nil {
		t.Fatal("corrupt run decoded")
	}
}

func TestLookupPagingAndLimits(t *testing.T) {
	st, f := fakeStore(t)
	f.PageSize = 1
	tp := tpOf("t", 0)
	var bs []core.PendingBatch
	for i := 0; i < 5; i++ {
		bs = append(bs, one(tp, "s", 2))
	}
	for i := 0; i < 3; i++ { // three runs of five batches
		k := fmt.Sprint("s", i)
		for j := range bs {
			bs[j].Loc.Key = k
		}
		if _, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: k, Batches: bs}); err != nil || code != core.OK {
			t.Fatal(code, err)
		}
	}
	l, code, err := st.Lookup(bg, core.Lookup{TP: tp, From: 13})
	if err != nil || code != core.OK || l.End != 30 || len(l.Ranges) != 9 || l.Ranges[0].Base != 12 {
		t.Fatalf("from 13: %+v %v %v", l, code, err)
	}
	l, _, _ = st.Lookup(bg, core.Lookup{TP: tp, From: 0, MaxBytes: 50})
	if len(l.Ranges) != 3 { // 20 bytes each: stops once 50 is reached
		t.Fatalf("max bytes: %d ranges", len(l.Ranges))
	}
	if l, _, _ = st.Lookup(bg, core.Lookup{TP: tp, From: 30}); len(l.Ranges) != 0 {
		t.Fatalf("at the end: %d ranges", len(l.Ranges))
	}
}

func TestCommitRejects(t *testing.T) {
	st, f := fakeStore(t)
	tp := tpOf("t", 0)
	ok := []core.PendingBatch{one(tp, "s", 1)}
	cases := map[string]core.CommitSegment{
		"no batches":     {SegmentKey: "s"},
		"no key":         {Batches: ok},
		"empty topic":    {SegmentKey: "s", Batches: []core.PendingBatch{one(tpOf("", 0), "s", 1)}},
		"negative part.": {SegmentKey: "s", Batches: []core.PendingBatch{one(tpOf("t", -1), "s", 1)}},
		"zero records":   {SegmentKey: "s", Batches: []core.PendingBatch{one(tp, "s", 0)}},
	}
	for name, op := range cases {
		if _, code, err := st.CommitSegment(bg, op); err != nil || code != core.ErrInvalid {
			t.Errorf("%s: %v %v", name, code, err)
		}
	}
	if f.Count(st.Table()) != 0 {
		t.Fatalf("%d items after rejects", f.Count(st.Table()))
	}
	// Replay answers the original; a different request conflicts, malformed or not.
	good := core.CommitSegment{SegmentKey: "s", Batches: ok}
	first, _, _ := st.CommitSegment(bg, good)
	if again, code, err := st.CommitSegment(bg, good); err != nil || code != core.OK || fmt.Sprint(again) != fmt.Sprint(first) {
		t.Fatalf("replay: %v %v", code, err)
	}
	good.Batches = []core.PendingBatch{one(tp, "s", 2)}
	if _, code, _ := st.CommitSegment(bg, good); code != core.ErrConflict {
		t.Fatalf("conflict: %v", code)
	}
	if _, code, _ := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "s"}); code != core.ErrConflict {
		t.Fatalf("malformed under a committed key: %v", code)
	}
}

// Consecutive batches of one partition in a request continue each other, and
// the end of a partition is that of its newest run.
func TestCommitBasesInRequestOrder(t *testing.T) {
	st, _ := fakeStore(t)
	a, b := tpOf("t", 0), tpOf("t", 1)
	resp, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{
		one(a, "s", 2), one(b, "s", 5), one(a, "s", 3), one(b, "s", 1)}})
	if err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	var bases []core.Offset
	for _, r := range resp.Assigned {
		bases = append(bases, r.Base)
	}
	if fmt.Sprint(bases) != "[0 0 2 5]" {
		t.Fatalf("bases %v", bases)
	}
	resp, _, _ = st.CommitSegment(bg, core.CommitSegment{SegmentKey: "s2", Batches: []core.PendingBatch{one(b, "s2", 1), one(a, "s2", 1)}})
	if resp.Assigned[0].Base != 6 || resp.Assigned[1].Base != 5 {
		t.Fatalf("second commit %+v", resp.Assigned)
	}
	if r, _ := st.DescribeTopics(bg, core.DescribeTopics{}); len(r.Topics) != 1 || r.Topics[0].Partitions != 2 {
		t.Fatalf("topics %+v", r)
	}
}

// 1 (segment) + one item per partition + one per grown topic must fit in a
// transaction of 100 items.
func TestTooLargeItems(t *testing.T) {
	st, f := fakeStore(t)
	mk := func(n int) []core.PendingBatch {
		var bs []core.PendingBatch
		for i := 0; i < n; i++ {
			bs = append(bs, one(tpOf("big", int32(i)), "b", 1))
		}
		return bs
	}
	if _, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "b", Batches: mk(99)}); err != nil || code != core.ErrTooLarge {
		t.Fatalf("1+99+1 items: %v %v", code, err)
	}
	if f.Count(st.Table()) != 0 {
		t.Fatal("a refused commit wrote")
	}
	if _, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "b", Batches: mk(98)}); err != nil || code != core.OK {
		t.Fatalf("1+98+1 items: %v %v", code, err)
	}
}

// The old transaction store refuses a segment as wide as the log store's cap
// (decision 52) with ErrTooLarge and writes nothing, so a rollback to it with
// -max-segment-partitions left at 4096 fails the segment cleanly.
func TestTooLargeAtLogStoreCap(t *testing.T) {
	st, f := fakeStore(t)
	var bs []core.PendingBatch
	for i := 0; i < 4096; i++ {
		bs = append(bs, one(tpOf("wide", int32(i)), "b", 1))
	}
	if _, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "w", Batches: bs}); err != nil || code != core.ErrTooLarge {
		t.Fatalf("4096 partitions: %v %v", code, err)
	}
	if f.Count(st.Table()) != 0 {
		t.Fatal("a refused commit wrote")
	}
}

// A read that fails stops the others; the first error wins.
func TestRunAllFirstError(t *testing.T) {
	boom := errors.New("boom")
	var ran atomic.Int32
	jobs := []func(context.Context) error{
		func(context.Context) error { ran.Add(1); return boom },
	}
	for i := 0; i < 5; i++ {
		jobs = append(jobs, func(ctx context.Context) error { ran.Add(1); <-ctx.Done(); return ctx.Err() })
	}
	if err := runAll(bg, 8, jobs); !errors.Is(err, boom) || ran.Load() != 6 {
		t.Fatalf("err %v after %d jobs", err, ran.Load())
	}
}

func TestTooLargeBlob(t *testing.T) {
	st, _ := fakeStore(t)
	b := one(tpOf("t", 0), strings.Repeat("k", maxBlob), 1)
	if _, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{b}}); err != nil || code != core.ErrTooLarge {
		t.Fatalf("%v %v", code, err)
	}
}

func TestCreateTables(t *testing.T) {
	st, f := fakeStore(t)
	if err := st.CreateTables(bg); err != nil { // already there: left alone
		t.Fatal(err)
	}
	f.Fail = func(call string) error {
		if call == "CreateTable" {
			return errors.New("denied")
		}
		return nil
	}
	if err := st.CreateTables(bg); err == nil || !strings.Contains(err.Error(), "create table") {
		t.Fatalf("create: %v", err)
	}
	f.Fail = func(call string) error {
		if call == "DescribeTable" {
			return errors.New("not yet")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(bg, 30*time.Millisecond)
	defer cancel()
	if err := st.CreateTables(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("describe: %v", err)
	}
	f.Fail = nil
	if err := st.DeleteTables(bg); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteTables(bg); err == nil {
		t.Fatal("deleted twice")
	}
}

func TestCapacityReported(t *testing.T) {
	st, f := fakeStore(t)
	st.Obs = cpobs.New()
	f.ReportCapacity = true
	if _, code, err := st.CommitSegment(bg, core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{one(tpOf("t", 0), "s", 1)}}); err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	if _, _, err := st.Lookup(bg, core.Lookup{TP: tpOf("t", 0)}); err != nil {
		t.Fatal(err)
	}
}

func TestDescribeNamed(t *testing.T) {
	st, _ := fakeStore(t)
	st.CreateTopic(bg, "a", 2)
	st.CreateTopic(bg, "b", 1)
	r, err := st.DescribeTopics(bg, core.DescribeTopics{Topics: []string{"b", "a", "b", "nope"}})
	if err != nil || len(r.Topics) != 2 || r.Topics[0].Name != "a" || r.Topics[0].Partitions != 2 {
		t.Fatalf("%+v %v", r, err)
	}
}
