package logcp_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/prometheus/client_golang/prometheus"

	"manifester/core"
	"manifester/cp/dynamo"
	"manifester/cp/dynamo/fakedb"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
)

// indexFixture is one active node on a DynLog whose R# rows can be read back
// through cp/dynamo's Store: the old store's read path.
type indexFixture struct {
	db   *fakedb.Fake
	objs *logcptest.Objects
	log  *logcp.DynLog
	reg  *prometheus.Registry
	s    *logcp.Service
}

func newIndexFixture(t testing.TB, mod func(*logcp.Config)) *indexFixture {
	t.Helper()
	f := &indexFixture{db: newDB(t), objs: logcptest.NewObjects(), reg: prometheus.NewRegistry()}
	f.log = logcp.NewDynLog(f.db, "cp", logcp.DynLogOptions{})
	f.s = node(t, "n0", 0, f.log, f.db, f.objs, func(c *logcp.Config) {
		c.Metrics = logcp.NewMetrics(f.reg)
		c.IndexEvery = time.Hour // flushed by the test
		if mod != nil {
			mod(c)
		}
	})
	return f
}

func (f *indexFixture) old() *dynamo.Store { return dynamo.New(f.db, "") }

// sameAsApplier compares the old store's answers with the node's, for every partition.
func (f *indexFixture) sameAsApplier(t testing.TB, tps ...core.TopicPartition) {
	t.Helper()
	for _, p := range tps {
		want, _, _ := f.s.Lookup(bg, core.Lookup{TP: p})
		got, code, err := f.old().Lookup(bg, core.Lookup{TP: p})
		if err != nil || code != core.OK || got.End != want.End || !slices.Equal(got.Ranges, want.Ranges) {
			t.Fatalf("%v: old store end %d ranges %d (%v %v), node end %d ranges %d", p, got.End, len(got.Ranges), code, err, want.End, len(want.Ranges))
		}
	}
}

func (f *indexFixture) watermark() int64 {
	it := f.db.Item("cp", map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "IDX"}, "sk": &types.AttributeValueMemberS{Value: "-"}})
	n, _ := it["ix_slot"].(*types.AttributeValueMemberN)
	if n == nil {
		return 0
	}
	v, _ := strconv.ParseInt(n.Value, 10, 64)
	return v
}

func commitMany(t testing.TB, s *logcp.Service, prefix string, n int, parts int) {
	for i := 0; i < n; i++ {
		commit(t, s, seg(fmt.Sprintf("%s%03d", prefix, i), tp("t", int32(i%parts)), tp("u", int32(i%2)), tp("t", int32(i%parts))))
	}
}

func TestIndexRowsAreReadableByTheOldStore(t *testing.T) {
	f := newIndexFixture(t, nil)
	if code, err := f.s.CreateTopic(bg, "empty", 3); err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	commitMany(t, f.s, "a", 60, 5)
	if err := f.s.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	if f.watermark() != f.s.Applied() {
		t.Fatalf("watermark %d, applied %d", f.watermark(), f.s.Applied())
	}
	var tps []core.TopicPartition
	for p := int32(0); p < 5; p++ {
		tps = append(tps, tp("t", p))
	}
	f.sameAsApplier(t, append(tps, tp("u", 0), tp("u", 1))...)
	d, err := f.old().DescribeTopics(bg, core.DescribeTopics{})
	if err != nil || len(d.Topics) != 3 {
		t.Fatalf("topics %+v %v", d, err)
	}
	// a second flush with nothing new writes nothing; with new commits it extends the rows
	before := f.db.Count("cp")
	if err := f.s.FlushIndex(bg); err != nil || f.db.Count("cp") != before {
		t.Fatalf("idle flush wrote rows: %v", err)
	}
	commitMany(t, f.s, "b", 10, 5)
	if err := f.s.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	f.sameAsApplier(t, tps...)
	// the old store carries on from the rows: a rollback loses nothing once the index is flushed
	r, code, err := f.old().CommitSegment(bg, seg("after-rollback", tp("t", 0)))
	if err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	if want, _, _ := f.s.Lookup(bg, core.Lookup{TP: tp("t", 0)}); r.Assigned[0].Base != want.End {
		t.Fatalf("rollback continues at %d, the node's end was %d", r.Assigned[0].Base, want.End)
	}
}

func TestIndexFlushFailureLeavesAPrefixAndIsRepaired(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.IndexParallel = 1 })
	commitMany(t, f.s, "a", 80, 3)
	var calls atomic.Int64
	var armed atomic.Bool
	armed.Store(true)
	f.db.Fail = func(call string) error {
		if armed.Load() && call == "BatchWriteItem" && calls.Add(1) == 3 {
			return errors.New("injected")
		}
		return nil
	}
	if err := f.s.FlushIndex(bg); err == nil {
		t.Fatal("the failure was swallowed")
	}
	if f.watermark() != 0 {
		t.Fatalf("watermark moved to %d though rows are missing", f.watermark())
	}
	// whatever was written is a prefix of each partition's runs: no run without its predecessors
	for p := int32(0); p < 3; p++ {
		l, _, err := f.old().Lookup(bg, core.Lookup{TP: tp("t", p)})
		if err != nil {
			t.Fatal(err)
		}
		var end core.Offset
		for _, r := range l.Ranges {
			if r.Base != end {
				t.Fatalf("t/%d: a run at %d after end %d: a gap", p, r.Base, end)
			}
			end += core.Offset(r.Count)
		}
	}
	armed.Store(false)
	if err := f.s.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	f.sameAsApplier(t, tp("t", 0), tp("t", 1), tp("t", 2), tp("u", 0))
}

func TestIndexCatchesUpFromTheLogAfterARestore(t *testing.T) {
	f := newIndexFixture(t, nil)
	commitMany(t, f.s, "a", 40, 4)
	if _, err := f.s.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	commitMany(t, f.s, "b", 10, 4)
	// n1 restores the checkpoint, so its feed starts above the (empty) index
	reg := prometheus.NewRegistry()
	n1 := node(t, "n1", 1, f.log, f.db, f.objs, func(c *logcp.Config) { c.Metrics = logcp.NewMetrics(reg); c.IndexEvery = time.Hour })
	waitApplied(t, f.s, n1)
	if err := n1.FlushIndex(bg); err != nil { // any node can flush; it reads the part below its feed from the log
		t.Fatal(err)
	}
	if f.watermark() != n1.Applied() {
		t.Fatalf("watermark %d, applied %d", f.watermark(), n1.Applied())
	}
	f.sameAsApplier(t, tp("t", 0), tp("t", 1), tp("t", 2), tp("t", 3), tp("u", 0), tp("u", 1))
	if metricSum(t, reg, "manicp_log_index_lost_runs_total") != 0 {
		t.Fatal("runs reported lost")
	}
}

func TestIndexFeedOverflowReadsTheLog(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.FeedMax = 8 })
	commitMany(t, f.s, "a", 50, 4)
	if err := f.s.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	f.sameAsApplier(t, tp("t", 0), tp("t", 3), tp("u", 1))
}

func TestIndexLostRunsWhenTheKeyLeftTheWindow(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.WindowSlots, c.TrimStride, c.FeedMax = 16, 4, 4 })
	commitMany(t, f.s, "a", 60, 2) // never flushed: the early keys leave the 16-slot window
	if err := f.s.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	if metricSum(t, f.reg, "manicp_log_index_lost_runs_total") == 0 {
		t.Fatal("a stalled index must say which runs it could not write")
	}
	if f.watermark() != f.s.Applied() {
		t.Fatal("the watermark still moves: the stall is reported, not endless")
	}
}

func TestIndexPacingBackoffAndMetrics(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.IndexRate = 2000; c.IndexParallel = 2 })
	commitMany(t, f.s, "a", 40, 20)
	f.db.UnprocessWrite = 3
	throttles := 0
	f.db.Fail = func(call string) error {
		if call == "BatchWriteItem" && throttles < 2 {
			throttles++
			return &types.ThrottlingException{}
		}
		return nil
	}
	t0 := time.Now()
	if err := f.s.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	rows := metricSum(t, f.reg, "manicp_log_index_rows_total")
	if min := time.Duration(rows/2000*float64(time.Second)) / 2; time.Since(t0) < min {
		t.Fatalf("%v rows at 2000/s took %v", rows, time.Since(t0))
	}
	if metricSum(t, f.reg, "manicp_log_index_unprocessed_total") == 0 || metricSum(t, f.reg, "manicp_log_index_throttles_total") != 2 {
		t.Fatal("unprocessed items and throttles are not counted")
	}
	f.sameAsApplier(t, tp("t", 0), tp("t", 19))
}

func TestNoIndexWritesNothing(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.NoIndex = true })
	commitMany(t, f.s, "a", 10, 2)
	if err := f.s.FlushIndex(bg); err != nil || f.db.Count("cp") > 20 {
		t.Fatalf("%v rows=%d", err, f.db.Count("cp"))
	}
	if _, _, err := f.old().Lookup(bg, core.Lookup{TP: tp("t", 0)}); err != nil {
		t.Fatal(err)
	}
	if l, _, _ := f.old().Lookup(bg, core.Lookup{TP: tp("t", 0)}); l.End != 0 {
		t.Fatal("rows written with NoIndex")
	}
}

func TestIndexLoopFlushesOnTheActiveNodeAndStandbysFollow(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.IndexEvery = 10 * time.Millisecond })
	standby := node(t, "n1", 1, f.log, f.db, f.objs, func(c *logcp.Config) { c.IndexEvery = 10 * time.Millisecond })
	commitMany(t, f.s, "a", 20, 2)
	for deadline := time.Now().Add(5 * time.Second); f.watermark() < f.s.Applied(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("index loop did not flush: watermark %d applied %d", f.watermark(), f.s.Applied())
		}
	}
	f.sameAsApplier(t, tp("t", 0), tp("t", 1))
	waitApplied(t, f.s, standby)
	time.Sleep(50 * time.Millisecond) // the standby's loop reads the watermark and drops its feed
}

func TestCheckpointLoopAndErrors(t *testing.T) {
	f := newIndexFixture(t, func(c *logcp.Config) { c.CkptEvery, c.CkptMinSlots = 10*time.Millisecond, 2 })
	commitMany(t, f.s, "a", 10, 2)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if keys, _ := f.objs.List(bg, "ckpt/"); len(keys) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no periodic checkpoint")
		}
	}
	if metricSum(t, f.reg, "manicp_log_checkpoint_slot") == 0 || metricSum(t, f.reg, "manicp_log_checkpoint_bytes") == 0 {
		t.Fatal("checkpoint gauges")
	}
	bad := node(t, "n9", 5, f.log, f.db, failingObjects{f.objs}, nil)
	if _, err := bad.Checkpoint(bg); err == nil {
		t.Fatal("a failed object put must fail the checkpoint")
	}
	plain := node(t, "n8", 6, logcp.NewMemLog(), nil, nil, nil)
	if _, err := plain.Checkpoint(bg); err == nil {
		t.Fatal("checkpoint without storage")
	}
}

type failingObjects struct{ *logcptest.Objects }

func (failingObjects) Put(context.Context, string, []byte) error { return errors.New("s3 down") }

func TestRoleRulesFailback(t *testing.T) {
	log := logcp.NewMemLog()
	mod := func(c *logcp.Config) { c.TakeoverAfter = 60 * time.Millisecond }
	a := node(t, "n0", 0, log, nil, nil, mod)
	b := node(t, "n1", 1, log, nil, nil, mod)
	commit(t, a, seg("k", tp("t", 0)))
	a.Stop()
	for deadline := time.Now().Add(5 * time.Second); !b.IsActive(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no takeover")
		}
	}
	a2 := node(t, "n0", 0, log, nil, nil, mod) // the preferred node returns and starts active
	commit(t, a2, seg("k2", tp("t", 0)))
	for deadline := time.Now().Add(5 * time.Second); b.IsActive(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the standby did not step down for the preferred node")
		}
	}
	b.Demote()
	b.Promote()
	b.Demote()
	if b.IsActive() {
		t.Fatal("demote")
	}
	if end, _ := b.End(bg, tp("t", 0)); end != 4 {
		t.Fatalf("end %d", end)
	}
	if b.Name() != "n1" {
		t.Fatal("name")
	}
}
