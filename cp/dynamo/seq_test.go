package dynamo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpobs"
	"manifester/cp/dynamo/fakedb"
	"manifester/cp/memstore"
)

func newSeq(t testing.TB, o SeqOptions) (*Sequencer, *fakedb.Fake) {
	t.Helper()
	st, f := fakeStore(t)
	st.Obs = cpobs.NewWithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := NewSequencer(st, o)
	t.Cleanup(s.Close)
	return s, f
}

// gate blocks the next n transactions until release is called.
type gate struct {
	ch   chan struct{}
	left atomic.Int32
}

func newGate(f *fakedb.Fake, n int) *gate {
	g := &gate{ch: make(chan struct{})}
	g.left.Store(int32(n))
	f.BeforeTransact = func() {
		if g.left.Add(-1) >= 0 {
			<-g.ch
		}
	}
	return g
}

func (g *gate) release() { close(g.ch) }

// waitFor polls cond for a second.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type commitResult struct {
	resp core.CommitSegmentResp
	code core.ErrCode
	err  error
}

// async commits in a goroutine.
func async(ctx context.Context, st cp.Store, op core.CommitSegment) <-chan commitResult {
	ch := make(chan commitResult, 1)
	go func() {
		r, c, err := st.CommitSegment(ctx, op)
		ch <- commitResult{r, c, err}
	}()
	return ch
}

func seg(key string, bs ...core.PendingBatch) core.CommitSegment {
	return core.CommitSegment{SegmentKey: key, Batches: bs}
}

func mustOK(t testing.TB, r commitResult) core.CommitSegmentResp {
	t.Helper()
	if r.err != nil || r.code != core.OK {
		t.Fatalf("commit: %v %v", r.code, r.err)
	}
	return r.resp
}

// ---- sequential lockstep against the spec ----

func TestSeqAgainstMemstore(t *testing.T) {
	for seed := uint64(1); seed <= 24; seed++ {
		o := SeqOptions{Pipeline: 1 + int(seed%3)*2, CacheKeys: []int{-1, 2, 4096}[seed%3]}
		s, f := newSeq(t, o)
		f.PageSize = 1 + int(seed%3)
		spec := memstore.New()
		r := rand.New(rand.NewPCG(seed, 11))
		for i := 0; i < 120; i++ {
			op := randOp(r, i)
			if got, want := exec(s, op), exec(spec, op); got != want {
				t.Fatalf("seed %d op %d %+v\n seq: %s\n spec: %s", seed, i, op, got, want)
			}
		}
		s.Close()
	}
}

// In steady state a commit costs one transaction and no reads: the ends are in memory.
func TestSeqSteadyStateNoReads(t *testing.T) {
	s, f := newSeq(t, SeqOptions{})
	var reads atomic.Int32
	f.Fail = func(call string) error {
		if call == "Query" || call == "BatchGetItem" || call == "GetItem" {
			reads.Add(1)
		}
		return nil
	}
	tps := []core.TopicPartition{tpOf("t", 0), tpOf("t", 1), tpOf("t", 2)}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		resp, code, err := s.CommitSegment(bg, seg(k, one(tps[0], k, 2), one(tps[1], k, 1), one(tps[2], k, 3)))
		if err != nil || code != core.OK || resp.Assigned[0].Base != core.Offset(2*i) || resp.Assigned[2].Base != core.Offset(3*i) {
			t.Fatalf("commit %d: %+v %v %v", i, resp, code, err)
		}
	}
	// first touch: 3 Queries + 1 BatchGet for the topic; then nothing
	if n := reads.Load(); n != 4 {
		t.Fatalf("%d reads, want 4", n)
	}
	st := s.Stats()
	if st.Txs != 10 || st.Canceled != 0 || st.EndMisses != 3 {
		t.Fatalf("%+v", st)
	}
	// a replay is answered from memory
	reads.Store(0)
	resp, code, err := s.CommitSegment(bg, seg("k3", one(tps[0], "k3", 2), one(tps[1], "k3", 1), one(tps[2], "k3", 3)))
	if err != nil || code != core.OK || resp.Assigned[0].Base != 6 || reads.Load() != 0 || s.Stats().SegHits != 1 {
		t.Fatalf("replay: %+v %v %v reads %d", resp, code, err, reads.Load())
	}
	if code, _ := commitCode(s, seg("k3", one(tps[0], "k3", 9))); code != core.ErrConflict {
		t.Fatalf("conflict: %v", code)
	}
}

func commitCode(st cp.Store, op core.CommitSegment) (core.ErrCode, error) {
	_, c, err := st.CommitSegment(bg, op)
	return c, err
}

// ---- group commit ----

// Requests queued behind a transaction in flight form one group: one
// transaction, runs of one partition merged, each answered with its own ranges
// in request order; a malformed, a too large and a conflicting request are
// answered alone and the rest still commit.
func TestSeqGroupCommitIsolation(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1})
	if _, _, err := s.CommitSegment(bg, seg("old", one(tpOf("t", 0), "old", 5))); err != nil {
		t.Fatal(err)
	}
	g := newGate(f, 1)
	first := async(bg, s, seg("first", one(tpOf("t", 9), "first", 1)))
	waitFor(t, "first in flight", func() bool { return s.Stats().Groups == 2 })
	var many []core.PendingBatch
	for i := 0; i < 101; i++ {
		many = append(many, one(tpOf("big", int32(i)), "huge", 1))
	}
	reqs := []core.CommitSegment{
		seg("a", one(tpOf("t", 0), "a", 2), one(tpOf("t", 1), "a", 1)),
		seg("b", one(tpOf("t", 0), "b", 3)),
		seg("old", one(tpOf("t", 0), "old", 6)), // other request under a committed key
		seg("huge", many...),
		seg("c", one(tpOf("t", 1), "c", 4), one(tpOf("t", 0), "c", 1), one(tpOf("t", 0), "c", 1)),
		seg("", one(tpOf("t", 0), "x", 1)), // malformed
	}
	var res []<-chan commitResult
	for i, op := range reqs {
		res = append(res, async(bg, s, op))
		if op.SegmentKey != "" {
			waitFor(t, "queued", func() bool { return s.Stats().Requests >= int64(3+i) })
		}
	}
	g.release()
	mustOK(t, <-first)
	a, b := mustOK(t, <-res[0]), mustOK(t, <-res[1])
	if r := <-res[2]; r.code != core.ErrConflict || r.err != nil {
		t.Fatalf("conflict: %+v", r)
	}
	if r := <-res[3]; r.code != core.ErrTooLarge || r.err != nil {
		t.Fatalf("too large: %+v", r)
	}
	c := mustOK(t, <-res[4])
	if r := <-res[5]; r.code != core.ErrInvalid {
		t.Fatalf("malformed: %+v", r)
	}
	if a.Assigned[0].Base != 5 || a.Assigned[1].Base != 0 || b.Assigned[0].Base != 7 ||
		c.Assigned[0].Base != 1 || c.Assigned[1].Base != 10 || c.Assigned[2].Base != 11 {
		t.Fatalf("a %+v b %+v c %+v", a, b, c)
	}
	st := s.Stats()
	if st.Txs != 3 || st.Canceled != 0 { // old, first, then a+b+c together
		t.Fatalf("%+v", st)
	}
	// one run item per partition for the group: t/0 holds a, b, c in one run
	l, _, _ := s.Lookup(bg, core.Lookup{TP: tpOf("t", 0), From: 5})
	if l.End != 12 || len(l.Ranges) != 4 {
		t.Fatalf("lookup %+v", l)
	}
	it := f.Item(s.st.Table(), runKey(tpOf("t", 0), 5))
	if it == nil || len(it) == 0 {
		t.Fatal("run at base 5 missing: runs were not merged")
	}
	if n, _ := getNum(it, "cnt"); n != 7 {
		t.Fatalf("merged run cnt %d, want 7", n)
	}
}

// A replay whose key fell out of the LRU hits the G# condition: only that
// request is answered from the table, the others of the group are retried and
// commit.
func TestSeqGroupReplayAfterEviction(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1, CacheKeys: -1})
	tp := tpOf("t", 0)
	first, _, _ := s.CommitSegment(bg, seg("old", one(tp, "old", 2)))
	g := newGate(f, 1)
	blocker := async(bg, s, seg("blocker", one(tpOf("t", 7), "blocker", 1)))
	waitFor(t, "blocker", func() bool { return s.Stats().Groups == 2 })
	replay := async(bg, s, seg("old", one(tp, "old", 2)))
	fresh := async(bg, s, seg("new", one(tp, "new", 1)))
	waitFor(t, "queued", func() bool { return s.Stats().Requests == 4 })
	g.release()
	mustOK(t, <-blocker)
	if r := mustOK(t, <-replay); r.Assigned[0] != first.Assigned[0] {
		t.Fatalf("replay %+v want %+v", r, first)
	}
	if r := mustOK(t, <-fresh); r.Assigned[0].Base != 2 {
		t.Fatalf("fresh %+v", r)
	}
	if st := s.Stats(); st.Canceled != 1 || st.SegReads != 1 {
		t.Fatalf("%+v", st)
	}
}

// A group never exceeds the item budget; the rest wait for the next group, in order.
func TestSeqGroupItemBudget(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1, MaxItems: 20})
	var maxItems atomic.Int32
	probe := &probeDB{DB: f, onTx: func(in *dynamodb.TransactWriteItemsInput) {
		for {
			cur := maxItems.Load()
			if int32(len(in.TransactItems)) <= cur || maxItems.CompareAndSwap(cur, int32(len(in.TransactItems))) {
				return
			}
		}
	}}
	st := New(probe, s.st.Prefix())
	s.Close()
	s = NewSequencer(st, SeqOptions{Pipeline: 1, MaxItems: 20})
	defer s.Close()
	g := newGate(f, 1)
	blocker := async(bg, s, seg("blocker", one(tpOf("z", 0), "blocker", 1)))
	waitFor(t, "blocker", func() bool { return s.Stats().Groups == 1 })
	var res []<-chan commitResult
	for i := 0; i < 12; i++ { // 5 partitions each: 3 per group at most (3 + 15 + 1 <= 20)
		k := fmt.Sprintf("s%d", i)
		var bs []core.PendingBatch
		for p := 0; p < 5; p++ {
			bs = append(bs, one(tpOf("t", int32(p)), k, 1))
		}
		res = append(res, async(bg, s, seg(k, bs...)))
		waitFor(t, "queued", func() bool { return s.Stats().Requests >= int64(2+i) })
	}
	g.release()
	mustOK(t, <-blocker)
	for i, ch := range res {
		r := mustOK(t, <-ch)
		if r.Assigned[0].Base != core.Offset(i) { // queue order is commit order
			t.Fatalf("segment %d got base %d", i, r.Assigned[0].Base)
		}
	}
	if m := maxItems.Load(); m > 20 || m < 16 {
		t.Fatalf("max items %d", m)
	}
	if st := s.Stats(); st.Txs > 5 {
		t.Fatalf("%d transactions for 13 segments", st.Txs)
	}
}

// probeDB observes transactions.
type probeDB struct {
	DB
	onTx    func(*dynamodb.TransactWriteItemsInput)
	afterTx func(*dynamodb.TransactWriteItemsInput)
	after   func()
}

func (p *probeDB) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, o ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if p.onTx != nil {
		p.onTx(in)
	}
	out, err := p.DB.TransactWriteItems(ctx, in, o...)
	if p.afterTx != nil {
		p.afterTx(in)
	}
	if p.after != nil {
		p.after()
	}
	return out, err
}

// A merged run too large for one item splits the group (and a lone one is ErrTooLarge).
func TestSeqRunTooLargeSplitsGroup(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1})
	s.maxRunBytes = 40
	g := newGate(f, 1)
	blocker := async(bg, s, seg("blocker", one(tpOf("z", 0), "blocker", 1)))
	waitFor(t, "blocker", func() bool { return s.Stats().Groups == 1 })
	tp := tpOf("t", 0)
	var res []<-chan commitResult
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("s%d", i)
		res = append(res, async(bg, s, seg(k, one(tp, k, 1))))
		waitFor(t, "queued", func() bool { return s.Stats().Requests >= int64(2+i) })
	}
	g.release()
	mustOK(t, <-blocker)
	for i, ch := range res {
		if r := mustOK(t, <-ch); r.Assigned[0].Base != core.Offset(i) {
			t.Fatalf("segment %d: %+v", i, r)
		}
	}
	s.maxRunBytes = 4
	if c, err := commitCode(s, seg("alone", one(tp, "alone", 1))); c != core.ErrTooLarge || err != nil {
		t.Fatalf("lone run too large: %v %v", c, err)
	}
	if l, _, _ := s.Lookup(bg, core.Lookup{TP: tp}); l.End != 4 {
		t.Fatalf("end %d", l.End)
	}
}

func TestSeqGroupBlobBudget(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1})
	s.maxGroupBytes = 1
	g := newGate(f, 1)
	blocker := async(bg, s, seg("blocker", one(tpOf("z", 0), "blocker", 1)))
	waitFor(t, "blocker", func() bool { return s.Stats().Groups == 1 })
	tp := tpOf("t", 0)
	a := async(bg, s, seg("a", one(tp, "a", 1)))
	waitFor(t, "a", func() bool { return s.Stats().Requests == 2 })
	b := async(bg, s, seg("b", one(tp, "b", 1)))
	waitFor(t, "b", func() bool { return s.Stats().Requests == 3 })
	g.release()
	mustOK(t, <-blocker)
	if mustOK(t, <-a).Assigned[0].Base != 0 || mustOK(t, <-b).Assigned[0].Base != 1 {
		t.Fatal("order lost")
	}
	if s.Stats().Txs != 3 {
		t.Fatalf("%+v", s.Stats())
	}
}

// ---- cancellation, close ----

func TestSeqContextCancelWhileQueued(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1})
	g := newGate(f, 1)
	tp := tpOf("t", 0)
	blocker := async(bg, s, seg("blocker", one(tp, "blocker", 1)))
	waitFor(t, "blocker", func() bool { return s.Stats().Groups == 1 })
	ctx, cancel := context.WithCancel(bg)
	gone := async(ctx, s, seg("gone", one(tp, "gone", 7)))
	waitFor(t, "queued", func() bool { return s.Stats().Requests == 2 })
	cancel()
	if r := <-gone; !errors.Is(r.err, context.Canceled) {
		t.Fatalf("got %+v", r)
	}
	g.release()
	mustOK(t, <-blocker)
	next := mustOK(t, commitSync(s, seg("next", one(tp, "next", 1))))
	if next.Assigned[0].Base != 1 { // the withdrawn request took no offsets
		t.Fatalf("next %+v", next)
	}
	if s.Stats().Txs != 2 {
		t.Fatalf("%+v", s.Stats())
	}
}

func commitSync(st cp.Store, op core.CommitSegment) commitResult { return <-async(bg, st, op) }

func TestSeqClose(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 1})
	g := newGate(f, 1)
	tp := tpOf("t", 0)
	blocker := async(bg, s, seg("blocker", one(tp, "blocker", 1)))
	waitFor(t, "blocker", func() bool { return s.Stats().Groups == 1 })
	queued := async(bg, s, seg("queued", one(tp, "queued", 1)))
	waitFor(t, "queued", func() bool { return s.Stats().Requests == 2 })
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	waitFor(t, "closing", func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.closed })
	g.release()
	<-done
	if r := <-queued; !errors.Is(r.err, ErrSequencerClosed) || !errors.Is(r.err, cp.ErrUnavailable) {
		t.Fatalf("queued: %+v", r)
	}
	<-blocker
	if r := commitSync(s, seg("late", one(tp, "late", 1))); !errors.Is(r.err, ErrSequencerClosed) {
		t.Fatalf("late: %+v", r)
	}
	s.Close() // idempotent
}

// ---- failures ----

func TestSeqThrottleRetriesInternally(t *testing.T) {
	s, f := newSeq(t, SeqOptions{})
	var n atomic.Int32
	f.Fail = func(call string) error {
		if call == "TransactWriteItems" && n.Add(1) <= 3 {
			return &types.ThrottlingException{Message: aws.String("slow down")}
		}
		return nil
	}
	r := mustOK(t, commitSync(s, seg("a", one(tpOf("t", 0), "a", 2))))
	if r.Assigned[0].Base != 0 || s.Stats().NotApplied != 3 || s.Stats().Retries != 3 {
		t.Fatalf("%+v %+v", r, s.Stats())
	}
}

func TestSeqFatalAndBudget(t *testing.T) {
	s, f := newSeq(t, SeqOptions{MaxAttempts: 3})
	f.Fail = func(call string) error {
		if call == "TransactWriteItems" {
			return &smithyErr{"ValidationException"}
		}
		return nil
	}
	r := commitSync(s, seg("a", one(tpOf("t", 0), "a", 2)))
	if r.err == nil || errors.Is(r.err, cp.ErrUnavailable) {
		t.Fatalf("fatal error expected: %+v", r)
	}
	f.Fail = throttle("TransactWriteItems")
	r = commitSync(s, seg("b", one(tpOf("t", 0), "b", 2)))
	if !errors.Is(r.err, cp.ErrUnavailable) {
		t.Fatalf("budget: %+v", r)
	}
	f.Fail = throttle("Query")
	r = commitSync(s, seg("c", one(tpOf("u", 0), "c", 2)))
	if !errors.Is(r.err, cp.ErrUnavailable) {
		t.Fatalf("read budget: %+v", r)
	}
	f.Fail = func(call string) error {
		if call == "Query" {
			return &smithyErr{"AccessDeniedException"}
		}
		return nil
	}
	if r = commitSync(s, seg("d", one(tpOf("v", 0), "d", 2))); r.err == nil || errors.Is(r.err, cp.ErrUnavailable) {
		t.Fatalf("fatal read: %+v", r)
	}
}

type smithyErr struct{ code string }

func (e *smithyErr) Error() string                 { return e.code }
func (e *smithyErr) ErrorCode() string             { return e.code }
func (e *smithyErr) ErrorMessage() string          { return e.code }
func (e *smithyErr) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

// A lost answer: the transaction DID apply and the caller got an error. The
// retry must not assign twice: it reads G#, finds its own answer, replays it.
func TestSeqLostAnswerReplays(t *testing.T) {
	s, f := newSeq(t, SeqOptions{})
	tp := tpOf("t", 0)
	mustOK(t, commitSync(s, seg("a", one(tp, "a", 2))))
	var lost atomic.Int32
	f.AfterApply = func() error {
		if lost.Add(1) == 1 {
			return &types.InternalServerError{Message: aws.String("answer lost")}
		}
		return nil
	}
	r := mustOK(t, commitSync(s, seg("b", one(tp, "b", 3), one(tpOf("t", 1), "b", 1))))
	if r.Assigned[0].Base != 2 || r.Assigned[1].Base != 0 {
		t.Fatalf("%+v", r)
	}
	if st := s.Stats(); st.Unknown != 1 || st.SegReads == 0 {
		t.Fatalf("%+v", st)
	}
	if l, _, _ := s.Lookup(bg, core.Lookup{TP: tp}); l.End != 5 || len(l.Ranges) != 2 {
		t.Fatalf("lookup %+v: applied twice or not at all", l)
	}
	if mustOK(t, commitSync(s, seg("c", one(tp, "c", 1)))).Assigned[0].Base != 5 {
		t.Fatal("next offset wrong after unknown outcome")
	}
}

// The reverse: a failure that did NOT apply is retried and applies once.
func TestSeqUnknownNotApplied(t *testing.T) {
	s, f := newSeq(t, SeqOptions{})
	var n atomic.Int32
	f.Fail = func(call string) error {
		if call == "TransactWriteItems" && n.Add(1) == 1 {
			return errors.New("connection reset by peer")
		}
		return nil
	}
	tp := tpOf("t", 0)
	r := mustOK(t, commitSync(s, seg("a", one(tp, "a", 2))))
	if r.Assigned[0].Base != 0 || s.Stats().Unknown != 1 {
		t.Fatalf("%+v %+v", r, s.Stats())
	}
	if l, _, _ := s.Lookup(bg, core.Lookup{TP: tp}); l.End != 2 {
		t.Fatalf("end %d", l.End)
	}
}

// A cancellation without reasons refreshes everything and retries.
func TestSeqCancelWithoutReasons(t *testing.T) {
	s, f := newSeq(t, SeqOptions{})
	var n atomic.Int32
	f.Fail = func(call string) error {
		if call == "TransactWriteItems" && n.Add(1) == 1 {
			return &types.TransactionCanceledException{Message: aws.String("cancelled")}
		}
		return nil
	}
	r := mustOK(t, commitSync(s, seg("a", one(tpOf("t", 0), "a", 2))))
	if r.Assigned[0].Base != 0 || s.Stats().Canceled != 1 {
		t.Fatalf("%+v %+v", r, s.Stats())
	}
}

// A topic count that is stale (CreateTopic from elsewhere) costs one retry.
func TestSeqStaleTopicAndCreateTopic(t *testing.T) {
	s, f := newSeq(t, SeqOptions{})
	other := New(f, s.st.Prefix())
	mustOK(t, commitSync(s, seg("a", one(tpOf("t", 1), "a", 1)))) // t has 2 partitions, cached
	if code, err := other.CreateTopic(bg, "t", 8); err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	r := mustOK(t, commitSync(s, seg("b", one(tpOf("t", 4), "b", 1)))) // wants to grow to 5: the condition fails
	if r.Assigned[0].Base != 0 || s.Stats().Canceled != 1 {
		t.Fatalf("%+v %+v", r, s.Stats())
	}
	if code, err := s.CreateTopic(bg, "t", 20); err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	if _, ok := s.state.topic("t"); ok {
		t.Fatal("CreateTopic must drop the cached count")
	}
	d, err := s.DescribeTopics(bg, core.DescribeTopics{Topics: []string{"t"}})
	if err != nil || d.Topics[0].Partitions != 20 {
		t.Fatalf("%+v %v", d, err)
	}
}

// ---- pipelining ----

// Groups on disjoint partitions are in flight together; a group touching a
// partition that is in flight waits for it.
func TestSeqPipelineDisjointOverlapConflictWaits(t *testing.T) {
	s, f := newSeq(t, SeqOptions{Pipeline: 4})
	f.Latency = func(call string) time.Duration {
		if call == "TransactWriteItems" {
			return 40 * time.Millisecond
		}
		return 0
	}
	var cur, peak atomic.Int32
	probe := &probeDB{DB: f}
	probe.onTx = func(*dynamodb.TransactWriteItemsInput) {
		n := cur.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
	}
	probe.after = func() { cur.Add(-1) }
	s.Close()
	s = NewSequencer(New(probe, s.st.Prefix()), SeqOptions{Pipeline: 4, MaxItems: 3}) // one segment per group
	defer s.Close()
	var res []<-chan commitResult
	for i := 0; i < 4; i++ { // warm: topic and ends known
		k := fmt.Sprintf("w%d", i)
		mustOK(t, commitSync(s, seg(k, one(tpOf("t", int32(i)), k, 1))))
	}
	peak.Store(0)
	t0 := time.Now()
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("d%d", i)
		res = append(res, async(bg, s, seg(k, one(tpOf("t", int32(i)), k, 1))))
	}
	for _, ch := range res {
		mustOK(t, <-ch)
	}
	if peak.Load() < 2 {
		t.Fatalf("disjoint groups never overlapped (peak %d)", peak.Load())
	}
	if d := time.Since(t0); d > 150*time.Millisecond {
		t.Fatalf("4 disjoint commits took %v, want about one latency", d)
	}
	// same partition: serialised, offsets in arrival order
	peak.Store(0)
	a := async(bg, s, seg("x1", one(tpOf("t", 0), "x1", 1)))
	waitFor(t, "x1 queued", func() bool { return s.Stats().Requests >= 9 })
	b := async(bg, s, seg("x2", one(tpOf("t", 0), "x2", 1)))
	ra, rb := mustOK(t, <-a), mustOK(t, <-b)
	if ra.Assigned[0].Base != 2 || rb.Assigned[0].Base != 3 {
		t.Fatalf("%+v %+v", ra, rb)
	}
}

// If the transaction in flight for a partition fails, the next one for it was
// not issued with a base chained after it: random commits over few partitions,
// pipeline 4, random failures (throttle, unknown, lost answer, a stale peer
// writing), then the table must tile every partition exactly once, per-key
// answers must match the table, and no two transactions that touch one
// partition may overlap in time.
func TestSeqPipelineRandomFailuresNoHoles(t *testing.T) {
	for seed := uint64(1); seed <= 6; seed++ {
		s, f := newSeq(t, SeqOptions{Pipeline: 4, MaxAttempts: 200, MaxTime: time.Minute})
		runPipelineChaos(t, s, f, seed)
	}
}

func runPipelineChaos(t *testing.T, s *Sequencer, f *fakedb.Fake, seed uint64) {
	t.Helper()
	var mu sync.Mutex
	rr := rand.New(rand.NewPCG(seed, 3))
	roll := func(n int) int { mu.Lock(); defer mu.Unlock(); return rr.IntN(n) }
	f.Latency = func(call string) time.Duration { return time.Duration(roll(3)) * time.Millisecond }
	f.Fail = func(call string) error {
		if call != "TransactWriteItems" && call != "Query" {
			return nil
		}
		switch roll(12) {
		case 0:
			return &types.ThrottlingException{Message: aws.String("slow")}
		case 1:
			return errors.New("connection reset")
		}
		return nil
	}
	f.AfterApply = func() error {
		if roll(10) == 0 {
			return &types.InternalServerError{Message: aws.String("lost answer")}
		}
		return nil
	}
	checkNoOverlap := newOverlapProbe(t, f, s)
	const workers, per, parts = 8, 12, 10
	type got struct {
		op   core.CommitSegment
		resp core.CommitSegmentResp
	}
	var all []got
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				k := fmt.Sprintf("w%d-%d", w, i)
				var bs []core.PendingBatch
				for _, p := range pickParts(roll, parts, 1+roll(3)) {
					bs = append(bs, one(tpOf("t", int32(p)), k, int64(1+roll(3))))
				}
				op := seg(k, bs...)
				resp := commitUntilOK(t, s, op)
				mu.Lock()
				all = append(all, got{op, resp})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	checkNoOverlap()
	f.Fail, f.AfterApply, f.Latency = nil, nil, nil
	ends := map[core.TopicPartition]int64{}
	for _, g := range all {
		for _, r := range g.resp.Assigned {
			ends[r.TP] += r.Count
		}
	}
	total := 0
	for tp, want := range ends {
		l, _, err := s.Lookup(bg, core.Lookup{TP: tp})
		if err != nil || int64(l.End) != want {
			t.Fatalf("seed %d %v: end %d, answers sum to %d (%v)", seed, tp, l.End, want, err)
		}
		next := int64(0)
		for _, r := range l.Ranges {
			if int64(r.Base) != next {
				t.Fatalf("seed %d %v: hole or overlap at %d (expected %d)", seed, tp, r.Base, next)
			}
			next += r.Count
			total++
		}
	}
	sum := 0
	for _, g := range all {
		sum += len(g.resp.Assigned)
	}
	if total != sum {
		t.Fatalf("seed %d: %d batches in the table, %d answered", seed, total, sum)
	}
}

func pickParts(roll func(int) int, n, k int) []int {
	seen := map[int]bool{}
	var out []int
	for len(out) < k {
		if p := roll(n); !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// commitUntilOK resends under the same key on 503, as a broker does.
func commitUntilOK(t *testing.T, st cp.Store, op core.CommitSegment) core.CommitSegmentResp {
	t.Helper()
	for try := 0; try < 50; try++ {
		resp, code, err := st.CommitSegment(bg, op)
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

// newOverlapProbe wraps the sequencer's DB so that two transactions touching
// one partition at the same time are reported. It returns the final check.
func newOverlapProbe(t *testing.T, f *fakedb.Fake, s *Sequencer) func() {
	var mu sync.Mutex
	busy := map[string]int{}
	var bad atomic.Int32
	probe := &probeDB{DB: f}
	probe.onTx = func(in *dynamodb.TransactWriteItemsInput) {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range runPKs(in) {
			if busy[k]++; busy[k] > 1 {
				bad.Add(1)
			}
		}
	}
	probe.afterTx = func(in *dynamodb.TransactWriteItemsInput) {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range runPKs(in) {
			busy[k]--
		}
	}
	s.st.db = probe
	return func() {
		if bad.Load() != 0 {
			t.Fatalf("%d overlapping transactions on one partition", bad.Load())
		}
	}
}

func runPKs(in *dynamodb.TransactWriteItemsInput) []string {
	var out []string
	for _, it := range in.TransactItems {
		if it.Put != nil {
			if pk := getStr(it.Put.Item, "pk"); len(pk) > 2 && pk[:2] == "R#" {
				out = append(out, pk)
			}
		}
	}
	return out
}

func TestSeqStateLRU(t *testing.T) {
	st := newSeqState(nil, 2)
	b := []core.PendingBatch{one(tpOf("t", 0), "a", 1)}
	st.putSeg("a", b, nil)
	st.putSeg("b", b, nil)
	st.putSeg("a", b, nil) // refresh
	st.putSeg("c", b, nil) // evicts b
	if _, ok := st.seg("b"); ok || st.size() != 2 {
		t.Fatalf("size %d", st.size())
	}
	if _, ok := st.seg("a"); !ok {
		t.Fatal("a evicted")
	}
	off := newSeqState(nil, -1)
	off.putSeg("a", b, nil)
	if off.size() != 0 {
		t.Fatal("disabled cache stored")
	}
	if outcomeOf(&types.ThrottlingException{}) != outcomeNotApplied || reasonLegacy("read_failed") != "unavailable" ||
		reasonLegacy("transaction_canceled") != "transaction_canceled" {
		t.Fatal("classification")
	}
	s, _ := newSeq(t, SeqOptions{})
	if s.Store() == nil {
		t.Fatal("store")
	}
	s.Close()
	if err := s.backoff(1, time.Now()); !errors.Is(err, ErrSequencerClosed) {
		t.Fatalf("backoff on closed: %v", err)
	}
	if err := s.backoff(1000, time.Now()); !errors.Is(err, cp.ErrUnavailable) {
		t.Fatalf("budget: %v", err)
	}
	if got := s.readFailed([]*seqReq{{done: make(chan struct{})}}, errors.New("x")); got != nil {
		t.Fatal("closed sequencer must fail reads")
	}
}
