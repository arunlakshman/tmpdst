package logsm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp/fakedb"
)

var bg = context.Background()

func tpOf(t string, p int32) core.TopicPartition { return core.TP(t, p) }

func commitEntry(key string, bs ...core.PendingBatch) Entry {
	return Entry{Kind: KindCommit, Commit: core.CommitSegment{SegmentKey: key, Batches: bs}}
}

func pb(t string, p int32, recs int64) core.PendingBatch {
	return core.PendingBatch{TP: tpOf(t, p), Records: recs, Loc: core.SegmentRef{Key: "o", Off: 7, Len: recs * 10}}
}

func TestCodecRoundTrip(t *testing.T) {
	es := []Entry{
		{ID: "w/1", Kind: KindNoop},
		{ID: "w/2", Kind: KindCreateTopic, Topic: "t", Parts: 5},
		commitEntry("k", pb("a", 0, 3), pb("b", -1, 0), pb("a", 7, 1)),
		commitEntry(""),
	}
	for _, e := range es {
		got, err := Decode(Encode(e))
		if err != nil || got.ID != e.ID || got.Kind != e.Kind || got.Topic != e.Topic || got.Parts != e.Parts ||
			got.Commit.SegmentKey != e.Commit.SegmentKey || !slices.Equal(got.Commit.Batches, e.Commit.Batches) {
			t.Fatalf("round trip %+v -> %+v (%v)", e, got, err)
		}
	}
}

func TestCodecCorrupt(t *testing.T) {
	good := Encode(commitEntry("k", pb("a", 0, 3), pb("a", 1, 3)))
	for n := 0; n < len(good); n++ { // every truncation fails or decodes cleanly, never panics
		_, _ = Decode(good[:n])
	}
	if _, err := Decode(good[:len(good)-1]); err == nil {
		t.Fatal("truncated entry decoded")
	}
	for _, b := range [][]byte{nil, {9}, {1, 0, 0, 5}, {1, 0, 0, 1, 1, 'x', 1, 5, 0, 0, 0, 0}, {1, 0, 0, 0, 0, 200}} {
		if _, err := Decode(b); err == nil {
			t.Fatalf("garbage %v decoded", b)
		}
	}
}

func testLog(t *testing.T, l Log) {
	t.Helper()
	if n, err := l.CheckTail(bg); err != nil || n != 0 {
		t.Fatalf("empty tail %d %v", n, err)
	}
	a, b := Entry{ID: "a", Kind: KindNoop}, Entry{ID: "b", Kind: KindNoop}
	if cur, won, err := l.Append(bg, 3, a); err != nil || !won || cur.ID != "a" {
		t.Fatalf("first append: %+v %v %v", cur, won, err)
	}
	if cur, won, err := l.Append(bg, 3, b); err != nil || won || cur.ID != "a" {
		t.Fatalf("second append must lose and show the winner: %+v %v %v", cur, won, err)
	}
	for _, n := range []int64{0, 5, 4} {
		if _, won, err := l.Append(bg, n, Entry{ID: fmt.Sprint(n)}); err != nil || !won {
			t.Fatalf("append %d: %v %v", n, won, err)
		}
	}
	if n, _ := l.CheckTail(bg); n != 6 {
		t.Fatalf("tail %d, want 6 (hole at 1, 2 does not lower it)", n)
	}
	got, err := l.Read(bg, 1, 10)
	if err != nil || len(got) != 3 || got[0].N != 3 || got[1].N != 4 || got[2].N != 5 {
		t.Fatalf("read from 1: %+v %v", got, err)
	}
	if got, _ := l.Read(bg, 0, 2); len(got) != 2 || got[0].N != 0 || got[1].N != 3 {
		t.Fatalf("read limit 2: %+v", got)
	}
	if got, _ := l.Read(bg, 6, 2); len(got) != 0 {
		t.Fatalf("read past the tail: %+v", got)
	}
}

func fakeLog(t testing.TB, db *fakedb.Fake) *DynLog {
	t.Helper()
	if _, err := db.CreateTable(bg, &dynamodb.CreateTableInput{TableName: aws.String("log")}); err != nil {
		t.Fatal(err)
	}
	return NewDynLog(db, "log")
}

func TestMemLog(t *testing.T) { testLog(t, NewMemLog()) }

func TestDynLog(t *testing.T) { testLog(t, fakeLog(t, fakedb.New())) }

func TestDynLogPaging(t *testing.T) {
	db := fakedb.New()
	db.PageSize = 2
	l := fakeLog(t, db)
	for n := int64(0); n < 7; n++ {
		if _, won, err := l.Append(bg, n, Entry{ID: fmt.Sprint(n)}); !won || err != nil {
			t.Fatal(won, err)
		}
	}
	if got, err := l.Read(bg, 1, 5); err != nil || len(got) != 5 || got[4].N != 5 {
		t.Fatalf("paged read: %+v %v", got, err)
	}
}

func TestDynLogErrors(t *testing.T) {
	db := fakedb.New()
	l := fakeLog(t, db)
	boom := errors.New("boom")
	db.Fail = func(string) error { return boom }
	if _, _, err := l.Append(bg, 0, Entry{}); !errors.Is(err, boom) {
		t.Fatalf("append: %v", err)
	}
	if _, err := l.Read(bg, 0, 1); !errors.Is(err, boom) {
		t.Fatalf("read: %v", err)
	}
	if _, err := l.CheckTail(bg); !errors.Is(err, boom) {
		t.Fatalf("tail: %v", err)
	}
	db.Fail = nil
	// A lost answer: the put landed, the caller got an error, the retry sees
	// its own entry.
	db.AfterPut = func() error { return boom }
	e := Entry{ID: "mine", Kind: KindNoop}
	if _, _, err := l.Append(bg, 0, e); err == nil {
		t.Fatal("lost answer not reported")
	}
	db.AfterPut = nil
	if cur, won, err := l.Append(bg, 0, e); err != nil || won || cur.ID != "mine" {
		t.Fatalf("retry after lost answer: %+v %v %v", cur, won, err)
	}
	// A corrupt item is an error, not a panic.
	db.Put("log", map[string]types.AttributeValue{"pk": sv("LOG"), "sk": sv(slotKey(1)), "d": sv("x")})
	if _, err := l.Read(bg, 1, 1); err == nil {
		t.Fatal("corrupt item read")
	}
	if _, _, err := l.Append(bg, 1, e); err == nil {
		t.Fatal("corrupt existing item appended over")
	}
}

func newMemReplica(name string, l Log) *Replica {
	return NewReplica(Config{Log: l, Writer: name, HoleAfter: int64(20 * time.Millisecond), TailPoll: int64(time.Millisecond)})
}

func TestReplicasConvergeAndDedupe(t *testing.T) {
	l := NewMemLog()
	r1, r2 := newMemReplica("r1", l), newMemReplica("r2", l)
	c1, c2 := NewCommitter(r1, 1), NewCommitter(r2, 1)
	a, err := c1.Do(bg, commitEntry("s1", pb("t", 0, 3), pb("t", 1, 2)))
	if err != nil || a.Err != core.OK {
		t.Fatalf("%+v %v", a, err)
	}
	b, _ := c2.Do(bg, commitEntry("s2", pb("t", 0, 4)))
	if got := b.Resp.(core.CommitSegmentResp).Assigned[0]; got.Base != 3 {
		t.Fatalf("second segment must start at 3: %+v", got)
	}
	again, _ := c2.Do(bg, commitEntry("s1", pb("t", 0, 3), pb("t", 1, 2)))
	if !slices.Equal(again.Resp.(core.CommitSegmentResp).Assigned, a.Resp.(core.CommitSegmentResp).Assigned) {
		t.Fatalf("replay differs: %+v vs %+v", again, a)
	}
	if conflict, _ := c1.Do(bg, commitEntry("s1", pb("t", 0, 9))); conflict.Err != core.ErrConflict {
		t.Fatalf("different payload: %+v", conflict)
	}
	if bad, _ := c1.Do(bg, commitEntry("")); bad.Err != core.ErrInvalid {
		t.Fatalf("invalid: %+v", bad)
	}
	if d, _ := c1.Do(bg, Entry{Kind: KindCreateTopic, Topic: "t9", Parts: 3}); d.Err != core.OK {
		t.Fatalf("create topic: %+v", d)
	}
	for _, r := range []*Replica{r1, r2} {
		if _, err := r.Read(bg, core.DescribeTopics{}); err != nil {
			t.Fatal(err)
		}
	}
	if r1.StateHash() != r2.StateHash() || r1.Applied() != r2.Applied() || r1.Applied() < 5 {
		t.Fatalf("replicas differ: %x/%d vs %x/%d", r1.StateHash(), r1.Applied(), r2.StateHash(), r2.Applied())
	}
	d, _ := r2.Read(bg, core.DescribeTopics{Topics: []string{"t9"}})
	if got := d.Resp.(core.DescribeTopicsResp).Topics; len(got) != 1 || got[0].Partitions != 3 {
		t.Fatalf("describe: %+v", got)
	}
}

// A writer that died after taking slot 0 leaves a hole under slot 1; a
// replica fills it with a no-op after HoleAfter, and the dead writer's late
// write loses.
func TestHoleFilled(t *testing.T) {
	l := NewMemLog()
	r := newMemReplica("r", l)
	late := commitEntry("late", pb("t", 0, 1))
	late.ID = "dead/1"
	if _, won, _ := l.Append(bg, 1, commitEntry("s1", pb("t", 0, 2))); !won {
		t.Fatal("setup")
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	d, err := r.Read(ctx, core.Lookup{TP: tpOf("t", 0)})
	if err != nil || d.Resp.(core.LookupResp).End != 2 || r.Stats().HoleFills != 1 {
		t.Fatalf("lookup %+v err %v fills %d", d, err, r.Stats().HoleFills)
	}
	if cur, won, _ := l.Append(bg, 0, late); won || cur.Kind != KindNoop {
		t.Fatalf("the dead writer's late write must lose to the no-op: %+v %v", cur, won)
	}
}

func TestLookupFreshFromAnotherReplica(t *testing.T) {
	l := NewMemLog()
	w, reader := newMemReplica("w", l), newMemReplica("r", l)
	c := NewCommitter(w, 1)
	if _, err := c.Do(bg, commitEntry("s1", pb("t", 0, 5))); err != nil {
		t.Fatal(err)
	}
	if got := reader.Local(core.Lookup{TP: tpOf("t", 0)}).Resp.(core.LookupResp).End; got != 0 {
		t.Fatalf("stale local read saw %d", got)
	}
	d, err := reader.Read(bg, core.Lookup{TP: tpOf("t", 0)})
	if err != nil || d.Resp.(core.LookupResp).End != 5 {
		t.Fatalf("fresh read: %+v %v", d, err)
	}
}

func TestDoHonoursContext(t *testing.T) {
	r := newMemReplica("r", NewMemLog())
	c := NewCommitter(r, 1)
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := c.Do(ctx, commitEntry("s", pb("t", 0, 1))); err == nil {
		t.Fatal("cancelled context accepted")
	}
	c.sem <- struct{}{} // K=1 appends already in flight
	if _, err := c.Do(ctx, commitEntry("s", pb("t", 0, 1))); err == nil {
		t.Fatal("cancelled context accepted while waiting for a window slot")
	}
}

// Real goroutines over the fake DynamoDB with latency, lost answers and lost
// writes: three replicas, K in flight each, a tailer each, shared keys.
func TestConcurrentFakeDB(t *testing.T) {
	db := fakedb.New()
	var n atomic.Int64
	db.Latency = func(string) time.Duration { return time.Duration(200+n.Add(1)%7*100) * time.Microsecond }
	db.AfterPut = func() error {
		if n.Add(1)%9 == 0 {
			return errors.New("lost answer")
		}
		return nil
	}
	db.Fail = func(call string) error {
		if call == "PutItem" && n.Add(1)%11 == 0 {
			return errors.New("lost write")
		}
		return nil
	}
	l := fakeLog(t, db)
	ctx, cancel := context.WithCancel(bg)
	var tail sync.WaitGroup
	var rs []*Replica
	for i := 0; i < 3; i++ {
		r := NewReplica(Config{Log: l, Writer: fmt.Sprint("r", i), HoleAfter: int64(30 * time.Millisecond), TailPoll: int64(time.Millisecond)})
		rs = append(rs, r)
		tail.Add(1)
		go func() { defer tail.Done(); r.Run(ctx) }()
	}
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i, r := range rs {
		c := NewCommitter(r, 4)
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 15; j++ {
					key := fmt.Sprintf("k%d", (i*7+w*3+j)%20)
					d, err := c.Do(bg, commitEntry(key, pb("t", int32(len(key)), 2), pb("u", 0, 1)))
					if err != nil || d.Err != core.OK {
						bad.Add(1)
					}
				}
			}()
		}
	}
	wg.Wait()
	cancel()
	tail.Wait()
	db.Latency, db.AfterPut, db.Fail = nil, nil, nil
	for _, r := range rs {
		if _, err := r.Read(bg, core.DescribeTopics{}); err != nil {
			t.Fatal(err)
		}
	}
	if bad.Load() != 0 || rs[0].StateHash() != rs[1].StateHash() || rs[1].StateHash() != rs[2].StateHash() {
		t.Fatalf("bad answers %d; hashes %x %x %x", bad.Load(), rs[0].StateHash(), rs[1].StateHash(), rs[2].StateHash())
	}
	end := rs[0].Local(core.Lookup{TP: tpOf("u", 0)}).Resp.(core.LookupResp).End
	if end == 0 || end > 20 { // 20 distinct keys, 1 record each in u/0
		t.Fatalf("u/0 end %d: idempotency broken", end)
	}
}

// noOld hides the existing item from a failed condition, like a backend that
// ignores ReturnValuesOnConditionCheckFailure.
type noOld struct{ DB }

func (n noOld) PutItem(ctx context.Context, in *dynamodb.PutItemInput, o ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	out, err := n.DB.PutItem(ctx, in, o...)
	var cc *types.ConditionalCheckFailedException
	if errors.As(err, &cc) {
		cc.Item = nil
	}
	return out, err
}

func TestDynLogFallbackQuery(t *testing.T) {
	db := fakedb.New()
	l := NewDynLog(noOld{fakeLog(t, db).db}, "log")
	if _, won, _ := l.Append(bg, 2, Entry{ID: "a", Kind: KindNoop}); !won {
		t.Fatal("setup")
	}
	if cur, won, err := l.Append(bg, 2, Entry{ID: "b"}); err != nil || won || cur.ID != "a" {
		t.Fatalf("fallback: %+v %v %v", cur, won, err)
	}
	db.Fail = func(call string) error {
		if call == "Query" {
			return errors.New("boom")
		}
		return nil
	}
	if _, _, err := l.Append(bg, 2, Entry{ID: "c"}); err == nil {
		t.Fatal("unreadable winner must be an error")
	}
}
