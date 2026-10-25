package logcp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp/fakedb"
	"manifester/cp/logsm"
)

var bg = context.Background()

func newFake(t testing.TB) *fakedb.Fake {
	t.Helper()
	db := fakedb.New()
	if _, err := db.CreateTable(bg, &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	return db
}

func entryOf(id string, keys ...string) Entry {
	e := Entry{Writer: "w", ID: id, TS: 5}
	for _, k := range keys {
		e.Cmds = append(e.Cmds, commitCmd(k, tpOf("t", 0)))
	}
	return e
}

// logs runs a test against both Log implementations.
func logs(t *testing.T, f func(t *testing.T, l Log)) {
	t.Run("mem", func(t *testing.T) { f(t, NewMemLog()) })
	t.Run("dynamo", func(t *testing.T) { f(t, NewDynLog(newFake(t), "cp", DynLogOptions{})) })
}

func TestLogAppendWinnerAndLoser(t *testing.T) {
	logs(t, func(t *testing.T, l Log) {
		cur, won, err := l.Append(bg, 3, entryOf("a", "k1"))
		if err != nil || !won || cur.ID != "a" {
			t.Fatalf("first: %v %v %+v", err, won, cur)
		}
		cur, won, err = l.Append(bg, 3, entryOf("b", "k2"))
		if err != nil || won || cur.ID != "a" || len(cur.Cmds) != 1 || cur.Cmds[0].Commit.SegmentKey != "k1" {
			t.Fatalf("loser must learn the winner: %v %v %+v", err, won, cur)
		}
		if tail, err := l.CheckTail(bg); err != nil || tail != 4 {
			t.Fatalf("tail %d %v", tail, err)
		}
		if _, _, err := l.Append(bg, 0, Entry{Writer: "w", ID: "x", Cmds: []logsm.Entry{commitCmd(strings.Repeat("k", MaxEntryBytes), tpOf("t", 0))}}); !errors.Is(err, ErrEntryTooLarge) {
			t.Fatalf("size guard: %v", err)
		}
	})
}

func TestLogReadSkipsHolesAndPages(t *testing.T) {
	logs(t, func(t *testing.T, l Log) {
		if tail, err := l.CheckTail(bg); err != nil || tail != 0 {
			t.Fatalf("empty tail %d %v", tail, err)
		}
		for _, n := range []int64{0, 1, 2, 5, 6, 9} {
			if _, won, err := l.Append(bg, n, entryOf("e"+string(rune('a'+n)))); err != nil || !won {
				t.Fatal(err)
			}
		}
		got, err := l.Read(bg, 2, 4)
		if err != nil || len(got) != 4 || got[0].N != 2 || got[1].N != 5 || got[3].N != 9 {
			t.Fatalf("%v %+v", err, got)
		}
		if got, _ := l.Read(bg, 7, 10); len(got) != 1 || got[0].N != 9 {
			t.Fatalf("from 7: %+v", got)
		}
		if got, _ := l.Read(bg, 100, 10); len(got) != 0 {
			t.Fatalf("past tail: %+v", got)
		}
	})
}

func TestDynLogPagesThroughQueryLimit(t *testing.T) {
	db := newFake(t)
	db.PageSize = 2
	l := NewDynLog(db, "cp", DynLogOptions{})
	for n := range int64(7) {
		_, _, _ = l.Append(bg, n, entryOf("e"))
	}
	got, err := l.Read(bg, 0, 100)
	if err != nil || len(got) != 7 {
		t.Fatalf("%v %d", err, len(got))
	}
}

// noOld hides the old item of a failed condition, like a server without
// ReturnValuesOnConditionCheckFailure.
type noOld struct{ DB }

func (n noOld) PutItem(ctx context.Context, in *dynamodb.PutItemInput, o ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	out, err := n.DB.PutItem(ctx, in, o...)
	if cc, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
		cc.Item = nil
	}
	return out, err
}

func TestDynLogLoserFallsBackToARead(t *testing.T) {
	l := NewDynLog(noOld{newFake(t)}, "cp", DynLogOptions{})
	_, _, _ = l.Append(bg, 1, entryOf("a"))
	cur, won, err := l.Append(bg, 1, entryOf("b"))
	if err != nil || won || cur.ID != "a" {
		t.Fatalf("%v %v %+v", err, won, cur)
	}
}

func TestDynLogErrorsAndTTL(t *testing.T) {
	db := newFake(t)
	now := time.Unix(1_000_000, 0)
	l := NewDynLog(db, "cp", DynLogOptions{TTL: time.Hour, Now: func() time.Time { return now }})
	if _, _, err := l.Append(bg, 0, entryOf("a", "k")); err != nil {
		t.Fatal(err)
	}
	it := db.Item("cp", map[string]types.AttributeValue{"pk": sv("LOG"), "sk": sv(slotKey(0))})
	if numAttr(it, "ttl") != 1_000_000+3600 || numAttr(it, "v") != Version || strAttr(it, "w") != "w" || numAttr(it, "n") != 1 {
		t.Fatalf("attributes ttl=%d v=%d w=%q n=%d", numAttr(it, "ttl"), numAttr(it, "v"), strAttr(it, "w"), numAttr(it, "n"))
	}
	db.Fail = func(string) error { return errors.New("down") }
	if _, _, err := l.Append(bg, 1, entryOf("b")); err == nil {
		t.Fatal("append error swallowed")
	}
	if _, err := l.Read(bg, 0, 1); err == nil {
		t.Fatal("read error swallowed")
	}
	if _, err := l.CheckTail(bg); err == nil {
		t.Fatal("tail error swallowed")
	}
	db.Fail = nil
	// a taken slot whose item has no payload is corrupt, not a hole
	db.Put("cp", map[string]types.AttributeValue{"pk": sv("LOG"), "sk": sv(slotKey(2))})
	if _, _, err := l.Append(bg, 2, entryOf("c")); !errors.Is(err, errCorrupt) {
		t.Fatalf("corrupt loser: %v", err)
	}
	if got, err := l.Read(bg, 0, 10); !errors.Is(err, errCorrupt) || len(got) != 1 {
		t.Fatalf("read through corruption: %v %d", err, len(got))
	}
}

func TestDynLogUnreadableLoser(t *testing.T) {
	db := newFake(t)
	l := NewDynLog(noOld{db}, "cp", DynLogOptions{})
	db.Put("cp", map[string]types.AttributeValue{"pk": sv("LOG"), "sk": sv(slotKey(4)), "d": &types.AttributeValueMemberB{Value: []byte{1, 9}}})
	if _, _, err := l.Append(bg, 4, entryOf("x")); err == nil {
		t.Fatal("undecodable winner accepted")
	}
}

func TestTrimLeavesTombstones(t *testing.T) {
	logs(t, func(t *testing.T, l Log) {
		for n := range int64(600) {
			_, _, _ = l.Append(bg, n, entryOf("e", "k"))
		}
		tr := l.(Trimmer)
		if n, err := tr.TrimBelow(bg, 0); err != nil || n != 0 {
			t.Fatalf("trim below 0: %d %v", n, err)
		}
		if n, err := tr.TrimBelow(bg, 520); err != nil || n != 520 {
			t.Fatalf("trim: %d %v", n, err)
		}
		if n, err := tr.TrimBelow(bg, 520); err != nil || n != 0 {
			t.Fatalf("trimming again must find nothing to do: %d %v", n, err)
		}
		got, err := l.Read(bg, 0, 1000)
		if !errors.Is(err, ErrTrimmed) || len(got) != 0 {
			t.Fatalf("reading from a trimmed slot: %d slots, %v", len(got), err)
		}
		got, err = l.Read(bg, 520, 1000)
		if err != nil || len(got) != 80 || got[0].N != 520 {
			t.Fatalf("left %d, %v", len(got), err)
		}
		// nobody can write into the trimmed region: the slot is taken, by a tombstone
		if _, won, err := l.Append(bg, 100, entryOf("late", "k")); won || !errors.Is(err, ErrTrimmed) {
			t.Fatalf("append into a trimmed slot: won=%v err=%v", won, err)
		}
		if tail, _ := l.CheckTail(bg); tail != 600 {
			t.Fatalf("tail %d", tail)
		}
		if n, err := tr.TrimBelow(bg, 600); err != nil || n != 80 {
			t.Fatalf("trim to the tail: %d %v", n, err)
		}
		if tail, _ := l.CheckTail(bg); tail != 600 {
			t.Fatalf("tail after trimming everything %d", tail)
		}
	})
}

func TestStubTTL(t *testing.T) {
	db := newFake(t)
	now := time.Unix(2_000_000, 0)
	l := NewDynLog(db, "cp", DynLogOptions{TTL: time.Hour, Now: func() time.Time { return now }})
	_, _, _ = l.Append(bg, 0, entryOf("a", "k"))
	if n, err := l.TrimBelow(bg, 1); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	it := db.Item("cp", map[string]types.AttributeValue{"pk": sv("LOG"), "sk": sv(slotKey(0))})
	if numAttr(it, "ttl") != 2_000_000+30*24*3600 || numAttr(it, "tomb") != 1 {
		t.Fatalf("stub attributes ttl=%d tomb=%d", numAttr(it, "ttl"), numAttr(it, "tomb"))
	}
	db.Fail = func(c string) error {
		if c == "GetItem" {
			return errors.New("down")
		}
		return nil
	}
	if _, err := l.TrimBelow(bg, 5); err == nil {
		t.Fatal("pointer read error swallowed")
	}
}

func TestBatchWriteRetries(t *testing.T) {
	db := newFake(t)
	put := func(sk string) types.WriteRequest {
		return types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": sv("p"), "sk": sv(sk)}}}
	}
	var unproc, throttled int
	var slept []time.Duration
	h := &writeHooks{
		sleep:       func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil },
		unprocessed: func(n int) { unproc += n }, throttled: func() { throttled++ },
	}
	db.UnprocessWrite = 2
	if err := writeAll(bg, db, "cp", []types.WriteRequest{put("a"), put("b"), put("c")}, h); err != nil || db.Count("cp") != 3 {
		t.Fatalf("%v %d", err, db.Count("cp"))
	}
	if unproc == 0 || len(slept) < 2 || slept[1] <= slept[0] {
		t.Fatalf("unprocessed %d, backoff %v", unproc, slept)
	}
	calls := 0
	db.Fail = func(string) error {
		calls++
		if calls < 3 {
			return &types.ThrottlingException{}
		}
		return nil
	}
	if err := writeAll(bg, db, "cp", []types.WriteRequest{put("d")}, h); err != nil || throttled != 2 {
		t.Fatalf("%v throttled %d", err, throttled)
	}
	db.Fail = func(string) error { return &types.ThrottlingException{} }
	if err := writeAll(bg, db, "cp", []types.WriteRequest{put("e")}, h); err == nil {
		t.Fatal("endless throttling must give up")
	}
	db.Fail = func(string) error { return errors.New("boom") }
	if err := writeAll(bg, db, "cp", []types.WriteRequest{put("f")}, nil); err == nil {
		t.Fatal("hard error swallowed")
	}
	// the real timer pause honours cancellation
	ctx, cancel := context.WithCancel(bg)
	cancel()
	db.Fail = func(string) error { return &types.ThrottlingException{} }
	if err := writeAll(ctx, db, "cp", []types.WriteRequest{put("g")}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	_ = core.OK
}
