package executor_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/controlplane/fakedynamodb"
	"github.com/manifesterio/mds/internal/controlplane/runloop"
	"github.com/manifesterio/mds/internal/core"
)

// newDynFake returns a fake DynamoDB with the log table "cp".
func newDynFake(t *testing.T) *fakedynamodb.FakeDynamoDB {
	t.Helper()
	db := fakedynamodb.New()
	if _, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	return db
}

// logKinds are the Log implementations. Every semantic test runs on all of them.
var logKinds = []struct {
	name string
	open func(t *testing.T) executor.Log
}{
	{"memory", func(*testing.T) executor.Log { return executor.NewMemoryLog() }},
	{"dynamo", func(t *testing.T) executor.Log {
		return executor.NewDynamoLog(newDynFake(t), "cp", executor.DynamoLogOptions{})
	}},
	{"postgres", openPostgresLog}, // skips unless MDS_TEST_POSTGRES_DSN is set
}

// recDB records the names of the DynamoDB calls it passes on, in order.
type recDB struct {
	executor.DynamoClient
	calls []string
}

func (r *recDB) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	r.calls = append(r.calls, "PutItem")
	return r.DynamoClient.PutItem(ctx, in, opts...)
}

func (r *recDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	r.calls = append(r.calls, "GetItem")
	return r.DynamoClient.GetItem(ctx, in, opts...)
}

func (r *recDB) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	r.calls = append(r.calls, "UpdateItem")
	return r.DynamoClient.UpdateItem(ctx, in, opts...)
}

func (r *recDB) Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	r.calls = append(r.calls, "Query")
	return r.DynamoClient.Query(ctx, in, opts...)
}

func (r *recDB) BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	r.calls = append(r.calls, "BatchWriteItem")
	return r.DynamoClient.BatchWriteItem(ctx, in, opts...)
}

// indexOf returns the position of the first call named name, or -1.
func indexOf(calls []string, name string) int {
	for i, c := range calls {
		if c == name {
			return i
		}
	}
	return -1
}

func TestClaimEpoch(t *testing.T) {
	type step struct {
		expect uint64
		owner  string
		epoch  uint64
		stored string
		won    bool
	}
	steps := []step{
		{expect: 0, owner: "cp-0", epoch: 1, stored: "cp-0", won: true},  // first claim
		{expect: 1, owner: "cp-1", epoch: 2, stored: "cp-1", won: true},  // correct expect
		{expect: 1, owner: "cp-2", epoch: 2, stored: "cp-1", won: false}, // stale expect loses
		{expect: 0, owner: "cp-3", epoch: 2, stored: "cp-1", won: false}, // "first start" loses now
		{expect: 9, owner: "cp-4", epoch: 2, stored: "cp-1", won: false}, // expect ahead loses
		{expect: 2, owner: "cp-5", epoch: 3, stored: "cp-5", won: true},  // winner keeps going
	}
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			l := kind.open(t)
			for i, s := range steps {
				epoch, owner, won, err := l.ClaimEpoch(t.Context(), s.expect, s.owner)
				if err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
				if epoch != s.epoch || owner != s.stored || won != s.won {
					t.Fatalf("step %d: got (%d, %q, %v), want (%d, %q, %v)",
						i, epoch, owner, won, s.epoch, s.stored, s.won)
				}
			}
		})
	}
}

func TestClaimEpochMissingItemLoses(t *testing.T) {
	// No epoch is stored yet. A claim that expects 3 must lose and see epoch 0.
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			epoch, owner, won, err := kind.open(t).ClaimEpoch(t.Context(), 3, "cp-0")
			if err != nil || won || epoch != 0 || owner != "" {
				t.Fatalf("got (%d, %q, %v, %v), want (0, \"\", false, nil)", epoch, owner, won, err)
			}
		})
	}
}

func TestClaimEpochRace(t *testing.T) {
	const writers = 20
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			l := kind.open(t)
			type result struct {
				epoch uint64
				owner string
				won   bool
				err   error
			}
			results := make([]result, writers)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range writers {
				wg.Go(func() {
					<-start
					epoch, owner, won, err := l.ClaimEpoch(t.Context(), 0, fmt.Sprintf("cp-%d", i))
					results[i] = result{epoch, owner, won, err}
				})
			}
			close(start)
			wg.Wait()

			var winner string
			winners := 0
			for i, r := range results {
				if r.err != nil {
					t.Fatalf("writer %d: %v", i, r.err)
				}
				if r.won {
					winners++
					winner = fmt.Sprintf("cp-%d", i)
				}
			}
			if winners != 1 {
				t.Fatalf("winners = %d, want exactly 1", winners)
			}
			for i, r := range results {
				if r.won {
					continue
				}
				if r.epoch != 1 || r.owner != winner {
					t.Fatalf("loser %d saw (%d, %q), want (1, %q)", i, r.epoch, r.owner, winner)
				}
			}
		})
	}
}

func TestClaimEpochError(t *testing.T) {
	db := newDynFake(t)
	boom := errors.New("boom")
	db.Fail = func(call string) error {
		if call == "UpdateItem" {
			return boom
		}
		return nil
	}
	l := executor.NewDynamoLog(db, "cp", executor.DynamoLogOptions{})
	_, _, won, err := l.ClaimEpoch(t.Context(), 0, "cp-0")
	if !errors.Is(err, boom) || won {
		t.Fatalf("got won=%v err=%v, want boom and not won", won, err)
	}
}

func TestAppendAfterTrim(t *testing.T) {
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := t.Context()
			l := kind.open(t)
			for slot, data := range []string{"a", "b", "c"} {
				if _, taken, err := l.Append(ctx, core.LogPosition(slot), []byte(data)); err != nil || taken {
					t.Fatalf("append %d: taken=%v err=%v", slot, taken, err)
				}
			}
			if err := l.TrimThrough(ctx, 1); err != nil {
				t.Fatal(err)
			}

			// Slots 0 and 1 are at or below the floor, so the puts are not commits.
			// The puts land (they were free), so these are stray rows.
			for _, slot := range []core.LogPosition{0, 1} {
				existing, taken, err := l.Append(ctx, slot, []byte("stale"))
				if err != nil || !taken || existing != nil {
					t.Fatalf("below floor slot %d: existing=%q taken=%v err=%v, want nil, true, nil",
						slot, existing, taken, err)
				}
			}
			// Slot 2 is above the floor and holds a live entry.
			existing, taken, err := l.Append(ctx, 2, []byte("z"))
			if err != nil || !taken || string(existing) != "c" {
				t.Fatalf("live slot 2: existing=%q taken=%v err=%v", existing, taken, err)
			}
			// Slot 3 is above the floor and free: a normal commit.
			if existing, taken, err := l.Append(ctx, 3, []byte("d")); err != nil || taken || existing != nil {
				t.Fatalf("free slot 3: existing=%q taken=%v err=%v", existing, taken, err)
			}
			// Read never returns the stray rows at 0 and 1.
			ents, err := l.Read(ctx, 0, 10)
			if err != nil || len(ents) != 2 || ents[0].Slot != 2 || string(ents[0].Data) != "c" ||
				ents[1].Slot != 3 || string(ents[1].Data) != "d" {
				t.Fatalf("read after trim: %+v %v, want slots 2 and 3", ents, err)
			}
		})
	}
}

func TestFloorNeverMovesDown(t *testing.T) {
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := t.Context()
			l := kind.open(t)
			if err := l.TrimThrough(ctx, 5); err != nil {
				t.Fatal(err)
			}
			// A lower trim must succeed and must not lower the floor.
			if err := l.TrimThrough(ctx, 3); err != nil {
				t.Fatalf("lower trim: %v", err)
			}
			if _, taken, err := l.Append(ctx, 4, []byte("x")); err != nil || !taken {
				t.Fatalf("slot 4 under floor 5: taken=%v err=%v, want taken", taken, err)
			}
			if _, taken, err := l.Append(ctx, 6, []byte("y")); err != nil || taken {
				t.Fatalf("slot 6 above floor 5: taken=%v err=%v, want free", taken, err)
			}
		})
	}
}

func TestTrimRaisesFloorBeforeDelete(t *testing.T) {
	ctx := t.Context()
	db := newDynFake(t)
	rec := &recDB{DynamoClient: db}
	l := executor.NewDynamoLog(rec, "cp", executor.DynamoLogOptions{})
	for slot := range 3 {
		if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	rec.calls = nil
	if err := l.TrimThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	up, del := indexOf(rec.calls, "UpdateItem"), indexOf(rec.calls, "BatchWriteItem")
	if up < 0 || del < 0 || up > del {
		t.Fatalf("call order %v: want UpdateItem (floor) before BatchWriteItem (delete)", rec.calls)
	}
	// Two items: the live row at slot 2 and the floor item.
	if got := db.Count("cp"); got != 2 {
		t.Fatalf("items after trim = %d, want 2 (row at slot 2 and the floor item)", got)
	}
	floor := db.Item("cp", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "META"}, "sk": &types.AttributeValueMemberS{Value: "FLOOR"},
	})
	if got := floor["floor"].(*types.AttributeValueMemberN).Value; got != "1" {
		t.Fatalf("floor = %s, want 1", got)
	}
}

func TestTrimKeepsRowsWhenFloorFails(t *testing.T) {
	ctx := t.Context()
	db := newDynFake(t)
	l := executor.NewDynamoLog(db, "cp", executor.DynamoLogOptions{})
	for slot := range 3 {
		if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("boom")
	rec := &recDB{DynamoClient: db}
	db.Fail = func(call string) error {
		if call == "UpdateItem" {
			return boom
		}
		return nil
	}
	l = executor.NewDynamoLog(rec, "cp", executor.DynamoLogOptions{})
	if err := l.TrimThrough(ctx, 1); !errors.Is(err, boom) {
		t.Fatalf("trim err = %v, want boom", err)
	}
	if indexOf(rec.calls, "BatchWriteItem") >= 0 {
		t.Fatalf("rows deleted although the floor was not raised: %v", rec.calls)
	}
	if got := db.Count("cp"); got != 3 {
		t.Fatalf("rows = %d, want all 3 kept", got)
	}
}

func TestAppendFloorReadFails(t *testing.T) {
	db := newDynFake(t)
	boom := errors.New("boom")
	db.Fail = func(call string) error {
		if call == "GetItem" {
			return boom
		}
		return nil
	}
	l := executor.NewDynamoLog(db, "cp", executor.DynamoLogOptions{})
	_, _, err := l.Append(t.Context(), 0, []byte("x"))
	if !errors.Is(err, boom) {
		t.Fatalf("append err = %v, want boom", err)
	}
	// The put landed before the floor read failed, so the answer is unknown.
	if code, out := executor.Classify(core.LogAppend{}, err); code != core.ErrIO || out != core.MaybeApplied {
		t.Fatalf("classify = %v %v, want ErrIO MaybeApplied", code, out)
	}
}

func TestReadFloorAfterQuery(t *testing.T) {
	ctx := t.Context()
	db := newDynFake(t)
	l := executor.NewDynamoLog(db, "cp", executor.DynamoLogOptions{})
	for slot := range 3 {
		if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	// No floor item yet: every row is returned.
	if ents, err := l.Read(ctx, 0, 10); err != nil || len(ents) != 3 {
		t.Fatalf("before any trim: %d rows, err %v; want 3", len(ents), err)
	}

	if err := l.TrimThrough(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// A stray row below the floor, left by a rejected put.
	if _, taken, err := l.Append(ctx, 0, []byte("stray")); err != nil || !taken {
		t.Fatalf("stray put: taken=%v err=%v", taken, err)
	}

	rec := &recDB{DynamoClient: db}
	l = executor.NewDynamoLog(rec, "cp", executor.DynamoLogOptions{})
	ents, err := l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 2 || ents[0].Slot != 1 || ents[1].Slot != 2 {
		t.Fatalf("read: %+v %v, want slots 1 and 2 only", ents, err)
	}
	if len(rec.calls) != 2 || rec.calls[0] != "Query" || rec.calls[1] != "GetItem" {
		t.Fatalf("calls %v, want exactly [Query GetItem]: the floor is read after the query", rec.calls)
	}
}

func TestReadNeverReturnsStrayRowsAtFloor(t *testing.T) {
	// The floor is raised to 5 and every row is trimmed. Then stray rows
	// land at 2 and 5. Rows above the floor are returned, and no others.
	ctx := t.Context()
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			l := kind.open(t)
			if err := l.TrimThrough(ctx, 5); err != nil {
				t.Fatal(err)
			}
			for _, slot := range []core.LogPosition{2, 5} {
				if _, taken, err := l.Append(ctx, slot, []byte("stray")); err != nil || !taken {
					t.Fatalf("stray at %d: taken=%v err=%v", slot, taken, err)
				}
			}
			if _, taken, err := l.Append(ctx, 6, []byte("live")); err != nil || taken {
				t.Fatalf("live at 6: taken=%v err=%v", taken, err)
			}
			ents, err := l.Read(ctx, 0, 10)
			if err != nil || len(ents) != 1 || ents[0].Slot != 6 || string(ents[0].Data) != "live" {
				t.Fatalf("read: %+v %v, want only slot 6", ents, err)
			}
		})
	}
}

func TestClassifyLogActions(t *testing.T) {
	cases := []struct {
		name string
		a    core.IOCall
		err  error
		code core.ErrCode
		out  core.Outcome
	}{
		{"claim timeout", core.LogClaimEpoch{}, context.DeadlineExceeded, core.ErrTimeout, core.MaybeApplied},
		{"claim io", core.LogClaimEpoch{}, errors.New("boom"), core.ErrIO, core.MaybeApplied},
		{"read timeout", core.LogRead{}, context.DeadlineExceeded, core.ErrTimeout, core.NotApplied},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, out := executor.Classify(c.a, c.err)
			if code != c.code || out != c.out {
				t.Fatalf("got %v %v, want %v %v", code, out, c.code, c.out)
			}
		})
	}
}

// slowClaimLog is a Log whose ClaimEpoch waits until the call context ends.
type slowClaimLog struct {
	executor.Log
}

func (slowClaimLog) ClaimEpoch(ctx context.Context, _ uint64, _ string) (uint64, string, bool, error) {
	<-ctx.Done()
	return 0, "", false, ctx.Err()
}

// nextResult waits for one completion on the inbox.
func nextResult(t *testing.T, in *runloop.Inbox) core.IOResult {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b, ok := in.Control().Pop(); ok {
			if r, ok := b.(core.IOResult); ok {
				return r
			}
			continue
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no completion within 2s")
	return nil
}

func TestExecutorAppendBelowFloorIsConflict(t *testing.T) {
	ctx := t.Context()
	l := executor.NewMemoryLog()
	for slot := range 2 {
		if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.TrimThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	in := runloop.NewInbox(8)
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second}, l, executor.NewMemoryObjects(), in)

	ex.Execute(core.LogAppend{Req: 1, Slot: 0, Data: []byte("late")})
	got, ok := nextResult(t, in).(core.LogAppendDone)
	if !ok || !got.Taken || got.Existing != nil || got.Err != core.ErrConflict || got.Outcome != core.NotApplied {
		t.Fatalf("below floor: got %+v, want taken, no existing, ErrConflict, NotApplied", got)
	}
}

func TestExecutorAppendLiveTakenKeepsExisting(t *testing.T) {
	ctx := t.Context()
	l := executor.NewMemoryLog()
	if _, _, err := l.Append(ctx, 4, []byte("live")); err != nil {
		t.Fatal(err)
	}
	in := runloop.NewInbox(8)
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second}, l, executor.NewMemoryObjects(), in)

	ex.Execute(core.LogAppend{Req: 2, Slot: 4, Data: []byte("other")})
	got, ok := nextResult(t, in).(core.LogAppendDone)
	if !ok || !got.Taken || string(got.Existing) != "live" || got.Err != core.OK || got.Outcome != core.Applied {
		t.Fatalf("live taken: %+v", got)
	}
}

func TestExecutorClaimEpoch(t *testing.T) {
	ctx := t.Context()
	in := runloop.NewInbox(8)
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second}, executor.NewMemoryLog(), executor.NewMemoryObjects(), in)

	ex.Execute(core.LogClaimEpoch{Req: 3, Expect: 0, Owner: "cp-0"})
	want := core.LogClaimEpochDone{Req: 3, Epoch: 1, Owner: "cp-0", Won: true, Err: core.OK, Outcome: core.Applied}
	if got := nextResult(t, in); got != want {
		t.Fatalf("claim: got %+v, want %+v", got, want)
	}

	ex.Execute(core.LogClaimEpoch{Req: 4, Expect: 0, Owner: "cp-1"})
	want = core.LogClaimEpochDone{Req: 4, Epoch: 1, Owner: "cp-0", Won: false, Err: core.OK, Outcome: core.Applied}
	if got := nextResult(t, in); got != want {
		t.Fatalf("lost claim: got %+v, want %+v", got, want)
	}
}

func TestExecutorClaimEpochTimeoutIsMaybeApplied(t *testing.T) {
	ctx := t.Context()
	in := runloop.NewInbox(8)
	log := slowClaimLog{Log: executor.NewMemoryLog()}
	ex := executor.New(ctx, executor.Config{LogTimeout: 20 * time.Millisecond}, log, executor.NewMemoryObjects(), in)

	ex.Execute(core.LogClaimEpoch{Req: 5, Expect: 0, Owner: "cp-0"})
	want := core.LogClaimEpochDone{Req: 5, Err: core.ErrTimeout, Outcome: core.MaybeApplied}
	if got := nextResult(t, in); got != want {
		t.Fatalf("timed-out claim: got %+v, want %+v", got, want)
	}
}

// A put that is refused because the slot is taken, and the slot is at or below
// the floor, is not a commit. The stray bytes must not come back as the
// existing entry. Otherwise a writer that retries the same slot would take its
// own stray row for "my entry landed".
func TestAppendRetryBelowFloorIsNotStray(t *testing.T) {
	ctx := t.Context()
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			l := kind.open(t)
			for slot := range 2 {
				if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("a")); err != nil {
					t.Fatal(err)
				}
			}
			if err := l.TrimThrough(ctx, 1); err != nil {
				t.Fatal(err)
			}
			// The first put lands in a trimmed slot below the floor: a stray row.
			if _, taken, err := l.Append(ctx, 0, []byte("mine")); err != nil || !taken {
				t.Fatalf("stray put: taken=%v err=%v", taken, err)
			}
			// The answer was lost, so the writer retries the same slot. The row
			// now holds "mine", but the slot is below the floor.
			existing, taken, err := l.Append(ctx, 0, []byte("mine"))
			if err != nil || !taken || existing != nil {
				t.Fatalf("retry below floor: existing=%q taken=%v err=%v, want nil, true, nil", existing, taken, err)
			}
		})
	}
}

// An occupied slot above the floor still returns the existing bytes.
func TestAppendLiveSlotAboveFloorReturnsExisting(t *testing.T) {
	ctx := t.Context()
	for _, kind := range logKinds {
		t.Run(kind.name, func(t *testing.T) {
			l := kind.open(t)
			if _, _, err := l.Append(ctx, 5, []byte("live")); err != nil {
				t.Fatal(err)
			}
			if err := l.TrimThrough(ctx, 2); err != nil {
				t.Fatal(err)
			}
			existing, taken, err := l.Append(ctx, 5, []byte("other"))
			if err != nil || !taken || string(existing) != "live" {
				t.Fatalf("live slot 5: existing=%q taken=%v err=%v, want live", existing, taken, err)
			}
		})
	}
}

// The floor is read only after a put, on the success path and on the taken
// path. It is one extra consistent GetItem each time.
func TestAppendFloorReadOnlyAfterPut(t *testing.T) {
	ctx := t.Context()
	db := newDynFake(t)
	rec := &recDB{DynamoClient: db}
	l := executor.NewDynamoLog(rec, "cp", executor.DynamoLogOptions{})
	for slot := range 2 {
		if _, _, err := l.Append(ctx, core.LogPosition(slot), []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.TrimThrough(ctx, 0); err != nil {
		t.Fatal(err)
	}

	rec.calls = nil
	if _, taken, err := l.Append(ctx, 2, []byte("d")); err != nil || taken {
		t.Fatalf("success put: taken=%v err=%v", taken, err)
	}
	if want := []string{"PutItem", "GetItem"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("success path calls %v, want %v", rec.calls, want)
	}

	rec.calls = nil
	existing, taken, err := l.Append(ctx, 2, []byte("x"))
	if err != nil || !taken || string(existing) != "d" {
		t.Fatalf("taken put: existing=%q taken=%v err=%v", existing, taken, err)
	}
	if want := []string{"PutItem", "GetItem"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("taken path calls %v, want %v", rec.calls, want)
	}
}
