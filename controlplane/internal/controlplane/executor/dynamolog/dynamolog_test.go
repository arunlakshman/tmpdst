//go:build integration

package dynamolog_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/dynamolog"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/logtest"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/testinfra"
	"github.com/manifesterio/mds/shared/clock"
)

func TestMain(m *testing.M) { testinfra.Main(m) }

// newDB returns a client for DynamoDB Local and the name of a fresh log table.
func newDB(t *testing.T) (*dynamodb.Client, string) {
	t.Helper()
	info := testinfra.DynamoDB(t)
	return info.Client, info.Table
}

// failDB passes calls to Client, except those for which Fail returns an error:
// DynamoDB Local cannot be made to fail, so the failure is injected here.
type failDB struct {
	dynamolog.Client
	Fail func(call string) error
}

func (f *failDB) failing(call string) error {
	if f.Fail == nil {
		return nil
	}
	return f.Fail(call)
}

func (f *failDB) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if err := f.failing("PutItem"); err != nil {
		return nil, err
	}
	return f.Client.PutItem(ctx, in, opts...)
}

func (f *failDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := f.failing("GetItem"); err != nil {
		return nil, err
	}
	return f.Client.GetItem(ctx, in, opts...)
}

func (f *failDB) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if err := f.failing("UpdateItem"); err != nil {
		return nil, err
	}
	return f.Client.UpdateItem(ctx, in, opts...)
}

// count returns how many items the table holds.
func count(t *testing.T, db *dynamodb.Client, table string) int {
	t.Helper()
	out, err := db.Scan(t.Context(), &dynamodb.ScanInput{TableName: &table, Select: types.SelectCount, ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	return int(out.Count)
}

// recDB records the names of the DynamoDB calls it passes on, in order.
type recDB struct {
	dynamolog.Client
	calls []string
}

func (r *recDB) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	r.calls = append(r.calls, "PutItem")
	return r.Client.PutItem(ctx, in, opts...)
}

func (r *recDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	r.calls = append(r.calls, "GetItem")
	return r.Client.GetItem(ctx, in, opts...)
}

func (r *recDB) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	r.calls = append(r.calls, "UpdateItem")
	return r.Client.UpdateItem(ctx, in, opts...)
}

func (r *recDB) Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	r.calls = append(r.calls, "Query")
	return r.Client.Query(ctx, in, opts...)
}

func (r *recDB) BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	r.calls = append(r.calls, "BatchWriteItem")
	return r.Client.BatchWriteItem(ctx, in, opts...)
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

func TestClaimEpochError(t *testing.T) {
	real, table := newDB(t)
	boom := errors.New("boom")
	db := &failDB{Client: real, Fail: func(call string) error {
		if call == "UpdateItem" {
			return boom
		}
		return nil
	}}
	l := dynamolog.New(db, table, dynamolog.Options{})
	_, _, won, err := l.ClaimEpoch(t.Context(), 0, "cp-0")
	if !errors.Is(err, boom) || won {
		t.Fatalf("got won=%v err=%v, want boom and not won", won, err)
	}
}

func TestTrimRaisesFloorBeforeDelete(t *testing.T) {
	ctx := t.Context()
	db, table := newDB(t)
	rec := &recDB{Client: db}
	l := dynamolog.New(rec, table, dynamolog.Options{})
	for slot := range 3 {
		if _, _, err := l.Append(ctx, runtime.LogPosition(slot), []byte("x")); err != nil {
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
	if got := count(t, db, table); got != 2 {
		t.Fatalf("items after trim = %d, want 2 (row at slot 2 and the floor item)", got)
	}
	got, err := db.GetItem(ctx, &dynamodb.GetItemInput{TableName: &table, ConsistentRead: aws.Bool(true), Key: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "META"}, "sk": &types.AttributeValueMemberS{Value: "FLOOR"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := got.Item["floor"].(*types.AttributeValueMemberN).Value; got != "1" {
		t.Fatalf("floor = %s, want 1", got)
	}
}

func TestTrimKeepsRowsWhenFloorFails(t *testing.T) {
	ctx := t.Context()
	db, table := newDB(t)
	l := dynamolog.New(db, table, dynamolog.Options{})
	for slot := range 3 {
		if _, _, err := l.Append(ctx, runtime.LogPosition(slot), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("boom")
	rec := &recDB{Client: &failDB{Client: db, Fail: func(call string) error {
		if call == "UpdateItem" {
			return boom
		}
		return nil
	}}}
	l = dynamolog.New(rec, table, dynamolog.Options{})
	if err := l.TrimThrough(ctx, 1); !errors.Is(err, boom) {
		t.Fatalf("trim err = %v, want boom", err)
	}
	if indexOf(rec.calls, "BatchWriteItem") >= 0 {
		t.Fatalf("rows deleted although the floor was not raised: %v", rec.calls)
	}
	if got := count(t, db, table); got != 3 {
		t.Fatalf("rows = %d, want all 3 kept", got)
	}
}

func TestAppendFloorReadFails(t *testing.T) {
	real, table := newDB(t)
	boom := errors.New("boom")
	db := &failDB{Client: real, Fail: func(call string) error {
		if call == "GetItem" {
			return boom
		}
		return nil
	}}
	l := dynamolog.New(db, table, dynamolog.Options{})
	_, _, err := l.Append(t.Context(), 0, []byte("x"))
	if !errors.Is(err, boom) {
		t.Fatalf("append err = %v, want boom", err)
	}
	// The put landed before the floor read failed, so the answer is unknown.
	if code, out := executor.Classify(runtime.LogAppend{}, err); code != runtime.ErrIO || out != runtime.MaybeApplied {
		t.Fatalf("classify = %v %v, want ErrIO MaybeApplied", code, out)
	}
}

func TestReadFloorAfterQuery(t *testing.T) {
	ctx := t.Context()
	db, table := newDB(t)
	l := dynamolog.New(db, table, dynamolog.Options{})
	for slot := range 3 {
		if _, _, err := l.Append(ctx, runtime.LogPosition(slot), []byte("x")); err != nil {
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

	rec := &recDB{Client: db}
	l = dynamolog.New(rec, table, dynamolog.Options{})
	ents, err := l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 2 || ents[0].Slot != 1 || ents[1].Slot != 2 {
		t.Fatalf("read: %+v %v, want slots 1 and 2 only", ents, err)
	}
	if len(rec.calls) != 2 || rec.calls[0] != "Query" || rec.calls[1] != "GetItem" {
		t.Fatalf("calls %v, want exactly [Query GetItem]: the floor is read after the query", rec.calls)
	}
}

// The floor is read only after a put, on the success path and on the taken
// path. It is one extra consistent GetItem each time.
func TestAppendFloorReadOnlyAfterPut(t *testing.T) {
	ctx := t.Context()
	db, table := newDB(t)
	rec := &recDB{Client: db}
	l := dynamolog.New(rec, table, dynamolog.Options{})
	for slot := range 2 {
		if _, _, err := l.Append(ctx, runtime.LogPosition(slot), []byte("a")); err != nil {
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

func TestDynLog(t *testing.T) {
	ctx := t.Context()
	db, table := newDB(t)
	l := dynamolog.New(db, table, dynamolog.Options{TTL: time.Hour, Clock: clock.NewFake(time.Unix(1_700_000_000, 0))})
	if _, taken, err := l.Append(ctx, 0, []byte("x")); err != nil || taken {
		t.Fatalf("append: taken=%v err=%v", taken, err)
	}
	exist, taken, err := l.Append(ctx, 0, []byte("y"))
	if err != nil || !taken || string(exist) != "x" {
		t.Fatalf("taken: %q taken=%v err=%v", exist, taken, err)
	}
	ents, err := l.Read(ctx, 0, 5)
	if err != nil || len(ents) != 1 || string(ents[0].Data) != "x" {
		t.Fatalf("read: %+v %v", ents, err)
	}
	tail, err := l.CheckTail(ctx)
	if err != nil || tail != 1 {
		t.Fatalf("tail=%d err=%v", tail, err)
	}
	if err := l.TrimThrough(ctx, 0); err != nil {
		t.Fatal(err)
	}
	ents, err = l.Read(ctx, 0, 5)
	if err != nil || len(ents) != 0 {
		t.Fatalf("after trim: %+v %v", ents, err)
	}
	if err := l.TrimThrough(ctx, -1); err == nil {
		t.Fatal("expected invalid trim")
	}
}

func TestConformance(t *testing.T) {
	logtest.Run(t, func(t *testing.T) executor.Log {
		db, table := newDB(t)
		return dynamolog.New(db, table, dynamolog.Options{})
	})
}
