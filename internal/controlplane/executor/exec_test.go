package executor_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/internal/controlplane/executor"
	"manifester/internal/controlplane/fakedynamodb"
	"manifester/internal/controlplane/runloop"
	"manifester/internal/core"
)

func TestMemLogRoundTrip(t *testing.T) {
	ctx := t.Context()
	l := executor.NewMemoryLog()
	if _, taken, err := l.Append(ctx, 0, []byte("a")); err != nil || taken {
		t.Fatalf("append0: taken=%v err=%v", taken, err)
	}
	exist, taken, err := l.Append(ctx, 0, []byte("b"))
	if err != nil || !taken || string(exist) != "a" {
		t.Fatalf("taken: %q taken=%v err=%v", exist, taken, err)
	}
	if _, _, err := l.Append(ctx, -1, nil); !errors.Is(err, executor.ErrInvalidSlot) {
		t.Fatalf("neg slot: %v", err)
	}
	ents, err := l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 1 || string(ents[0].Data) != "a" {
		t.Fatalf("read: %+v %v", ents, err)
	}
	if _, err := l.Read(ctx, -1, 1); !errors.Is(err, executor.ErrInvalidSlot) {
		t.Fatalf("bad read: %v", err)
	}
	tail, err := l.CheckTail(ctx)
	if err != nil || tail != 1 {
		t.Fatalf("tail=%d err=%v", tail, err)
	}
	if err := l.TrimThrough(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := l.TrimThrough(ctx, -1); !errors.Is(err, executor.ErrInvalidSlot) {
		t.Fatalf("bad trim: %v", err)
	}
	ents, err = l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 0 {
		t.Fatalf("after trim: %+v %v", ents, err)
	}
}

func TestMemObjects(t *testing.T) {
	ctx := t.Context()
	o := executor.NewMemoryObjects()
	if err := o.Put(ctx, "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := o.Get(ctx, core.SegmentRef{Key: "k", Off: 1, Len: 3})
	if err != nil || string(got) != "ell" {
		t.Fatalf("get slice: %q %v", got, err)
	}
	got, err = o.Get(ctx, core.SegmentRef{Key: "k", Off: 2, Len: 0})
	if err != nil || string(got) != "llo" {
		t.Fatalf("get rest: %q %v", got, err)
	}
	if _, err := o.Get(ctx, core.SegmentRef{Key: "missing"}); !errors.Is(err, executor.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := o.Get(ctx, core.SegmentRef{Key: "k", Off: 10, Len: 1}); !errors.Is(err, executor.ErrNotFound) {
		t.Fatalf("oob: %v", err)
	}
	keys, err := o.List(ctx, "k")
	if err != nil || len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("list: %v %v", keys, err)
	}
	if err := o.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	keys, err = o.List(ctx, "")
	if err != nil || len(keys) != 0 {
		t.Fatalf("empty list: %v %v", keys, err)
	}
}

func TestClassify(t *testing.T) {
	code, out := executor.Classify(core.LogAppend{}, nil)
	if code != core.OK || out != core.Applied {
		t.Fatalf("nil: %v %v", code, out)
	}
	code, out = executor.Classify(core.LogAppend{}, context.Canceled)
	if code != core.ErrTimeout || out != core.MaybeApplied {
		t.Fatalf("cancel append: %v %v", code, out)
	}
	code, out = executor.Classify(core.LogRead{}, context.DeadlineExceeded)
	if code != core.ErrTimeout || out != core.NotApplied {
		t.Fatalf("deadline read: %v %v", code, out)
	}
	code, out = executor.Classify(core.ObjectGet{}, executor.ErrNotFound)
	if code != core.ErrNotFound || out != core.NotApplied {
		t.Fatalf("not found: %v %v", code, out)
	}
	code, out = executor.Classify(core.LogAppend{}, executor.ErrInvalidSlot)
	if code != core.ErrInvalid || out != core.NotApplied {
		t.Fatalf("invalid: %v %v", code, out)
	}
	code, out = executor.Classify(core.ObjectPut{}, errors.New("boom"))
	if code != core.ErrIO || out != core.MaybeApplied {
		t.Fatalf("io: %v %v", code, out)
	}
	var ne net.Error = timeoutError{}
	code, out = executor.Classify(core.LogDeleteOld{}, ne)
	if code != core.ErrTimeout || out != core.MaybeApplied {
		t.Fatalf("net timeout: %v %v", code, out)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestDynLog(t *testing.T) {
	ctx := t.Context()
	db := fakedynamodb.New()
	if _, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	l := executor.NewDynamoLog(db, "cp", executor.DynamoLogOptions{TTL: time.Hour, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }})
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

func TestExecutorIOResults(t *testing.T) {
	ctx := t.Context()
	in := runloop.NewInbox(8)
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second, ObjTimeout: time.Second}, executor.NewMemoryLog(), executor.NewMemoryObjects(), in)
	ex.Execute(core.LogAppend{Req: 1, Slot: 0, Data: []byte("a")})
	ex.Execute(core.ObjectPut{Req: 2, Key: "k", Data: []byte("v")})
	ex.Execute(core.LogGetEnd{Req: 3})
	ex.Execute(core.LogRead{Req: 4, From: 0, Limit: 1})
	ex.Execute(core.ObjectGet{Req: 5, Ref: core.SegmentRef{Key: "k", Len: 1}})
	ex.Execute(core.ObjectList{Req: 6, Prefix: "k"})
	ex.Execute(core.LogDeleteOld{Req: 7, Through: 0})
	ex.Execute(core.ObjectDelete{Req: 8, Key: "k"})
	deadline := time.Now().Add(2 * time.Second)
	got := 0
	for got < 8 && time.Now().Before(deadline) {
		if b, ok := in.Control().Pop(); ok {
			if _, ok := b.(core.IOResult); ok {
				got++
			}
			continue
		}
		time.Sleep(time.Millisecond)
	}
	if got < 8 {
		t.Fatalf("completions=%d", got)
	}
}
