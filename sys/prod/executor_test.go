package prod

import (
	"context"
	"errors"
	"testing"
	"time"

	"manifester/core"
	"manifester/sys"
	"manifester/sys/s3store"
	"manifester/veritas/vsys"
)

type slowObjs struct {
	sys.ObjectStore
	block chan struct{}
	err   error
}

func (s slowObjs) Put(ctx context.Context, key string, data []byte) error {
	select {
	case <-s.block:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.err
}

type nilConns struct{ *Net }

func (nilConns) WriteAsync(core.ConnID, []byte, func(error)) error { return ErrNoConn }

func wait(t *testing.T, in *sys.Inbox) core.EventBody {
	t.Helper()
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if b, ok := in.Control().Pop(); ok {
			return b
		}
	}
	t.Fatal("no completion")
	return nil
}

func TestExecutorCompletesOnceAndNeverBlocks(t *testing.T) {
	in := sys.NewInbox(8)
	objs := slowObjs{ObjectStore: vsys.NewObjectStore(), block: make(chan struct{})}
	cls := Classifier{Objects: MemClassifier{}, CP: MemClassifier{}}
	e := NewExecutor(t.Context(), ExecConfig{ObjTimeout: 50 * time.Millisecond}, objs, nil, nilConns{}, cls, in)

	start := time.Now()
	e.Execute(core.ObjPut{Req: 7, Key: "k"}) // blocked in the store
	e.Execute(core.Send{Req: 8, Conn: 1, Data: []byte("x")})
	if time.Since(start) > 20*time.Millisecond {
		t.Fatal("Execute blocked")
	}
	var put core.ObjPutDone
	var send core.SendDone
	for range 2 {
		switch d := wait(t, in).(type) {
		case core.ObjPutDone:
			put = d
		case core.SendDone:
			send = d
		}
	}
	if put.Req != 7 || put.Err != core.ErrIO || put.Outcome != core.MaybeApplied { // MemClassifier: unknown error
		t.Errorf("put %+v", put)
	}
	if send.Req != 8 || send.Err != core.ErrConnClosed {
		t.Errorf("send %+v", send)
	}
	if _, ok := in.Control().Pop(); ok {
		t.Fatal("more than one completion per action")
	}

	e.Execute(core.ObjGet{Req: 9, Ref: core.SegmentRef{Key: "missing"}})
	if g := wait(t, in).(core.ObjGetDone); g.Req != 9 || g.Err != core.ErrNotFound {
		t.Errorf("get %+v", g)
	}
}

func TestExecutorDropsAfterClose(t *testing.T) {
	in := sys.NewInbox(8)
	e := NewExecutor(t.Context(), ExecConfig{}, vsys.NewObjectStore(), nil, nilConns{},
		Classifier{Objects: MemClassifier{}}, in)
	in.Close()
	e.Execute(core.ObjList{Req: 1})
	time.Sleep(30 * time.Millisecond)
	if _, ok := in.Control().Pop(); ok {
		t.Fatal("delivered after close")
	}
}

func TestClassifierRoutes(t *testing.T) {
	c := Classifier{Objects: s3store.Classifier{}, CP: MemClassifier{}}
	if code, out := c.Classify(core.ObjPut{}, context.DeadlineExceeded); code != core.ErrTimeout || out != core.MaybeApplied {
		t.Errorf("obj: %v %v", code, out)
	}
	if code, _ := c.Classify(core.Send{}, errors.New("x")); code != core.ErrIO {
		t.Errorf("other: %v", code)
	}
}
