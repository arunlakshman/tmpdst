package production

import (
	"context"
	"errors"
	"testing"
	"time"

	"mds/internal/core"
	"mds/internal/system"
	"mds/internal/system/s3store"
	"mds/internal/verify/speccheck/specsystem"
)

type slowObjs struct {
	system.ObjectStore
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

func wait(t *testing.T, in *system.Inbox) core.EventData {
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
	in := system.NewInbox(8)
	objs := slowObjs{ObjectStore: specsystem.NewObjectStore(), block: make(chan struct{})}
	cls := Classifier{Objects: MemoryErrorClassifier{}, CP: MemoryErrorClassifier{}}
	e := NewExecutor(t.Context(), ExecConfig{ObjTimeout: 50 * time.Millisecond}, objs, nil, nilConns{}, cls, in)

	start := time.Now()
	e.Execute(core.ObjectPut{Req: 7, Key: "k"}) // blocked in the store
	e.Execute(core.Send{Req: 8, Conn: 1, Data: []byte("x")})
	if time.Since(start) > 20*time.Millisecond {
		t.Fatal("Execute blocked")
	}
	var put core.ObjectPutDone
	var send core.SendDone
	for range 2 {
		switch d := wait(t, in).(type) {
		case core.ObjectPutDone:
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

	e.Execute(core.ObjectGet{Req: 9, Ref: core.SegmentRef{Key: "missing"}})
	if g := wait(t, in).(core.ObjectGetDone); g.Req != 9 || g.Err != core.ErrNotFound {
		t.Errorf("get %+v", g)
	}
}

func TestExecutorDropsAfterClose(t *testing.T) {
	in := system.NewInbox(8)
	e := NewExecutor(t.Context(), ExecConfig{}, specsystem.NewObjectStore(), nil, nilConns{},
		Classifier{Objects: MemoryErrorClassifier{}}, in)
	in.Close()
	e.Execute(core.ObjectList{Req: 1})
	time.Sleep(30 * time.Millisecond)
	if _, ok := in.Control().Pop(); ok {
		t.Fatal("delivered after close")
	}
}

func TestClassifierRoutes(t *testing.T) {
	c := Classifier{Objects: s3store.Classifier{}, CP: MemoryErrorClassifier{}}
	if code, out := c.Classify(core.ObjectPut{}, context.DeadlineExceeded); code != core.ErrTimeout || out != core.MaybeApplied {
		t.Errorf("obj: %v %v", code, out)
	}
	if code, _ := c.Classify(core.Send{}, errors.New("x")); code != core.ErrIO {
		t.Errorf("other: %v", code)
	}
}

// slowCP answers after d, or with the call's context error.
type slowCP struct{ d time.Duration }

func (s slowCP) Call(ctx context.Context, _ core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	select {
	case <-time.After(s.d):
		return core.GroupLeaveResp{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A control plane call takes its own Timeout when it has one (group joins wait
// for a rebalance), and the executor's default otherwise.
func TestControlPlaneCallOwnTimeout(t *testing.T) {
	in := system.NewInbox(8)
	cls := Classifier{Objects: MemoryErrorClassifier{}, CP: MemoryErrorClassifier{}}
	e := NewExecutor(t.Context(), ExecConfig{CPTimeout: 10 * time.Millisecond}, nil, slowCP{d: 60 * time.Millisecond}, nilConns{}, cls, in)

	e.Execute(core.ControlPlaneCall{Req: 1, Op: core.GroupLeave{Group: "g"}}) // default 10 ms: too slow
	if d := wait(t, in).(core.ControlPlaneDone); d.Req != 1 || d.Err == core.OK {
		t.Errorf("default deadline: %+v, want a failure", d)
	}

	e.Execute(core.ControlPlaneCall{Req: 2, Op: core.GroupLeave{Group: "g"}, Timeout: core.Duration(time.Second)})
	if d := wait(t, in).(core.ControlPlaneDone); d.Req != 2 || d.Err != core.OK {
		t.Errorf("own deadline: %+v, want OK", d)
	}
}
