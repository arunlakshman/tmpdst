package cpclient

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/exec"
	"manifester/internal/controlplane/machine"
	"manifester/internal/controlplane/server"
	"manifester/internal/core"
)

func realClient(t *testing.T) *Client {
	t.Helper()
	n := server.New(server.Config{
		Machine: machine.Config{
			K: 4, CatchUpEvery: core.Duration(time.Millisecond),
			Rank: 0, Heartbeat: core.Duration(time.Second),
		},
		Log: exec.NewMemLog(), Objects: exec.NewMemObjects(),
		Self: core.NodeRef{Node: "cp-0"},
	})
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	n.Register(gs)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() {
		gs.GracefulStop()
		_ = n.Close()
	})
	return New(ln.Addr().String(), nil)
}

func TestClientAgainstMachine(t *testing.T) {
	c := realClient(t)
	ctx := t.Context()
	var ops []string
	c.Observe(func(op string, _ time.Duration) { ops = append(ops, op) })

	if err := c.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatal(err)
	}
	tp := core.TP("t", 0)
	cs, err := c.Call(ctx, core.CommitSegment{SegmentKey: "s1", Batches: []core.PendingBatch{
		{TP: tp, Records: 4, Loc: core.SegmentRef{Key: "s1", Off: 0, Len: 40}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if as := cs.(core.CommitSegmentResp).Assigned; len(as) != 1 || as[0].Count != 4 {
		t.Fatalf("assigned %+v", as)
	}
	lk, err := c.Call(ctx, core.Lookup{TP: tp, From: 0, MaxBytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if l := lk.(core.LookupResp); len(l.Ranges) != 1 || l.End != 4 {
		t.Fatalf("lookup %+v", l)
	}
	ds, err := c.Call(ctx, core.DescribeTopics{Topics: []string{"t"}})
	if err != nil || len(ds.(core.DescribeTopicsResp).Topics) != 1 {
		t.Fatalf("describe %v %v", ds, err)
	}
	if len(ops) != 4 {
		t.Errorf("observer saw %v", ops)
	}

	_, err = c.Call(ctx, core.CommitSegment{SegmentKey: "s1", Batches: []core.PendingBatch{
		{TP: tp, Records: 1, Loc: core.SegmentRef{Key: "s1", Len: 1}},
	}})
	var be *Error
	if !errors.As(err, &be) || be.Code != core.ErrConflict {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestCallCtxAndUnknownOp(t *testing.T) {
	c := realClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Call(ctx, core.CommitSegment{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
	_, err := c.Call(t.Context(), nil)
	var be *Error
	if !errors.As(err, &be) || be.Code != core.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}

func TestCallDialError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, err = New(addr, nil).Call(t.Context(), core.CommitSegment{
		SegmentKey: "k", Batches: []core.PendingBatch{{TP: core.TP("t", 0), Records: 1}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	code, out := Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, err)
	if code != core.ErrUnavailable && code != core.ErrIO {
		t.Errorf("dial: %v %v (%v)", code, out, err)
	}
}

func TestClassifyBusinessAndTimeout(t *testing.T) {
	be := &Error{Code: core.ErrConflict}
	code, out := Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, be)
	if code != core.ErrConflict || out != core.NotApplied {
		t.Fatalf("business: %v %v", code, out)
	}
	ce := &CallError{Op: "CommitSegment", Sent: true, Code: codes.DeadlineExceeded, Err: context.DeadlineExceeded}
	code, out = Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, ce)
	if code != core.ErrTimeout || out != core.MaybeApplied {
		t.Fatalf("timeout: %v %v", code, out)
	}
	_, out = Classifier{}.Classify(core.CPCall{Op: core.Lookup{}}, ce)
	if out != core.NotApplied {
		t.Fatalf("read timeout outcome %v", out)
	}
	ce = &CallError{Op: "x", Sent: false, Err: syscall.ECONNREFUSED}
	code, out = Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, ce)
	if code != core.ErrUnavailable || out != core.NotApplied {
		t.Fatalf("unsent: %v %v", code, out)
	}
}

func TestFailoverSkipsUnavailable(t *testing.T) {
	bad := New("127.0.0.1:1", nil) // nothing listening
	// Preferred bad, then a real node.
	good := realClient(t)
	c := New("127.0.0.1:1,"+good.addrs[0], nil)
	_ = bad
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
}

func TestMapRPCErrDetail(t *testing.T) {
	st := status.New(codes.AlreadyExists, "ErrConflict")
	st, err := st.WithDetails(&cpv1.ErrorDetail{
		Code: cpv1.ErrCode_ERR_CODE_CONFLICT, Outcome: cpv1.Outcome_OUTCOME_NOT_APPLIED,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := mapRPCErr("CommitSegment", st.Err())
	var be *Error
	if !errors.As(got, &be) || be.Code != core.ErrConflict {
		t.Fatalf("got %v", got)
	}
	if ce, ok := errors.AsType[*CallError](mapRPCErr("x", errors.New("plain"))); !ok || !ce.Sent {
		t.Fatalf("plain: %v", mapRPCErr("x", errors.New("plain")))
	}
	for _, tc := range []struct {
		code codes.Code
		want core.ErrCode
	}{
		{codes.Unavailable, core.ErrUnavailable},
		{codes.InvalidArgument, core.ErrInvalid},
		{codes.NotFound, core.ErrNotFound},
		{codes.AlreadyExists, core.ErrConflict},
	} {
		got := mapRPCErr("op", status.Error(tc.code, "x"))
		be, ok := errors.AsType[*Error](got)
		if !ok || be.Code != tc.want {
			t.Fatalf("%v: %v", tc.code, got)
		}
	}
	got = mapRPCErr("op", status.Error(codes.DeadlineExceeded, "late"))
	ce, ok := errors.AsType[*CallError](got)
	if !ok || ce.Code != codes.DeadlineExceeded {
		t.Fatalf("deadline: %v", got)
	}
	got = mapRPCErr("op", status.Error(codes.Internal, "boom"))
	if _, ok := errors.AsType[*CallError](got); !ok {
		t.Fatalf("internal: %v", got)
	}
}
