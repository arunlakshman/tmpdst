package cpclient

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/api/controlplane/v1"
	"github.com/manifesterio/mds/internal/core"
)

func deadClient(t *testing.T) *Client {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	c := New(addr, nil)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestCallDialError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, err = New(addr, nil).Call(t.Context(), core.CommitSegment{
		SegmentKey: "k", Batches: []core.PendingBatch{{TopicPartition: core.NewTopicPartition("t", 0), Records: 1}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	code, out := Classifier{}.Classify(core.ControlPlaneCall{Op: core.CommitSegment{}}, err)
	if code != core.ErrUnavailable && code != core.ErrIO {
		t.Errorf("dial: %v %v (%v)", code, out, err)
	}
}

func TestCallCtxAndUnknownOp(t *testing.T) {
	c := deadClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Call(ctx, core.CommitSegment{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
	_, err := c.Call(t.Context(), nil)
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != core.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}

func TestClassifyBusinessAndTimeout(t *testing.T) {
	be := &Error{Code: core.ErrConflict}
	code, out := Classifier{}.Classify(core.ControlPlaneCall{Op: core.CommitSegment{}}, be)
	if code != core.ErrConflict || out != core.NotApplied {
		t.Fatalf("business: %v %v", code, out)
	}
	ce := &CallError{Op: "CommitSegment", Sent: true, Code: codes.DeadlineExceeded, Err: context.DeadlineExceeded}
	code, out = Classifier{}.Classify(core.ControlPlaneCall{Op: core.CommitSegment{}}, ce)
	if code != core.ErrTimeout || out != core.MaybeApplied {
		t.Fatalf("timeout: %v %v", code, out)
	}
	_, out = Classifier{}.Classify(core.ControlPlaneCall{Op: core.FindSegments{}}, ce)
	if out != core.NotApplied {
		t.Fatalf("read timeout outcome %v", out)
	}
	ce = &CallError{Op: "x", Sent: false, Err: syscall.ECONNREFUSED}
	code, out = Classifier{}.Classify(core.ControlPlaneCall{Op: core.CommitSegment{}}, ce)
	if code != core.ErrUnavailable || out != core.NotApplied {
		t.Fatalf("unsent: %v %v", code, out)
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
	got := convertRPCError("CommitSegment", st.Err())
	be, ok := errors.AsType[*Error](got)
	if !ok || be.Code != core.ErrConflict {
		t.Fatalf("got %v", got)
	}
	if ce, ok := errors.AsType[*CallError](convertRPCError("x", errors.New("plain"))); !ok || !ce.Sent {
		t.Fatalf("plain: %v", convertRPCError("x", errors.New("plain")))
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
		got := convertRPCError("op", status.Error(tc.code, "x"))
		be, ok := errors.AsType[*Error](got)
		if !ok || be.Code != tc.want {
			t.Fatalf("%v: %v", tc.code, got)
		}
	}
	got = convertRPCError("op", status.Error(codes.DeadlineExceeded, "late"))
	ce, ok := errors.AsType[*CallError](got)
	if !ok || ce.Code != codes.DeadlineExceeded {
		t.Fatalf("deadline: %v", got)
	}
	got = convertRPCError("op", status.Error(codes.Internal, "boom"))
	if _, ok := errors.AsType[*CallError](got); !ok {
		t.Fatalf("internal: %v", got)
	}
}

func TestCallGroupTransportErrors(t *testing.T) {
	c := deadClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ops := []core.ControlPlaneOperation{
		core.GroupJoin{Group: "g", ClientID: "c", ProtocolType: "consumer",
			Protocols: []core.GroupProtocol{{Name: "range", Metadata: []byte("m")}}},
		core.GroupSync{Group: "g", MemberID: "m", Generation: 1},
		core.GroupHeartbeat{Group: "g", MemberID: "m", Generation: 1},
		core.GroupLeave{Group: "g", Members: []string{"m"}},
		core.OffsetCommit{Group: "g", Member: "m", Generation: 1},
		core.OffsetFetch{Group: "g"},
		core.DescribeGroups{Groups: []string{"g"}},
		core.ListGroups{},
	}
	for _, operation := range ops {
		response, err := c.callGroup(ctx, operation)
		if err == nil {
			t.Fatalf("%T: no error, resp %+v", operation, response)
		}
		if code, _ := (Classifier{}).Classify(core.ControlPlaneCall{Op: operation}, err); code == core.OK {
			t.Fatalf("%T: classified as OK: %v", operation, err)
		}
	}
}

func TestCallGroupUnknownOp(t *testing.T) {
	c := deadClient(t)
	_, err := c.callGroup(t.Context(), core.CommitSegment{})
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != core.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}

func TestNewParsesAddresses(t *testing.T) {
	c := New("http://a:1, https://b:2, c:3", nil)
	if len(c.addrs) != 3 || c.addrs[0] != "a:1" || c.addrs[1] != "b:2" || c.addrs[2] != "c:3" {
		t.Fatalf("addrs %+v", c.addrs)
	}
	c = New("", nil)
	if err := c.do(t.Context(), "x", func(context.Context, cpv1.ControlPlaneClient) error { return nil }); err == nil {
		t.Fatal("expected no addresses error")
	}
}
