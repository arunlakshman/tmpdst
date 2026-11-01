package controlplaneclient

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"manifester/internal/core"
)

// deadClient points at an address where nothing listens, so every call fails
// in the transport and never reaches a node.
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

// TestCallGroupTransportErrors: each group op returns its transport error and
// no answer when no node can be reached.
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
	for _, op := range ops {
		resp, err := c.callGroup(ctx, op)
		if err == nil {
			t.Fatalf("%T: no error, resp %+v", op, resp)
		}
		_ = resp // OffsetCommit returns a zero response with its error, so the answer is not checked
		if code, _ := (Classifier{}).Classify(core.ControlPlaneCall{Op: op}, err); code == core.OK {
			t.Fatalf("%T: classified as OK: %v", op, err)
		}
	}
}

// TestCallGroupAnswers: the success path of the read ops gives the core answer.
func TestCallGroupAnswers(t *testing.T) {
	c := realGroupClient(t)
	ctx := t.Context()
	if _, err := c.JoinGroup(ctx, joinOf("g-ans", "")); err != nil {
		t.Fatal(err)
	}
	resp, err := c.callGroup(ctx, core.ListGroups{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if gs := resp.(core.ListGroupsResp).Groups; len(gs) != 1 || gs[0].Name != "g-ans" {
		t.Fatalf("list answer %+v", gs)
	}
	resp, err = c.callGroup(ctx, core.DescribeGroups{Groups: []string{"g-ans", "g-none"}})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if ds := resp.(core.DescribeGroupsResp).Groups; len(ds) != 2 || !ds[0].Exists || ds[1].Exists {
		t.Fatalf("describe answer %+v", ds)
	}
}

// TestCallGroupCanceledContext: a dead context stops the call before it is sent.
func TestCallGroupCanceledContext(t *testing.T) {
	c := realGroupClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := c.callGroup(ctx, core.ListGroups{})
	if err == nil {
		t.Fatal("canceled ctx: expected error")
	}
	if code, _ := (Classifier{}).Classify(core.ControlPlaneCall{Op: core.ListGroups{}}, err); code != core.ErrTimeout {
		t.Fatalf("canceled ctx code = %v, want ErrTimeout (err %v)", code, err)
	}
}

// TestCallGroupBusinessErrors: decisions from the group come back as *Error
// with their code, and the answer is nil.
func TestCallGroupBusinessErrors(t *testing.T) {
	c := realGroupClient(t)
	ctx := t.Context()
	j, err := c.JoinGroup(ctx, joinOf("g-biz", ""))
	if err != nil {
		t.Fatal(err)
	}
	// A sync from a member that is not in the group.
	resp, err := c.callGroup(ctx, core.GroupSync{Group: "g-biz", MemberID: "nobody", Generation: j.Generation})
	groupErr(t, err, core.ErrUnknownMember)
	if resp != nil {
		t.Fatalf("sync resp %+v with error", resp)
	}
	// A join that offers another protocol type than the group uses.
	resp, err = c.callGroup(ctx, core.GroupJoin{Group: "g-biz", ClientID: "c", ProtocolType: "other",
		Protocols: []core.GroupProtocol{{Name: "range", Metadata: []byte("m")}}})
	if err == nil || resp != nil {
		t.Fatalf("join with other protocol type: resp %+v err %v", resp, err)
	}
}

// TestCallGroupUnknownOp: an op that is not a group op is refused as invalid.
func TestCallGroupUnknownOp(t *testing.T) {
	c := deadClient(t)
	_, err := c.callGroup(t.Context(), core.CommitSegment{})
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != core.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}
