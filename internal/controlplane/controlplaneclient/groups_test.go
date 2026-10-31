package controlplaneclient

import (
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"

	"manifester/internal/controlplane/executor"
	"manifester/internal/controlplane/groupcoordinator"
	"manifester/internal/controlplane/server"
	"manifester/internal/controlplane/statemachine"
	"manifester/internal/core"
)

// realGroupClient starts an active node with a short initial rebalance delay.
func realGroupClient(t *testing.T) *Client {
	t.Helper()
	n := server.New(server.Config{
		Machine: statemachine.Config{
			K: 4, CatchUpEvery: core.Duration(time.Millisecond),
			Rank: 0, Heartbeat: core.Duration(time.Second),
		},
		Log: executor.NewMemoryLog(), Objects: executor.NewMemoryObjects(),
		Self:                       core.NodeInstance{Node: "cp-0"},
		GroupInitialRebalanceDelay: 20 * time.Millisecond,
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
	c := New(ln.Addr().String(), nil)
	t.Cleanup(func() { _ = c.Close() })
	waitGroupsServing(t, c)
	return c
}

// waitGroupsServing waits until the watcher has rebuilt the coordinator.
func waitGroupsServing(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := c.ListGroups(t.Context())
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("groups never served: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func groupErr(t *testing.T, err error, want core.ErrCode) {
	t.Helper()
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != want {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func joinOf(group, member string) groupcoordinator.JoinReq {
	return groupcoordinator.JoinReq{
		Group: group, MemberID: member, ClientID: "cli", ClientHost: "h",
		SessionTimeoutMs: 10000, RebalanceTimeoutMs: 10000, ProtocolType: "consumer",
		Protocols: []groupcoordinator.Protocol{{Name: "range", Metadata: []byte("m-" + member)}},
	}
}

func TestClientGroupCallsOverGRPC(t *testing.T) {
	c := realGroupClient(t)
	ctx := t.Context()
	if err := c.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatal(err)
	}

	// JoinGroup and SyncGroup: one member, one generation.
	ja, err := c.JoinGroup(ctx, joinOf("g", ""))
	if err != nil || ja.Err != core.OK || ja.Generation != 1 || ja.MemberID == "" || ja.Leader != ja.MemberID {
		t.Fatalf("join: %+v %v", ja, err)
	}
	a := ja.MemberID
	if len(ja.Members) != 1 || string(ja.Members[0].Metadata) != "m-" || ja.Protocol != "range" {
		t.Fatalf("leader list: %+v", ja)
	}
	sa, err := c.SyncGroup(ctx, groupcoordinator.SyncReq{Group: "g", MemberID: a, Generation: 1,
		Assignments: []groupcoordinator.Assignment{{Member: a, Bytes: []byte("all")}}})
	if err != nil || string(sa.Assignment) != "all" {
		t.Fatalf("sync: %+v %v", sa, err)
	}

	// Heartbeat: OK for the member, codes for the others.
	if err := c.GroupHeartbeat(ctx, groupcoordinator.HeartbeatReq{Group: "g", MemberID: a, Generation: 1}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	groupErr(t, c.GroupHeartbeat(ctx, groupcoordinator.HeartbeatReq{Group: "g", MemberID: "zz", Generation: 1}), core.ErrUnknownMember)
	groupErr(t, c.GroupHeartbeat(ctx, groupcoordinator.HeartbeatReq{Group: "g", MemberID: a, Generation: 5}), core.ErrIllegalGeneration)

	// A second member makes the first one rebalance. Both rejoin.
	type joinOut struct {
		resp groupcoordinator.JoinResp
		err  error
	}
	bDone := make(chan joinOut, 1)
	go func() {
		r, err := c.JoinGroup(ctx, joinOf("g", ""))
		bDone <- joinOut{r, err}
	}()
	waitUntilGroup(t, func() bool {
		err := c.GroupHeartbeat(ctx, groupcoordinator.HeartbeatReq{Group: "g", MemberID: a, Generation: 1})
		be, ok := errors.AsType[*Error](err)
		return ok && be.Code == core.ErrRebalanceInProgress
	})
	ra, err := c.JoinGroup(ctx, joinOf("g", a))
	if err != nil || ra.Generation != 2 {
		t.Fatalf("rejoin a: %+v %v", ra, err)
	}
	rb := <-bDone
	if rb.err != nil || rb.resp.Generation != 2 {
		t.Fatalf("join b: %+v %v", rb.resp, rb.err)
	}
	b := rb.resp.MemberID
	sbDone := make(chan groupcoordinator.SyncResp, 1)
	go func() {
		r, _ := c.SyncGroup(ctx, groupcoordinator.SyncReq{Group: "g", MemberID: b, Generation: 2})
		sbDone <- r
	}()
	if _, err := c.SyncGroup(ctx, groupcoordinator.SyncReq{Group: "g", MemberID: a, Generation: 2,
		Assignments: []groupcoordinator.Assignment{{Member: a, Bytes: []byte("a2")}, {Member: b, Bytes: []byte("b2")}}}); err != nil {
		t.Fatalf("leader sync: %v", err)
	}
	if sb := <-sbDone; string(sb.Assignment) != "b2" {
		t.Fatalf("follower sync: %+v", sb)
	}

	// LeaveGroup gives one code per member.
	codes, err := c.LeaveGroup(ctx, groupcoordinator.LeaveReq{Group: "g", Members: []string{b, "nobody"}})
	if err != nil || !slices.Equal(codes, []core.ErrCode{core.OK, core.ErrUnknownMember}) {
		t.Fatalf("leave: %v %v", codes, err)
	}
	ra3, err := c.JoinGroup(ctx, joinOf("g", a))
	if err != nil || ra3.Generation != 3 || len(ra3.Members) != 1 {
		t.Fatalf("join after leave: %+v %v", ra3, err)
	}
	if s3, err := c.SyncGroup(ctx, groupcoordinator.SyncReq{Group: "g", MemberID: a, Generation: 3,
		Assignments: []groupcoordinator.Assignment{{Member: a, Bytes: []byte("a3")}}}); err != nil || string(s3.Assignment) != "a3" {
		t.Fatalf("sync gen 3: %+v %v", s3, err)
	}

	// GroupOffsetCommit: a stale generation is refused, the right one stores.
	tp := core.NewTopicPartition("t", 0)
	commit := core.OffsetCommit{Group: "g", Member: a, Generation: 1, Offsets: []core.PartitionOffset{{TopicPartition: tp, Offset: 7, Metadata: "m"}}}
	_, err = c.GroupOffsetCommit(ctx, commit)
	groupErr(t, err, core.ErrIllegalGeneration)
	commit.Generation = 3
	cr, err := c.GroupOffsetCommit(ctx, commit)
	if err != nil || !slices.Equal(cr.Errs, []core.ErrCode{core.OK}) {
		t.Fatalf("commit: %+v %v", cr, err)
	}

	// GroupOffsetFetch: one partition, then all partitions.
	f, err := c.GroupOffsetFetch(ctx, core.OffsetFetch{Group: "g", Partitions: []core.TopicPartition{tp}})
	if err != nil || len(f.Offsets) != 1 || f.Offsets[0].Offset != 7 || f.Offsets[0].Metadata != "m" {
		t.Fatalf("fetch: %+v %v", f, err)
	}
	all, err := c.GroupOffsetFetch(ctx, core.OffsetFetch{Group: "g"})
	if err != nil || len(all.Offsets) != 1 {
		t.Fatalf("fetch all: %+v %v", all, err)
	}

	// DescribeGroups keeps the request order. A missing group has Exists false.
	ds, err := c.DescribeGroups(ctx, []string{"g", "nope"})
	if err != nil || len(ds) != 2 || ds[0].Name != "g" || !ds[0].Exists || ds[0].State != "Stable" ||
		ds[0].Generation != 3 || len(ds[0].Members) != 1 || ds[1].Exists {
		t.Fatalf("describe: %+v %v", ds, err)
	}
	if string(ds[0].Members[0].Assignment) != "a3" {
		t.Fatalf("assignment: %q", ds[0].Members[0].Assignment)
	}

	gs, err := c.ListGroups(ctx)
	if err != nil || len(gs) != 1 || gs[0].Name != "g" || gs[0].State != "Stable" {
		t.Fatalf("list: %+v %v", gs, err)
	}
}

// waitUntilGroup polls cond until it holds, for calls that must see a state change.
func waitUntilGroup(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("state not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

// Call sends the broker's group operations to the control plane, and the
// answers come back as core responses. A group decision comes back as *Error.
func TestCallRoutesGroupOperations(t *testing.T) {
	c := realGroupClient(t)
	ctx := t.Context()

	resp, err := c.Call(ctx, core.GroupJoin{Group: "g-call", ClientID: "cli", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 10000,
		ProtocolType: "consumer", Protocols: []core.GroupProtocol{{Name: "range", Metadata: []byte("sub")}}})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	j := resp.(core.GroupJoinResp)
	if j.MemberID == "" || j.Leader != j.MemberID || j.Generation != 1 || len(j.Members) != 1 {
		t.Fatalf("join answer %+v", j)
	}

	// The leader stores the assignment, so the group is stable for heartbeats.
	if _, err := c.Call(ctx, core.GroupSync{Group: "g-call", MemberID: j.MemberID, Generation: j.Generation,
		Assignments: []core.GroupAssignment{{Member: j.MemberID, Bytes: []byte("a")}}}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if _, err := c.Call(ctx, core.GroupHeartbeat{Group: "g-call", MemberID: j.MemberID, Generation: j.Generation}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// A heartbeat from an unknown member is a group decision, not a transport error.
	_, err = c.Call(ctx, core.GroupHeartbeat{Group: "g-call", MemberID: "nobody", Generation: 1})
	groupErr(t, err, core.ErrUnknownMember)

	resp, err = c.Call(ctx, core.DescribeGroups{Groups: []string{"g-call", "g-none"}})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	ds := resp.(core.DescribeGroupsResp).Groups
	if len(ds) != 2 || !ds[0].Exists || ds[0].Name != "g-call" || ds[1].Exists {
		t.Fatalf("describe answer %+v", ds)
	}

	resp, err = c.Call(ctx, core.GroupLeave{Group: "g-call", Members: []string{j.MemberID, "nobody"}})
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	if errs := resp.(core.GroupLeaveResp).Errs; len(errs) != 2 || errs[0] != core.OK || errs[1] != core.ErrUnknownMember {
		t.Fatalf("leave codes %v", errs)
	}

	resp, err = c.Call(ctx, core.OffsetFetch{Group: "g-call"})
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	if len(resp.(core.OffsetFetchResp).Offsets) != 0 {
		t.Fatalf("offset fetch answer %+v, want none", resp)
	}
}
