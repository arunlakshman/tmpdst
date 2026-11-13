package server_test

import (
	"context"
	goruntime "runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds/dst/machine"
)

func newClosingNode() *server.Node {
	return server.New(server.Config{
		Machine: replica.Config{CatchUpEvery: machine.Duration(time.Millisecond)},
		Log:     executor.NewMemoryLog(), Objects: executor.NewMemoryObjects(),
		Self: machine.NodeInstance{Node: "cp-0"},
	})
}

// closeWithin fails the test if Close does not return soon.
func closeWithin(t *testing.T, n *server.Node) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- n.Close() }()
	select {
	case err := <-done:
		t.Logf("Close returned: %v", err) // a stop error is fine; it must only return
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}
}

// Start waits for the loop to end when it gives up, so the loop result was
// already taken. Close used to wait for it a second time and block for ever.
func TestCloseAfterFailedStartReturns(t *testing.T) {
	n := newClosingNode()
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Start gives up at once
	if err := n.Start(ctx); err == nil {
		t.Fatal("Start with an ended context must fail")
	}
	closeWithin(t, n)
	closeWithin(t, n) // and again: safe to repeat
}

func TestCloseWithoutStartReturns(t *testing.T) {
	closeWithin(t, newClosingNode())
}

func TestCloseAfterStartStopsTheLoop(t *testing.T) {
	n := newClosingNode()
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	closeWithin(t, n)
}

// Close answers a parked group call Unavailable, returns soon, and leaves no
// goroutine of the node behind. The join parks for the initial delay of a new
// group, which is an hour here.
func TestCloseAnswersParkedGroupCall(t *testing.T) {
	before := goruntime.NumGoroutine()
	n := server.New(server.Config{
		Machine: replica.Config{CatchUpEvery: machine.Duration(time.Millisecond), Heartbeat: machine.Duration(time.Second)},
		Log:     executor.NewMemoryLog(), Objects: executor.NewMemoryObjects(),
		Self:                       machine.NodeInstance{Node: "cp-0"},
		GroupInitialRebalanceDelay: time.Hour,
	})
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	// The machine serves once it has read the groups of the log.
	for _, err := n.ListGroups(ctx, &cpv1.ListGroupsRequest{}); err != nil; _, err = n.ListGroups(ctx, &cpv1.ListGroupsRequest{}) {
		time.Sleep(time.Millisecond)
	}
	joined := make(chan error, 1)
	go func() {
		_, err := n.JoinGroup(ctx, &cpv1.JoinGroupRequest{Group: "g", SessionTimeoutMs: 30000, ProtocolType: "consumer",
			Protocols: []*cpv1.GroupProtocol{{Name: "range"}}})
		joined <- err
	}()
	for {
		d, err := n.DescribeGroups(ctx, &cpv1.DescribeGroupsRequest{Groups: []string{"g"}})
		if err == nil && d.GetGroups()[0].GetDetails().GetState() == "PreparingRebalance" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	closeWithin(t, n)
	select {
	case err := <-joined:
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
			t.Fatalf("parked join after Close: %v, want Unavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the parked join was not answered")
	}
	for end := time.Now().Add(5 * time.Second); goruntime.NumGoroutine() > before; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			buf := make([]byte, 1<<20)
			t.Fatalf("%d goroutines after Close, %d before:\n%s", goruntime.NumGoroutine(), before, buf[:goruntime.Stack(buf, true)])
		}
	}
}
