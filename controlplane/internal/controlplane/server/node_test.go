package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	sharedv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func startNode(t *testing.T, config replica.Config) (cpv1.ControlPlaneClient, healthpb.HealthClient, func()) {
	t.Helper()
	return startNodeWithLease(t, config, 0)
}

// startNodeWithLease is startNode with a broker lease TTL. Zero keeps the default.
func startNodeWithLease(t *testing.T, config replica.Config, leaseTTL time.Duration) (cpv1.ControlPlaneClient, healthpb.HealthClient, func()) {
	t.Helper()
	if config.CatchUpEvery == 0 {
		config.CatchUpEvery = runtime.Duration(time.Millisecond)
	}
	n := server.New(server.Config{
		Machine:        config,
		Log:            executor.NewMemoryLog(),
		Objects:        executor.NewMemoryObjects(),
		Self:           runtime.NodeInstance{Node: "cp-0"},
		BrokerLeaseTTL: leaseTTL,
	})
	ctx := t.Context()
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	n.Register(gs)
	go func() { _ = gs.Serve(ln) }()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	stop := func() {
		gs.GracefulStop()
		_ = conn.Close()
		_ = n.Close()
	}
	return cpv1.NewControlPlaneClient(conn), healthpb.NewHealthClient(conn), stop
}

func TestHealthServingAfterReady(t *testing.T) {
	_, hc, stop := startNode(t, replica.Config{Rank: 0, Heartbeat: runtime.Duration(time.Second)})
	defer stop()
	response, err := hc.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v", response.Status)
	}
}

func TestCommitLookupRoundTrip(t *testing.T) {
	cli, _, stop := startNode(t, replica.Config{Rank: 0, Heartbeat: runtime.Duration(time.Second)})
	defer stop()
	_, err := cli.CreateTopic(t.Context(), &cpv1.CreateTopicRequest{Topic: "t", Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	cr, err := cli.CommitSegment(t.Context(), &cpv1.CommitSegmentRequest{
		SegmentKey: "s1",
		Batches: []*sharedv1.PendingBatch{{
			Tp: &sharedv1.TopicPartition{Topic: "t", Partition: 0}, Records: 3,
			Loc: &sharedv1.SegmentRef{Key: "s1", Off: 0, Len: 9},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cr.Assigned) != 1 || cr.Assigned[0].Base != 0 || cr.Assigned[0].Count != 3 {
		t.Fatalf("assigned = %+v", cr.Assigned)
	}
	lr, err := cli.FindSegments(t.Context(), &cpv1.FindSegmentsRequest{
		Tp: &sharedv1.TopicPartition{Topic: "t", Partition: 0}, MaxBytes: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if lr.End != 3 || len(lr.Ranges) != 1 {
		t.Fatalf("lookup = %+v", lr)
	}
	st, err := cli.GetStatus(t.Context(), &cpv1.GetStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Role != "active" {
		t.Fatalf("role = %q", st.Role)
	}
	// The node wrote commits, so it holds an epoch; it has never stepped down.
	if st.Epoch == 0 || st.StepDowns != 0 {
		t.Fatalf("epoch = %d, step_downs = %d; want an epoch and no step-down", st.Epoch, st.StepDowns)
	}
}

func TestStandbyCommitUnavailable(t *testing.T) {
	cli, _, stop := startNode(t, replica.Config{
		Rank: 1, Heartbeat: runtime.Duration(time.Second), TakeoverAfter: runtime.Duration(time.Hour),
	})
	defer stop()
	_, err := cli.CommitSegment(t.Context(), &cpv1.CommitSegmentRequest{
		SegmentKey: "x",
		Batches: []*sharedv1.PendingBatch{{
			Tp: &sharedv1.TopicPartition{Topic: "t", Partition: 0}, Records: 1,
			Loc: &sharedv1.SegmentRef{Key: "x"},
		}},
	})
	if err == nil {
		t.Fatal("expected unavailable")
	}
}

func TestStartCanceledContext(t *testing.T) {
	n := server.New(server.Config{
		Machine: replica.Config{CatchUpEvery: runtime.Duration(time.Hour)},
		Log:     executor.NewMemoryLog(),
		Objects: executor.NewMemoryObjects(),
		Self:    runtime.NodeInstance{Node: "cp-0"},
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := n.Start(ctx); err == nil {
		_ = n.Close()
		t.Fatal("expected Start to fail on canceled context")
	}
}

func TestStartCancelWhileWaiting(t *testing.T) {
	n := server.New(server.Config{
		Machine: replica.Config{CatchUpEvery: runtime.Duration(time.Hour)},
		Log:     executor.NewMemoryLog(),
		Objects: executor.NewMemoryObjects(),
		Self:    runtime.NodeInstance{Node: "cp-0"},
	})
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := n.Start(ctx); err == nil {
		_ = n.Close()
		t.Fatal("expected Start to fail when context cancels during wait")
	}
}

// TestExclusiveCreateTopicOverGRPC: a create with fail_if_exists on a topic
// that exists is AlreadyExists with ERR_CODE_TOPIC_EXISTS, and it changes nothing.
func TestExclusiveCreateTopicOverGRPC(t *testing.T) {
	cli, _, stop := startNode(t, replica.Config{Rank: 0, Heartbeat: runtime.Duration(time.Second)})
	defer stop()
	ctx := t.Context()
	if _, err := cli.CreateTopic(ctx, &cpv1.CreateTopicRequest{Topic: "x", Partitions: 2, FailIfExists: true}); err != nil {
		t.Fatalf("new topic: %v", err)
	}
	_, err := cli.CreateTopic(ctx, &cpv1.CreateTopicRequest{Topic: "x", Partitions: 5, FailIfExists: true})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("existing topic: %v, want AlreadyExists", err)
	}
	var detail *cpv1.ErrorDetail
	for _, d := range st.Details() {
		if e, ok := d.(*cpv1.ErrorDetail); ok {
			detail = e
		}
	}
	if detail == nil || detail.Code != cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS {
		t.Fatalf("detail %+v, want ERR_CODE_TOPIC_EXISTS", detail)
	}
	d, err := cli.DescribeTopics(ctx, &cpv1.DescribeTopicsRequest{Topics: []string{"x"}})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 2 {
		t.Fatalf("topics %+v %v, want x with 2 partitions", d, err)
	}
}
