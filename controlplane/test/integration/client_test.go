//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
)

// client is a small test client over the generated gRPC client, with the
// broker's failover rule: a call goes to the node that answered last; when a
// node is down or not the writer, it tries the next one, and after a full round
// it waits a little and starts again, until the context ends. The same request
// is sent each time, so a retried commit keeps its segment key.
type client struct {
	apis []cpv1.ControlPlaneClient
	cur  atomic.Int32
}

// newClient dials every node, preferred node first (the brokers' URL list).
func newClient(t *testing.T, nodes ...*node) *client {
	t.Helper()
	c := &client{}
	for _, n := range nodes {
		conn, err := grpc.NewClient(n.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		c.apis = append(c.apis, cpv1.NewControlPlaneClient(conn))
	}
	return c
}

// retryable says whether another node (or a later try) may answer. An answer
// with an ErrorDetail is a business answer, and only UNAVAILABLE (not the
// writer, or not ready) is retried. A status without one is a transport
// failure, except for the codes the server sends for a bad request.
func retryable(err error) bool {
	st := status.Convert(err)
	for _, d := range st.Details() {
		if ed, ok := d.(*cpv1.ErrorDetail); ok {
			return ed.GetCode() == cpv1.ErrCode_ERR_CODE_UNAVAILABLE
		}
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists:
		return false
	}
	return true
}

// do runs fn with failover. Each try has its own short deadline, so a node
// that hangs does not use up the whole budget.
func (c *client) do(ctx context.Context, fn func(context.Context, cpv1.ControlPlaneClient) error) error {
	for {
		start := int(c.cur.Load())
		var err error
		for i := range len(c.apis) {
			idx := (start + i) % len(c.apis)
			tryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = fn(tryCtx, c.apis[idx])
			cancel()
			if err == nil {
				c.cur.Store(int32(idx))
				return nil
			}
			if ctx.Err() != nil || !retryable(err) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// call sends one request with failover. The default budget is 20 s: the real
// broker deadline is 120 s, and a takeover takes about a second here.
func call[Req, Resp any](t *testing.T, c *client, method func(cpv1.ControlPlaneClient, context.Context, Req, ...grpc.CallOption) (Resp, error), in Req) (Resp, error) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var out Resp
	err := c.do(ctx, func(ctx context.Context, api cpv1.ControlPlaneClient) error {
		r, err := method(api, ctx, in)
		if err == nil {
			out = r
		}
		return err
	})
	return out, err
}

func (c *client) createTopic(t *testing.T, name string, partitions int32) {
	t.Helper()
	if _, err := call(t, c, cpv1.ControlPlaneClient.CreateTopic, &cpv1.CreateTopicRequest{Topic: name, Partitions: partitions}); err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
}

// commit commits one segment with one batch of records on topic/partition.
func (c *client) commit(t *testing.T, key, topic string, partition int32, records int64) (*cpv1.CommitSegmentResponse, error) {
	return call(t, c, cpv1.ControlPlaneClient.CommitSegment, &cpv1.CommitSegmentRequest{
		SegmentKey: key,
		Batches: []*sharedv1.PendingBatch{{
			Tp: &sharedv1.TopicPartition{Topic: topic, Partition: partition}, Records: records,
			Loc: &sharedv1.SegmentRef{Key: key, Len: 10},
		}},
	})
}

func (c *client) findSegments(t *testing.T, topic string, partition int32, from int64) (*cpv1.FindSegmentsResponse, error) {
	return call(t, c, cpv1.ControlPlaneClient.FindSegments, &cpv1.FindSegmentsRequest{
		Tp: &sharedv1.TopicPartition{Topic: topic, Partition: partition}, From: from, MaxBytes: 1 << 30,
	})
}
