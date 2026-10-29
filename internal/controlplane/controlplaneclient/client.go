// Package controlplaneclient is the broker-side adapter for the control plane: a
// system.ControlPlane over the gRPC API and the system.ErrorClassifier for ControlPlaneCall.
package controlplaneclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/apiconvert"
	"manifester/internal/core"
	"manifester/internal/system"
)

// Error is a business error answered by the control plane (non-OK status with
// ErrorDetail).
type Error struct {
	Code    core.ErrCode
	Outcome core.Outcome
}

func (e *Error) Error() string { return "cp: " + e.Code.String() }

// CallError is a failed exchange (dial, transport, or undecodable status).
// Sent reports whether the request may have reached the server.
type CallError struct {
	Op   string
	Sent bool
	Code codes.Code
	Err  error
}

func (e *CallError) Error() string {
	if e.Code != codes.OK {
		return fmt.Sprintf("cp: %s: grpc %s: %v", e.Op, e.Code, e.Err)
	}
	return fmt.Sprintf("cp: %s: %v", e.Op, e.Err)
}
func (e *CallError) Unwrap() error { return e.Err }

// Client implements system.ControlPlane over plaintext gRPC.
type Client struct {
	addrs []string
	cur   atomic.Int32
	dial  func(ctx context.Context, addr string) (*grpc.ClientConn, error)
	obs   func(op string, d time.Duration)

	mu    atomic.Pointer[grpc.ClientConn]
	muIdx atomic.Int32
}

// Observe sets a callback receiving, per exchange, the op name and duration.
func (c *Client) Observe(f func(op string, d time.Duration)) { c.obs = f }

var _ system.ControlPlane = (*Client)(nil)

// New returns a client for target. target may be a comma-separated list of
// host:port addresses (decision 51 / Appendix E): calls go to the address that
// last worked; Unavailable moves on to the next within the same call.
func New(target string, _ any) *Client {
	c := &Client{
		dial: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	}
	for a := range strings.SplitSeq(target, ",") {
		if a = strings.TrimSpace(a); a != "" {
			// Accept legacy http(s)://host:port by stripping the scheme.
			a = strings.TrimPrefix(a, "https://")
			a = strings.TrimPrefix(a, "http://")
			c.addrs = append(c.addrs, a)
		}
	}
	return c
}

// Close tears down the cached connection.
func (c *Client) Close() error {
	if conn := c.mu.Swap(nil); conn != nil {
		return conn.Close()
	}
	return nil
}

// Call performs op. A business error is *Error; other failures are *CallError.
func (c *Client) Call(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch op := op.(type) {
	case core.CommitSegment:
		return c.commitSegment(ctx, op)
	case core.FindSegments:
		return c.lookup(ctx, op)
	case core.DescribeTopics:
		return c.describeTopics(ctx, op)
	}
	return nil, &Error{Code: core.ErrInvalid}
}

//nolint:dupl // each ControlPlaneOperation keeps its own request/response conversion
func (c *Client) commitSegment(ctx context.Context, op core.CommitSegment) (core.ControlPlaneResponse, error) {
	var out *cpv1.CommitSegmentResponse
	err := c.do(ctx, "CommitSegment", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.CommitSegment(ctx, apiconvert.CommitSegmentFromCore(op))
		return e
	})
	if err != nil {
		return nil, err
	}
	return apiconvert.CommitSegmentRespToCore(out), nil
}

//nolint:dupl // each ControlPlaneOperation keeps its own request/response conversion
func (c *Client) lookup(ctx context.Context, op core.FindSegments) (core.ControlPlaneResponse, error) {
	var out *cpv1.LookupResponse
	err := c.do(ctx, "Lookup", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.Lookup(ctx, apiconvert.LookupFromCore(op))
		return e
	})
	if err != nil {
		return nil, err
	}
	return apiconvert.LookupRespToCore(out), nil
}

//nolint:dupl // each ControlPlaneOperation keeps its own request/response conversion
func (c *Client) describeTopics(ctx context.Context, op core.DescribeTopics) (core.ControlPlaneResponse, error) {
	var out *cpv1.DescribeTopicsResponse
	err := c.do(ctx, "DescribeTopics", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.DescribeTopics(ctx, apiconvert.DescribeTopicsFromCore(op))
		return e
	})
	if err != nil {
		return nil, err
	}
	return apiconvert.DescribeTopicsRespToCore(out), nil
}

// CreateTopic is the dev/test helper over CreateTopic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) error {
	return c.do(ctx, "CreateTopic", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.CreateTopic(ctx, &cpv1.CreateTopicRequest{Topic: name, Partitions: partitions})
		return err
	})
}

// GetStatus calls GetStatus on the preferred address.
func (c *Client) GetStatus(ctx context.Context) (*cpv1.GetStatusResponse, error) {
	var out *cpv1.GetStatusResponse
	err := c.do(ctx, "GetStatus", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.GetStatus(ctx, &cpv1.GetStatusRequest{})
		return e
	})
	return out, err
}

func (c *Client) do(ctx context.Context, name string, fn func(context.Context, cpv1.ControlPlaneClient) error) error {
	if c.obs != nil {
		t0 := time.Now()
		defer func() { c.obs(name, time.Since(t0)) }()
	}
	return c.failover(ctx, name, fn)
}

func (c *Client) failover(ctx context.Context, name string, fn func(context.Context, cpv1.ControlPlaneClient) error) error {
	n := len(c.addrs)
	if n == 0 {
		return &CallError{Op: name, Err: errors.New("no addresses")}
	}
	start := int(c.cur.Load())
	var err error
	for i := range n {
		idx := (start + i) % n
		err = c.doAt(ctx, c.addrs[idx], idx, name, fn)
		if !shouldTryOtherNode(ctx, err) {
			if i > 0 && err == nil {
				c.cur.Store(int32(idx))
			}
			return err
		}
	}
	return err
}

func shouldTryOtherNode(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if ce, ok := errors.AsType[*CallError](err); ok {
		return !ce.Sent || ce.Code == codes.Unavailable
	}
	if be, ok := errors.AsType[*Error](err); ok {
		return be.Code == core.ErrUnavailable
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

func (c *Client) doAt(ctx context.Context, addr string, idx int, name string, fn func(context.Context, cpv1.ControlPlaneClient) error) error {
	conn, err := c.conn(ctx, addr, idx)
	if err != nil {
		return &CallError{Op: name, Sent: false, Err: err}
	}
	err = fn(ctx, cpv1.NewControlPlaneClient(conn))
	if err == nil {
		return nil
	}
	return convertRPCError(name, err)
}

func (c *Client) conn(ctx context.Context, addr string, idx int) (*grpc.ClientConn, error) {
	if conn := c.mu.Load(); conn != nil && int(c.muIdx.Load()) == idx {
		return conn, nil
	}
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	if old := c.mu.Swap(conn); old != nil {
		_ = old.Close()
	}
	c.muIdx.Store(int32(idx))
	return conn, nil
}

func convertRPCError(name string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return &CallError{Op: name, Sent: true, Err: err}
	}
	for _, d := range st.Details() {
		if ed, ok := d.(*cpv1.ErrorDetail); ok {
			return &Error{
				Code:    apiconvert.ToCoreErr(ed.Code),
				Outcome: apiconvert.ToCoreOutcome(ed.Outcome),
			}
		}
	}
	code := core.ErrIO
	switch st.Code() {
	case codes.Unavailable:
		code = core.ErrUnavailable
	case codes.InvalidArgument:
		code = core.ErrInvalid
	case codes.NotFound:
		code = core.ErrNotFound
	case codes.AlreadyExists:
		code = core.ErrConflict
	case codes.DeadlineExceeded, codes.Canceled:
		code = core.ErrTimeout
	}
	// Prefer typed Error when the status maps cleanly so Classify sees business codes.
	if code == core.ErrUnavailable || code == core.ErrInvalid || code == core.ErrNotFound || code == core.ErrConflict {
		return &Error{Code: code}
	}
	return &CallError{Op: name, Sent: true, Code: st.Code(), Err: err}
}
