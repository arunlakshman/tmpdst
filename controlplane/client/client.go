// Package client is the gRPC client of the control plane. It speaks the types of package
// contract: Call takes a contract.Operation and returns a contract.Response. The broker
// uses it as its control plane adapter. Classify turns a failed call into an error code and
// an outcome.
//
// This package imports no broker package.
package client

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

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

// Error is a business error answered by the control plane (non-OK status with
// ErrorDetail).
type Error struct {
	Code    contract.ErrCode
	Outcome contract.Outcome
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

// Client calls the control plane over plaintext gRPC.
type Client struct {
	addrs []string
	cur   atomic.Int32
	dial  func(ctx context.Context, addr string) (*grpc.ClientConn, error)
	obs   func(operation string, d time.Duration)

	mu    atomic.Pointer[grpc.ClientConn]
	muIdx atomic.Int32
}

// Observe sets a callback receiving, per exchange, the op name and duration.
func (c *Client) Observe(f func(operation string, d time.Duration)) { c.obs = f }

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
func (c *Client) Call(ctx context.Context, operation contract.Operation) (contract.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch operation.(type) {
	case contract.CommitSegment, contract.FindSegments, contract.DescribeTopics, contract.CreateTopic:
		return c.callTopic(ctx, operation)
	case contract.GroupJoin, contract.GroupSync, contract.GroupHeartbeat, contract.GroupLeave, contract.OffsetCommit,
		contract.OffsetFetch, contract.DescribeGroups, contract.ListGroups:
		return c.callGroup(ctx, operation)
	}
	return nil, &Error{Code: contract.ErrInvalid}
}

// callOp is callRPC for the calls that return a contract.Response.
// On error it returns a nil interface, not a typed zero value.
func callOp[Req, Resp any, Out contract.Response](ctx context.Context, c *Client, name string,
	rpc func(cpv1.ControlPlaneClient, context.Context, Req, ...grpc.CallOption) (Resp, error), in Req, toContract func(Resp) Out,
) (contract.Response, error) {
	r, err := callRPC(ctx, c, name, rpc, in, toContract)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// callRPC runs one RPC through do and converts the reply to the core type.
// On error it returns the zero value and the error.
func callRPC[Req, Resp, Out any](ctx context.Context, c *Client, name string,
	rpc func(cpv1.ControlPlaneClient, context.Context, Req, ...grpc.CallOption) (Resp, error), in Req, toContract func(Resp) Out,
) (Out, error) {
	var zero Out
	var out Resp
	err := c.do(ctx, name, func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = rpc(cli, ctx, in)
		return e
	})
	if err != nil {
		return zero, err
	}
	return toContract(out), nil
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
		return be.Code == contract.ErrUnavailable
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
				Code:    toContractErr(ed.Code),
				Outcome: toContractOutcome(ed.Outcome),
			}
		}
	}
	code := contract.ErrIO
	switch st.Code() {
	case codes.Unavailable:
		code = contract.ErrUnavailable
	case codes.InvalidArgument:
		code = contract.ErrInvalid
	case codes.NotFound:
		code = contract.ErrNotFound
	case codes.AlreadyExists:
		code = contract.ErrConflict
	case codes.DeadlineExceeded, codes.Canceled:
		code = contract.ErrTimeout
	}
	// Prefer typed Error when the status maps cleanly so Classify sees business codes.
	if code == contract.ErrUnavailable || code == contract.ErrInvalid || code == contract.ErrNotFound || code == contract.ErrConflict {
		return &Error{Code: code}
	}
	return &CallError{Op: name, Sent: true, Code: st.Code(), Err: err}
}
