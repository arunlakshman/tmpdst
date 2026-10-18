// Package cpclient is the broker-side adapter for the control plane: a
// sys.ControlPlane over the cp HTTP API and the sys.Classifier for CPCall.
package cpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/sys"
)

// Error is a business error answered by the control plane (HTTP 200).
type Error struct{ Code core.ErrCode }

func (e *Error) Error() string { return "cp: " + e.Code.String() }

// CallError is a failed exchange. Sent reports whether the request may have
// reached the server (a connection was obtained); Status is the HTTP status
// if a response arrived.
type CallError struct {
	Op     string
	Sent   bool
	Status int
	Err    error
}

func (e *CallError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("cp: %s: http %d: %v", e.Op, e.Status, e.Err)
	}
	return fmt.Sprintf("cp: %s: %v", e.Op, e.Err)
}
func (e *CallError) Unwrap() error { return e.Err }

// Client implements sys.ControlPlane.
type Client struct {
	base string
	hc   *http.Client
	obs  func(op string, d time.Duration)
}

// Observe sets a callback receiving, per exchange, the op name and the time
// from sending the request to reading the whole response (the client-side
// round trip, errors included). Call before the first Call.
func (c *Client) Observe(f func(op string, d time.Duration)) { c.obs = f }

var _ sys.ControlPlane = (*Client)(nil)

// New returns a client for the service at baseURL. A nil hc gets a default
// client; deadlines come from the context of each call.
func New(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 16, IdleConnTimeout: 60 * time.Second,
		}}
	}
	return &Client{base: strings.TrimRight(baseURL, "/"), hc: hc}
}

// Call performs op. A business error is returned as *Error, any other
// failure as *CallError (or the context error if ctx was already done).
func (c *Client) Call(ctx context.Context, op core.CPOp) (core.CPResp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch op := op.(type) {
	case core.Register:
		return c.register(ctx, op)
	case core.InitProducer:
		return c.initProducer(ctx)
	case core.CommitSegment:
		return c.commitSegment(ctx, op)
	case core.Lookup:
		return c.lookup(ctx, op)
	case core.DescribeTopics:
		return c.describeTopics(ctx, op)
	}
	return nil, &Error{Code: core.ErrInvalid}
}

// exchange posts req to the named endpoint, decodes into out and turns its
// business error field (errOf, read after decoding) into *Error.
func (c *Client) exchange(ctx context.Context, name string, req, out any, errOf func() string) error {
	if err := c.do(ctx, name, req, out); err != nil {
		return err
	}
	return business(errOf())
}

func (c *Client) register(ctx context.Context, op core.Register) (core.CPResp, error) {
	var out cp.RegisterResp
	if err := c.exchange(ctx, "register", cp.RegisterReq{Self: cp.NodeToWire(op.Self)}, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	return core.RegisterResp{Epoch: core.Epoch(out.Epoch)}, nil
}

func (c *Client) initProducer(ctx context.Context) (core.CPResp, error) {
	var out cp.InitProducerResp
	if err := c.exchange(ctx, "init-producer", struct{}{}, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	return core.InitProducerResp{Producer: core.ProducerID(out.Producer), PEpoch: out.PEpoch}, nil
}

func (c *Client) commitSegment(ctx context.Context, op core.CommitSegment) (core.CPResp, error) {
	req := cp.CommitReq{SegmentKey: op.SegmentKey, Owner: cp.NodeToWire(op.Owner), Epoch: uint64(op.Epoch)}
	for _, b := range op.Batches {
		req.Batches = append(req.Batches, cp.BatchToWire(b))
	}
	var out cp.CommitResp
	if err := c.exchange(ctx, "commit-segment", req, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	rs, err := ranges(out.Assigned)
	if err != nil {
		return nil, &CallError{Op: "commit-segment", Sent: true, Status: 200, Err: err}
	}
	return core.CommitSegmentResp{Assigned: rs}, nil
}

func (c *Client) lookup(ctx context.Context, op core.Lookup) (core.CPResp, error) {
	req := cp.LookupReq{Topic: op.TP.Topic, Partition: op.TP.Partition, From: int64(op.From), MaxBytes: op.MaxBytes}
	var out cp.LookupResp
	if err := c.exchange(ctx, "lookup", req, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	rs, err := ranges(out.Ranges)
	if err != nil {
		return nil, &CallError{Op: "lookup", Sent: true, Status: 200, Err: err}
	}
	return core.LookupResp{Ranges: rs, HighWatermark: core.Offset(out.HighWatermark)}, nil
}

func (c *Client) describeTopics(ctx context.Context, op core.DescribeTopics) (core.CPResp, error) {
	var out cp.DescribeResp
	if err := c.exchange(ctx, "describe-topics", cp.DescribeReq{Topics: op.Topics}, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	var resp core.DescribeTopicsResp
	for _, t := range out.Topics {
		resp.Topics = append(resp.Topics, core.TopicInfo{Name: t.Name, Partitions: t.Partitions})
	}
	return resp, nil
}

// CreateTopic is the dev/test helper over POST /v1/create-topic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) error {
	var out cp.CreateTopicResp
	if err := c.do(ctx, "create-topic", cp.CreateTopicReq{Topic: name, Partitions: partitions}, &out); err != nil {
		return err
	}
	return business(out.Err)
}

func business(s string) error {
	code, err := cp.ErrFromWire(s)
	if err != nil {
		return &CallError{Op: "decode", Sent: true, Status: 200, Err: err}
	}
	if code != core.OK {
		return &Error{Code: code}
	}
	return nil
}

func ranges(in []cp.Range) ([]core.BatchRange, error) {
	var out []core.BatchRange
	for _, r := range in {
		br, err := cp.RangeFromWire(r)
		if err != nil {
			return nil, err
		}
		out = append(out, br)
	}
	return out, nil
}

func (c *Client) do(ctx context.Context, name string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return &CallError{Op: name, Err: err}
	}
	sent := false
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { sent = true }})
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/"+name, bytes.NewReader(body))
	if err != nil {
		return &CallError{Op: name, Err: err}
	}
	hreq.Header.Set("Content-Type", "application/json")
	if c.obs != nil {
		t0 := time.Now()
		defer func() { c.obs(name, time.Since(t0)) }()
	}
	hres, err := c.hc.Do(hreq)
	if err != nil {
		return &CallError{Op: name, Sent: sent, Err: err}
	}
	defer hres.Body.Close()
	return decodeReply(name, hres, resp)
}

// decodeReply reads a response that has arrived: non-200 and unreadable or
// malformed bodies are *CallError with Sent set.
func decodeReply(name string, hres *http.Response, resp any) error {
	data, err := io.ReadAll(io.LimitReader(hres.Body, 64<<20))
	if err != nil {
		return &CallError{Op: name, Sent: true, Status: hres.StatusCode, Err: err}
	}
	if hres.StatusCode != http.StatusOK {
		return &CallError{Op: name, Sent: true, Status: hres.StatusCode, Err: fmt.Errorf("%s", strings.TrimSpace(string(data)))}
	}
	if err := json.Unmarshal(data, resp); err != nil {
		return &CallError{Op: name, Sent: true, Status: hres.StatusCode, Err: err}
	}
	return nil
}
