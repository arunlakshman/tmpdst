// Package cpclient is the broker-side adapter for the control plane: a
// sys.ControlPlane over the cp HTTP API and the sys.Classifier for CPCall.
package cpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/sys"
	"manifester/sys/httppool"
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
	bases []string
	cur   atomic.Int32 // index of the URL that last worked
	hc    *http.Client
	obs   func(op string, d time.Duration)
}

// Observe sets a callback receiving, per exchange, the op name and the time
// from sending the request to reading the whole response (the client-side
// round trip, errors included). Call before the first Call.
func (c *Client) Observe(f func(op string, d time.Duration)) { c.obs = f }

var _ sys.ControlPlane = (*Client)(nil)

// New returns a client for the service at baseURL. baseURL may be a comma
// separated list of control-plane URLs (decision 51): calls go to the URL that
// last worked, and a connection that could not be made or a 503 (a standby, a
// node that is starting or cannot apply the log) moves on to the next URL
// within the same call, each URL at most once. A commit retried this way is
// safe: it carries its segment key. A nil hc gets a default client; deadlines
// come from the context of each call.
func New(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		// Sized for the real concurrency (audit item 4): see httppool.
		hc = &http.Client{Transport: httppool.Transport()}
	}
	c := &Client{hc: hc}
	for _, u := range strings.Split(baseURL, ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			c.bases = append(c.bases, u)
		}
	}
	return c
}

// Call performs op. A business error is returned as *Error, any other
// failure as *CallError (or the context error if ctx was already done).
func (c *Client) Call(ctx context.Context, op core.CPOp) (core.CPResp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch op := op.(type) {
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

func (c *Client) commitSegment(ctx context.Context, op core.CommitSegment) (core.CPResp, error) {
	req := cp.CommitReq{SegmentKey: op.SegmentKey}
	for _, b := range op.Batches {
		req.Batches = append(req.Batches, cp.BatchToWire(b))
	}
	var out cp.CommitResp
	if err := c.exchange(ctx, "commit-segment", req, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	return core.CommitSegmentResp{Assigned: ranges(out.Assigned)}, nil
}

func (c *Client) lookup(ctx context.Context, op core.Lookup) (core.CPResp, error) {
	req := cp.LookupReq{Topic: op.TP.Topic, Partition: op.TP.Partition, From: int64(op.From), MaxBytes: op.MaxBytes}
	var out cp.LookupResp
	if err := c.exchange(ctx, "lookup", req, &out, func() string { return out.Err }); err != nil {
		return nil, err
	}
	return core.LookupResp{Ranges: ranges(out.Ranges), End: core.Offset(out.End)}, nil
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

func ranges(in []cp.Range) []core.BatchRange {
	var out []core.BatchRange
	for _, r := range in {
		out = append(out, cp.RangeFromWire(r))
	}
	return out
}

func (c *Client) do(ctx context.Context, name string, req, resp any) error {
	n := len(c.bases)
	start := int(c.cur.Load())
	var err error
	for i := range n {
		idx := (start + i) % n
		if err = c.doAt(ctx, c.bases[idx], name, req, resp); !shouldFailover(ctx, err) {
			if i > 0 && err == nil {
				c.cur.Store(int32(idx))
			}
			return err
		}
	}
	return err
}

// shouldFailover: the request certainly did not run here (no connection) or
// the node said it will not take it now (503), and the caller is still waiting.
func shouldFailover(ctx context.Context, err error) bool {
	var ce *CallError
	if err == nil || ctx.Err() != nil || !errors.As(err, &ce) {
		return false
	}
	return !ce.Sent || ce.Status == http.StatusServiceUnavailable
}

func (c *Client) doAt(ctx context.Context, base, name string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return &CallError{Op: name, Err: err}
	}
	sent := false
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { sent = true }})
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/"+name, bytes.NewReader(body))
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
	defer func() { _ = hres.Body.Close() }() // body already read by decodeReply
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
