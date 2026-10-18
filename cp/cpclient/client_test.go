package cpclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/memstore"
)

func realClient(t *testing.T) *Client {
	t.Helper()
	srv := httptest.NewServer(cp.NewHandler(memstore.New()))
	t.Cleanup(srv.Close)
	return New(srv.URL+"/", nil)
}

func TestClientAgainstMemstore(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	var ops []string
	c.Observe(func(op string, d time.Duration) { ops = append(ops, op) })

	if err := c.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatal(err)
	}
	tp := core.TopicPartition{Topic: "t", Partition: 0}
	cs, err := c.Call(ctx, core.CommitSegment{SegmentKey: "s1", Batches: []core.PendingBatch{
		{TP: tp, Records: 4, Loc: core.SegmentRef{Key: "s1", Off: 0, Len: 40}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if as := cs.(core.CommitSegmentResp).Assigned; len(as) != 1 || as[0].Count != 4 {
		t.Fatalf("assigned %+v", as)
	}
	lk, err := c.Call(ctx, core.Lookup{TP: tp, From: 0, MaxBytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if l := lk.(core.LookupResp); len(l.Ranges) != 1 || l.End != 4 {
		t.Fatalf("lookup %+v", l)
	}
	ds, err := c.Call(ctx, core.DescribeTopics{Topics: []string{"t"}})
	if err != nil || len(ds.(core.DescribeTopicsResp).Topics) != 1 {
		t.Fatalf("describe %v %v", ds, err)
	}
	if len(ops) != 4 {
		t.Errorf("observer saw %v", ops)
	}

	// business error: the same key with another request conflicts.
	_, err = c.Call(ctx, core.CommitSegment{SegmentKey: "s1", Batches: []core.PendingBatch{{TP: tp, Records: 1, Loc: core.SegmentRef{Key: "s1", Len: 1}}}})
	var be *Error
	if !errors.As(err, &be) || be.Code != core.ErrConflict || be.Error() != "cp: ErrConflict" {
		t.Fatalf("want conflict, got %v", err)
	}
	if _, err = c.Call(ctx, core.DescribeTopics{}); err != nil {
		t.Fatal(err)
	}
	if err = c.CreateTopic(ctx, "bad", 0); err != nil && !errors.As(err, &be) {
		t.Fatalf("create-topic: %v", err)
	}
}

func TestCallCtxAndUnknownOp(t *testing.T) {
	c := realClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Call(ctx, core.CommitSegment{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
	_, err := c.Call(context.Background(), nil)
	var be *Error
	if !errors.As(err, &be) || be.Code != core.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}

// fake serves a fixed status and body for every request.
func fake(t *testing.T, status int, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, srv.Client())
}

func TestCallErrorClasses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		code   core.ErrCode
		out    core.Outcome
	}{
		{"business", 200, `{"err":"ErrConflict"}`, core.ErrConflict, core.NotApplied},
		{"unavailable", 503, "busy", core.ErrUnavailable, core.NotApplied},
		{"throttled", 429, "slow", core.ErrUnavailable, core.NotApplied},
		{"4xx", 400, "bad", core.ErrIO, core.NotApplied},
		{"5xx", 500, "oops", core.ErrIO, core.MaybeApplied},
		{"malformed", 200, `{"epoch":`, core.ErrIO, core.MaybeApplied},
		{"unknown err", 200, `{"err":"ErrWhat"}`, core.ErrIO, core.MaybeApplied},
	}
	for _, tc := range cases {
		c := fake(t, tc.status, tc.body)
		_, err := c.Call(context.Background(), core.CommitSegment{})
		if err == nil {
			t.Fatalf("%s: no error", tc.name)
		}
		code, out := Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, err)
		if code != tc.code || out != tc.out {
			t.Errorf("%s: %v %v, want %v %v (err %v)", tc.name, code, out, tc.code, tc.out, err)
		}
		// reads are never MaybeApplied
		if _, rout := (Classifier{}).Classify(core.CPCall{Op: core.Lookup{}}, err); rout == core.MaybeApplied {
			t.Errorf("%s: read classified MaybeApplied", tc.name)
		}
	}
	// CallError text carries the status.
	_, err := fake(t, 500, " oops \n").Call(context.Background(), core.CommitSegment{})
	if !strings.Contains(err.Error(), "commit-segment: http 500: oops") {
		t.Errorf("error text %q", err)
	}
}

func TestCallDialError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := New(url, nil).Call(context.Background(), core.CommitSegment{})
	var ce *CallError
	if !errors.As(err, &ce) || ce.Sent {
		t.Fatalf("want unsent CallError, got %v", err)
	}
	code, out := Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, err)
	if code != core.ErrUnavailable || out != core.NotApplied {
		t.Errorf("dial: %v %v", code, out)
	}
}

func TestCallTimeoutAfterSend(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := New(srv.URL, nil).Call(ctx, core.CommitSegment{})
	code, out := Classifier{}.Classify(core.CPCall{Op: core.CommitSegment{}}, err)
	if code != core.ErrTimeout || out != core.MaybeApplied {
		t.Errorf("timeout: %v %v (%v)", code, out, err)
	}
	_, out = Classifier{}.Classify(core.CPCall{Op: core.Lookup{}}, err)
	if out != core.NotApplied {
		t.Errorf("read timeout outcome %v", out)
	}
}

func TestBadBaseURL(t *testing.T) {
	_, err := New("http://bad host\x7f", nil).Call(context.Background(), core.CommitSegment{})
	var ce *CallError
	if !errors.As(err, &ce) || ce.Sent {
		t.Fatalf("got %v", err)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "t" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }

func TestClassifyDirect(t *testing.T) {
	put := core.CPCall{Op: core.CommitSegment{}}
	notCP := core.ObjPut{}
	dial := &net.OpError{Op: "dial", Err: errors.New("x")}
	cases := []struct {
		name string
		a    core.IOAction
		err  error
		code core.ErrCode
		out  core.Outcome
	}{
		{"plain error", put, errors.New("x"), core.ErrIO, core.MaybeApplied},
		{"plain timeout", put, timeoutErr{}, core.ErrTimeout, core.MaybeApplied},
		{"deadline", put, context.DeadlineExceeded, core.ErrTimeout, core.MaybeApplied},
		{"not a cp call", notCP, errors.New("x"), core.ErrIO, core.MaybeApplied},
		{"unsent ctx", put, &CallError{Err: context.Canceled}, core.ErrIO, core.NotApplied},
		{"unsent timeout", put, &CallError{Err: timeoutErr{}}, core.ErrTimeout, core.NotApplied},
		{"unsent dial", put, &CallError{Err: dial}, core.ErrUnavailable, core.NotApplied},
		{"sent eof", put, &CallError{Sent: true, Err: errors.New("EOF")}, core.ErrIO, core.MaybeApplied},
		{"sent timeout", put, &CallError{Sent: true, Err: timeoutErr{}}, core.ErrTimeout, core.MaybeApplied},
		{"business", put, fmt.Errorf("w: %w", &Error{Code: core.ErrNotFound}), core.ErrNotFound, core.NotApplied},
	}
	for _, tc := range cases {
		code, out := classify(tc.a, tc.err)
		if code != tc.code || out != tc.out {
			t.Errorf("%s: %v %v, want %v %v", tc.name, code, out, tc.code, tc.out)
		}
	}
}

func TestIsDial(t *testing.T) {
	yes := []error{
		&net.DNSError{Err: "no such host"},
		&net.OpError{Op: "dial", Err: errors.New("x")},
		syscall.ECONNREFUSED, syscall.EHOSTUNREACH, syscall.ENETUNREACH,
	}
	for _, e := range yes {
		if !isDial(e) {
			t.Errorf("isDial(%v) = false", e)
		}
	}
	for _, e := range []error{errors.New("x"), &net.OpError{Op: "read", Err: errors.New("x")}, syscall.ECONNRESET} {
		if isDial(e) {
			t.Errorf("isDial(%v) = true", e)
		}
	}
}
