package cpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	ctx := t.Context()
	var ops []string
	c.Observe(func(op string, d time.Duration) { ops = append(ops, op) })

	if err := c.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatal(err)
	}
	tp := core.TP("t", 0)
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
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Call(ctx, core.CommitSegment{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
	_, err := c.Call(t.Context(), nil)
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
		_, _ = fmt.Fprint(w, body)
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
		_, err := c.Call(t.Context(), core.CommitSegment{})
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
	_, err := fake(t, 500, " oops \n").Call(t.Context(), core.CommitSegment{})
	if !strings.Contains(err.Error(), "commit-segment: http 500: oops") {
		t.Errorf("error text %q", err)
	}
}

func TestCallDialError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := New(url, nil).Call(t.Context(), core.CommitSegment{})
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
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
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
	_, err := New("http://bad host\x7f", nil).Call(t.Context(), core.CommitSegment{})
	var ce *CallError
	if !errors.As(err, &ce) || ce.Sent {
		t.Fatalf("got %v", err)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "t" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return false }

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
		{"plain timeout", put, timeoutError{}, core.ErrTimeout, core.MaybeApplied},
		{"deadline", put, context.DeadlineExceeded, core.ErrTimeout, core.MaybeApplied},
		{"not a cp call", notCP, errors.New("x"), core.ErrIO, core.MaybeApplied},
		{"unsent ctx", put, &CallError{Err: context.Canceled}, core.ErrIO, core.NotApplied},
		{"unsent timeout", put, &CallError{Err: timeoutError{}}, core.ErrTimeout, core.NotApplied},
		{"unsent dial", put, &CallError{Err: dial}, core.ErrUnavailable, core.NotApplied},
		{"sent eof", put, &CallError{Sent: true, Err: errors.New("EOF")}, core.ErrIO, core.MaybeApplied},
		{"sent timeout", put, &CallError{Sent: true, Err: timeoutError{}}, core.ErrTimeout, core.MaybeApplied},
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

func TestFailoverAcrossURLs(t *testing.T) {
	var standby atomic.Int64
	sb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		standby.Add(1)
		http.Error(w, "not active", http.StatusServiceUnavailable)
	}))
	t.Cleanup(sb.Close)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // connection refused
	up := httptest.NewServer(cp.NewHandler(memstore.New()))
	t.Cleanup(up.Close)

	c := New(deadURL+", "+sb.URL+","+up.URL+"/", nil)
	ctx := t.Context()
	if err := c.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatalf("create through three urls: %v", err)
	}
	if got := c.cur.Load(); got != 2 {
		t.Fatalf("sticky url %d, want 2", got)
	}
	before := standby.Load()
	if _, err := c.Call(ctx, core.DescribeTopics{}); err != nil || standby.Load() != before {
		t.Fatalf("a later call must start at the url that worked: %v (standby hits %d -> %d)", err, before, standby.Load())
	}
}

func TestFailoverGivesUpAfterEachURLOnce(t *testing.T) {
	var hits atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "no", http.StatusServiceUnavailable)
	}))
	t.Cleanup(bad.Close)
	c := New(bad.URL+","+bad.URL, nil)
	err := c.CreateTopic(t.Context(), "t", 1)
	var ce *CallError
	if !errors.As(err, &ce) || ce.Status != 503 || hits.Load() != 2 {
		t.Fatalf("err %v hits %d", err, hits.Load())
	}
}

func TestNoFailoverOnBusinessErrorOrTimeout(t *testing.T) {
	var hits atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body) // lets the server notice the client going away
		<-r.Context().Done()
	}))
	t.Cleanup(slow.Close)
	up := httptest.NewServer(cp.NewHandler(memstore.New()))
	t.Cleanup(up.Close)
	c := New(slow.URL+","+up.URL, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := c.CreateTopic(ctx, "t", 1); err == nil || hits.Load() != 1 {
		t.Fatalf("a sent request that times out must not be resent elsewhere: %v hits %d", err, hits.Load())
	}
	if c2 := New(up.URL, nil); c2.CreateTopic(t.Context(), "", 1) == nil {
		t.Fatal("business error lost")
	}
}

// Failback: the client is sticky to the URL that last worked (the promoted
// standby); when that node steps down (503) the call wraps around to the
// preferred node, which becomes the sticky URL again.
func TestFailbackWrapsAround(t *testing.T) {
	var demoted atomic.Bool
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if demoted.Load() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "not active", http.StatusServiceUnavailable)
			return
		}
		cp.NewHandler(memstore.New()).ServeHTTP(w, r)
	}))
	t.Cleanup(b.Close)
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	aURL := a.URL
	c := New(aURL+","+b.URL, nil)
	ctx := t.Context()
	if err := c.CreateTopic(ctx, "t", 1); err != nil || c.cur.Load() != 1 {
		t.Fatalf("failover to b: %v cur %d", err, c.cur.Load())
	}
	a.Close()
	a2 := httptest.NewUnstartedServer(cp.NewHandler(memstore.New()))
	_ = a2.Listener.Close()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", strings.TrimPrefix(aURL, "http://"))
	if err != nil {
		t.Skip("port not reusable:", err)
	}
	a2.Listener = l
	a2.Start()
	t.Cleanup(a2.Close)
	demoted.Store(true)
	if err := c.CreateTopic(ctx, "t2", 1); err != nil || c.cur.Load() != 0 {
		t.Fatalf("failback to the preferred url: %v cur %d", err, c.cur.Load())
	}
}
