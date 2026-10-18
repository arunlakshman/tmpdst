package cptest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"manifester/core"
	"manifester/cp/cpclient"
)

func TestClassifier(t *testing.T) {
	commit := core.CPCall{Op: core.CommitSegment{}}
	look := core.CPCall{Op: core.Lookup{}}
	desc := core.CPCall{Op: core.DescribeTopics{}}
	dial := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	dns := &net.DNSError{Err: "no such host", Name: "cp"}
	ce := func(sent bool, status int, err error) error {
		return &cpclient.CallError{Op: "x", Sent: sent, Status: status, Err: err}
	}
	cases := []struct {
		name string
		a    core.IOAction
		err  error
		code core.ErrCode
		out  core.Outcome
	}{
		{"business too large", commit, &cpclient.Error{Code: core.ErrTooLarge}, core.ErrTooLarge, core.NotApplied},
		{"business conflict", commit, &cpclient.Error{Code: core.ErrConflict}, core.ErrConflict, core.NotApplied},
		{"business invalid lookup", look, &cpclient.Error{Code: core.ErrInvalid}, core.ErrInvalid, core.NotApplied},
		{"refused commit", commit, ce(false, 0, dial), core.ErrUnavailable, core.NotApplied},
		{"dns commit", commit, ce(false, 0, dns), core.ErrUnavailable, core.NotApplied},
		{"503 commit", commit, ce(true, 503, errors.New("unavailable")), core.ErrUnavailable, core.NotApplied},
		{"429 commit", commit, ce(true, 429, nil), core.ErrUnavailable, core.NotApplied},
		{"500 commit", commit, ce(true, 500, errors.New("boom")), core.ErrIO, core.MaybeApplied},
		{"502 commit", commit, ce(true, 502, nil), core.ErrIO, core.MaybeApplied},
		{"500 lookup", look, ce(true, 500, nil), core.ErrIO, core.NotApplied},
		{"404 commit", commit, ce(true, 404, nil), core.ErrIO, core.NotApplied},
		{"deadline sent commit", commit, ce(true, 0, context.DeadlineExceeded), core.ErrTimeout, core.MaybeApplied},
		{"deadline sent commit again", commit, ce(true, 0, context.DeadlineExceeded), core.ErrTimeout, core.MaybeApplied},
		{"deadline sent describe", desc, ce(true, 0, context.DeadlineExceeded), core.ErrTimeout, core.NotApplied},
		{"deadline before conn", commit, ce(false, 0, context.DeadlineExceeded), core.ErrTimeout, core.NotApplied},
		{"reset commit", commit, ce(true, 0, reset), core.ErrIO, core.MaybeApplied},
		{"eof commit", commit, ce(true, 0, io.ErrUnexpectedEOF), core.ErrIO, core.MaybeApplied},
		{"reset lookup", look, ce(true, 0, reset), core.ErrIO, core.NotApplied},
		{"bad reply commit", commit, ce(true, 200, errors.New("bad json")), core.ErrIO, core.MaybeApplied},
		{"unknown commit", commit, errors.New("?"), core.ErrIO, core.MaybeApplied},
		{"unknown lookup", look, errors.New("?"), core.ErrIO, core.NotApplied},
		{"wrapped business", commit, fmt.Errorf("w: %w", &cpclient.Error{Code: core.ErrConflict}), core.ErrConflict, core.NotApplied},
	}
	for _, c := range cases {
		code, out := cpclient.Classifier{}.Classify(c.a, c.err)
		if code != c.code || out != c.out {
			t.Errorf("%s: got %v/%v want %v/%v", c.name, code, out, c.code, c.out)
		}
	}
}

// Real failures through a real client, classified end to end.
func TestClassifierLive(t *testing.T) {
	ctx := context.Background()
	commit := core.CommitSegment{SegmentKey: "k"}
	classify := func(url string, hc *http.Client, op core.CPOp, ctx context.Context) (core.ErrCode, core.Outcome) {
		_, err := cpclient.New(url, hc).Call(ctx, op)
		if err == nil {
			t.Fatal("want error")
		}
		return cpclient.Classifier{}.Classify(core.CPCall{Op: op}, err)
	}
	// Connection refused: a port nobody listens on.
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	l.Close()
	if c, o := classify("http://"+addr, nil, commit, ctx); c != core.ErrUnavailable || o != core.NotApplied {
		t.Errorf("refused: %v %v", c, o)
	}
	// 503.
	s503 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "x", 503) }))
	defer s503.Close()
	if c, o := classify(s503.URL, nil, commit, ctx); c != core.ErrUnavailable || o != core.NotApplied {
		t.Errorf("503: %v %v", c, o)
	}
	// Connection dropped after the request arrived.
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer drop.Close()
	if c, o := classify(drop.URL, nil, commit, ctx); c != core.ErrIO || o != core.MaybeApplied {
		t.Errorf("drop: %v %v", c, o)
	}
	if c, o := classify(drop.URL, nil, core.Lookup{}, ctx); c != core.ErrIO || o != core.NotApplied {
		t.Errorf("drop lookup: %v %v", c, o)
	}
	// Server too slow.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer slow.Close()
	tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if c, o := classify(slow.URL, nil, commit, tctx); c != core.ErrTimeout || o != core.MaybeApplied {
		t.Errorf("slow: %v %v", c, o)
	}
}
