package cptest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"manifester/internal/controlplane/cpclient"
	"manifester/internal/core"
)

func TestClassifier(t *testing.T) {
	commit := core.ControlPlaneCall{Op: core.CommitSegment{}}
	look := core.ControlPlaneCall{Op: core.FindSegments{}}
	desc := core.ControlPlaneCall{Op: core.DescribeTopics{}}
	dial := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	dns := &net.DNSError{Err: "no such host", Name: "cp"}
	ce := func(sent bool, code codes.Code, err error) error {
		return &cpclient.CallError{Op: "x", Sent: sent, Code: code, Err: err}
	}
	cases := []struct {
		name string
		a    core.IOCall
		err  error
		code core.ErrCode
		out  core.Outcome
	}{
		{"business too large", commit, &cpclient.Error{Code: core.ErrTooLarge}, core.ErrTooLarge, core.NotApplied},
		{"business conflict", commit, &cpclient.Error{Code: core.ErrConflict}, core.ErrConflict, core.NotApplied},
		{"business invalid lookup", look, &cpclient.Error{Code: core.ErrInvalid}, core.ErrInvalid, core.NotApplied},
		{"refused commit", commit, ce(false, codes.OK, dial), core.ErrUnavailable, core.NotApplied},
		{"dns commit", commit, ce(false, codes.OK, dns), core.ErrUnavailable, core.NotApplied},
		{"unavailable commit", commit, ce(true, codes.Unavailable, errors.New("unavailable")), core.ErrUnavailable, core.NotApplied},
		{"internal commit", commit, ce(true, codes.Internal, errors.New("boom")), core.ErrIO, core.MaybeApplied},
		{"internal lookup", look, ce(true, codes.Internal, nil), core.ErrIO, core.NotApplied},
		{"deadline sent commit", commit, ce(true, codes.DeadlineExceeded, context.DeadlineExceeded), core.ErrTimeout, core.MaybeApplied},
		{"deadline sent describe", desc, ce(true, codes.DeadlineExceeded, context.DeadlineExceeded), core.ErrTimeout, core.NotApplied},
		{"deadline before conn", commit, ce(false, codes.OK, context.DeadlineExceeded), core.ErrTimeout, core.NotApplied},
		{"reset commit", commit, ce(true, codes.OK, reset), core.ErrIO, core.MaybeApplied},
		{"eof commit", commit, ce(true, codes.OK, io.ErrUnexpectedEOF), core.ErrIO, core.MaybeApplied},
		{"reset lookup", look, ce(true, codes.OK, reset), core.ErrIO, core.NotApplied},
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

func TestClassifierLiveDial(t *testing.T) {
	l, _ := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := cpclient.New(addr, nil).Call(ctx, core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{
		{TP: core.NewTopicPartition("t", 0), Records: 1},
	}})
	if err == nil {
		t.Fatal("want error")
	}
	c, o := cpclient.Classifier{}.Classify(core.ControlPlaneCall{Op: core.CommitSegment{}}, err)
	if c != core.ErrUnavailable && c != core.ErrIO {
		t.Errorf("refused: %v %v (%v)", c, o, err)
	}
}
