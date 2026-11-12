package s3

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/manifesterio/mds/shared/objectstore"
	"github.com/manifesterio/mds/shared/status"
)

type apiError struct{ code string }

func (e apiError) Error() string                 { return e.code }
func (e apiError) ErrorCode() string             { return e.code }
func (e apiError) ErrorMessage() string          { return e.code }
func (e apiError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestClassify(t *testing.T) {
	dial := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	put, get, del, list := objectstore.Put{}, objectstore.Get{}, objectstore.Delete{}, objectstore.List{}
	for _, c := range []struct {
		name string
		a    objectstore.Call
		err  error
		code status.ErrCode
		out  status.Outcome
	}{
		{"notfound", get, fail(status.ErrNotFound), status.ErrNotFound, status.NotApplied},
		{"notfound delete", del, fmt.Errorf("w: %w", fail(status.ErrNotFound)), status.ErrNotFound, status.NotApplied},
		{"dial put", put, fmt.Errorf("w: %w", dial), status.ErrUnavailable, status.NotApplied},
		{"slowdown put", put, apiError{"SlowDown"}, status.ErrUnavailable, status.NotApplied},
		{"timeout put", put, context.DeadlineExceeded, status.ErrTimeout, status.MaybeApplied},
		{"timeout delete", del, context.DeadlineExceeded, status.ErrTimeout, status.MaybeApplied},
		{"reset put", put, reset, status.ErrIO, status.MaybeApplied},
		{"reset delete", del, reset, status.ErrIO, status.MaybeApplied},
		{"timeout get", get, context.DeadlineExceeded, status.ErrTimeout, status.NotApplied},
		{"reset get", get, reset, status.ErrIO, status.NotApplied},
		{"timeout list", list, context.DeadlineExceeded, status.ErrTimeout, status.NotApplied},
		{"unknown put", put, errors.New("boom"), status.ErrIO, status.MaybeApplied},
	} {
		code, out := classify(c.a, c.err)
		if code != c.code || out != c.out {
			t.Errorf("%s: got %v/%v want %v/%v", c.name, code, out, c.code, c.out)
		}
	}
}
