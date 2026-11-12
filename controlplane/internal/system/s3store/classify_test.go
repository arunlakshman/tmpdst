package s3store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

type apiError struct{ code string }

func (e apiError) Error() string                 { return e.code }
func (e apiError) ErrorCode() string             { return e.code }
func (e apiError) ErrorMessage() string          { return e.code }
func (e apiError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestClassify(t *testing.T) {
	dial := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	put, get, del, list := runtime.ObjectPut{}, runtime.ObjectGet{}, runtime.ObjectDelete{}, runtime.ObjectList{}
	for _, c := range []struct {
		name string
		a    runtime.IOCall
		err  error
		code runtime.ErrCode
		out  runtime.Outcome
	}{
		{"notfound", get, fail(runtime.ErrNotFound), runtime.ErrNotFound, runtime.NotApplied},
		{"notfound delete", del, fmt.Errorf("w: %w", fail(runtime.ErrNotFound)), runtime.ErrNotFound, runtime.NotApplied},
		{"dial put", put, fmt.Errorf("w: %w", dial), runtime.ErrUnavailable, runtime.NotApplied},
		{"slowdown put", put, apiError{"SlowDown"}, runtime.ErrUnavailable, runtime.NotApplied},
		{"timeout put", put, context.DeadlineExceeded, runtime.ErrTimeout, runtime.MaybeApplied},
		{"timeout delete", del, context.DeadlineExceeded, runtime.ErrTimeout, runtime.MaybeApplied},
		{"reset put", put, reset, runtime.ErrIO, runtime.MaybeApplied},
		{"reset delete", del, reset, runtime.ErrIO, runtime.MaybeApplied},
		{"timeout get", get, context.DeadlineExceeded, runtime.ErrTimeout, runtime.NotApplied},
		{"reset get", get, reset, runtime.ErrIO, runtime.NotApplied},
		{"timeout list", list, context.DeadlineExceeded, runtime.ErrTimeout, runtime.NotApplied},
		{"unknown put", put, errors.New("boom"), runtime.ErrIO, runtime.MaybeApplied},
	} {
		code, out := classify(c.a, c.err)
		if code != c.code || out != c.out {
			t.Errorf("%s: got %v/%v want %v/%v", c.name, code, out, c.code, c.out)
		}
	}
}
