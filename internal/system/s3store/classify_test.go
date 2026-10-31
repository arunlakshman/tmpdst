package s3store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/aws/smithy-go"

	"mds/internal/core"
)

type apiError struct{ code string }

func (e apiError) Error() string                 { return e.code }
func (e apiError) ErrorCode() string             { return e.code }
func (e apiError) ErrorMessage() string          { return e.code }
func (e apiError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestClassify(t *testing.T) {
	dial := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	put, get, del, list := core.ObjectPut{}, core.ObjectGet{}, core.ObjectDelete{}, core.ObjectList{}
	for _, c := range []struct {
		name string
		a    core.IOCall
		err  error
		code core.ErrCode
		out  core.Outcome
	}{
		{"notfound", get, fail(core.ErrNotFound), core.ErrNotFound, core.NotApplied},
		{"notfound delete", del, fmt.Errorf("w: %w", fail(core.ErrNotFound)), core.ErrNotFound, core.NotApplied},
		{"dial put", put, fmt.Errorf("w: %w", dial), core.ErrUnavailable, core.NotApplied},
		{"slowdown put", put, apiError{"SlowDown"}, core.ErrUnavailable, core.NotApplied},
		{"timeout put", put, context.DeadlineExceeded, core.ErrTimeout, core.MaybeApplied},
		{"timeout delete", del, context.DeadlineExceeded, core.ErrTimeout, core.MaybeApplied},
		{"reset put", put, reset, core.ErrIO, core.MaybeApplied},
		{"reset delete", del, reset, core.ErrIO, core.MaybeApplied},
		{"timeout get", get, context.DeadlineExceeded, core.ErrTimeout, core.NotApplied},
		{"reset get", get, reset, core.ErrIO, core.NotApplied},
		{"timeout list", list, context.DeadlineExceeded, core.ErrTimeout, core.NotApplied},
		{"unknown put", put, errors.New("boom"), core.ErrIO, core.MaybeApplied},
	} {
		code, out := classify(c.a, c.err)
		if code != c.code || out != c.out {
			t.Errorf("%s: got %v/%v want %v/%v", c.name, code, out, c.code, c.out)
		}
	}
}
