package exec

import (
	"context"
	"errors"
	"net"

	"manifester/internal/core"
)

// Classify maps an executor error for a into ErrCode and Outcome.
func Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	if err == nil {
		return core.OK, core.Applied
	}
	maybe := core.NotApplied
	if mutating(a) {
		maybe = core.MaybeApplied
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return core.ErrTimeout, maybe
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return core.ErrTimeout, maybe
	}
	if errors.Is(err, ErrNotFound) {
		return core.ErrNotFound, core.NotApplied
	}
	if errors.Is(err, ErrInvalidSlot) {
		return core.ErrInvalid, core.NotApplied
	}
	return core.ErrIO, maybe
}

func mutating(a core.IOCall) bool {
	switch a.(type) {
	case core.LogAppend, core.LogDeleteOld, core.ObjectPut, core.ObjectDelete:
		return true
	}
	return false
}
