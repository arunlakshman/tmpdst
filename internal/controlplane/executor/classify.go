package executor

import (
	"context"
	"errors"
	"net"

	"github.com/manifesterio/mds/internal/core"
)

// Classify maps an executor error for a into ErrCode and Outcome.
func Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	if err == nil {
		return core.OK, core.Applied
	}
	maybe := core.NotApplied
	if changesData(a) {
		maybe = core.MaybeApplied
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return core.ErrTimeout, maybe
	}
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
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

func changesData(a core.IOCall) bool {
	switch a.(type) {
	case core.LogAppend, core.LogDeleteOld, core.ObjectPut, core.ObjectDelete:
		return true
	}
	return false
}
