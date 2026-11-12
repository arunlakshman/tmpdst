package executor

import (
	"context"
	"errors"
	"net"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Classify maps an executor error for a into ErrCode and Outcome.
func Classify(a runtime.IOCall, err error) (runtime.ErrCode, runtime.Outcome) {
	if err == nil {
		return runtime.OK, runtime.Applied
	}
	maybe := runtime.NotApplied
	if changesData(a) {
		maybe = runtime.MaybeApplied
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return runtime.ErrTimeout, maybe
	}
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		return runtime.ErrTimeout, maybe
	}
	if errors.Is(err, ErrNotFound) {
		return runtime.ErrNotFound, runtime.NotApplied
	}
	if errors.Is(err, ErrInvalidSlot) {
		return runtime.ErrInvalid, runtime.NotApplied
	}
	return runtime.ErrIO, maybe
}

func changesData(a runtime.IOCall) bool {
	switch a.(type) {
	case runtime.LogAppend, runtime.LogDeleteOld, runtime.LogClaimEpoch, runtime.ObjectPut, runtime.ObjectDelete:
		return true
	}
	return false
}
