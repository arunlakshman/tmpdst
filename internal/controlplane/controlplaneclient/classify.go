package controlplaneclient

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"syscall"

	"google.golang.org/grpc/codes"

	"manifester/internal/core"
	"manifester/internal/system"
)

// Classifier is the system.ErrorClassifier for ControlPlaneCall (lesson 1). A mutating op
// (CommitSegment) whose request may have reached the server and whose reply
// is lost or unusable is MaybeApplied; only failures that prove nothing was
// sent (dial, Unavailable) are NotApplied. Reads are always NotApplied.
// Cancel/deadline without a prior success is ErrTimeout + MaybeApplied
// (Appendix E).
type Classifier struct{}

var _ system.ErrorClassifier = Classifier{}

func changesData(a core.IOCall) bool {
	call, ok := a.(core.ControlPlaneCall)
	if !ok {
		return true
	}
	switch call.Op.(type) {
	case core.FindSegments, core.DescribeTopics:
		return false
	}
	return true
}

// Classify maps a failed cp call to its error code and outcome.
func (Classifier) Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	code, out := classify(a, err)
	slog.Debug("cp call failed", "action", a, "err", err, "code", code, "outcome", out)
	return code, out
}

func classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	maybe := core.NotApplied
	if changesData(a) {
		maybe = core.MaybeApplied
	}
	if be, ok := errors.AsType[*Error](err); ok {
		out := be.Outcome
		if out == 0 && be.Code != core.OK {
			out = core.NotApplied
		}
		return be.Code, out
	}
	if ce, ok := errors.AsType[*CallError](err); ok {
		return classifyCall(ce, maybe)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return core.ErrTimeout, maybe
	}
	return isTimeoutOrIOError(err), maybe
}

func classifyCall(ce *CallError, maybe core.Outcome) (core.ErrCode, core.Outcome) {
	switch {
	case ce.Code == codes.Unavailable:
		return core.ErrUnavailable, core.NotApplied
	case ce.Code == codes.InvalidArgument:
		return core.ErrInvalid, core.NotApplied
	case ce.Code == codes.Canceled || ce.Code == codes.DeadlineExceeded:
		return core.ErrTimeout, maybe
	case !ce.Sent:
		return classifyBeforeSend(ce.Err), core.NotApplied
	}
	return isTimeoutOrIOError(ce.Err), maybe
}

func classifyBeforeSend(err error) core.ErrCode {
	if isDial(err) {
		return core.ErrUnavailable
	}
	return isTimeoutOrIOError(err)
}

func isTimeoutOrIOError(err error) core.ErrCode {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return core.ErrTimeout
	}
	return core.ErrIO
}

func isDial(err error) bool {
	var dns *net.DNSError
	var op *net.OpError
	switch {
	case errors.As(err, &dns):
		return true
	case errors.As(err, &op) && op.Op == "dial":
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH)
}
