package cpclient

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"syscall"

	"manifester/core"
	"manifester/sys"
)

// Classifier is the sys.Classifier for CPCall (lesson 1). A mutating op
// (CommitSegment) whose request may have reached the
// server and whose reply is lost or unusable is MaybeApplied; only failures
// that prove nothing was sent (dial, DNS, 503) are NotApplied. Reads are
// always NotApplied.
//
//	business error (*Error)                  its code            NotApplied
//	503, 429                                 ErrUnavailable      NotApplied
//	dial/DNS error, no connection obtained   ErrUnavailable      NotApplied
//	other 4xx                                ErrIO               NotApplied
//	not sent, not a dial error (ctx done)   ErrTimeout or ErrIO NotApplied
//	timeout / deadline, request sent         ErrTimeout          MaybeApplied (mutating)
//	reset, EOF, 5xx != 503, bad reply        ErrIO               MaybeApplied (mutating)
//	anything unknown                         ErrIO               MaybeApplied (mutating)
type Classifier struct{}

var _ sys.Classifier = Classifier{}

func mutating(a core.IOAction) bool {
	call, ok := a.(core.CPCall)
	if !ok {
		return true // not ours to judge: be conservative
	}
	switch call.Op.(type) {
	case core.Lookup, core.DescribeTopics:
		return false
	}
	return true
}

// Classify maps a failed cp call to its error code and outcome (logged at debug).
func (Classifier) Classify(a core.IOAction, err error) (core.ErrCode, core.Outcome) {
	code, out := classify(a, err)
	slog.Debug("cp call failed", "action", a, "err", err, "code", code, "outcome", out)
	return code, out
}

func classify(a core.IOAction, err error) (core.ErrCode, core.Outcome) {
	maybe := core.NotApplied
	if mutating(a) {
		maybe = core.MaybeApplied
	}
	if be, ok := errors.AsType[*Error](err); ok {
		return be.Code, core.NotApplied
	}
	if ce, ok := errors.AsType[*CallError](err); ok {
		return classifyCall(ce, maybe)
	}
	return timeoutOrIO(err), maybe
}

func classifyCall(ce *CallError, maybe core.Outcome) (core.ErrCode, core.Outcome) {
	switch {
	case ce.Status == http.StatusServiceUnavailable || ce.Status == http.StatusTooManyRequests:
		return core.ErrUnavailable, core.NotApplied
	case ce.Status >= 400 && ce.Status < 500:
		return core.ErrIO, core.NotApplied
	case ce.Status != 0:
		return core.ErrIO, maybe
	case !ce.Sent:
		return classifyNotSent(ce.Err), core.NotApplied
	}
	return timeoutOrIO(ce.Err), maybe
}

func classifyNotSent(err error) core.ErrCode {
	if isDial(err) {
		return core.ErrUnavailable
	}
	return timeoutOrIO(err)
}

func timeoutOrIO(err error) core.ErrCode {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return core.ErrTimeout
	}
	return core.ErrIO
}

// isDial reports a failure to establish a connection (nothing was sent).
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
