package s3store

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"syscall"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"manifester/internal/core"
	"manifester/internal/sys"
)

// Classifier is the sys.ErrorClassifier for Obj* actions (lesson 1). Only PUT and
// DELETE mutate; GET and LIST never leave side effects.
//
//	*Error (truthful business error)          its code          NotApplied
//	SlowDown, 503, 429                        ErrUnavailable    NotApplied
//	dial/DNS error, no connection obtained    ErrUnavailable    NotApplied
//	other 4xx                                 ErrIO             NotApplied
//	timeout or deadline                       ErrTimeout        MaybeApplied (PUT, DELETE)
//	reset, EOF, 5xx != 503, anything unknown  ErrIO             MaybeApplied (PUT, DELETE)
//
// GET and LIST failures are always NotApplied with the same codes.
type Classifier struct{}

var _ sys.ErrorClassifier = Classifier{}

// Classify maps an S3 error to an error code and outcome.
func (Classifier) Classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	code, out := classify(a, err)
	slog.Debug("object call failed", "action", a, "err", err, "code", code, "outcome", out)
	return code, out
}

func classify(a core.IOCall, err error) (core.ErrCode, core.Outcome) {
	maybe := core.NotApplied
	switch a.(type) {
	case core.ObjectPut, core.ObjectDelete:
		maybe = core.MaybeApplied
	}
	if se, ok := errors.AsType[*Error](err); ok {
		return se.Code, core.NotApplied
	}
	if isDial(err) {
		return core.ErrUnavailable, core.NotApplied
	}
	switch apiCode(err) {
	case "SlowDown", "ServiceUnavailable", "RequestLimitExceeded", "Throttling", "ThrottlingException":
		return core.ErrUnavailable, core.NotApplied
	}
	if re, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		switch st := re.HTTPStatusCode(); {
		case st == http.StatusServiceUnavailable || st == http.StatusTooManyRequests:
			return core.ErrUnavailable, core.NotApplied
		case st >= 400 && st < 500:
			return core.ErrIO, core.NotApplied
		}
		return core.ErrIO, maybe
	}
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return core.ErrTimeout, maybe
	}
	return core.ErrIO, maybe
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
