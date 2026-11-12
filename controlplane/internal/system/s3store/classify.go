package s3store

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"syscall"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/system"
)

// Classifier is the system.ErrorClassifier for Obj* actions (lesson 1). Only PUT and
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

var _ system.ErrorClassifier = Classifier{}

// Classify maps an S3 error to an error code and outcome.
func (Classifier) Classify(a runtime.IOCall, err error) (runtime.ErrCode, runtime.Outcome) {
	code, out := classify(a, err)
	slog.Debug("object call failed", "action", a, "err", err, "code", code, "outcome", out)
	return code, out
}

func classify(a runtime.IOCall, err error) (runtime.ErrCode, runtime.Outcome) {
	maybe := runtime.NotApplied
	switch a.(type) {
	case runtime.ObjectPut, runtime.ObjectDelete:
		maybe = runtime.MaybeApplied
	}
	if se, ok := errors.AsType[*Error](err); ok {
		return se.Code, runtime.NotApplied
	}
	if isDial(err) {
		return runtime.ErrUnavailable, runtime.NotApplied
	}
	switch apiCode(err) {
	case "SlowDown", "ServiceUnavailable", "RequestLimitExceeded", "Throttling", "ThrottlingException":
		return runtime.ErrUnavailable, runtime.NotApplied
	}
	if re, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		switch st := re.HTTPStatusCode(); {
		case st == http.StatusServiceUnavailable || st == http.StatusTooManyRequests:
			return runtime.ErrUnavailable, runtime.NotApplied
		case st >= 400 && st < 500:
			return runtime.ErrIO, runtime.NotApplied
		}
		return runtime.ErrIO, maybe
	}
	ne, isNet := errors.AsType[net.Error](err)
	if errors.Is(err, context.DeadlineExceeded) || (isNet && ne.Timeout()) {
		return runtime.ErrTimeout, maybe
	}
	return runtime.ErrIO, maybe
}

// isDial reports a failure to establish a connection (nothing was sent).
func isDial(err error) bool {
	_, isDNS := errors.AsType[*net.DNSError](err)
	operation, isOp := errors.AsType[*net.OpError](err)
	switch {
	case isDNS:
		return true
	case isOp && operation.Op == "dial":
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH)
}
