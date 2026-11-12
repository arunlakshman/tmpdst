package client

import (
	"context"
	"errors"
	"net"
	"syscall"

	"google.golang.org/grpc/codes"

	"github.com/manifesterio/mds/controlplane/contract"
)

// Classify maps a failed control plane call to its error code and outcome (lesson 1).
// op is the operation that was called. nil, or an operation that is not known, counts as
// a write.
//
// A mutating op (CommitSegment) whose request may have reached the server and whose reply
// is lost or unusable is MaybeApplied; only failures that prove nothing was
// sent (dial, Unavailable) are NotApplied. Reads are always NotApplied.
// Cancel/deadline without a prior success is ErrTimeout + MaybeApplied
// (Appendix E).
func Classify(op contract.Operation, err error) (contract.ErrCode, contract.Outcome) {
	return classify(op, err)
}

func changesData(op contract.Operation) bool {
	switch op.(type) {
	case contract.FindSegments, contract.DescribeTopics, contract.OffsetFetch, contract.DescribeGroups, contract.ListGroups:
		return false
	}
	return true
}

func classify(op contract.Operation, err error) (contract.ErrCode, contract.Outcome) {
	maybe := contract.NotApplied
	if changesData(op) {
		maybe = contract.MaybeApplied
	}
	if be, ok := errors.AsType[*Error](err); ok {
		out := be.Outcome
		if out == 0 && be.Code != contract.OK {
			out = contract.NotApplied
		}
		return be.Code, out
	}
	if ce, ok := errors.AsType[*CallError](err); ok {
		return classifyCall(ce, maybe)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return contract.ErrTimeout, maybe
	}
	return isTimeoutOrIOError(err), maybe
}

func classifyCall(ce *CallError, maybe contract.Outcome) (contract.ErrCode, contract.Outcome) {
	switch {
	case ce.Code == codes.Unavailable:
		return contract.ErrUnavailable, contract.NotApplied
	case ce.Code == codes.InvalidArgument:
		return contract.ErrInvalid, contract.NotApplied
	case ce.Code == codes.Canceled || ce.Code == codes.DeadlineExceeded:
		return contract.ErrTimeout, maybe
	case !ce.Sent:
		return classifyBeforeSend(ce.Err), contract.NotApplied
	}
	return isTimeoutOrIOError(ce.Err), maybe
}

func classifyBeforeSend(err error) contract.ErrCode {
	if isDial(err) {
		return contract.ErrUnavailable
	}
	return isTimeoutOrIOError(err)
}

func isTimeoutOrIOError(err error) contract.ErrCode {
	ne, isNetErr := errors.AsType[net.Error](err)
	if errors.Is(err, context.DeadlineExceeded) || (isNetErr && ne.Timeout()) {
		return contract.ErrTimeout
	}
	return contract.ErrIO
}

func isDial(err error) bool {
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	if operation, ok := errors.AsType[*net.OpError](err); ok && operation.Op == "dial" {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH)
}
