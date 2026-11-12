package server

import (
	"context"
	"errors"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/wire"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The topics RPCs: CommitSegment, FindSegments, DescribeTopics and CreateTopic. Each
// one is a module operation. It converts the request, runs the operation with
// runOperation (a command goes to the log, a read is answered by the fold), and
// converts the answer.

// isCommand reports whether operation is written to the log. Any other
// operation is a read, answered by the fold.
func isCommand(operation runtime.ControlPlaneOperation) bool {
	_, logged := metadb.EntryOf(operation)
	return logged
}

// Exec runs one operation and is the only place that splits commands from reads.
// A command is sent to the replica as a ControlPlaneDeliver and answered after
// its slot applies. A read is sent as a LocalRead and answered at once by the
// fold of this node.
// A business error is the Err of the answer; the error result is for the run
// loop and the context. Node is the log and read side of every topics RPC and
// of OffsetFetch.
func (n *Node) Exec(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if _, logged := metadb.EntryOf(operation); logged {
		return n.ask(ctx, true, func(request runtime.RequestID) runtime.EventData {
			return runtime.ControlPlaneDeliver{From: serverFrom, Req: request, Op: operation}
		})
	}
	return n.ask(ctx, false, func(request runtime.RequestID) runtime.EventData {
		return runtime.LocalRead{From: serverFrom, Req: request, Op: operation}
	})
}

var _ runtime.OperationRunner = (*Node)(nil)

// runOperation runs one module operation for an RPC and maps a failure to the
// RPC error. A call that ends with its context may have been applied, so for a
// command it is MaybeApplied. A failed read changed nothing, so it is NotApplied.
func (n *Node) runOperation(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	d, err := n.Exec(ctx, operation)
	if err != nil {
		if !isCommand(operation) {
			return d, wire.ToRPCError(runtime.ErrIO, runtime.NotApplied)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return d, wire.ToRPCError(runtime.ErrTimeout, runtime.MaybeApplied)
		}
		return d, wire.ToRPCError(runtime.ErrIO, runtime.MaybeApplied)
	}
	return d, answerError(d)
}

// answerError is the RPC error of a business answer that failed, or nil.
func answerError(d runtime.ControlPlaneDone) error {
	if d.Err != runtime.OK {
		return wire.ToRPCError(d.Err, d.Outcome)
	}
	return nil
}

// moduleRPC runs one module operation and converts the answer, as R, to the wire
// type Out.
func moduleRPC[R, Out any](ctx context.Context, n *Node, operation runtime.ControlPlaneOperation, convert func(R) Out) (Out, error) {
	var zero Out
	d, err := n.runOperation(ctx, operation)
	if err != nil {
		return zero, err
	}
	return convert(runtime.ResponseAs[R](d)), nil
}

// CommitSegment implements ControlPlane.
func (n *Node) CommitSegment(ctx context.Context, request *cpv1.CommitSegmentRequest) (*cpv1.CommitSegmentResponse, error) {
	return moduleRPC(ctx, n, wire.CommitSegmentRequestToCore(request), wire.CommitSegmentResponseFromCore)
}

// FindSegments implements ControlPlane.
func (n *Node) FindSegments(ctx context.Context, request *cpv1.FindSegmentsRequest) (*cpv1.FindSegmentsResponse, error) {
	return moduleRPC(ctx, n, wire.FindSegmentsRequestToCore(request), wire.FindSegmentsResponseFromCore)
}

// DescribeTopics implements ControlPlane.
func (n *Node) DescribeTopics(ctx context.Context, request *cpv1.DescribeTopicsRequest) (*cpv1.DescribeTopicsResponse, error) {
	return moduleRPC(ctx, n, wire.DescribeTopicsRequestToCore(request), wire.DescribeTopicsResponseFromCore)
}

// CreateTopic implements ControlPlane.
func (n *Node) CreateTopic(ctx context.Context, request *cpv1.CreateTopicRequest) (*cpv1.CreateTopicResponse, error) {
	if request == nil {
		return nil, wire.ToRPCError(runtime.ErrInvalid, runtime.NotApplied)
	}
	operation := wire.CreateTopicRequestToCore(request)
	return moduleRPC(ctx, n, operation, func(runtime.ControlPlaneResponse) *cpv1.CreateTopicResponse { return &cpv1.CreateTopicResponse{} })
}
