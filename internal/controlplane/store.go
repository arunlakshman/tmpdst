package controlplane

import (
	"context"
	"errors"

	"manifester/internal/core"
)

// ErrUnavailable is the infrastructure error for throttling, overload and
// exhausted optimistic retries. The gRPC server maps it to codes.Unavailable.
var ErrUnavailable = errors.New("cp: unavailable")

// Store is the control-plane state machine behind the gRPC server. It must
// behave exactly like speccheck.ControlPlane (op for op) and be safe for
// concurrent use by many replicas. A business error is returned as a non-OK
// core.ErrCode with a nil error; error is infrastructure only.
type Store interface {
	CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error)
	Lookup(ctx context.Context, op core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error)
	DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error)
	CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error)
}
