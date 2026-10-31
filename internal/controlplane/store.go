package controlplane

import (
	"context"
	"errors"

	"mds/internal/core"
)

// ErrUnavailable is the infrastructure error for throttling, overload and
// exhausted optimistic retries. The gRPC server maps it to codes.Unavailable.
var ErrUnavailable = errors.New("cp: unavailable")

// Store is the control-plane state machine behind the gRPC server. It must
// behave exactly like speccheck.ControlPlane (op for op) and be safe for
// concurrent use by many replicas. A business error is returned as a non-OK
// core.ErrCode with a nil error; error is infrastructure only.
type Store interface {
	CommitSegment(ctx context.Context, operation core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error)
	Lookup(ctx context.Context, operation core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error)
	DescribeTopics(ctx context.Context, operation core.DescribeTopics) (core.DescribeTopicsResp, error)
	CreateTopic(ctx context.Context, operation core.CreateTopic) (core.ErrCode, error)
}

// GroupStore is the consumer group part of the control plane store (slice
// 4a: the state and its fold, no gRPC yet). It is a separate interface so
// that Store keeps its four methods. A business error is a non-OK ErrCode
// with a nil error; error is infrastructure only.
type GroupStore interface {
	GroupInstall(ctx context.Context, operation core.GroupInstall) (core.ErrCode, error)
	GroupAssign(ctx context.Context, operation core.GroupAssign) (core.ErrCode, error)
	GroupRemove(ctx context.Context, operation core.GroupRemove) (core.ErrCode, error)
	OffsetCommit(ctx context.Context, operation core.OffsetCommit) (core.OffsetCommitResp, core.ErrCode, error)
	OffsetFetch(ctx context.Context, operation core.OffsetFetch) (core.OffsetFetchResp, error)
	DescribeGroup(ctx context.Context, operation core.DescribeGroup) (core.DescribeGroupResp, error)
	ListGroups(ctx context.Context, operation core.ListGroups) (core.ListGroupsResp, error)
}
