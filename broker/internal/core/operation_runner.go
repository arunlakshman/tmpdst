package core

import "context"

// OperationRunner runs one control-plane operation and returns its answer. It is
// the one shape that the group coordinator's store, the control-plane node and
// the test stores all share.
//
// A command (GroupInstall, GroupAssign, GroupRemove, OffsetCommit, CommitSegment,
// CreateTopic) is written to the log. A read (OffsetFetch, DescribeGroup,
// ListGroups, FindSegments, DescribeTopics) is not logged. A business error is the
// Err of the answer; the error result is for infrastructure only (the context,
// the log, the store).
type OperationRunner interface {
	Exec(ctx context.Context, operation ControlPlaneOperation) (ControlPlaneDone, error)
}
