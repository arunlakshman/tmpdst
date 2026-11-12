package runtime

import "context" //determinismcheck:ok context is only a parameter type here; the implementations are outside the deterministic code

// OperationRunner runs one control-plane operation and returns its answer. It is
// the one shape that the control-plane node and the test stores share.
//
// A command (GroupInstall, GroupAssign, GroupRemove, OffsetCommit, CommitSegment,
// CreateTopic) is written to the log. A read (OffsetFetch, DescribeGroup,
// ListGroups, FindSegments, DescribeTopics) is not logged. A business error is the
// Err of the answer; the error result is for infrastructure only (the context,
// the log, the store).
type OperationRunner interface {
	Exec(ctx context.Context, operation ControlPlaneOperation) (ControlPlaneDone, error)
}
