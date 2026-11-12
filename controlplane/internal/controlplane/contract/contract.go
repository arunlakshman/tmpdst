// Package contract is the control plane's request and response vocabulary: the
// operations a broker can ask for (commit a segment, find segments, topics,
// consumer groups, offsets) and the answers. It is plain data.
//
// contract imports runtime. runtime must never import contract: runtime carries
// an operation only as the runtime.ControlPlaneOperation interface, which the
// types here implement.
package contract

import (
	"fmt"
)

// Offset is a partition log offset.
type Offset int64

// NewTopicPartition builds a TopicPartition.
func NewTopicPartition(topic string, partition int32) TopicPartition {
	return TopicPartition{Topic: topic, Partition: partition}
}

func (topicPartition TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", topicPartition.Topic, topicPartition.Partition)
}

// IsControlPlaneOperation marks CommitSegment as a control-plane operation.
func (CommitSegment) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks FindSegments as a control-plane operation.
func (FindSegments) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks DescribeTopics as a control-plane operation.
func (DescribeTopics) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks CreateTopic as a control-plane operation.
func (CreateTopic) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupInstall as a control-plane operation.
func (GroupInstall) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupAssign as a control-plane operation.
func (GroupAssign) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupRemove as a control-plane operation.
func (GroupRemove) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks OffsetCommit as a control-plane operation.
func (OffsetCommit) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks OffsetFetch as a control-plane operation.
func (OffsetFetch) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks DescribeGroup as a control-plane operation.
func (DescribeGroup) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks ListGroups as a control-plane operation.
func (ListGroups) IsControlPlaneOperation() {}

// IsControlPlaneResponse marks CommitSegmentResp as a control-plane response.
func (CommitSegmentResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks FindSegmentsResp as a control-plane response.
func (FindSegmentsResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks DescribeTopicsResp as a control-plane response.
func (DescribeTopicsResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks OffsetCommitResp as a control-plane response.
func (OffsetCommitResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks OffsetFetchResp as a control-plane response.
func (OffsetFetchResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks DescribeGroupResp as a control-plane response.
func (DescribeGroupResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks ListGroupsResp as a control-plane response.
func (ListGroupsResp) IsControlPlaneResponse() {}
