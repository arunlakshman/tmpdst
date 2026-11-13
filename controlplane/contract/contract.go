// Package contract is the control plane's public vocabulary: the operations a
// broker can ask for (commit a segment, find segments, topics, consumer groups,
// offsets), the answers, and the small value types they use (ErrCode, Outcome,
// SegmentRef). It is plain data.
//
// Operation and Response are sealed sums: only this package can add a case,
// because the marker method is not exported. Every case has Tag and Fold, which
// write it into the trace hash with a fixed tag number (see fold.go).
//
// contract imports only dst. The control plane runtime imports contract, never
// the reverse. The broker imports contract to speak to the control plane.
package contract

import (
	"fmt"

	"github.com/manifesterio/mds/dst/machine"
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

// Operation is a control-plane operation as seen by a broker. The cases are the
// plain data types in this package that have an isOperation method.
//
// Brokers are stateless: there is no registration, epoch or fencing, and no
// idempotent producers. The control plane assigns offsets: the broker never
// proposes one. How a store stays correct under concurrent committers (a
// compare-and-set on the run base with an internal retry) is the store's
// business, not part of this contract.
//
// The wire protocol is gRPC (controlplane/api/controlplane/v1). These types are
// the in-process vocabulary on both sides of it.
type Operation interface {
	isOperation()
	// Tag is the frozen number of the case in the trace hash.
	Tag() uint64
	machine.Message
}

// Response is the answer to an Operation. The cases are the plain data types in
// this package that have an isResponse method.
type Response interface {
	isResponse()
	// Tag is the frozen number of the case in the trace hash.
	Tag() uint64
	machine.Message
}

// Done is the full answer of the control plane to one operation: the response
// (nil unless Err is OK, and nil for a write that has no response data), the
// error code and the outcome.
type Done struct {
	Resp    Response
	Err     ErrCode
	Outcome Outcome
}

// ResponseAs returns d.Resp asserted to T, or the zero value.
func ResponseAs[T any](d Done) T {
	r, _ := d.Resp.(T)
	return r
}

func (CommitSegment) isOperation()  {}
func (FindSegments) isOperation()   {}
func (DescribeTopics) isOperation() {}
func (CreateTopic) isOperation()    {}
func (GroupInstall) isOperation()   {}
func (GroupAssign) isOperation()    {}
func (GroupRemove) isOperation()    {}
func (OffsetCommit) isOperation()   {}
func (OffsetFetch) isOperation()    {}
func (DescribeGroup) isOperation()  {}
func (ListGroups) isOperation()     {}
func (GroupJoin) isOperation()      {}
func (GroupSync) isOperation()      {}
func (GroupHeartbeat) isOperation() {}
func (GroupLeave) isOperation()     {}
func (DescribeGroups) isOperation() {}

func (CommitSegmentResp) isResponse()  {}
func (FindSegmentsResp) isResponse()   {}
func (DescribeTopicsResp) isResponse() {}
func (OffsetCommitResp) isResponse()   {}
func (OffsetFetchResp) isResponse()    {}
func (DescribeGroupResp) isResponse()  {}
func (ListGroupsResp) isResponse()     {}
func (GroupJoinResp) isResponse()      {}
func (GroupSyncResp) isResponse()      {}
func (GroupLeaveResp) isResponse()     {}
func (DescribeGroupsResp) isResponse() {}
