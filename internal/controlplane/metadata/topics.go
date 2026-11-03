package metadata

import (
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/topics"
	"github.com/manifesterio/mds/internal/core"
)

// These methods are the typed calls on the topics module. Each one opens the
// transaction and calls the matching function of the topics package.

// AssignState says what Assigned knows about a command.
type AssignState = topics.AssignState

// Assigned results.
const (
	OffsetOwnerUnknown   = topics.OffsetOwnerUnknown
	OffsetOwnedElsewhere = topics.OffsetOwnedElsewhere
	OffsetOwnedHere      = topics.OffsetOwnedHere
)

// Commit applies a CommitSegment that sits at log slot slot.
func (a *Store) Commit(slot int64, operation core.CommitSegment) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.keyValue.Update(func(transaction kv.Transaction) { d = topics.Commit(transaction, slot, operation) })
	return d, err
}

// KnownCommit answers operation from the idempotency window when the key is
// already present. ok is false when the key is unknown (the caller must append).
func (a *Store) KnownCommit(operation core.CommitSegment) (core.ControlPlaneDone, bool, error) {
	var d core.ControlPlaneDone
	var ok bool
	err := a.keyValue.View(func(transaction kv.Transaction) { d, ok = topics.Known(transaction, operation) })
	return d, ok, err
}

// commitWithBases is the BugWriterOffsets apply path.
func (a *Store) commitWithBases(slot int64, operation core.CommitSegment, bases []core.Offset) core.ControlPlaneDone {
	var d core.ControlPlaneDone
	mustStore(a.keyValue.Update(func(transaction kv.Transaction) { d = topics.CommitWithBases(transaction, slot, operation, bases) }))
	return d
}

// Lookup answers a Lookup from the stored runs.
func (a *Store) Lookup(operation core.FindSegments) (core.ControlPlaneDone, error) {
	return a.Read(operation)
}

// Describe answers a DescribeTopics.
func (a *Store) Describe(operation core.DescribeTopics) (core.ControlPlaneDone, error) {
	return a.Read(operation)
}

// CreateTopic creates name with at least partitions partitions (never
// shrinks). ErrInvalid for an empty name or partitions <= 0.
func (a *Store) CreateTopic(name string, partitions int32) core.ErrCode {
	return a.CreateTopicIf(name, partitions, false)
}

// CreateTopicIf is CreateTopic. With failIfExists, an existing topic gives
// ErrTopicExists and nothing changes.
func (a *Store) CreateTopicIf(name string, partitions int32, failIfExists bool) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	code := core.OK
	mustStore(a.keyValue.Update(func(transaction kv.Transaction) {
		code = topics.Create(transaction, name, partitions, failIfExists)
	}))
	return code
}

// End is the next offset of topicPartition (0 if unknown).
func (a *Store) End(topicPartition core.TopicPartition) core.Offset {
	var end core.Offset
	mustStore(a.keyValue.View(func(transaction kv.Transaction) { end = topics.End(transaction, topicPartition) }))
	return end
}

// Trim forgets the idempotency records of commands at slots below slot and
// returns how many keys it dropped.
func (a *Store) Trim(slot int64) (int, error) {
	var dropped int
	err := a.keyValue.Update(func(transaction kv.Transaction) { dropped = topics.Trim(transaction, slot) })
	return dropped, err
}

// Window is the number of segment keys currently remembered.
func (a *Store) Window() int {
	var n int
	_ = a.keyValue.View(func(transaction kv.Transaction) { n = topics.Window(transaction) })
	return n
}

// Assigned reports the ranges operation was given if it is the first commit of
// its key at slot. An indexer uses it to write the run rows of a slot later.
func (a *Store) Assigned(slot int64, operation core.CommitSegment) ([]core.BatchRange, AssignState, error) {
	var rs []core.BatchRange
	var st AssignState
	err := a.keyValue.View(func(transaction kv.Transaction) { rs, st = topics.Assigned(transaction, slot, operation) })
	return rs, st, err
}

// OffsetFetch, DescribeGroup and ListGroups are the typed reads of the groups
// module.

// OffsetFetch answers an OffsetFetch.
func (a *Store) OffsetFetch(operation core.OffsetFetch) (core.ControlPlaneDone, error) {
	return a.Read(operation)
}

// DescribeGroup answers a DescribeGroup.
func (a *Store) DescribeGroup(operation core.DescribeGroup) (core.ControlPlaneDone, error) {
	return a.Read(operation)
}

// ListGroups answers a ListGroups.
func (a *Store) ListGroups(operation core.ListGroups) (core.ControlPlaneDone, error) {
	return a.Read(operation)
}
