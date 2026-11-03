package metadata

import (
	"github.com/manifesterio/mds/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/core"
)

// The consumer group state is kept by groups/storage, in the same key-value
// store and the same log order as the topics. This file only connects them.

// partitionCount is how many partitions topic has (0 when it does not exist).
func partitionCount(transaction kv.Transaction, topic string) int64 {
	return readVarint(transaction.Get(topicKey(topic)))
}

// groupWrite runs one group write in its own transaction.
func (a *Store) groupWrite(operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.keyValue.Update(func(transaction kv.Transaction) { d = storage.Write(transaction, operation, partitionCount) })
	return d, err
}

// view answers one read in a read transaction.
func (a *Store) view(fn func(kv.Transaction) core.ControlPlaneDone) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.keyValue.View(func(transaction kv.Transaction) { d = fn(transaction) })
	return d, err
}

// OffsetFetch answers an OffsetFetch.
func (a *Store) OffsetFetch(operation core.OffsetFetch) (core.ControlPlaneDone, error) {
	return a.view(func(transaction kv.Transaction) core.ControlPlaneDone {
		return storage.OffsetFetch(transaction, operation)
	})
}

// DescribeGroup answers a DescribeGroup.
func (a *Store) DescribeGroup(operation core.DescribeGroup) (core.ControlPlaneDone, error) {
	return a.view(func(transaction kv.Transaction) core.ControlPlaneDone {
		return storage.DescribeGroup(transaction, operation)
	})
}

// ListGroups answers a ListGroups.
func (a *Store) ListGroups(operation core.ListGroups) (core.ControlPlaneDone, error) {
	return a.view(func(transaction kv.Transaction) core.ControlPlaneDone {
		return storage.ListGroups(transaction, operation)
	})
}
