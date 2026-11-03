// Package updaterstore adapts a stateupdater.StateUpdater to the test-only store
// interfaces. It is test support: the log-backed service applies commands at
// their real slots, not through this adapter.
package updaterstore

import (
	"context"
	"sync"

	"github.com/manifesterio/mds/internal/controlplane"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
)

// Store drives a StateUpdater with a local slot counter: a single node, no log.
// It is a controlplane.GroupStore, and it has the four segment and topic calls
// that the conformance suite uses.
type Store struct {
	A *stateupdater.StateUpdater

	mu   sync.Mutex
	next int64
}

var _ controlplane.GroupStore = (*Store)(nil)

// CommitSegment applies a commit at the next local slot.
func (s *Store) CommitSegment(_ context.Context, operation core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	s.mu.Lock()
	slot := s.next
	s.next++
	d, err := s.A.Commit(slot, operation)
	s.mu.Unlock()
	return core.ResponseAs[core.CommitSegmentResp](d), d.Err, err
}

// Lookup answers a Lookup.
func (s *Store) Lookup(_ context.Context, operation core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error) {
	d, err := s.A.Lookup(operation)
	return core.ResponseAs[core.FindSegmentsResp](d), d.Err, err
}

// DescribeTopics answers a DescribeTopics.
func (s *Store) DescribeTopics(_ context.Context, operation core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.A.Describe(operation)
	return core.ResponseAs[core.DescribeTopicsResp](d), err
}

// CreateTopic creates a topic.
func (s *Store) CreateTopic(_ context.Context, operation core.CreateTopic) (core.ErrCode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.A.CreateTopicIf(operation.Name, operation.Partitions, operation.FailIfExists), nil
}

// groupWrite runs one group write under the store lock, like CreateTopic.
func (s *Store) groupWrite(operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := stateupdater.GroupEntry(operation)
	if !ok {
		return core.ControlPlaneDone{Err: core.ErrInvalid}, nil
	}
	return s.A.Apply(0, entry), nil
}

// GroupInstall implements controlplane.GroupStore.
func (s *Store) GroupInstall(_ context.Context, operation core.GroupInstall) (core.ErrCode, error) {
	d, err := s.groupWrite(operation)
	return d.Err, err
}

// GroupAssign implements controlplane.GroupStore.
func (s *Store) GroupAssign(_ context.Context, operation core.GroupAssign) (core.ErrCode, error) {
	d, err := s.groupWrite(operation)
	return d.Err, err
}

// GroupRemove implements controlplane.GroupStore.
func (s *Store) GroupRemove(_ context.Context, operation core.GroupRemove) (core.ErrCode, error) {
	d, err := s.groupWrite(operation)
	return d.Err, err
}

// OffsetCommit implements controlplane.GroupStore.
func (s *Store) OffsetCommit(_ context.Context, operation core.OffsetCommit) (core.OffsetCommitResp, core.ErrCode, error) {
	d, err := s.groupWrite(operation)
	return core.ResponseAs[core.OffsetCommitResp](d), d.Err, err
}

// OffsetFetch implements controlplane.GroupStore.
func (s *Store) OffsetFetch(_ context.Context, operation core.OffsetFetch) (core.OffsetFetchResp, error) {
	d, err := s.A.OffsetFetch(operation)
	return core.ResponseAs[core.OffsetFetchResp](d), err
}

// DescribeGroup implements controlplane.GroupStore.
func (s *Store) DescribeGroup(_ context.Context, operation core.DescribeGroup) (core.DescribeGroupResp, error) {
	d, err := s.A.DescribeGroup(operation)
	return core.ResponseAs[core.DescribeGroupResp](d), err
}

// ListGroups implements controlplane.GroupStore.
func (s *Store) ListGroups(_ context.Context, operation core.ListGroups) (core.ListGroupsResp, error) {
	d, err := s.A.ListGroups(operation)
	return core.ResponseAs[core.ListGroupsResp](d), err
}
