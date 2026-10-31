package stateupdater

import (
	"context"
	"sync"

	"manifester/internal/controlplane"
	"manifester/internal/core"
)

// Store is a controlplane.Store over a StateUpdater with a local slot counter: a single
// node, no log. It is for the conformance suite and tools; the log-backed
// service (stage 2) applies commands at their real slots.
type Store struct {
	A *StateUpdater

	mu   sync.Mutex
	next int64
}

var (
	_ controlplane.Store      = (*Store)(nil)
	_ controlplane.GroupStore = (*Store)(nil)
)

// CommitSegment implements controlplane.Store.
func (s *Store) CommitSegment(_ context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	s.mu.Lock()
	slot := s.next
	s.next++
	d, err := s.A.Commit(slot, op)
	s.mu.Unlock()
	return core.ResponseAs[core.CommitSegmentResp](d), d.Err, err
}

// Lookup implements controlplane.Store.
func (s *Store) Lookup(_ context.Context, op core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error) {
	d, err := s.A.Lookup(op)
	return core.ResponseAs[core.FindSegmentsResp](d), d.Err, err
}

// DescribeTopics implements controlplane.Store.
func (s *Store) DescribeTopics(_ context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.A.Describe(op)
	return core.ResponseAs[core.DescribeTopicsResp](d), err
}

// CreateTopic implements controlplane.Store.
func (s *Store) CreateTopic(_ context.Context, op core.CreateTopic) (core.ErrCode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.A.CreateTopicIf(op.Name, op.Partitions, op.FailIfExists), nil
}

// groupWrite runs one group write under the store lock, like CreateTopic.
func (s *Store) groupWrite(op core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.A.groupWrite(op)
}

// GroupInstall implements controlplane.GroupStore.
func (s *Store) GroupInstall(_ context.Context, op core.GroupInstall) (core.ErrCode, error) {
	d, err := s.groupWrite(op)
	return d.Err, err
}

// GroupAssign implements controlplane.GroupStore.
func (s *Store) GroupAssign(_ context.Context, op core.GroupAssign) (core.ErrCode, error) {
	d, err := s.groupWrite(op)
	return d.Err, err
}

// GroupRemove implements controlplane.GroupStore.
func (s *Store) GroupRemove(_ context.Context, op core.GroupRemove) (core.ErrCode, error) {
	d, err := s.groupWrite(op)
	return d.Err, err
}

// OffsetCommit implements controlplane.GroupStore.
func (s *Store) OffsetCommit(_ context.Context, op core.OffsetCommit) (core.OffsetCommitResp, core.ErrCode, error) {
	d, err := s.groupWrite(op)
	return core.ResponseAs[core.OffsetCommitResp](d), d.Err, err
}

// OffsetFetch implements controlplane.GroupStore.
func (s *Store) OffsetFetch(_ context.Context, op core.OffsetFetch) (core.OffsetFetchResp, error) {
	d, err := s.A.OffsetFetch(op)
	return core.ResponseAs[core.OffsetFetchResp](d), err
}

// DescribeGroup implements controlplane.GroupStore.
func (s *Store) DescribeGroup(_ context.Context, op core.DescribeGroup) (core.DescribeGroupResp, error) {
	d, err := s.A.DescribeGroup(op)
	return core.ResponseAs[core.DescribeGroupResp](d), err
}

// ListGroups implements controlplane.GroupStore.
func (s *Store) ListGroups(_ context.Context, op core.ListGroups) (core.ListGroupsResp, error) {
	d, err := s.A.ListGroups(op)
	return core.ResponseAs[core.ListGroupsResp](d), err
}
