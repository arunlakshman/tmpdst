// Package memorystore is an in-memory store over the speccheck spec behind a mutex, for
// local development and handler tests.
package memorystore

import (
	"context"
	"sync"

	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/speccheck"
)

// Store is an in-memory store backed by the speccheck control-plane spec.
type Store struct {
	mu   sync.Mutex
	spec *speccheck.ControlPlane
}

// New returns an empty store.
func New() *Store { return &Store{spec: speccheck.NewControlPlane()} }

func (s *Store) exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return core.ControlPlaneDone{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone), nil
}

func executeAs[R any](ctx context.Context, s *Store, operation core.ControlPlaneOperation) (R, core.ErrCode, error) {
	d, err := s.exec(ctx, operation)
	return core.ResponseAs[R](d), d.Err, err
}

// CommitSegment answers one store call.
func (s *Store) CommitSegment(ctx context.Context, operation core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	return executeAs[core.CommitSegmentResp](ctx, s, operation)
}

// Lookup answers one store call.
func (s *Store) Lookup(ctx context.Context, operation core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error) {
	return executeAs[core.FindSegmentsResp](ctx, s, operation)
}

// DescribeTopics answers one store call.
func (s *Store) DescribeTopics(ctx context.Context, operation core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.exec(ctx, operation)
	return core.ResponseAs[core.DescribeTopicsResp](d), err
}

// CreateTopic answers one store call.
func (s *Store) CreateTopic(ctx context.Context, operation core.CreateTopic) (core.ErrCode, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec.CreateTopicIf(operation.Name, operation.Partitions, operation.FailIfExists), nil
}
