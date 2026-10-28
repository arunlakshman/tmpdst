// Package memstore is a cp.Store over the veritas spec behind a mutex, for
// local development and handler tests.
package memstore

import (
	"context"
	"sync"

	cp "manifester/internal/controlplane"
	"manifester/internal/core"
	"manifester/internal/verify/veritas"
)

// Store is an in-memory cp.Store backed by the veritas control-plane spec.
type Store struct {
	mu   sync.Mutex
	spec *veritas.ControlPlane
}

var _ cp.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store { return &Store{spec: veritas.NewControlPlane()} }

func (s *Store) exec(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return core.ControlPlaneDone{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec.Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone), nil
}

func execAs[R any](ctx context.Context, s *Store, op core.ControlPlaneOperation) (R, core.ErrCode, error) {
	d, err := s.exec(ctx, op)
	return core.ResponseAs[R](d), d.Err, err
}

// CommitSegment implements cp.Store.
func (s *Store) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	return execAs[core.CommitSegmentResp](ctx, s, op)
}

// Lookup implements cp.Store.
func (s *Store) Lookup(ctx context.Context, op core.FindSegments) (core.FindSegmentsResp, core.ErrCode, error) {
	return execAs[core.FindSegmentsResp](ctx, s, op)
}

// DescribeTopics implements cp.Store.
func (s *Store) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.exec(ctx, op)
	return core.ResponseAs[core.DescribeTopicsResp](d), err
}

// CreateTopic implements cp.Store.
func (s *Store) CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec.CreateTopic(name, partitions), nil
}
