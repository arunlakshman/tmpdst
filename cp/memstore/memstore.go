// Package memstore is a cp.Store over the veritas spec behind a mutex, for
// local development and handler tests.
package memstore

import (
	"context"
	"sync"

	"manifester/core"
	"manifester/cp"
	"manifester/veritas"
)

// Store is an in-memory cp.Store backed by the veritas control-plane spec.
type Store struct {
	mu   sync.Mutex
	spec *veritas.ControlPlane
}

var _ cp.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store { return &Store{spec: veritas.NewControlPlane()} }

func (s *Store) exec(ctx context.Context, op core.CPOp) (core.CPDone, error) {
	if err := ctx.Err(); err != nil {
		return core.CPDone{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec.Exec(core.CPCall{Op: op}).(core.CPDone), nil
}

func execAs[R any](ctx context.Context, s *Store, op core.CPOp) (R, core.ErrCode, error) {
	d, err := s.exec(ctx, op)
	return core.RespAs[R](d), d.Err, err
}

// CommitSegment implements cp.Store.
func (s *Store) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	return execAs[core.CommitSegmentResp](ctx, s, op)
}

// Lookup implements cp.Store.
func (s *Store) Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	return execAs[core.LookupResp](ctx, s, op)
}

// DescribeTopics implements cp.Store.
func (s *Store) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.exec(ctx, op)
	return core.RespAs[core.DescribeTopicsResp](d), err
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
