package applier

import (
	"context"
	"sync"

	"manifester/core"
	"manifester/cp"
)

// Store is a cp.Store over an Applier with a local slot counter: a single
// node, no log. It is for the conformance suite and tools; the log-backed
// service (stage 2) applies commands at their real slots.
type Store struct {
	A *Applier

	mu   sync.Mutex
	next int64
}

var _ cp.Store = (*Store)(nil)

// CommitSegment implements cp.Store.
func (s *Store) CommitSegment(_ context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	s.mu.Lock()
	slot := s.next
	s.next++
	d, err := s.A.Commit(slot, op)
	s.mu.Unlock()
	resp, _ := d.Resp.(core.CommitSegmentResp)
	return resp, d.Err, err
}

// Lookup implements cp.Store.
func (s *Store) Lookup(_ context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	d, err := s.A.Lookup(op)
	resp, _ := d.Resp.(core.LookupResp)
	return resp, d.Err, err
}

// DescribeTopics implements cp.Store.
func (s *Store) DescribeTopics(_ context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.A.Describe(op)
	resp, _ := d.Resp.(core.DescribeTopicsResp)
	return resp, err
}

// CreateTopic implements cp.Store.
func (s *Store) CreateTopic(_ context.Context, name string, partitions int32) (core.ErrCode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.A.CreateTopic(name, partitions), nil
}
