package logsm

import (
	"context"

	"manifester/core"
	"manifester/cp"
)

// Store is a cp.Store over a Replica and a Committer: writes are appended as
// commands and answered from the applied state, reads are fresh (tail read,
// apply, answer from memory). It is for tests and the model; no binary uses it.
type Store struct {
	R *Replica
	C *Committer
}

var _ cp.Store = (*Store)(nil)

// NewMemStore is a one-replica store over a fresh in-memory log (test
// constructor).
func NewMemStore(k int) *Store {
	r := NewReplica(Config{Log: NewMemLog(), Writer: "w0"})
	return &Store{R: r, C: NewCommitter(r, k)}
}

// CommitSegment implements cp.Store.
func (s *Store) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	d, err := s.C.Do(ctx, Entry{Kind: KindCommit, Commit: op})
	if err != nil {
		return core.CommitSegmentResp{}, core.OK, err
	}
	resp, _ := d.Resp.(core.CommitSegmentResp)
	return resp, d.Err, nil
}

// Lookup implements cp.Store.
func (s *Store) Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	d, err := s.R.Read(ctx, op)
	if err != nil {
		return core.LookupResp{}, core.OK, err
	}
	resp, _ := d.Resp.(core.LookupResp)
	return resp, d.Err, nil
}

// DescribeTopics implements cp.Store.
func (s *Store) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	d, err := s.R.Read(ctx, op)
	resp, _ := d.Resp.(core.DescribeTopicsResp)
	return resp, err
}

// CreateTopic implements cp.Store.
func (s *Store) CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error) {
	d, err := s.C.Do(ctx, Entry{Kind: KindCreateTopic, Topic: name, Parts: partitions})
	return d.Err, err
}
