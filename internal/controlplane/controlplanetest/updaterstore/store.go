// Package updaterstore runs a metadata.Store as a single node with no
// log, for tests. The log-backed service applies commands at their real slots,
// not through this adapter.
package updaterstore

import (
	"context"
	"sync"

	"github.com/manifesterio/mds/internal/controlplane"
	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
)

// Store drives a Store with a local slot counter. It is a
// controlplane.GroupStore, and its Exec also takes the segment and topic
// operations that the conformance suite sends.
type Store struct {
	A *metadata.Store

	mu   sync.Mutex
	next int64
}

var _ controlplane.GroupStore = (*Store)(nil)

// Exec runs one operation. A business error is the Err of the answer. The error
// result is for the context and the store.
func (s *Store) Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return core.ControlPlaneDone{}, err
	}
	switch x := operation.(type) {
	case core.CommitSegment:
		s.mu.Lock()
		defer s.mu.Unlock()
		slot := s.next
		s.next++
		return s.A.Commit(slot, x)
	case core.FindSegments:
		return s.A.Lookup(x)
	case core.DescribeTopics:
		return s.A.Describe(x)
	case core.CreateTopic:
		s.mu.Lock()
		defer s.mu.Unlock()
		return core.ControlPlaneDone{Err: s.A.CreateTopicIf(x.Name, x.Partitions, x.FailIfExists)}, nil
	case core.GroupInstall, core.GroupAssign, core.GroupRemove, core.OffsetCommit:
		s.mu.Lock()
		defer s.mu.Unlock()
		entry, _ := metadata.GroupEntry(x)
		return s.A.Apply(0, entry), nil
	case core.OffsetFetch:
		return s.A.OffsetFetch(x)
	case core.DescribeGroup:
		return s.A.DescribeGroup(x)
	case core.ListGroups:
		return s.A.ListGroups(x)
	}
	return core.ControlPlaneDone{Err: core.ErrInvalid}, nil
}
