// Package metadatastore runs a metadata.Store as a single node with no
// log, for tests. The log-backed service applies commands at their real slots,
// not through this adapter.
package metadatastore

import (
	"context"
	"sync"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

// Store drives a metadata.Store with a local slot counter. It is a
// groups.Store (the groups tests check it), and its Exec also takes the segment and topic
// operations that the conformance suite sends.
type Store struct {
	A *metadata.Store

	mu   sync.Mutex
	next int64
}

// Exec runs one operation. A business error is the Err of the answer. The error
// result is for the context and the store. A command gets the next local slot.
func (s *Store) Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return core.ControlPlaneDone{}, err
	}
	switch s.A.Mode(operation) {
	case module.Logged:
		s.mu.Lock()
		defer s.mu.Unlock()
		slot := s.next
		s.next++
		return s.A.Write(slot, operation)
	case module.ReadOnly:
		return s.A.Read(operation)
	}
	return core.ControlPlaneDone{Err: core.ErrInvalid}, nil
}
