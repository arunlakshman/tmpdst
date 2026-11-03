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

// Exec runs one operation on the spec. A business error is the Err of the
// answer. The error result is for the context.
func (s *Store) Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return core.ControlPlaneDone{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if x, ok := operation.(core.CreateTopic); ok {
		return core.ControlPlaneDone{Err: s.spec.CreateTopicIf(x.Name, x.Partitions, x.FailIfExists)}, nil
	}
	return s.spec.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone), nil
}
