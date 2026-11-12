package server

import (
	"context"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/wire"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// GetStatus implements ControlPlane.
func (n *Node) GetStatus(ctx context.Context, _ *cpv1.GetStatusRequest) (*cpv1.GetStatusResponse, error) {
	type st struct {
		role             string
		applied, logTail int64
		epoch, stepDowns uint64
		ready            bool
	}
	ch := make(chan st, 1)
	err := n.onLoop(ctx, func(m *replica.Replica) []any {
		role := "standby"
		if m.IsActive() {
			role = "active"
		}
		ch <- st{
			role: role, applied: int64(m.Applied()), logTail: int64(m.Applied()),
			epoch: m.Epoch(), stepDowns: m.StepDowns(), ready: m.Ready(),
		}
		return nil
	})
	if err != nil {
		return nil, wire.ToRPCError(runtime.ErrIO, runtime.NotApplied)
	}
	select {
	case s := <-ch:
		if !s.ready {
			return nil, wire.ToRPCError(runtime.ErrUnavailable, runtime.NotApplied)
		}
		return &cpv1.GetStatusResponse{Role: s.role, AppliedSlot: s.applied, LogTail: s.logTail,
			Epoch: s.epoch, StepDowns: s.stepDowns}, nil
	case <-ctx.Done():
		return nil, wire.ToRPCError(runtime.ErrTimeout, runtime.NotApplied)
	}
}
