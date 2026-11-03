package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

func toRPCError(code core.ErrCode, outcome core.Outcome) error {
	st := codes.Internal
	switch code {
	case core.ErrUnavailable:
		st = codes.Unavailable
	case core.ErrInvalid:
		st = codes.InvalidArgument
	case core.ErrNotFound:
		st = codes.NotFound
	case core.ErrConflict, core.ErrTopicExists:
		st = codes.AlreadyExists
	case core.ErrTimeout:
		st = codes.DeadlineExceeded
	case core.ErrTooLarge:
		st = codes.ResourceExhausted
	case core.ErrUnknownMember, core.ErrUnknownPartition:
		st = codes.NotFound
	case core.ErrIllegalGeneration, core.ErrRebalanceInProgress, core.ErrInconsistentGroupProtocol:
		st = codes.FailedPrecondition
	case core.OK:
		return nil
	}
	detail := &cpv1.ErrorDetail{
		Code:    wireboundary.FromCoreErr(code),
		Outcome: wireboundary.FromCoreOutcome(outcome),
	}
	s, err := status.New(st, code.String()).WithDetails(detail)
	if err != nil {
		return status.Error(st, code.String())
	}
	return s.Err()
}

// GetStatus implements ControlPlane.
func (n *Node) GetStatus(ctx context.Context, _ *cpv1.GetStatusRequest) (*cpv1.GetStatusResponse, error) {
	type st struct {
		role             string
		applied, logTail int64
		epoch, stepDowns uint64
		ready            bool
	}
	ch := make(chan st, 1)
	err := n.onLoop(ctx, func(m *replica.Replica) []core.Action {
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
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	select {
	case s := <-ch:
		if !s.ready {
			return nil, toRPCError(core.ErrUnavailable, core.NotApplied)
		}
		return &cpv1.GetStatusResponse{Role: s.role, AppliedSlot: s.applied, LogTail: s.logTail,
			Epoch: s.epoch, StepDowns: s.stepDowns}, nil
	case <-ctx.Done():
		return nil, toRPCError(core.ErrTimeout, core.NotApplied)
	}
}
