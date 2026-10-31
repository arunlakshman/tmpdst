package controlplanetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	cp "manifester/internal/controlplane"
	"manifester/internal/controlplane/controlplaneclient"
	"manifester/internal/controlplane/grpctestserver"
	"manifester/internal/controlplane/memorystore"
	"manifester/internal/core"
	"manifester/internal/verify/speccheck/checker/generator"
	"manifester/internal/verify/speccheck/checker/observation"
	"manifester/internal/verify/speccheck/checker/suites"
)

// impl is a control plane as the conformance runner drives it.
type impl interface {
	Exec(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneResponse, core.ErrCode, error)
	CreateTopic(ctx context.Context, op core.CreateTopic) (core.ErrCode, error)
}

// storeImpl drives a controlplane.Store directly.
type storeImpl struct{ st cp.Store }

func (s storeImpl) Exec(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneResponse, core.ErrCode, error) {
	switch op := op.(type) {
	case core.CommitSegment:
		r, c, err := s.st.CommitSegment(ctx, op)
		return r, c, err
	case core.FindSegments:
		r, c, err := s.st.Lookup(ctx, op)
		return r, c, err
	case core.DescribeTopics:
		r, err := s.st.DescribeTopics(ctx, op)
		return r, core.OK, err
	case core.CreateTopic:
		code, err := s.st.CreateTopic(ctx, op)
		return nil, code, err
	}
	return nil, 0, fmt.Errorf("unknown op %T", op)
}

func (s storeImpl) CreateTopic(ctx context.Context, op core.CreateTopic) (core.ErrCode, error) {
	return s.st.CreateTopic(ctx, op)
}

// grpcImpl drives a machine node through controlplaneclient.
type grpcImpl struct{ c *controlplaneclient.Client }

func (h grpcImpl) Exec(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneResponse, core.ErrCode, error) {
	r, err := h.c.Call(ctx, op)
	return r, errCode(err), nonBusiness(err)
}

func (h grpcImpl) CreateTopic(ctx context.Context, op core.CreateTopic) (core.ErrCode, error) {
	_, err := h.c.Call(ctx, op)
	return errCode(err), nonBusiness(err)
}

func errCode(err error) core.ErrCode {
	if be, ok := errors.AsType[*controlplaneclient.Error](err); ok {
		return be.Code
	}
	return core.OK
}

func nonBusiness(err error) error {
	if _, ok := errors.AsType[*controlplaneclient.Error](err); ok {
		return nil
	}
	return err
}

func grpcOver(t testing.TB) impl {
	return grpcImpl{grpctestserver.StartMem(t).Client}
}

func newMem() cp.Store { return memorystore.New() }

// implSubject makes an impl usable as a suite subject. The stores keep no spec
// hash, so StateHash says no. A commit that succeeds is Applied, as in the spec.
type implSubject struct{ im impl }

func (s implSubject) Do(op generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch {
	case op.CreateTopic != nil:
		code, err := s.im.CreateTopic(ctx, core.CreateTopic{Name: op.CreateTopic.Name, Partitions: op.CreateTopic.Partitions})
		return observation.ControlPlaneResult{Code: code}, err
	case op.Call != nil:
		resp, code, err := s.im.Exec(ctx, op.Call.Op)
		if err != nil || code != core.OK {
			return observation.ControlPlaneResult{Code: code}, err
		}
		done := core.ControlPlaneDone{Resp: resp}
		switch op.Call.Op.(type) {
		case core.CommitSegment, core.CreateTopic:
			done.Outcome = core.Applied
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (implSubject) StateHash() (uint64, bool) { return 0, false }

// subjectOf returns a factory of fresh subjects for the suite.
func subjectOf(newImpl func() impl) func() suites.ControlPlaneSubject {
	return func() suites.ControlPlaneSubject { return implSubject{newImpl()} }
}
