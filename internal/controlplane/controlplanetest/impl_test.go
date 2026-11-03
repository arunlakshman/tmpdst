package controlplanetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/manifesterio/mds/internal/controlplane/controlplaneclient"
	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/memorystore"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/generator"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/observation"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/suites"
)

// impl is a control plane as the conformance runner drives it. A business error
// is the Err of the answer. The error result is for infrastructure failures only.
type impl interface {
	Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error)
}

// grpcImpl drives a machine node through controlplaneclient.
type grpcImpl struct{ c *controlplaneclient.Client }

func (h grpcImpl) Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	r, err := h.c.Call(ctx, operation)
	return core.ControlPlaneDone{Resp: r, Err: errCode(err)}, nonBusiness(err)
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
	return grpcImpl{StartMem(t).Client}
}

func newMem() impl { return memorystore.New() }

// implSubject makes an impl usable as a suite subject. The stores keep no spec
// hash, so StateHash says no. A commit that succeeds is Applied, as in the spec.
type implSubject struct{ im impl }

func (s implSubject) Do(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch {
	case operation.CreateTopic != nil:
		d, err := s.im.Exec(ctx, core.CreateTopic{Name: operation.CreateTopic.Name, Partitions: operation.CreateTopic.Partitions})
		return observation.ControlPlaneResult{Code: d.Err}, err
	case operation.Call != nil:
		done, err := s.im.Exec(ctx, operation.Call.Op)
		if err != nil || done.Err != core.OK {
			return observation.ControlPlaneResult{Code: done.Err}, err
		}
		switch operation.Call.Op.(type) {
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
