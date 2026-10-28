package cptest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	cp "manifester/internal/controlplane"
	"manifester/internal/controlplane/cpclient"
	"manifester/internal/controlplane/grpctest"
	"manifester/internal/controlplane/memstore"
	"manifester/internal/core"
)

// impl is a control plane as the conformance runner drives it.
type impl interface {
	Exec(ctx context.Context, op core.CPOp) (core.CPResp, core.ErrCode, error)
	CreateTopic(ctx context.Context, name string, parts int32) (core.ErrCode, error)
}

// storeImpl drives a cp.Store directly.
type storeImpl struct{ st cp.Store }

func (s storeImpl) Exec(ctx context.Context, op core.CPOp) (core.CPResp, core.ErrCode, error) {
	switch op := op.(type) {
	case core.CommitSegment:
		r, c, err := s.st.CommitSegment(ctx, op)
		return r, c, err
	case core.Lookup:
		r, c, err := s.st.Lookup(ctx, op)
		return r, c, err
	case core.DescribeTopics:
		r, err := s.st.DescribeTopics(ctx, op)
		return r, core.OK, err
	}
	return nil, 0, fmt.Errorf("unknown op %T", op)
}

func (s storeImpl) CreateTopic(ctx context.Context, name string, parts int32) (core.ErrCode, error) {
	return s.st.CreateTopic(ctx, name, parts)
}

// grpcImpl drives a machine node through cpclient.
type grpcImpl struct{ c *cpclient.Client }

func (h grpcImpl) Exec(ctx context.Context, op core.CPOp) (core.CPResp, core.ErrCode, error) {
	r, err := h.c.Call(ctx, op)
	return r, errCode(err), nonBusiness(err)
}

func (h grpcImpl) CreateTopic(ctx context.Context, name string, parts int32) (core.ErrCode, error) {
	err := h.c.CreateTopic(ctx, name, parts)
	return errCode(err), nonBusiness(err)
}

func errCode(err error) core.ErrCode {
	if be, ok := errors.AsType[*cpclient.Error](err); ok {
		return be.Code
	}
	return core.OK
}

func nonBusiness(err error) error {
	if _, ok := errors.AsType[*cpclient.Error](err); ok {
		return nil
	}
	return err
}

func grpcOver(t testing.TB) impl {
	return grpcImpl{grpctest.StartMem(t).Client}
}

func newMem() cp.Store { return memstore.New() }
