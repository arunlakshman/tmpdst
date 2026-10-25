package cptest

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpclient"
	"manifester/cp/memstore"
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

// httpImpl drives a store through handler + client over httptest.
type httpImpl struct{ c *cpclient.Client }

func (h httpImpl) Exec(ctx context.Context, op core.CPOp) (core.CPResp, core.ErrCode, error) {
	r, err := h.c.Call(ctx, op)
	return r, errCode(err), nonBusiness(err)
}

func (h httpImpl) CreateTopic(ctx context.Context, name string, parts int32) (core.ErrCode, error) {
	err := h.c.CreateTopic(ctx, name, parts)
	return errCode(err), nonBusiness(err)
}

func errCode(err error) core.ErrCode {
	var be *cpclient.Error
	if errors.As(err, &be) {
		return be.Code
	}
	return core.OK
}

func nonBusiness(err error) error {
	var be *cpclient.Error
	if errors.As(err, &be) {
		return nil
	}
	return err
}

func httpOver(t testing.TB, st cp.Store) impl {
	srv := httptest.NewServer(cp.NewHandler(st))
	t.Cleanup(srv.Close)
	return httpImpl{cpclient.New(srv.URL, srv.Client())}
}

func newMem() cp.Store { return memstore.New() }
