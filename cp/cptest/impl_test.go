package cptest

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpclient"
	"manifester/cp/dynamo"
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
	case core.Register:
		r, err := s.st.Register(ctx, op)
		return r, core.OK, err
	case core.InitProducer:
		r, err := s.st.InitProducer(ctx)
		return r, core.OK, err
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

var tableSeq atomic.Int64

func needDynamo(t testing.TB) string {
	t.Helper()
	e := os.Getenv("MANIFESTER_DYNAMO_ENDPOINT")
	if e == "" {
		t.Skip("MANIFESTER_DYNAMO_ENDPOINT not set (scripts/it-up.sh)")
	}
	return e
}

// dynamoStore returns a store over fresh uniquely prefixed tables, or skips.
func dynamoStore(t testing.TB) *dynamo.Store {
	t.Helper()
	db := dynamoClient(needDynamo(t))
	prefix := fmt.Sprintf("t%d_%d_", time.Now().UnixNano(), tableSeq.Add(1))
	st := dynamo.New(db, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.DeleteTables(context.Background()) })
	return st
}

func dynamoClient(endpoint string) *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", ""),
	})
}

func newMem() cp.Store { return memstore.New() }

// newReplica is another Store over the same table as st.
func newReplica(t testing.TB, st *dynamo.Store) cp.Store {
	return dynamo.New(dynamoClient(needDynamo(t)), st.Prefix())
}
