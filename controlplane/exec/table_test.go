package exec_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/controlplane/exec"
)

// tableAdmin is a scripted TableAdmin: create is the CreateTable result and
// statuses are the successive DescribeTable outcomes (the last one repeats).
type tableAdmin struct {
	create    error
	statuses  []types.TableStatus
	descErr   error
	created   *dynamodb.CreateTableInput
	described int
}

func (a *tableAdmin) CreateTable(_ context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	a.created = in
	return &dynamodb.CreateTableOutput{}, a.create
}

func (a *tableAdmin) DescribeTable(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	i := min(a.described, len(a.statuses)-1)
	a.described++
	if a.descErr != nil {
		return nil, a.descErr
	}
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: a.statuses[i]}}, nil
}

func TestCreateTableActive(t *testing.T) {
	a := &tableAdmin{statuses: []types.TableStatus{types.TableStatusActive}}
	if err := exec.CreateTable(t.Context(), a, "t1"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	in := a.created
	if in == nil || *in.TableName != "t1" {
		t.Fatalf("created = %+v, want table t1", in)
	}
	if in.BillingMode != types.BillingModePayPerRequest {
		t.Errorf("billing = %v, want pay-per-request", in.BillingMode)
	}
	if len(in.KeySchema) != 2 || *in.KeySchema[0].AttributeName != "pk" || *in.KeySchema[1].AttributeName != "sk" {
		t.Errorf("key schema = %+v, want pk hash + sk range", in.KeySchema)
	}
}

func TestCreateTableAlreadyExists(t *testing.T) {
	a := &tableAdmin{
		create:   &types.ResourceInUseException{},
		statuses: []types.TableStatus{types.TableStatusActive},
	}
	if err := exec.CreateTable(t.Context(), a, "t1"); err != nil {
		t.Fatalf("existing table must be left alone, got %v", err)
	}
}

func TestCreateTableCreateError(t *testing.T) {
	boom := errors.New("boom")
	a := &tableAdmin{create: boom, statuses: []types.TableStatus{types.TableStatusActive}}
	err := exec.CreateTable(t.Context(), a, "t1")
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "create table t1") {
		t.Fatalf("err = %v, want wrapped boom naming the table", err)
	}
	if a.described != 0 {
		t.Errorf("DescribeTable called %d times after a failed create", a.described)
	}
}

func TestCreateTableWaitsUntilActive(t *testing.T) {
	a := &tableAdmin{statuses: []types.TableStatus{types.TableStatusCreating, types.TableStatusActive}}
	if err := exec.CreateTable(t.Context(), a, "t1"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if a.described != 2 {
		t.Errorf("DescribeTable called %d times, want 2", a.described)
	}
}

func TestCreateTableContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	a := &tableAdmin{statuses: []types.TableStatus{types.TableStatusCreating}}
	if err := exec.CreateTable(ctx, a, "t1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
