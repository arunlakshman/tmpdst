//go:build integration

package dynamolog_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/dynamolog"
	"github.com/manifesterio/mds/controlplane/internal/testinfra"
)

// tableAdmin wraps a real client. DynamoDB Local cannot be made to fail a
// create or to report CREATING, so create and statuses inject those; every
// other outcome comes from the real service.
type tableAdmin struct {
	*dynamodb.Client
	create    error               // returned instead of creating, when set
	statuses  []types.TableStatus // injected DescribeTable statuses, then the real one
	created   *dynamodb.CreateTableInput
	described int
}

func (a *tableAdmin) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	a.created = in
	if a.create != nil {
		return nil, a.create
	}
	return a.Client.CreateTable(ctx, in, opts...)
}

func (a *tableAdmin) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	i := a.described
	a.described++
	if i < len(a.statuses) {
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: a.statuses[i]}}, nil
	}
	return a.Client.DescribeTable(ctx, in, opts...)
}

// freshAdmin returns a tableAdmin and the name of a table that does not exist
// yet (testinfra.DynamoDB pre-creates its own). The table is deleted at the end.
func freshAdmin(t *testing.T) (*tableAdmin, string) {
	t.Helper()
	info := testinfra.DynamoDB(t)
	name := fmt.Sprintf("ct_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = info.Client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &name})
	})
	return &tableAdmin{Client: info.Client}, name
}

func TestCreateTableActive(t *testing.T) {
	a, name := freshAdmin(t)
	if err := dynamolog.CreateTable(t.Context(), a, name); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	in := a.created
	if in == nil || *in.TableName != name {
		t.Fatalf("created = %+v, want table %s", in, name)
	}
	if in.BillingMode != types.BillingModePayPerRequest {
		t.Errorf("billing = %v, want pay-per-request", in.BillingMode)
	}
	if len(in.KeySchema) != 2 || *in.KeySchema[0].AttributeName != "pk" || *in.KeySchema[1].AttributeName != "sk" {
		t.Errorf("key schema = %+v, want pk hash + sk range", in.KeySchema)
	}
	// The real service made the table, and it is active with that schema.
	out, err := a.Client.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		t.Fatal(err)
	}
	ks := out.Table.KeySchema
	if out.Table.TableStatus != types.TableStatusActive || len(ks) != 2 ||
		*ks[0].AttributeName != "pk" || ks[0].KeyType != types.KeyTypeHash ||
		*ks[1].AttributeName != "sk" || ks[1].KeyType != types.KeyTypeRange {
		t.Errorf("table = %+v, want active with pk hash + sk range", out.Table)
	}
}

func TestCreateTableAlreadyExists(t *testing.T) {
	a, name := freshAdmin(t)
	if err := dynamolog.CreateTable(t.Context(), a, name); err != nil {
		t.Fatal(err)
	}
	// The second create hits the real ResourceInUseException.
	if err := dynamolog.CreateTable(t.Context(), a, name); err != nil {
		t.Fatalf("existing table must be left alone, got %v", err)
	}
}

func TestCreateTableCreateError(t *testing.T) {
	boom := errors.New("boom")
	a, name := freshAdmin(t)
	a.create = boom
	err := dynamolog.CreateTable(t.Context(), a, name)
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "create table "+name) {
		t.Fatalf("err = %v, want wrapped boom naming the table", err)
	}
	if a.described != 0 {
		t.Errorf("DescribeTable called %d times after a failed create", a.described)
	}
}

func TestCreateTableWaitsUntilActive(t *testing.T) {
	a, name := freshAdmin(t)
	a.statuses = []types.TableStatus{types.TableStatusCreating}
	if err := dynamolog.CreateTable(t.Context(), a, name); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// One injected CREATING, then the real ACTIVE.
	if a.described != 2 {
		t.Errorf("DescribeTable called %d times, want 2", a.described)
	}
}

func TestCreateTableContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	a, name := freshAdmin(t)
	a.statuses = []types.TableStatus{types.TableStatusCreating}
	if err := dynamolog.CreateTable(ctx, a, name); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
