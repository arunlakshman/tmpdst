package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TableManager is the part of the DynamoDB client CreateTable uses.
type TableManager interface {
	CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// CreateDynamoTable creates the table (string keys pk and sk, pay-per-request) and
// waits for it to be active. An existing table is left alone.
func CreateDynamoTable(ctx context.Context, db TableManager, table string) error {
	_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &table,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceInUseException](err); !ok {
			return fmt.Errorf("create table %s: %w", table, err)
		}
	}
	for range 100 {
		out, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
		if err == nil && out.Table.TableStatus == types.TableStatusActive {
			return nil
		}
		t := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return fmt.Errorf("table %s not active", table)
}
