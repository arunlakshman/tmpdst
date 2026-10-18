package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTables creates the table (pay-per-request) and waits for it to be
// active. An existing table is left alone.
func (s *Store) CreateTables(ctx context.Context) error {
	if err := s.createTable(ctx); err != nil {
		return err
	}
	return s.waitActive(ctx)
}

func (s *Store) createTable(ctx context.Context) error {
	_, err := s.db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &s.table,
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
	var inUse *types.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return fmt.Errorf("create table %s: %w", s.table, err)
	}
	return nil
}

func (s *Store) waitActive(ctx context.Context) error {
	for i := 0; i < 100; i++ {
		out, err := s.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &s.table})
		if err == nil && out.Table.TableStatus == types.TableStatusActive {
			return nil
		}
		if err := sleep(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("table %s not active", s.table)
}

// DeleteTables drops the table (tests).
func (s *Store) DeleteTables(ctx context.Context) error {
	_, err := s.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &s.table})
	return err
}
