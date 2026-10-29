// Package fakedynamodb is an in-memory DynamoDB for control-plane log tests: it
// implements exec.DynamoClient faithfully enough for the expressions DynamoLog uses
// (conditions, SET updates, key conditions with BETWEEN), with the semantics
// the log depends on: single-item atomicity, conditional puts, Query paging
// and ordering, BatchWriteItem UnprocessedItems. It also injects failures. It
// is a test double, not a database: no concurrency beyond one mutex.
package fakedynamodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

type item = map[string]types.AttributeValue

// FakeDynamoDB is the in-memory service. The zero value is not usable; use New.
type FakeDynamoDB struct {
	mu     sync.Mutex
	tables map[string]map[string]item                // table -> "pk\x00sk" -> item
	byPK   map[string]map[string]map[string]struct{} // table -> pk -> ids: a Query must not scan the table

	// PageSize caps the items of one Query page when > 0 (the real service
	// pages at 1 MB; a small cap makes tests exercise pagination).
	PageSize int
	// ReportCapacity makes calls return ConsumedCapacity (ExtendDB does not).
	ReportCapacity bool
	// Fail is consulted at the start of every call with its name ("GetItem",
	// "PutItem", "Query", "UpdateItem", "BatchWriteItem", "CreateTable",
	// "DescribeTable", "DeleteTable"); a non-nil result is
	// returned as the call's error and nothing happens.
	Fail func(call string) error
	// Latency, if set, is the time every call sleeps (before it takes effect:
	// a read sees the state at the end of the delay, a write is
	// evaluated and applied at the end of it).
	Latency func(call string) time.Duration
	// AfterPut, if set, runs after a PutItem has been applied; a non-nil
	// result is returned as the call's error (the put DID land, the answer is
	// lost).
	AfterPut func() error
	// UnprocessWrite is the number of upcoming BatchWriteItem calls that leave
	// their last request in UnprocessedItems.
	UnprocessWrite int
}

// New returns an empty service.
func New() *FakeDynamoDB {
	return &FakeDynamoDB{tables: map[string]map[string]item{}, byPK: map[string]map[string]map[string]struct{}{}}
}

// store writes it and keeps the partition index.
func (f *FakeDynamoDB) store(table string, it item) {
	f.tables[table][itemIDOf(it)] = it
	pks := f.byPK[table]
	pk := stringOf(it, "pk")
	if pks[pk] == nil {
		pks[pk] = map[string]struct{}{}
	}
	pks[pk][itemIDOf(it)] = struct{}{}
}

// remove deletes the item with the key.
func (f *FakeDynamoDB) remove(table string, key item) {
	delete(f.tables[table], itemIDOf(key))
	delete(f.byPK[table][stringOf(key, "pk")], itemIDOf(key))
}

func validation(format string, a ...any) error {
	return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf(format, a...)}
}

func (f *FakeDynamoDB) failed(ctx context.Context, call string) error {
	if f.Latency != nil {
		if d := f.Latency(call); d > 0 {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	if f.Fail == nil {
		return nil
	}
	return f.Fail(call)
}

// capacity is the ConsumedCapacity of one call if reporting is on.
func (f *FakeDynamoDB) capacity(table string) *types.ConsumedCapacity {
	if !f.ReportCapacity {
		return nil
	}
	return &types.ConsumedCapacity{TableName: aws.String(table), CapacityUnits: aws.Float64(1)}
}

func (f *FakeDynamoDB) capacities(table string) []types.ConsumedCapacity {
	if c := f.capacity(table); c != nil {
		return []types.ConsumedCapacity{*c}
	}
	return nil
}

// table returns the named table's rows; the lock is held by the caller.
func (f *FakeDynamoDB) table(name *string) (map[string]item, error) {
	t, ok := f.tables[aws.ToString(name)]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String("table not found: " + aws.ToString(name))}
	}
	return t, nil
}

// Put stores an item directly (tests; no conditions). The table must exist.
func (f *FakeDynamoDB) Put(table string, it item) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store(table, cloneItem(it))
}

// Item returns a copy of the stored item, or nil.
func (f *FakeDynamoDB) Item(table string, key item) map[string]types.AttributeValue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneItem(f.tables[table][itemIDOf(key)])
}

// Count is the number of items in the table.
func (f *FakeDynamoDB) Count(table string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tables[table])
}

// CreateTable creates a table in the fake.
func (f *FakeDynamoDB) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	if err := f.failed(ctx, "CreateTable"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name := aws.ToString(in.TableName)
	if _, ok := f.tables[name]; ok {
		return nil, &types.ResourceInUseException{Message: aws.String("table exists: " + name)}
	}
	f.tables[name] = map[string]item{}
	f.byPK[name] = map[string]map[string]struct{}{}
	return &dynamodb.CreateTableOutput{}, nil
}

// DescribeTable describes a table in the fake.
func (f *FakeDynamoDB) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if err := f.failed(ctx, "DescribeTable"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.table(in.TableName); err != nil {
		return nil, err
	}
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableName: in.TableName, TableStatus: types.TableStatusActive}}, nil
}

// DeleteTable deletes a table from the fake.
func (f *FakeDynamoDB) DeleteTable(ctx context.Context, in *dynamodb.DeleteTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	if err := f.failed(ctx, "DeleteTable"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.table(in.TableName); err != nil {
		return nil, err
	}
	delete(f.tables, aws.ToString(in.TableName))
	delete(f.byPK, aws.ToString(in.TableName))
	return &dynamodb.DeleteTableOutput{}, nil
}

// GetItem reads one item from the fake.
func (f *FakeDynamoDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := f.failed(ctx, "GetItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.table(in.TableName)
	if err != nil {
		return nil, err
	}
	return &dynamodb.GetItemOutput{Item: cloneItem(t[itemIDOf(in.Key)]), ConsumedCapacity: f.capacity(aws.ToString(in.TableName))}, nil
}
