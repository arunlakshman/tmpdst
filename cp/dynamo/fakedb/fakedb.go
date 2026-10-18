// Package fakedb is an in-memory DynamoDB for the cp/dynamo store's tests: it
// implements dynamo.DB faithfully enough for the expressions the store uses
// (conditions, ADD/SET updates, key conditions with BETWEEN), with the
// semantics the store depends on: single-item atomicity, all-or-nothing
// TransactWriteItems that fails with TransactionCanceledException and a
// reason per item, Query paging and ordering, BatchGetItem UnprocessedKeys.
// It also injects failures. It is a test double, not a database: no
// concurrency beyond one mutex, no size accounting beyond the transaction cap.
package fakedb

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

type item = map[string]types.AttributeValue

// Fake is the in-memory service. The zero value is not usable; use New.
type Fake struct {
	mu     sync.Mutex
	tables map[string]map[string]item // table -> "pk\x00sk" -> item

	// PageSize caps the items of one Query page when > 0 (the real service
	// pages at 1 MB; a small cap makes tests exercise pagination).
	PageSize int
	// ReportCapacity makes calls return ConsumedCapacity (ExtendDB does not).
	ReportCapacity bool
	// Fail is consulted at the start of every call with its name ("GetItem",
	// "BatchGetItem", "Query", "UpdateItem", "TransactWriteItems",
	// "CreateTable", "DescribeTable", "DeleteTable"); a non-nil result is
	// returned as the call's error and nothing happens.
	Fail func(call string) error
	// Unprocess is the number of upcoming BatchGetItem calls that leave their
	// last key in UnprocessedKeys.
	Unprocess int
	// BeforeTransact runs (without the lock) before each TransactWriteItems is
	// evaluated: a test writes through Put to lose the race on purpose.
	BeforeTransact func()
}

// New returns an empty service.
func New() *Fake { return &Fake{tables: map[string]map[string]item{}} }

func validation(format string, a ...any) error {
	return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf(format, a...)}
}

func (f *Fake) failed(call string) error {
	if f.Fail == nil {
		return nil
	}
	return f.Fail(call)
}

// capacity is the ConsumedCapacity of one call if reporting is on.
func (f *Fake) capacity(table string) *types.ConsumedCapacity {
	if !f.ReportCapacity {
		return nil
	}
	return &types.ConsumedCapacity{TableName: aws.String(table), CapacityUnits: aws.Float64(1)}
}

func (f *Fake) capacities(table string) []types.ConsumedCapacity {
	if c := f.capacity(table); c != nil {
		return []types.ConsumedCapacity{*c}
	}
	return nil
}

// table returns the named table's rows; the lock is held by the caller.
func (f *Fake) table(name *string) (map[string]item, error) {
	t, ok := f.tables[aws.ToString(name)]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String("table not found: " + aws.ToString(name))}
	}
	return t, nil
}

// Put stores an item directly (tests; no conditions). The table must exist.
func (f *Fake) Put(table string, it item) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tables[table][idOf(it)] = cloneItem(it)
}

// Item returns a copy of the stored item, or nil.
func (f *Fake) Item(table string, key item) item {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneItem(f.tables[table][idOf(key)])
}

// Count is the number of items in the table.
func (f *Fake) Count(table string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tables[table])
}

func (f *Fake) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	if err := f.failed("CreateTable"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name := aws.ToString(in.TableName)
	if _, ok := f.tables[name]; ok {
		return nil, &types.ResourceInUseException{Message: aws.String("table exists: " + name)}
	}
	f.tables[name] = map[string]item{}
	return &dynamodb.CreateTableOutput{}, nil
}

func (f *Fake) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if err := f.failed("DescribeTable"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.table(in.TableName); err != nil {
		return nil, err
	}
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableName: in.TableName, TableStatus: types.TableStatusActive}}, nil
}

func (f *Fake) DeleteTable(ctx context.Context, in *dynamodb.DeleteTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	if err := f.failed("DeleteTable"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.table(in.TableName); err != nil {
		return nil, err
	}
	delete(f.tables, aws.ToString(in.TableName))
	return &dynamodb.DeleteTableOutput{}, nil
}

func (f *Fake) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := f.failed("GetItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.table(in.TableName)
	if err != nil {
		return nil, err
	}
	return &dynamodb.GetItemOutput{Item: cloneItem(t[idOf(in.Key)]), ConsumedCapacity: f.capacity(aws.ToString(in.TableName))}, nil
}

func (f *Fake) BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if err := f.failed("BatchGetItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &dynamodb.BatchGetItemOutput{Responses: map[string][]item{}, UnprocessedKeys: map[string]types.KeysAndAttributes{}}
	for name, ka := range in.RequestItems {
		if len(ka.Keys) > 100 {
			return nil, validation("too many items requested for the BatchGetItem call")
		}
		t, err := f.table(&name)
		if err != nil {
			return nil, err
		}
		keys, left := f.splitUnprocessed(ka.Keys)
		for _, k := range keys {
			if it := t[idOf(k)]; it != nil {
				out.Responses[name] = append(out.Responses[name], cloneItem(it))
			}
		}
		if len(left) > 0 {
			out.UnprocessedKeys[name] = types.KeysAndAttributes{Keys: left, ConsistentRead: ka.ConsistentRead}
		}
		out.ConsumedCapacity = append(out.ConsumedCapacity, f.capacities(name)...)
	}
	return out, nil
}

// splitUnprocessed holds back the last key while Unprocess calls remain.
func (f *Fake) splitUnprocessed(keys []item) (processed, left []item) {
	if f.Unprocess <= 0 || len(keys) == 0 {
		return keys, nil
	}
	f.Unprocess--
	return keys[:len(keys)-1], keys[len(keys)-1:]
}
