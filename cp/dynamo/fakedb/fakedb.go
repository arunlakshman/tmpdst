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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

type item = map[string]types.AttributeValue

// Fake is the in-memory service. The zero value is not usable; use New.
type Fake struct {
	mu     sync.Mutex
	tables map[string]map[string]item                // table -> "pk\x00sk" -> item
	byPK   map[string]map[string]map[string]struct{} // table -> pk -> ids: a Query must not scan the table

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
	// Latency, if set, is the time every call sleeps (before it takes effect:
	// a read sees the state at the end of the delay, a transaction is
	// evaluated and applied at the end of it).
	Latency func(call string) time.Duration
	// AfterApply, if set, runs after a TransactWriteItems has been applied; a
	// non-nil result is returned as the call's error: the transaction DID
	// commit but the caller never learns it (a lost answer).
	AfterApply func() error
	// AfterPut, if set, runs after a PutItem has been applied; a non-nil
	// result is returned as the call's error (the put DID land, the answer is
	// lost).
	AfterPut func() error
	// UnprocessWrite is the number of upcoming BatchWriteItem calls that leave
	// their last request in UnprocessedItems.
	UnprocessWrite int
	// Unprocess is the number of upcoming BatchGetItem calls that leave their
	// last key in UnprocessedKeys.
	Unprocess int
	// BeforeTransact runs (without the lock) before each TransactWriteItems is
	// evaluated: a test writes through Put to lose the race on purpose.
	BeforeTransact func()
}

// New returns an empty service.
func New() *Fake {
	return &Fake{tables: map[string]map[string]item{}, byPK: map[string]map[string]map[string]struct{}{}}
}

// store writes it and keeps the partition index.
func (f *Fake) store(table string, it item) {
	f.tables[table][idOf(it)] = it
	pks := f.byPK[table]
	pk := strOf(it, "pk")
	if pks[pk] == nil {
		pks[pk] = map[string]struct{}{}
	}
	pks[pk][idOf(it)] = struct{}{}
}

// remove deletes the item with the key.
func (f *Fake) remove(table string, key item) {
	delete(f.tables[table], idOf(key))
	delete(f.byPK[table][strOf(key, "pk")], idOf(key))
}

func validation(format string, a ...any) error {
	return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf(format, a...)}
}

func (f *Fake) failed(ctx context.Context, call string) error {
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
	f.store(table, cloneItem(it))
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

func (f *Fake) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
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

func (f *Fake) DeleteTable(ctx context.Context, in *dynamodb.DeleteTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
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

func (f *Fake) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := f.failed(ctx, "GetItem"); err != nil {
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
	if err := f.failed(ctx, "BatchGetItem"); err != nil {
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
