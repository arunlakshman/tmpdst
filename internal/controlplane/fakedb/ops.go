package fakedb

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// UpdateItem applies an update expression to one item in the fake.
func (f *FakeDynamoDB) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if err := f.failed(ctx, "UpdateItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.table(in.TableName)
	if err != nil {
		return nil, err
	}
	cur := t[itemIDOf(in.Key)]
	if err := rejectCond(in.ConditionExpression, in.ExpressionAttributeValues, cur); err != nil {
		return nil, err
	}
	next, set, err := applyUpdate(aws.ToString(in.UpdateExpression), in.ExpressionAttributeValues, in.Key, cur)
	if err != nil {
		return nil, err
	}
	f.store(aws.ToString(in.TableName), next)
	out := &dynamodb.UpdateItemOutput{ConsumedCapacity: f.capacity(aws.ToString(in.TableName))}
	if in.ReturnValues == types.ReturnValueUpdatedNew {
		out.Attributes = chooseItem(next, set)
	}
	return out, nil
}

func chooseItem(it item, names []string) item {
	out := item{}
	for _, n := range names {
		out[n] = it[n]
	}
	return cloneItem(out)
}

// Query returns one page: the items of the partition matching the key
// condition in sort-key order, at most Limit and PageSize, with a
// LastEvaluatedKey when more remain.
func (f *FakeDynamoDB) Query(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if err := f.failed(ctx, "Query"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.table(in.TableName)
	if err != nil {
		return nil, err
	}
	kc, err := parseKeyCond(aws.ToString(in.KeyConditionExpression), in.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	rows := f.rowsAfter(t, kc, in)
	n := f.pageLen(in.Limit, len(rows))
	out := &dynamodb.QueryOutput{ConsumedCapacity: f.capacity(aws.ToString(in.TableName)), Count: int32(n)}
	for _, it := range rows[:n] {
		out.Items = append(out.Items, cloneItem(it))
	}
	if n < len(rows) {
		out.LastEvaluatedKey = item{"pk": rows[n-1]["pk"], "sk": rows[n-1]["sk"]}
	}
	return out, nil
}

// rowsAfter is the matching rows in query order, after ExclusiveStartKey.
func (f *FakeDynamoDB) rowsAfter(t map[string]item, kc keyCondition, in *dynamodb.QueryInput) []item {
	var rows []item
	for id := range f.byPK[aws.ToString(in.TableName)][kc.pk] {
		if it := t[id]; kc.matches(it) {
			rows = append(rows, it)
		}
	}
	slices.SortFunc(rows, func(a, b item) int { return strings.Compare(stringOf(a, "sk"), stringOf(b, "sk")) })
	if in.ScanIndexForward != nil && !*in.ScanIndexForward {
		slices.Reverse(rows)
	}
	if in.ExclusiveStartKey == nil {
		return rows
	}
	start := stringOf(in.ExclusiveStartKey, "sk")
	forward := in.ScanIndexForward == nil || *in.ScanIndexForward
	return slices.DeleteFunc(rows, func(it item) bool {
		sk := stringOf(it, "sk")
		return (forward && sk <= start) || (!forward && sk >= start)
	})
}

// pageLen is how many of avail rows one page returns.
func (f *FakeDynamoDB) pageLen(limit *int32, avail int) int {
	n := avail
	if limit != nil {
		n = min(n, int(*limit))
	}
	if f.PageSize > 0 {
		n = min(n, f.PageSize)
	}
	return n
}

// PutItem stores the item if the condition holds. On a failed condition with
// ReturnValuesOnConditionCheckFailure ALL_OLD the error carries the existing
// item, like the service.
func (f *FakeDynamoDB) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if err := f.failed(ctx, "PutItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.table(in.TableName)
	if err != nil {
		return nil, err
	}
	cur := t[itemIDOf(in.Item)]
	if err := rejectCond(in.ConditionExpression, in.ExpressionAttributeValues, cur); err != nil {
		if failed, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok && in.ReturnValuesOnConditionCheckFailure == types.ReturnValuesOnConditionCheckFailureAllOld {
			failed.Item = cloneItem(cur)
		}
		return nil, err
	}
	f.store(aws.ToString(in.TableName), cloneItem(in.Item))
	if f.AfterPut != nil {
		if err := f.AfterPut(); err != nil {
			return nil, err
		}
	}
	return &dynamodb.PutItemOutput{ConsumedCapacity: f.capacity(aws.ToString(in.TableName))}, nil
}

func rejectCond(expr *string, vals, cur item) error {
	ok, err := evalCondition(expr, vals, cur)
	if err != nil {
		return err
	}
	if !ok {
		return &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
	}
	return nil
}

// BatchWriteItem applies up to 25 puts and deletes (no conditions, like the
// service). While UnprocessWrite > 0 a call leaves its last request in
// UnprocessedItems and decrements it, so tests exercise the retry.
func (f *FakeDynamoDB) BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if err := f.failed(ctx, "BatchWriteItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	total := writeCount(in)
	if total == 0 || total > 25 {
		return nil, validation("BatchWriteItem takes 1 to 25 requests, got %d", total)
	}
	out := &dynamodb.BatchWriteItemOutput{UnprocessedItems: map[string][]types.WriteRequest{}}
	hold := f.UnprocessWrite > 0
	if hold {
		f.UnprocessWrite--
	}
	for name, reqs := range in.RequestItems {
		if _, err := f.table(&name); err != nil {
			return nil, err
		}
		for i, r := range reqs {
			if hold && i == len(reqs)-1 {
				out.UnprocessedItems[name] = append(out.UnprocessedItems[name], r)
				continue
			}
			f.applyWrite(name, r)
		}
		out.ConsumedCapacity = append(out.ConsumedCapacity, f.capacities(name)...)
	}
	return out, nil
}

func writeCount(in *dynamodb.BatchWriteItemInput) int {
	total := 0
	for _, reqs := range in.RequestItems {
		total += len(reqs)
	}
	return total
}

func (f *FakeDynamoDB) applyWrite(name string, r types.WriteRequest) {
	if r.PutRequest != nil {
		f.store(name, cloneItem(r.PutRequest.Item))
		return
	}
	if r.DeleteRequest != nil {
		f.remove(name, r.DeleteRequest.Key)
	}
}
