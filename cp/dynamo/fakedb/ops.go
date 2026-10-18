package fakedb

import (
	"context"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (f *Fake) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if err := f.failed("UpdateItem"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.table(in.TableName)
	if err != nil {
		return nil, err
	}
	cur := t[idOf(in.Key)]
	if ok, err := evalCond(in.ConditionExpression, in.ExpressionAttributeValues, cur); err != nil {
		return nil, err
	} else if !ok {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
	}
	next, set, err := applyUpdate(aws.ToString(in.UpdateExpression), in.ExpressionAttributeValues, in.Key, cur)
	if err != nil {
		return nil, err
	}
	t[idOf(in.Key)] = next
	out := &dynamodb.UpdateItemOutput{ConsumedCapacity: f.capacity(aws.ToString(in.TableName))}
	if in.ReturnValues == types.ReturnValueUpdatedNew {
		out.Attributes = pick(next, set)
	}
	return out, nil
}

func pick(it item, names []string) item {
	out := item{}
	for _, n := range names {
		out[n] = it[n]
	}
	return cloneItem(out)
}

// Query returns one page: the items of the partition matching the key
// condition in sort-key order, at most Limit and PageSize, with a
// LastEvaluatedKey when more remain.
func (f *Fake) Query(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if err := f.failed("Query"); err != nil {
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
func (f *Fake) rowsAfter(t map[string]item, kc keyCond, in *dynamodb.QueryInput) []item {
	var rows []item
	for _, it := range t {
		if kc.matches(it) {
			rows = append(rows, it)
		}
	}
	slices.SortFunc(rows, func(a, b item) int { return strings.Compare(strOf(a, "sk"), strOf(b, "sk")) })
	if in.ScanIndexForward != nil && !*in.ScanIndexForward {
		slices.Reverse(rows)
	}
	if in.ExclusiveStartKey == nil {
		return rows
	}
	start := strOf(in.ExclusiveStartKey, "sk")
	forward := in.ScanIndexForward == nil || *in.ScanIndexForward
	return slices.DeleteFunc(rows, func(it item) bool {
		sk := strOf(it, "sk")
		return (forward && sk <= start) || (!forward && sk >= start)
	})
}

// pageLen is how many of avail rows one page returns.
func (f *Fake) pageLen(limit *int32, avail int) int {
	n := avail
	if limit != nil {
		n = min(n, int(*limit))
	}
	if f.PageSize > 0 {
		n = min(n, f.PageSize)
	}
	return n
}
