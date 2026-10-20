package fakedb

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// txOp is one TransactWriteItem reduced to what evaluation needs.
type txOp struct {
	table string
	key   item
	cond  *string
	vals  item
	put   item    // Put: the new item
	upd   *string // Update: the expression
}

func toOp(t types.TransactWriteItem) (txOp, error) {
	switch {
	case t.Put != nil:
		return txOp{table: aws.ToString(t.Put.TableName), key: t.Put.Item, cond: t.Put.ConditionExpression,
			vals: t.Put.ExpressionAttributeValues, put: t.Put.Item}, nil
	case t.Update != nil:
		return txOp{table: aws.ToString(t.Update.TableName), key: t.Update.Key, cond: t.Update.ConditionExpression,
			vals: t.Update.ExpressionAttributeValues, upd: t.Update.UpdateExpression}, nil
	case t.ConditionCheck != nil:
		return txOp{table: aws.ToString(t.ConditionCheck.TableName), key: t.ConditionCheck.Key, cond: t.ConditionCheck.ConditionExpression,
			vals: t.ConditionCheck.ExpressionAttributeValues}, nil
	}
	return txOp{}, validation("unsupported transaction item")
}

// ops validates a transaction request: 1..100 items, one operation per item.
func ops(items []types.TransactWriteItem) ([]txOp, error) {
	if len(items) == 0 || len(items) > 100 {
		return nil, validation("transaction must have 1 to 100 items, has %d", len(items))
	}
	out := make([]txOp, 0, len(items))
	seen := map[string]bool{}
	for _, t := range items {
		o, err := toOp(t)
		if err != nil {
			return nil, err
		}
		if id := o.table + "\x00" + idOf(o.key); seen[id] {
			return nil, validation("Transaction request cannot include multiple operations on one item")
		} else {
			seen[id] = true
		}
		out = append(out, o)
	}
	return out, nil
}

// TransactWriteItems checks every condition against the state before the
// transaction; if any fails nothing is written and the error is a
// TransactionCanceledException with a reason per item.
func (f *Fake) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if err := f.failed(ctx, "TransactWriteItems"); err != nil {
		return nil, err
	}
	if f.BeforeTransact != nil {
		f.BeforeTransact()
	}
	list, err := ops(in.TransactItems)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	reasons, err := f.evaluate(list)
	if err != nil {
		return nil, err
	}
	if reasons != nil {
		return nil, &types.TransactionCanceledException{
			Message:             aws.String("Transaction cancelled, please refer cancellation reasons for specific reasons [" + codes(reasons) + "]"),
			CancellationReasons: reasons,
		}
	}
	next, err := f.plan(list)
	if err != nil {
		return nil, err
	}
	f.apply(list, next)
	if f.AfterApply != nil {
		if err := f.AfterApply(); err != nil {
			return nil, err
		}
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func codes(rs []types.CancellationReason) string {
	var cs []string
	for _, r := range rs {
		cs = append(cs, aws.ToString(r.Code))
	}
	return strings.Join(cs, ", ")
}

// evaluate returns nil if every condition holds, else a reason per item.
func (f *Fake) evaluate(list []txOp) ([]types.CancellationReason, error) {
	reasons := make([]types.CancellationReason, len(list))
	failed := false
	for i, o := range list {
		t, ok := f.tables[o.table]
		if !ok {
			return nil, &types.ResourceNotFoundException{Message: aws.String("table not found: " + o.table)}
		}
		pass, err := evalCond(o.cond, o.vals, t[idOf(o.key)])
		if err != nil {
			return nil, err
		}
		reasons[i].Code = aws.String("None")
		if !pass {
			reasons[i].Code, failed = aws.String("ConditionalCheckFailed"), true
		}
	}
	if !failed {
		return nil, nil
	}
	return reasons, nil
}

// plan computes the item each operation writes (nil: none) before anything is
// stored, so a malformed update cannot leave a transaction half applied.
func (f *Fake) plan(list []txOp) ([]item, error) {
	next := make([]item, len(list))
	for i, o := range list {
		switch {
		case o.put != nil:
			next[i] = cloneItem(o.put)
		case o.upd != nil:
			it, _, err := applyUpdate(*o.upd, o.vals, o.key, f.tables[o.table][idOf(o.key)])
			if err != nil {
				return nil, err
			}
			next[i] = it
		}
	}
	return next, nil
}

// apply stores the planned items.
func (f *Fake) apply(list []txOp, next []item) {
	for i, o := range list {
		if next[i] != nil {
			f.store(o.table, next[i])
		}
	}
}
