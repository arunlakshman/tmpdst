package logsm

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

// DB is the part of the DynamoDB client DynLog uses. *dynamodb.Client and
// cp/dynamo/fakedb.Fake satisfy it.
type DB interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// DynLog is a Log over one DynamoDB partition: LOG/<slot as 20 digits>, one
// item per slot, written with attribute_not_exists. The table must exist
// (string keys pk and sk).
type DynLog struct {
	db    DB
	table string
}

// NewDynLog returns a log in table.
func NewDynLog(db DB, table string) *DynLog { return &DynLog{db: db, table: table} }

const (
	logPK   = "LOG"
	slotMax = "99999999999999999999"
)

func slotKey(n int64) string { return fmt.Sprintf("%020d", n) }

func sv(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

// Append implements Log with one conditional PutItem. A failed condition
// returns the existing item (ALL_OLD); if the service did not, one consistent
// Query fetches it.
func (l *DynLog) Append(ctx context.Context, slot core.LogSlot, e Entry) (Entry, bool, error) {
	_, err := l.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &l.table,
		Item: map[string]types.AttributeValue{
			"pk": sv(logPK), "sk": sv(slotKey(int64(slot))), "id": sv(e.ID),
			"d": &types.AttributeValueMemberB{Value: Encode(e)},
		},
		ConditionExpression:                 aws.String("attribute_not_exists(pk)"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	var cc *types.ConditionalCheckFailedException
	switch {
	case err == nil:
		return e, true, nil
	case !errors.As(err, &cc):
		return Entry{}, false, err
	case cc.Item != nil:
		cur, err := decodeItem(cc.Item)
		return cur, false, err
	}
	return l.existing(ctx, slot)
}

func (l *DynLog) existing(ctx context.Context, slot core.LogSlot) (Entry, bool, error) {
	got, err := l.Read(ctx, slot, 1)
	if err != nil || len(got) == 0 || got[0].N != slot {
		return Entry{}, false, fmt.Errorf("logsm: slot %d taken but unreadable: %w", slot, err)
	}
	return got[0].E, false, nil
}

func decodeItem(it map[string]types.AttributeValue) (Entry, error) {
	b, ok := it["d"].(*types.AttributeValueMemberB)
	if !ok {
		return Entry{}, errCorrupt
	}
	return Decode(b.Value)
}

// Read implements Log with consistent Queries, paging until limit.
func (l *DynLog) Read(ctx context.Context, from core.LogSlot, limit int) ([]Slot, error) {
	var out []Slot
	var start map[string]types.AttributeValue
	for len(out) < limit {
		page, err := l.query(ctx, slotKey(int64(max(from, 0))), true, int32(limit-len(out)), start)
		if err != nil {
			return nil, err
		}
		for _, it := range page.Items {
			s, err := decodeSlot(it)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		if start = page.LastEvaluatedKey; start == nil {
			break
		}
	}
	return out, nil
}

func decodeSlot(it map[string]types.AttributeValue) (Slot, error) {
	var n int64
	sk, _ := it["sk"].(*types.AttributeValueMemberS)
	if sk == nil {
		return Slot{}, errCorrupt
	}
	if _, err := fmt.Sscanf(sk.Value, "%d", &n); err != nil {
		return Slot{}, err
	}
	e, err := decodeItem(it)
	return Slot{N: core.LogSlot(n), E: e}, err
}

func (l *DynLog) query(ctx context.Context, lo string, forward bool, limit int32, start map[string]types.AttributeValue) (*dynamodb.QueryOutput, error) {
	return l.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              &l.table,
		KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": sv(logPK), ":lo": sv(lo), ":hi": sv(slotMax),
		},
		ConsistentRead:    aws.Bool(true),
		ScanIndexForward:  aws.Bool(forward),
		Limit:             aws.Int32(limit),
		ExclusiveStartKey: start,
	})
}

// CheckTail implements Log with a consistent descending Query, Limit 1.
func (l *DynLog) CheckTail(ctx context.Context) (core.LogSlot, error) {
	page, err := l.query(ctx, slotKey(0), false, 1, nil)
	if err != nil || len(page.Items) == 0 {
		return 0, err
	}
	s, err := decodeSlot(page.Items[0])
	return s.N + 1, err
}
