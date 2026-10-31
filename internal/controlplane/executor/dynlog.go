package executor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"mds/internal/core"
)

// DynamoClient is the DynamoDB surface DynamoLog needs.
type DynamoClient interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// DynamoLogOptions tune a DynamoLog.
type DynamoLogOptions struct {
	TTL     time.Duration
	Now     func() time.Time
	StubTTL time.Duration
}

// DynamoLog stores opaque machine payloads in one DynamoDB partition
// (pk=LOG, sk=20-digit slot). Bytes in attribute d are not decoded here.
type DynamoLog struct {
	db    DynamoClient
	table string
	o     DynamoLogOptions
}

const (
	logPartitionKey    = "LOG"
	slotMax            = "99999999999999999999"
	deletedMarkerBytes = 0xFE
)

// NewDynamoLog returns a byte log over table.
func NewDynamoLog(db DynamoClient, table string, o DynamoLogOptions) *DynamoLog {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.StubTTL == 0 && o.TTL > 0 {
		o.StubTTL = 30 * 24 * time.Hour
	}
	return &DynamoLog{db: db, table: table, o: o}
}

func slotKey(n core.LogPosition) string { return fmt.Sprintf("%020d", n) }
func stringValue(v string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: v}
}
func numberValue(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

// Append implements Log with a conditional PutItem.
func (l *DynamoLog) Append(ctx context.Context, slot core.LogPosition, data []byte) ([]byte, bool, error) {
	it := map[string]types.AttributeValue{
		"pk": stringValue(logPartitionKey), "sk": stringValue(slotKey(slot)),
		"d": &types.AttributeValueMemberB{Value: data},
	}
	if l.o.TTL > 0 {
		it["ttl"] = numberValue(l.o.Now().Add(l.o.TTL).Unix())
	}
	_, err := l.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                           &l.table,
		Item:                                it,
		ConditionExpression:                 aws.String("attribute_not_exists(pk)"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	var cc *types.ConditionalCheckFailedException
	switch {
	case err == nil:
		return nil, false, nil
	case !errors.As(err, &cc):
		return nil, false, err
	case cc.Item != nil:
		return itemData(cc.Item), true, nil
	}
	ents, err := l.Read(ctx, slot, 1)
	if err != nil || len(ents) == 0 || ents[0].Slot != slot {
		return nil, false, fmt.Errorf("executor: slot %d taken but unreadable: %w", slot, err)
	}
	return ents[0].Data, true, nil
}

func (l *DynamoLog) between(lo, hi string) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		TableName:              &l.table,
		KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": stringValue(logPartitionKey), ":lo": stringValue(lo), ":hi": stringValue(hi),
		},
	}
}

func itemData(it map[string]types.AttributeValue) []byte {
	b, ok := it["d"].(*types.AttributeValueMemberB)
	if !ok {
		return nil
	}
	return b.Value
}

func itemSlot(it map[string]types.AttributeValue) (core.LogPosition, error) {
	sortKey, ok := it["sk"].(*types.AttributeValueMemberS)
	if !ok {
		return 0, ErrInvalidSlot
	}
	n, err := strconv.ParseInt(sortKey.Value, 10, 64)
	return core.LogPosition(n), err
}

// Read implements Log with consistent Queries.
func (l *DynamoLog) Read(ctx context.Context, from core.LogPosition, limit int) ([]core.LogEntry, error) {
	var out []core.LogEntry
	var start map[string]types.AttributeValue
	for len(out) < limit {
		q := l.between(slotKey(max(from, 0)), slotMax)
		q.ConsistentRead = aws.Bool(true)
		q.ScanIndexForward = aws.Bool(true)
		q.Limit = aws.Int32(int32(limit - len(out))) // limit is small
		q.ExclusiveStartKey = start
		page, err := l.db.Query(ctx, q)
		if err != nil {
			return out, err
		}
		for _, it := range page.Items {
			n, err := itemSlot(it)
			if err != nil {
				return out, err
			}
			data := itemData(it)
			if len(data) == 1 && data[0] == deletedMarkerBytes {
				continue
			}
			out = append(out, core.LogEntry{Slot: n, Data: append([]byte(nil), data...)})
		}
		if start = page.LastEvaluatedKey; start == nil {
			break
		}
	}
	return out, nil
}

// CheckTail implements Log.
func (l *DynamoLog) CheckTail(ctx context.Context) (core.LogPosition, error) {
	q := l.between(slotKey(0), slotMax)
	q.ConsistentRead = aws.Bool(true)
	q.ScanIndexForward = aws.Bool(false)
	q.Limit = aws.Int32(1)
	page, err := l.db.Query(ctx, q)
	if err != nil || len(page.Items) == 0 {
		return 0, err
	}
	n, err := itemSlot(page.Items[0])
	return n + 1, err
}

// TrimThrough tombstones every slot at or below through.
func (l *DynamoLog) TrimThrough(ctx context.Context, through core.LogPosition) error {
	if through < 0 {
		return ErrInvalidSlot
	}
	const batch = 25
	var start map[string]types.AttributeValue
	for {
		q := l.between(slotKey(0), slotKey(through))
		q.ProjectionExpression = aws.String("pk, sk")
		q.Limit = aws.Int32(batch)
		q.ExclusiveStartKey = start
		page, err := l.db.Query(ctx, q)
		if err != nil {
			return err
		}
		if len(page.Items) == 0 {
			return nil
		}
		reqs := make([]types.WriteRequest, 0, len(page.Items))
		for _, it := range page.Items {
			item := map[string]types.AttributeValue{
				"pk": it["pk"], "sk": it["sk"],
				"d": &types.AttributeValueMemberB{Value: []byte{deletedMarkerBytes}},
			}
			if l.o.StubTTL > 0 {
				item["ttl"] = numberValue(l.o.Now().Add(l.o.StubTTL).Unix())
			}
			reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		}
		_, err = l.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{l.table: reqs},
		})
		if err != nil {
			return err
		}
		if start = page.LastEvaluatedKey; start == nil {
			return nil
		}
	}
}
