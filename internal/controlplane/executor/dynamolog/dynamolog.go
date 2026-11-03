// Package dynamolog is the DynamoDB implementation of executor.Log. It keeps the
// AWS SDK out of the packages that only need the Log interface.
package dynamolog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/core"
)

// Client is the DynamoDB surface Log needs. *dynamodb.Client
// satisfies it.
type Client interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// Options tune a Log.
type Options struct {
	TTL time.Duration
	Now func() time.Time
}

// Log stores opaque machine payloads in one DynamoDB partition
// (pk=LOG, sk=20-digit slot). Bytes in attribute d are not decoded here.
//
// The writer epoch and the floor live in another partition (pk=META):
// sk=WRITER holds epoch and owner, and sk=FLOOR holds floor. The log Query
// never sees them.
type Log struct {
	db    Client
	table string
	o     Options
}

const (
	logPartitionKey  = "LOG"
	metaPartitionKey = "META"
	writerSortKey    = "WRITER"
	floorSortKey     = "FLOOR"
	slotMax          = "99999999999999999999"
)

// New returns a byte log over table.
func New(db Client, table string, o Options) *Log {
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Log{db: db, table: table, o: o}
}

func slotKey(n core.LogPosition) string { return fmt.Sprintf("%020d", n) }
func stringValue(v string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: v}
}
func numberValue(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func uintValue(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

// metaKey is the key of one item in the META partition.
func metaKey(sortKey string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": stringValue(metaPartitionKey), "sk": stringValue(sortKey)}
}

// Append implements Log with a conditional PutItem. After a put that succeeds
// it reads the floor. A slot at or below the floor is not a commit (see Log).
func (l *Log) Append(ctx context.Context, slot core.LogPosition, data []byte) ([]byte, bool, error) {
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
		ConditionExpression:                 new("attribute_not_exists(pk)"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		below, err := l.atOrBelowFloor(ctx, slot)
		if err != nil {
			return nil, false, err
		}
		if below {
			return nil, true, nil
		}
		return nil, false, nil
	}
	cc, isCC := errors.AsType[*types.ConditionalCheckFailedException](err)
	if !isCC {
		return nil, false, err
	}
	// The slot is taken. Bytes at or below the floor are a stray row or a trimmed
	// entry, and the caller must not treat them as its own entry. So that case
	// is not a commit either.
	below, err := l.atOrBelowFloor(ctx, slot)
	if err != nil {
		return nil, false, err
	}
	if below {
		return nil, true, nil
	}
	if cc.Item != nil {
		return itemData(cc.Item), true, nil
	}
	ents, err := l.Read(ctx, slot, 1)
	if err != nil || len(ents) == 0 || ents[0].Slot != slot {
		return nil, false, fmt.Errorf("executor: slot %d taken but unreadable: %w", slot, err)
	}
	return ents[0].Data, true, nil
}

// atOrBelowFloor reports whether slot is at or below the floor. It reads the
// floor with one consistent GetItem.
func (l *Log) atOrBelowFloor(ctx context.Context, slot core.LogPosition) (bool, error) {
	floor, ok, err := l.floor(ctx)
	return ok && int64(slot) <= floor, err
}

// getMeta reads one META item with a consistent read. It returns nil when the
// item does not exist.
func (l *Log) getMeta(ctx context.Context, sortKey string) (map[string]types.AttributeValue, error) {
	out, err := l.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &l.table, Key: metaKey(sortKey), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	return out.Item, nil
}

// floor reads META/FLOOR. ok is false when the item does not exist yet, which
// means nothing was trimmed.
func (l *Log) floor(ctx context.Context) (int64, bool, error) {
	it, err := l.getMeta(ctx, floorSortKey)
	if err != nil || it == nil {
		return 0, false, err
	}
	n, isNum := it["floor"].(*types.AttributeValueMemberN)
	if !isNum {
		return 0, false, fmt.Errorf("executor: %s has no numeric floor", floorSortKey)
	}
	value, err := strconv.ParseInt(n.Value, 10, 64)
	return value, err == nil, err
}

// raiseFloor moves META/FLOOR up to through. It never moves it down: a
// ConditionalCheckFailed means a higher floor is already stored, which is fine.
func (l *Log) raiseFloor(ctx context.Context, through core.LogPosition) error {
	_, err := l.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &l.table,
		Key:                       metaKey(floorSortKey),
		UpdateExpression:          new("SET #floor = :through"),
		ConditionExpression:       new("attribute_not_exists(#floor) OR #floor < :through"),
		ExpressionAttributeNames:  map[string]string{"#floor": "floor"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":through": numberValue(int64(through))},
	})
	if _, isCC := errors.AsType[*types.ConditionalCheckFailedException](err); isCC {
		return nil
	}
	if err != nil {
		return fmt.Errorf("executor: raise floor to %d: %w", through, err)
	}
	return nil
}

// ClaimEpoch implements Log with a conditional UpdateItem on META/WRITER.
func (l *Log) ClaimEpoch(ctx context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	cond := "#epoch = :expect"
	if expect == 0 {
		// No epoch stored yet counts as 0.
		cond = "attribute_not_exists(#epoch) OR #epoch = :expect"
	}
	next := expect + 1
	_, err := l.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                           &l.table,
		Key:                                 metaKey(writerSortKey),
		UpdateExpression:                    new("SET #epoch = :next, #owner = :owner"),
		ConditionExpression:                 new(cond),
		ExpressionAttributeNames:            map[string]string{"#epoch": "epoch", "#owner": "owner"},
		ExpressionAttributeValues:           map[string]types.AttributeValue{":expect": uintValue(expect), ":next": uintValue(next), ":owner": stringValue(owner)},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return next, owner, true, nil
	}
	cc, isCC := errors.AsType[*types.ConditionalCheckFailedException](err)
	if !isCC {
		return 0, "", false, err
	}
	it := cc.Item
	if it == nil {
		// The item does not exist yet, or the service did not send it back.
		if it, err = l.getMeta(ctx, writerSortKey); err != nil {
			return 0, "", false, err
		}
	}
	epoch, stored, err := writerOf(it)
	return epoch, stored, false, err
}

// writerOf reads epoch and owner from a META/WRITER item. A missing item means
// epoch 0 and no owner.
func writerOf(it map[string]types.AttributeValue) (uint64, string, error) {
	var epoch uint64
	if n, ok := it["epoch"].(*types.AttributeValueMemberN); ok {
		v, err := strconv.ParseUint(n.Value, 10, 64)
		if err != nil {
			return 0, "", fmt.Errorf("executor: bad writer epoch %q: %w", n.Value, err)
		}
		epoch = v
	}
	owner := ""
	if s, ok := it["owner"].(*types.AttributeValueMemberS); ok {
		owner = s.Value
	}
	return epoch, owner, nil
}

func (l *Log) between(lo, hi string) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		TableName:              &l.table,
		KeyConditionExpression: new("pk = :p AND sk BETWEEN :lo AND :hi"),
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
		return 0, executor.ErrInvalidSlot
	}
	n, err := strconv.ParseInt(sortKey.Value, 10, 64)
	return core.LogPosition(n), err
}

// Read implements Log with consistent Queries. It runs the Query first and
// reads the floor after it. Then it drops every row at or below the floor. A
// stray row (see Append) can be in the Query result, and a trim that raises the
// floor during the Query is still caught, because the floor is read last.
func (l *Log) Read(ctx context.Context, from core.LogPosition, limit int) ([]core.LogEntry, error) {
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
			out = append(out, core.LogEntry{Slot: n, Data: bytes.Clone(itemData(it))})
		}
		if start = page.LastEvaluatedKey; start == nil {
			break
		}
	}
	if len(out) == 0 {
		return out, nil
	}
	floor, ok, err := l.floor(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return out, nil
	}
	return slices.DeleteFunc(out, func(e core.LogEntry) bool { return int64(e.Slot) <= floor }), nil
}

// CheckTail implements Log.
func (l *Log) CheckTail(ctx context.Context) (core.LogPosition, error) {
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

// TrimThrough raises the floor to through, and only after that deletes every
// slot at or below through. A slot must be covered by the floor before it is
// deleted.
func (l *Log) TrimThrough(ctx context.Context, through core.LogPosition) error {
	if through < 0 {
		return executor.ErrInvalidSlot
	}
	if err := l.raiseFloor(ctx, through); err != nil {
		return err
	}
	const batch = 25
	var start map[string]types.AttributeValue
	for {
		q := l.between(slotKey(0), slotKey(through))
		q.ProjectionExpression = new("pk, sk")
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
			key := map[string]types.AttributeValue{"pk": it["pk"], "sk": it["sk"]}
			reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}})
		}
		if err := l.deleteBatch(ctx, reqs); err != nil {
			return err
		}
		if start = page.LastEvaluatedKey; start == nil {
			return nil
		}
	}
}

// deleteBatch sends one batch of deletes and repeats the part DynamoDB did not
// process. It fails if some deletes are still left after a few rounds, so a
// trim is never reported as done when old slots remain.
func (l *Log) deleteBatch(ctx context.Context, reqs []types.WriteRequest) error {
	const rounds = 5
	for range rounds {
		out, err := l.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{l.table: reqs},
		})
		if err != nil {
			return err
		}
		reqs = out.UnprocessedItems[l.table]
		if len(reqs) == 0 {
			return nil
		}
	}
	return fmt.Errorf("executor: %d slot deletes were not processed", len(reqs))
}
