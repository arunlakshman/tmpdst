package logcp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

// DB is the part of the DynamoDB client the service uses. *dynamodb.Client and
// cp/fakedb.Fake satisfy it.
type DB interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

var _ DB = (*dynamodb.Client)(nil)

// DynLogOptions tune a DynLog.
type DynLogOptions struct {
	// TTL, if > 0, stamps every slot with a ttl attribute (epoch seconds)
	// that far ahead, for the table's TTL setting to expire it. It must be
	// much longer than the checkpoint interval: a slot at or above the newest
	// checkpoint is needed to restart.
	TTL time.Duration
	// Now is the clock for ttl (default time.Now).
	Now func() time.Time
	// StubTTL, if > 0, lets a tombstone expire this long after it was written
	// (default 30 days when TTL is set). After that a slot is empty again, so
	// it must exceed the longest time a node can be stale and still running.
	StubTTL time.Duration
	// Version is the format written (default Version). During a rolling
	// upgrade to a new format keep the old value until every node reads the new one.
	Version byte
}

// DynLog is a Log over one DynamoDB partition: LOG/<slot as 20 digits>, one
// item per slot, written with attribute_not_exists. The table must exist
// (string keys pk and sk).
type DynLog struct {
	db    DB
	table string
	o     DynLogOptions
}

// NewDynLog returns a log in table.
func NewDynLog(db DB, table string, o DynLogOptions) *DynLog {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Version == 0 {
		o.Version = Version
	}
	if o.StubTTL == 0 && o.TTL > 0 {
		o.StubTTL = 30 * 24 * time.Hour
	}
	return &DynLog{db: db, table: table, o: o}
}

const (
	logPK   = "LOG"
	slotMax = "99999999999999999999"
)

func slotKey(n int64) string { return fmt.Sprintf("%020d", n) }

func sv(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func nv(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func (l *DynLog) item(slot core.LogSlot, e Entry) map[string]types.AttributeValue {
	it := map[string]types.AttributeValue{
		"pk": sv(logPK), "sk": sv(slotKey(int64(slot))),
		"d": &types.AttributeValueMemberB{Value: MarshalEntry(e, l.o.Version)},
		"v": nv(int64(l.o.Version)), "w": sv(e.Writer), "id": sv(e.ID), "ts": nv(e.TS), "n": nv(int64(len(e.Cmds))),
	}
	if l.o.TTL > 0 {
		it["ttl"] = nv(l.o.Now().Add(l.o.TTL).Unix())
	}
	return it
}

// Append implements Log with one conditional PutItem. A failed condition
// returns the existing item (ALL_OLD), so a loser learns the winner without
// another read; if the service did not return it, one consistent Query does.
func (l *DynLog) Append(ctx context.Context, slot core.LogSlot, e Entry) (Entry, bool, error) {
	it := l.item(slot, e)
	if sz := len(it["d"].(*types.AttributeValueMemberB).Value) + itemOverhead; sz > MaxEntryBytes {
		return Entry{}, false, ErrEntryTooLarge
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
		return Entry{}, false, fmt.Errorf("logcp: slot %d taken but unreadable: %w", slot, err)
	}
	return got[0].E, false, nil
}

func decodeItem(it map[string]types.AttributeValue) (Entry, error) {
	b, ok := it["d"].(*types.AttributeValueMemberB)
	if !ok {
		return Entry{}, errCorrupt
	}
	return UnmarshalEntry(b.Value)
}

func decodeSlot(it map[string]types.AttributeValue) (Slot, error) {
	n, err := decodeSlotNum(it)
	if err != nil {
		return Slot{}, err
	}
	e, err := decodeItem(it)
	return Slot{N: core.LogSlot(n), E: e}, err
}

// Read implements Log with consistent Queries, paging until limit.
func (l *DynLog) Read(ctx context.Context, from core.LogSlot, limit int) ([]Slot, error) {
	var out []Slot
	var start map[string]types.AttributeValue
	for len(out) < limit {
		page, err := l.query(ctx, slotKey(int64(max(from, 0))), true, int32(limit-len(out)), start)
		if err != nil {
			return out, err
		}
		for _, it := range page.Items {
			s, err := decodeSlot(it)
			if err != nil {
				return out, err
			}
			out = append(out, s)
		}
		if start = page.LastEvaluatedKey; start == nil {
			break
		}
	}
	return out, nil
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
	n, err := decodeSlotNum(page.Items[0])
	return core.LogSlot(n) + 1, err // whatever the slot holds (an unknown format, a tombstone), it is written
}

// trimBatch is the number of slots TrimBelow looks up and stubs per round.
const trimBatch = 250

// trimPK is the partition of the item that remembers how far the log has been
// trimmed (tr_below), so a restarted node does not rescan from slot 0.
const trimPK = "TRIM"

// TrimBelow implements Trimmer: every slot below the position is overwritten
// with a tombstone (a one-byte item), 25 per BatchWriteItem. A tombstone, not
// a deleted item, so a node with a stale slot number that appends or fills
// there is refused (the condition fails and the loser learns "trimmed") and
// stops, instead of writing into an empty slot the others have long applied.
// The tombstones expire by TTL after StubTTL.
func (l *DynLog) TrimBelow(ctx context.Context, slot core.LogSlot) (int, error) {
	if slot <= 0 {
		return 0, nil
	}
	from, err := l.trimmedBelow(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for from < slot {
		page, err := l.db.Query(ctx, &dynamodb.QueryInput{
			TableName:              &l.table,
			KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":p": sv(logPK), ":lo": sv(slotKey(int64(from))), ":hi": sv(slotKey(int64(slot - 1))),
			},
			ProjectionExpression: aws.String("pk, sk"),
			Limit:                aws.Int32(trimBatch),
		})
		if err != nil || len(page.Items) == 0 {
			break
		}
		if err := l.stub(ctx, page.Items); err != nil {
			return total, err
		}
		total += len(page.Items)
		last, _ := decodeSlotNum(page.Items[len(page.Items)-1])
		from = core.LogSlot(last) + 1
	}
	return total, l.setTrimmed(ctx, slot)
}

func (l *DynLog) trimmedBelow(ctx context.Context) (core.LogSlot, error) {
	out, err := l.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: &l.table, Key: pkSK(trimPK), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return 0, err
	}
	return core.LogSlot(numAttr(out.Item, "tr_below")), nil
}

func (l *DynLog) setTrimmed(ctx context.Context, slot core.LogSlot) error {
	_, err := l.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 &l.table,
		Item:                      map[string]types.AttributeValue{"pk": sv(trimPK), "sk": sv("-"), "tr_below": nv(int64(slot))},
		ConditionExpression:       aws.String("attribute_not_exists(tr_below) OR tr_below < :s"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":s": nv(int64(slot))},
	})
	var cc *types.ConditionalCheckFailedException
	if errors.As(err, &cc) {
		return nil
	}
	return err
}

func (l *DynLog) stub(ctx context.Context, keys []map[string]types.AttributeValue) error {
	for len(keys) > 0 {
		n := min(len(keys), 25)
		reqs := make([]types.WriteRequest, n)
		for i, k := range keys[:n] {
			it := map[string]types.AttributeValue{"pk": k["pk"], "sk": k["sk"],
				"d": &types.AttributeValueMemberB{Value: tombstone}, "tomb": nv(1)}
			if l.o.StubTTL > 0 {
				it["ttl"] = nv(l.o.Now().Add(l.o.StubTTL).Unix())
			}
			reqs[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: it}}
		}
		if err := writeAll(ctx, l.db, l.table, reqs, nil); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// decodeSlotNum parses the slot number of an item (or key).
func decodeSlotNum(it map[string]types.AttributeValue) (int64, error) {
	sk, _ := it["sk"].(*types.AttributeValueMemberS)
	if sk == nil {
		return 0, errCorrupt
	}
	n, err := strconv.ParseInt(sk.Value, 10, 64)
	if err != nil {
		return 0, errCorrupt
	}
	return n, nil
}
