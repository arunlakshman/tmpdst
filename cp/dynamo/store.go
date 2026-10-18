// Package dynamo is a cp.Store over DynamoDB. See cp/doc.go for the table
// design and the commit transaction.
package dynamo

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
	"manifester/cp"
	"manifester/cp/cpobs"
)

// MaxTxItems is DynamoDB's TransactWriteItems limit. A commit needs
// 2 + (changed partitions) + (grown topics) + (producer states touched) +
// (assigned batches) items, so about 30 partitions per segment with
// idempotent producers, 48 without.
const MaxTxItems = 100

// maxBlob keeps the segment item (batches + ranges as JSON) well under
// DynamoDB's 400 KB item limit.
const maxBlob = 300 << 10

const maxAttempts = 20

// Store implements cp.Store. Any number of replicas may share one table.
type Store struct {
	db     *dynamodb.Client
	prefix string
	table  string
	// Obs, if set, records call timings and retries (non-deterministic edge).
	Obs *cpobs.Metrics
}

var _ cp.Store = (*Store)(nil)

// New returns a store over the table prefix+"cp".
func New(db *dynamodb.Client, prefix string) *Store {
	return &Store{db: db, prefix: prefix, table: prefix + "cp"}
}

// Prefix is the table name prefix.
func (s *Store) Prefix() string { return s.prefix }

// Table is the table name.
func (s *Store) Table() string { return s.table }

type item = map[string]types.AttributeValue

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func num(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func blob(v []byte) types.AttributeValue { return &types.AttributeValueMemberB{Value: v} }

func key(pk, sk string) item { return item{"pk": str(pk), "sk": str(sk)} }

func partKey(prefix string, tp core.TopicPartition) string {
	return prefix + tp.Topic + "#" + strconv.Itoa(int(tp.Partition))
}

func offSK(o int64) string { return fmt.Sprintf("%020d", o) }

const hwmSK = "!" // sorts before every digit string: never inside a range query

func nodeKey(n core.NodeID) item         { return key("N#"+string(n), "-") }
func pidKey() item                       { return key("PID", "-") }
func topicKey(t string) item             { return key("TOPICS", t) }
func segKey(k string) item               { return key("G#"+k, "-") }
func hwmKey(tp core.TopicPartition) item { return key(partKey("R#", tp), hwmSK) }
func stateKey(k skey) item {
	return key(partKey("S#", k.tp), fmt.Sprintf("%020d", int64(k.p)))
}

func getNum(it item, name string) (int64, bool) {
	v, ok := it[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v.Value, 10, 64)
	return n, err == nil
}

func getStr(it item, name string) string {
	v, _ := it[name].(*types.AttributeValueMemberS)
	if v == nil {
		return ""
	}
	return v.Value
}

func getBlob(it item, name string) []byte {
	v, _ := it[name].(*types.AttributeValueMemberB)
	if v == nil {
		return nil
	}
	return v.Value
}

// infra maps SDK errors that mean "try again later" to cp.ErrUnavailable.
func infra(err error) error {
	if err == nil {
		return nil
	}
	var (
		a *types.ProvisionedThroughputExceededException
		b *types.RequestLimitExceeded
		c *types.ThrottlingException
		d *types.TransactionInProgressException
		e *types.TransactionConflictException
		f *types.InternalServerError
	)
	if errors.As(err, &a) || errors.As(err, &b) || errors.As(err, &c) || errors.As(err, &d) || errors.As(err, &e) || errors.As(err, &f) {
		return fmt.Errorf("%w: %v", cp.ErrUnavailable, err)
	}
	return err
}

// getItem is a consistent GetItem; nil if absent.
func (s *Store) getItem(ctx context.Context, k item) (item, error) {
	t0 := time.Now()
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.table, Key: k, ConsistentRead: aws.Bool(true)})
	s.Obs.Call("GetItem", t0)
	if err != nil {
		return nil, infra(err)
	}
	return out.Item, nil
}

// batchGet reads keys consistently; the result is keyed by "pk\x00sk".
func (s *Store) batchGet(ctx context.Context, keys []item) (map[string]item, error) {
	res := map[string]item{}
	for len(keys) > 0 {
		n := min(len(keys), 100)
		req := map[string]types.KeysAndAttributes{s.table: {Keys: keys[:n], ConsistentRead: aws.Bool(true)}}
		keys = keys[n:]
		for backoff := 5 * time.Millisecond; len(req) > 0; backoff *= 2 {
			t0 := time.Now()
			out, err := s.db.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			s.Obs.Call("BatchGetItem", t0)
			if err != nil {
				return nil, infra(err)
			}
			for _, it := range out.Responses[s.table] {
				res[getStr(it, "pk")+"\x00"+getStr(it, "sk")] = it
			}
			req = out.UnprocessedKeys
			if len(req) > 0 {
				if backoff > time.Second {
					return nil, fmt.Errorf("%w: batch get unprocessed", cp.ErrUnavailable)
				}
				if err := sleep(ctx, backoff); err != nil {
					return nil, err
				}
			}
		}
	}
	return res, nil
}

func lookupKey(m map[string]item, k item) item {
	return m[k["pk"].(*types.AttributeValueMemberS).Value+"\x00"+k["sk"].(*types.AttributeValueMemberS).Value]
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
