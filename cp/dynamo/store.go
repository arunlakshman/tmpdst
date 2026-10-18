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
	"github.com/aws/smithy-go"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpobs"
	"manifester/sys/awsobs"
)

// MaxTxItems is DynamoDB's TransactWriteItems limit. A commit needs
// 2 (fence, segment) + 2 x (partitions that got a new batch: HWM and one range
// run item) + (grown topics) + (producer states touched) items, whatever the
// number of batches: the batches of one partition share one run item (runs.go).
// So 24 partitions with up to 50 producer states fit.
const MaxTxItems = 100

// maxBlob keeps the segment item (batches + ranges as JSON) well under
// DynamoDB's 400 KB item limit.
const maxBlob = 300 << 10

const maxAttempts = 20

// DB is the part of the AWS DynamoDB client the store calls. *dynamodb.Client
// implements it; dynamo/fakedb is an in-memory implementation for tests that
// must not need Docker.
type DB interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	DeleteTable(ctx context.Context, in *dynamodb.DeleteTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
}

var _ DB = (*dynamodb.Client)(nil)

// Store implements cp.Store. Any number of replicas may share one table.
type Store struct {
	db     DB
	prefix string
	table  string
	// Obs, if set, records call timings and retries (non-deterministic edge).
	Obs *cpobs.Metrics
}

var _ cp.Store = (*Store)(nil)

// New returns a store over the table prefix+"cp".
func New(db DB, prefix string) *Store {
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

// retryable lists the SDK errors that mean "try again later".
var retryable = []func(error) bool{
	isType[*types.ProvisionedThroughputExceededException],
	isType[*types.RequestLimitExceeded],
	isType[*types.ThrottlingException],
	isType[*types.TransactionInProgressException],
	isType[*types.TransactionConflictException],
	isType[*types.InternalServerError],
}

func isType[T error](err error) bool {
	var t T
	return errors.As(err, &t)
}

// infra maps SDK errors that mean "try again later" to cp.ErrUnavailable.
func infra(err error) error {
	for _, is := range retryable {
		if is(err) {
			return fmt.Errorf("%w: %v", cp.ErrUnavailable, err)
		}
	}
	return err
}

// capacities adapts the single ConsumedCapacity of GetItem and Query. A server
// that does not report it (ExtendDB) leaves it nil.
func capacities(c *types.ConsumedCapacity) []types.ConsumedCapacity {
	if c == nil {
		return nil
	}
	return []types.ConsumedCapacity{*c}
}

// note logs a failed DynamoDB call that infra() maps to "try again later"
// (throttling, conflicts, 5xx) at WARN, with the error code. The caller
// returns the error; the HTTP layer answers 503 and the broker retries.
func (s *Store) note(ctx context.Context, call string, err error) {
	if s.Obs == nil || errors.Is(err, context.Canceled) || !errors.Is(infra(err), cp.ErrUnavailable) {
		return
	}
	var ae smithy.APIError
	code := ""
	if errors.As(err, &ae) {
		code = ae.ErrorCode()
	}
	s.Obs.Logger().Warn("dynamodb call failed", "event", "dynamo_call_failed", "call", call, "code", code,
		"throttled", awsobs.IsThrottle(code), "err", err.Error())
}

// getItem is a consistent GetItem; nil if absent.
func (s *Store) getItem(ctx context.Context, k item) (item, error) {
	t0 := time.Now()
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.table, Key: k, ConsistentRead: aws.Bool(true),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
	var cc []types.ConsumedCapacity
	if out != nil {
		cc = capacities(out.ConsumedCapacity)
	}
	s.Obs.CallCtx(ctx, "GetItem", t0, cc)
	if err != nil {
		s.note(ctx, "GetItem", err)
		return nil, infra(err)
	}
	return out.Item, nil
}

// batchGet reads keys consistently; the result is keyed by "pk\x00sk".
func (s *Store) batchGet(ctx context.Context, keys []item) (map[string]item, error) {
	res := map[string]item{}
	for len(keys) > 0 {
		n := min(len(keys), 100)
		if err := s.batchGetChunk(ctx, keys[:n], res); err != nil {
			return nil, err
		}
		keys = keys[n:]
	}
	return res, nil
}

// batchGetChunk reads up to 100 keys into res, resending UnprocessedKeys with
// a doubling backoff, bounded.
func (s *Store) batchGetChunk(ctx context.Context, keys []item, res map[string]item) error {
	req := map[string]types.KeysAndAttributes{s.table: {Keys: keys, ConsistentRead: aws.Bool(true)}}
	for backoff := 5 * time.Millisecond; len(req) > 0; backoff *= 2 {
		out, err := s.batchGetPage(ctx, req)
		if err != nil {
			return err
		}
		for _, it := range out.Responses[s.table] {
			res[keyID(it)] = it
		}
		if req = out.UnprocessedKeys; len(req) == 0 {
			return nil
		}
		if backoff > time.Second {
			return fmt.Errorf("%w: batch get unprocessed", cp.ErrUnavailable)
		}
		if err := sleep(ctx, backoff); err != nil {
			return err
		}
	}
	return nil
}

// batchGetPage is one measured BatchGetItem call.
func (s *Store) batchGetPage(ctx context.Context, req map[string]types.KeysAndAttributes) (*dynamodb.BatchGetItemOutput, error) {
	t0 := time.Now()
	out, err := s.db.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
	var cc []types.ConsumedCapacity
	if out != nil {
		cc = out.ConsumedCapacity
	}
	s.Obs.CallCtx(ctx, "BatchGetItem", t0, cc)
	if err != nil {
		s.note(ctx, "BatchGetItem", err)
		return nil, infra(err)
	}
	return out, nil
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
