package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// API is the subset of the DynamoDB client the probe uses (the real client satisfies it).
type API interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opt ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opt ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opt ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opt ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

const logPK = "LOG"

func slotKey(n int64) string { return fmt.Sprintf("%020d", n) }

func str(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func blob(n int) types.AttributeValue { return &types.AttributeValueMemberB{Value: make([]byte, n)} }

func capSum(cs []types.ConsumedCapacity) float64 {
	t := 0.0
	for _, c := range cs {
		t += aws.ToFloat64(c.CapacityUnits)
	}
	return t
}

func logItem(slot int64, size int) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": str(logPK), "sk": str(slotKey(slot)), "v": blob(size)}
}

const notExists = "attribute_not_exists(pk)"

func putSlot(ctx context.Context, api API, table string, slot int64, size int) result {
	out, err := api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &table, Item: logItem(slot, size), ConditionExpression: aws.String(notExists),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	r := result{calls: 1, err: err}
	if out != nil && out.ConsumedCapacity != nil {
		r.capacity = aws.ToFloat64(out.ConsumedCapacity.CapacityUnits)
	}
	return r
}

// putOp writes consecutive slots start+i.
func putOp(api API, table string, start int64, size int) op {
	return func(ctx context.Context, i int64) result { return putSlot(ctx, api, table, start+i, size) }
}

// txnOp commits 2 G# items of gSize plus the LOG slot, all attribute_not_exists.
func txnOp(api API, table string, start int64, logSize, gSize int) op {
	put := func(item map[string]types.AttributeValue) types.TransactWriteItem {
		return types.TransactWriteItem{Put: &types.Put{TableName: &table, Item: item, ConditionExpression: aws.String(notExists)}}
	}
	return func(ctx context.Context, i int64) result {
		slot := start + i
		g := func(s string) map[string]types.AttributeValue {
			return map[string]types.AttributeValue{"pk": str(fmt.Sprintf("G#%d%s", slot, s)), "sk": str("x"), "v": blob(gSize)}
		}
		out, err := api.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems:          []types.TransactWriteItem{put(g("a")), put(g("b")), put(logItem(slot, logSize))},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		})
		r := result{calls: 1, err: err}
		if out != nil {
			r.capacity = capSum(out.ConsumedCapacity)
		}
		return r
	}
}

// contenders models n appenders that each keep their own next-slot cursor and race for the same slots. A loser
// (ConditionalCheckFailed) moves its cursor past the highest slot any appender has committed (a free "tail"
// signal, optimistic versus real tailing) and retries.
type contenders struct {
	api   API
	table string
	size  int
	tail  atomic.Int64
	next  []atomic.Int64
}

const maxAttempts = 50

func newContenders(api API, table string, start int64, size, n int) *contenders {
	c := &contenders{api: api, table: table, size: size, next: make([]atomic.Int64, n)}
	for i := range c.next {
		c.next[i].Store(start)
	}
	c.tail.Store(start - 1)
	return c
}

func (c *contenders) bump(t int64) {
	for {
		cur := c.tail.Load()
		if t <= cur || c.tail.CompareAndSwap(cur, t) {
			return
		}
	}
}

// opFor returns the op for appender w.
func (c *contenders) opFor(w int) op {
	return func(ctx context.Context, _ int64) result {
		var tot result
		slot := c.next[w].Add(1) - 1
		for a := 0; a < maxAttempts; a++ {
			r := putSlot(ctx, c.api, c.table, slot, c.size)
			tot.calls++
			tot.capacity += r.capacity
			if r.err == nil {
				c.bump(slot)
				return tot
			}
			if classify(r.err) != "ConditionalCheckFailed" {
				tot.err = r.err
				return tot
			}
			tot.ccf++
			slot = c.retrySlot(w, slot)
		}
		tot.err = errors.New("too many attempts")
		return tot
	}
}

func (c *contenders) retrySlot(w int, lost int64) int64 {
	c.bump(lost) // the slot exists, so it is at most the tail
	slot := c.tail.Load() + 1
	for {
		cur := c.next[w].Load()
		if cur > slot || c.next[w].CompareAndSwap(cur, slot+1) {
			break
		}
	}
	return slot
}

// lookupQuery runs one consistent Query on pk=LOG: "desc" is Limit 1 descending; "after" is sk > n
// (n = tail-gap, found with the desc query at setup).
func lookupQuery(ctx context.Context, api API, table, kind string, after int64) result {
	in := &dynamodb.QueryInput{TableName: &table, ConsistentRead: aws.Bool(true), ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal}
	if kind == "desc" {
		in.KeyConditionExpression = aws.String("pk = :p")
		in.ExpressionAttributeValues = map[string]types.AttributeValue{":p": str(logPK)}
		in.Limit, in.ScanIndexForward = aws.Int32(1), aws.Bool(false)
	} else {
		in.KeyConditionExpression = aws.String("pk = :p AND sk > :s")
		in.ExpressionAttributeValues = map[string]types.AttributeValue{":p": str(logPK), ":s": str(slotKey(after))}
	}
	out, err := api.Query(ctx, in)
	r := result{calls: 1, err: err}
	if out != nil && out.ConsumedCapacity != nil {
		r.capacity = aws.ToFloat64(out.ConsumedCapacity.CapacityUnits)
	}
	return r
}

// tailSlot returns the highest slot in the table (desc Limit 1), or 0.
func tailSlot(ctx context.Context, api API, table string) (int64, error) {
	out, err := api.Query(ctx, &dynamodb.QueryInput{TableName: &table, ConsistentRead: aws.Bool(true),
		KeyConditionExpression:    aws.String("pk = :p"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": str(logPK)},
		Limit:                     aws.Int32(1), ScanIndexForward: aws.Bool(false)})
	if err != nil || len(out.Items) == 0 {
		return 0, err
	}
	var n int64
	_, err = fmt.Sscanf(out.Items[0]["sk"].(*types.AttributeValueMemberS).Value, "%d", &n)
	return n, err
}

// coalescer batches lookups arriving within a window into one Query; the first waiter carries the call's cost.
type coalescer struct {
	mu      sync.Mutex
	waiters []chan result
	fn      func(context.Context) result
}

func newCoalescer(ctx context.Context, window time.Duration, fn func(context.Context) result) *coalescer {
	c := &coalescer{fn: fn}
	go c.loop(ctx, window)
	return c
}

func (c *coalescer) loop(ctx context.Context, window time.Duration) {
	t := time.NewTicker(window)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.flush(ctx)
		}
	}
}

func (c *coalescer) flush(ctx context.Context) {
	c.mu.Lock()
	ws := c.waiters
	c.waiters = nil
	c.mu.Unlock()
	if len(ws) == 0 {
		return
	}
	r := c.fn(ctx)
	for i, w := range ws {
		out := r
		if i > 0 {
			out = result{err: r.err}
		}
		w <- out
	}
}

func (c *coalescer) lookup(ctx context.Context, _ int64) result {
	ch := make(chan result, 1)
	c.mu.Lock()
	c.waiters = append(c.waiters, ch)
	c.mu.Unlock()
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		return result{err: ctx.Err()}
	}
}

// batchOp writes per items of itemBytes (total item size, names included) to partitions drawn from parts keys.
func batchOp(api API, table string, per, parts, itemBytes int) op {
	pad := itemBytes - 20 // pk "I#00000"(2+7) + sk "r"(2+1) + attr name "v"(1) + overhead
	if pad < 0 {
		pad = 0
	}
	return func(ctx context.Context, i int64) result {
		rng := rand.New(rand.NewSource(i))
		reqs := make([]types.WriteRequest, 0, per)
		for _, p := range distinctKeys(rng, per, parts) {
			it := map[string]types.AttributeValue{"pk": str(fmt.Sprintf("I#%05d", p)), "sk": str("r"), "v": blob(pad)}
			reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: it}})
		}
		return sendBatch(ctx, api, table, reqs)
	}
}

const batchRetries = 3

func sendBatch(ctx context.Context, api API, table string, reqs []types.WriteRequest) result {
	var r result
	pending := map[string][]types.WriteRequest{table: reqs}
	for a := 0; a <= batchRetries && len(pending[table]) > 0; a++ {
		out, err := api.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending, ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
		r.calls++
		if err != nil {
			r.err = err
			return r
		}
		r.capacity += capSum(out.ConsumedCapacity)
		pending = out.UnprocessedItems
		r.unproc += len(pending[table])
		time.Sleep(time.Duration(a) * 20 * time.Millisecond)
	}
	r.items = len(reqs) - len(pending[table])
	if len(pending[table]) > 0 {
		r.err = errors.New("unprocessed after retries")
	}
	return r
}

// distinctKeys draws n different values in [0, parts); BatchWriteItem rejects duplicate keys in one call.
func distinctKeys(rng *rand.Rand, n, parts int) []int {
	seen := make(map[int]bool, n)
	out := make([]int, 0, n)
	for len(out) < n && len(out) < parts {
		if p := rng.Intn(parts); !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}
