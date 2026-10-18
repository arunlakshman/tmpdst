package dynamo

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/core"
)

// countDB counts the data calls of a store (name -> number).
type countDB struct {
	DB
	mu sync.Mutex
	n  map[string]int
}

func (c *countDB) inc(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[call]++
}

func (c *countDB) total() (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.n {
		n += v
	}
	return n
}

func (c *countDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	c.inc("GetItem")
	return c.DB.GetItem(ctx, in, o...)
}

func (c *countDB) BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	c.inc("BatchGetItem")
	return c.DB.BatchGetItem(ctx, in, o...)
}

func (c *countDB) Query(ctx context.Context, in *dynamodb.QueryInput, o ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	c.inc("Query")
	return c.DB.Query(ctx, in, o...)
}

func (c *countDB) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, o ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	c.inc("TransactWriteItems")
	return c.DB.TransactWriteItems(ctx, in, o...)
}

// benchStore is a store over a fresh table on the endpoint (ExtendDB via
// scripts/it-up.sh), or skips.
func benchStore(tb testing.TB) *Store {
	tb.Helper()
	e := os.Getenv("MANIFESTER_DYNAMO_ENDPOINT")
	if e == "" {
		tb.Skip("MANIFESTER_DYNAMO_ENDPOINT not set (scripts/it-up.sh)")
	}
	db := dynamodb.New(dynamodb.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(e),
		Credentials: credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", ""),
	})
	st := New(&countDB{DB: db}, fmt.Sprintf("b%d_", time.Now().UnixNano()))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := st.CreateTables(ctx); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { st.DeleteTables(context.Background()) })
	return st
}

// A segment of 200 small batches for one partition and 40 for another is
// stored as one run item per partition, not 240 items.
func TestOneRunItemPerPartition(t *testing.T) {
	st := benchStore(t)
	ctx := context.Background()
	tpa, tpb := core.TopicPartition{Topic: "x", Partition: 0}, core.TopicPartition{Topic: "x", Partition: 1}
	var bs []core.PendingBatch
	for i := 0; i < 240; i++ {
		tp := tpa
		if i%6 == 5 {
			tp = tpb
		}
		bs = append(bs, core.PendingBatch{TP: tp, Records: 2, Loc: core.SegmentRef{Key: "s", Off: int64(i) * 9, Len: 9}})
	}
	for _, key := range []string{"s", "s2"} { // two segments: two runs per partition
		if _, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: key, Batches: bs}); err != nil || code != core.OK {
			t.Fatalf("%s: %v %v", key, code, err)
		}
	}
	for tp, want := range map[core.TopicPartition]int{tpa: 2, tpb: 2} {
		items, err := st.query(ctx, &dynamodb.QueryInput{KeyConditionExpression: aws.String("pk = :p"),
			ExpressionAttributeValues: item{":p": str(partKey("R#", tp))}}, 0)
		if err != nil || len(items) != want {
			t.Fatalf("%v: %d items (want %d), err %v", tp, len(items), want, err)
		}
	}
	l, _, err := st.Lookup(ctx, core.Lookup{TP: tpa, From: 199})
	if err != nil || l.End != 800 || len(l.Ranges) == 0 || l.Ranges[0].Base > 199 || l.Ranges[0].Base+core.Offset(l.Ranges[0].Count) <= 199 {
		t.Fatalf("lookup at the run boundary: end %d, %d ranges, %v", l.End, len(l.Ranges), err)
	}
}

// BenchmarkCommit times CommitSegment of one segment of batches spread
// round-robin over parts partitions (the shape of a busy broker) and reports
// the DynamoDB calls per commit. Run: go test -run X -bench Commit ./cp/dynamo
func BenchmarkCommit(b *testing.B) {
	for _, c := range []struct{ batches, parts int }{{32, 24}, {32, 6}, {256, 24}, {256, 6}} {
		b.Run(fmt.Sprintf("batches=%d/parts=%d", c.batches, c.parts), func(b *testing.B) {
			st := benchStore(b)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("seg-%d", i)
				var bs []core.PendingBatch
				for j := 0; j < c.batches; j++ {
					bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "bench", Partition: int32(j % c.parts)},
						Records: 10, Loc: core.SegmentRef{Key: key, Off: int64(j) * 1000, Len: 1000}})
				}
				_, code, err := st.CommitSegment(ctx, core.CommitSegment{SegmentKey: key, Batches: bs})
				if err != nil || code != core.OK {
					b.Skipf("commit refused: code %v err %v", code, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(st.db.(*countDB).total())/float64(b.N), "calls/op")
		})
	}
}
