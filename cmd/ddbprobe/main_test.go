package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

// fakeAPI is an in-memory table with attribute_not_exists semantics on (pk, sk).
type fakeAPI struct {
	mu        sync.Mutex
	items     map[string]bool
	puts      int
	failBatch int // number of BatchWriteItem calls that return everything unprocessed
}

func newFake() *fakeAPI { return &fakeAPI{items: map[string]bool{}} }

func key(it map[string]types.AttributeValue) string {
	return it["pk"].(*types.AttributeValueMemberS).Value + "|" + it["sk"].(*types.AttributeValueMemberS).Value
}

func cu(v float64) *types.ConsumedCapacity {
	return &types.ConsumedCapacity{CapacityUnits: aws.Float64(v)}
}

func (f *fakeAPI) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.items[key(in.Item)] {
		return nil, &types.ConditionalCheckFailedException{}
	}
	f.items[key(in.Item)] = true
	return &dynamodb.PutItemOutput{ConsumedCapacity: cu(5)}, nil
}

func (f *fakeAPI) TransactWriteItems(_ context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range in.TransactItems {
		if f.items[key(t.Put.Item)] {
			return nil, &smithy.GenericAPIError{Code: "TransactionCanceledException", Message: "[None, ConditionalCheckFailed]"}
		}
	}
	for _, t := range in.TransactItems {
		f.items[key(t.Put.Item)] = true
	}
	return &dynamodb.TransactWriteItemsOutput{ConsumedCapacity: []types.ConsumedCapacity{*cu(7), *cu(7)}}, nil
}

func (f *fakeAPI) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &dynamodb.QueryOutput{ConsumedCapacity: cu(1)}
	for n := int64(100); n >= 1; n-- {
		if f.items[logPK+"|"+slotKey(n)] {
			out.Items = append(out.Items, map[string]types.AttributeValue{"sk": str(slotKey(n))})
			break
		}
	}
	return out, nil
}

func (f *fakeAPI) BatchWriteItem(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failBatch > 0 {
		f.failBatch--
		return &dynamodb.BatchWriteItemOutput{UnprocessedItems: in.RequestItems, ConsumedCapacity: []types.ConsumedCapacity{*cu(0)}}, nil
	}
	n := 0
	for _, rs := range in.RequestItems {
		n += len(rs)
	}
	return &dynamodb.BatchWriteItemOutput{ConsumedCapacity: []types.ConsumedCapacity{*cu(float64(n))}}, nil
}

func runMode(t *testing.T, f *fakeAPI, args ...string) summary {
	t.Helper()
	var out, errb bytes.Buffer
	mk := func(context.Context, string) (API, error) { return f, nil }
	code := run(context.Background(), append([]string{"-table", "t"}, args...), &out, &errb, mk)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	var s summary
	if err := json.Unmarshal(out.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutMode(t *testing.T) {
	s := runMode(t, newFake(), "-mode", "put", "-rate", "200", "-k", "4", "-dur", "200ms")
	if s.OK < 30 || len(s.Errors) != 0 || s.CapPerOp != 5 || s.CallsPerOp != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestTxnMode(t *testing.T) {
	s := runMode(t, newFake(), "-mode", "txn", "-rate", "200", "-k", "4", "-dur", "100ms")
	if s.OK < 10 || s.CapPerOp != 14 {
		t.Fatalf("%+v", s)
	}
}

func TestContendCountsRetries(t *testing.T) {
	f := newFake()
	s := runMode(t, f, "-mode", "contend", "-rate", "300", "-k", "4", "-writers", "3", "-dur", "300ms")
	if s.OK < 20 || s.CallsPerOp <= 1 || s.CCF == 0 || len(s.Errors) != 0 {
		t.Fatalf("%+v", s)
	}
	if len(f.items) != s.OK {
		t.Fatalf("committed %d slots for %d ok", len(f.items), s.OK)
	}
}

func TestQueryModes(t *testing.T) {
	f := newFake()
	runMode(t, f, "-mode", "put", "-rate", "200", "-dur", "100ms", "-start", "1")
	for _, kind := range []string{"desc", "after"} {
		s := runMode(t, f, "-mode", "query", "-qkind", kind, "-rate", "100", "-dur", "100ms")
		if s.OK < 5 || s.CapPerOp != 1 {
			t.Fatalf("%s %+v", kind, s)
		}
	}
	s := runMode(t, f, "-mode", "query", "-rate", "300", "-window", "50ms", "-k", "1000", "-dur", "300ms")
	if s.OK < 50 || s.Calls >= s.OK || s.Calls == 0 {
		t.Fatalf("coalescing %+v", s)
	}
}

func TestBatchModeRetriesUnprocessed(t *testing.T) {
	f := newFake()
	f.failBatch = 1
	s := runMode(t, f, "-mode", "batch", "-rate", "500", "-dur", "100ms")
	if s.Unprocessed != 25 || s.CapPerItem != 1 || len(s.Errors) != 0 {
		t.Fatalf("%+v", s)
	}
	f.failBatch = 1000
	s = runMode(t, f, "-mode", "batch", "-rate", "500", "-dur", "100ms")
	if s.Errors["other"] == 0 {
		t.Fatalf("%+v", s)
	}
}

func TestBadArgsAndMode(t *testing.T) {
	var out, errb bytes.Buffer
	mk := func(context.Context, string) (API, error) { return newFake(), nil }
	if run(context.Background(), []string{"-rate", "1"}, &out, &errb, mk) != 2 {
		t.Fatal("want 2 without table")
	}
	if run(context.Background(), []string{"-table", "t", "-mode", "nope", "-start", "1"}, &out, &errb, mk) != 1 {
		t.Fatal("want 1 for bad mode")
	}
	bad := func(context.Context, string) (API, error) { return nil, errors.New("boom") }
	if run(context.Background(), []string{"-table", "t"}, &out, &errb, bad) != 1 || !strings.Contains(errb.String(), "boom") {
		t.Fatal("api failure")
	}
}

func TestClassify(t *testing.T) {
	cases := map[string]error{
		"ConditionalCheckFailed": &types.ConditionalCheckFailedException{},
		"throttle":               &types.ProvisionedThroughputExceededException{},
		"other":                  errors.New("x"),
		"other:Foo":              &smithy.GenericAPIError{Code: "Foo"},
	}
	for want, err := range cases {
		if got := classify(err); got != want {
			t.Errorf("%v: got %s want %s", err, got, want)
		}
	}
	tc := func(m string) error { return &smithy.GenericAPIError{Code: "TransactionCanceledException", Message: m} }
	if classify(tc("[ThrottlingError]")) != "throttle" || classify(tc("[ConditionalCheckFailed]")) != "ConditionalCheckFailed" || classify(tc("[None]")) != "TransactionCanceled" {
		t.Fatal("cancel classification")
	}
}

func TestQuantAndEmpty(t *testing.T) {
	if (quant(nil) != quantiles{}) {
		t.Fatal("empty")
	}
	q := quant([]time.Duration{3 * time.Millisecond, time.Millisecond, 2 * time.Millisecond})
	if q.P50 != 2 || q.Max != 3 {
		t.Fatalf("%+v", q)
	}
}

func TestPacerStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := newRecorder(time.Now())
	runPaced(ctx, 10, 1, time.Hour, func(context.Context, int64) result { return result{} }, rec)
	if rec.ok != 0 {
		t.Fatal("ran after cancel")
	}
}

func TestPacerDelaysWhenKExhausted(t *testing.T) {
	rec := newRecorder(time.Now())
	slow := func(context.Context, int64) result { time.Sleep(20 * time.Millisecond); return result{} }
	runPaced(context.Background(), 200, 1, 100*time.Millisecond, slow, rec)
	if rec.delayed == 0 {
		t.Fatal("expected delayed ticks")
	}
}

func TestTailSlotEmptyAndRealAPI(t *testing.T) {
	n, err := tailSlot(context.Background(), newFake(), "t")
	if n != 0 || err != nil {
		t.Fatal(n, err)
	}
	if _, err := realAPI(context.Background(), "ap-south-2"); err != nil {
		t.Fatal(err)
	}
}

func TestDistinctKeys(t *testing.T) {
	ks := distinctKeys(rand.New(rand.NewSource(1)), 25, 30)
	seen := map[int]bool{}
	for _, k := range ks {
		seen[k] = true
	}
	if len(ks) != 25 || len(seen) != 25 {
		t.Fatalf("%v", ks)
	}
	if len(distinctKeys(rand.New(rand.NewSource(1)), 10, 3)) != 3 {
		t.Fatal("capped by parts")
	}
}
