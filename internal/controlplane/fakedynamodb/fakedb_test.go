package fakedynamodb

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var ctx = context.Background()

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.Itoa(v)}
}
func k(partitionKey, sortKey string) item { return item{"pk": s(partitionKey), "sk": s(sortKey)} }

func fresh(t *testing.T) *FakeDynamoDB {
	t.Helper()
	f := New()
	if _, err := f.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("t")}); err != nil {
		t.Fatal(err)
	}
	return f
}

func upd(key item, expr string, cond string, vals item) *dynamodb.UpdateItemInput {
	in := &dynamodb.UpdateItemInput{TableName: aws.String("t"), Key: key, UpdateExpression: aws.String(expr),
		ExpressionAttributeValues: vals, ReturnValues: types.ReturnValueUpdatedNew}
	if cond != "" {
		in.ConditionExpression = aws.String(cond)
	}
	return in
}

func TestTables(t *testing.T) {
	f := fresh(t)
	var inUse *types.ResourceInUseException
	if _, err := f.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("t")}); !errors.As(err, &inUse) {
		t.Fatalf("create twice: %v", err)
	}
	d, err := f.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("t")})
	if err != nil || d.Table.TableStatus != types.TableStatusActive {
		t.Fatalf("describe: %v", err)
	}
	if _, err := f.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("t")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := f.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("t")}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted: %v", err)
	}
	if _, err := f.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("t")}); !errors.As(err, &nf) {
		t.Fatalf("delete deleted: %v", err)
	}
	if _, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: k("a", "b")}); !errors.As(err, &nf) {
		t.Fatalf("get from deleted: %v", err)
	}
}

func TestFailInjection(t *testing.T) {
	f := fresh(t)
	boom := errors.New("boom")
	f.Fail = func(call string) error {
		if call == "GetItem" || call == "DescribeTable" || call == "CreateTable" || call == "DeleteTable" ||
			call == "PutItem" || call == "Query" || call == "UpdateItem" || call == "BatchWriteItem" {
			return boom
		}
		return nil
	}
	tn := aws.String("t")
	for name, err := range map[string]error{
		"GetItem":    func() error { _, e := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: tn}); return e }(),
		"Put":        func() error { _, e := f.PutItem(ctx, &dynamodb.PutItemInput{TableName: tn}); return e }(),
		"Query":      func() error { _, e := f.Query(ctx, &dynamodb.QueryInput{TableName: tn}); return e }(),
		"Update":     func() error { _, e := f.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tn}); return e }(),
		"BatchWrite": func() error { _, e := f.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{}); return e }(),
		"Create":     func() error { _, e := f.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: tn}); return e }(),
		"Describe":   func() error { _, e := f.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: tn}); return e }(),
		"DeleteTbl":  func() error { _, e := f.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: tn}); return e }(),
	} {
		if !errors.Is(err, boom) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestUpdateItem(t *testing.T) {
	f := fresh(t)
	out, err := f.UpdateItem(ctx, upd(k("N", "-"), "ADD epoch :one", "", item{":one": n(1)}))
	if err != nil || out.Attributes["epoch"].(*types.AttributeValueMemberN).Value != "1" {
		t.Fatalf("add: %v %v", out, err)
	}
	if out, _ = f.UpdateItem(ctx, upd(k("N", "-"), "ADD epoch :one", "", item{":one": n(1)})); out.Attributes["epoch"].(*types.AttributeValueMemberN).Value != "2" {
		t.Fatalf("add twice: %v", out.Attributes)
	}
	cond := "attribute_not_exists(parts) OR parts < :n"
	set := func(v int) error {
		_, err := f.UpdateItem(ctx, upd(k("TOPICS", "x"), "SET parts = :n", cond, item{":n": n(v)}))
		return err
	}
	var ccf *types.ConditionalCheckFailedException
	if err := set(3); err != nil {
		t.Fatal(err)
	}
	if err := set(3); !errors.As(err, &ccf) {
		t.Fatalf("equal: %v", err)
	}
	if err := set(2); !errors.As(err, &ccf) {
		t.Fatalf("lower: %v", err)
	}
	if err := set(4); err != nil {
		t.Fatalf("higher: %v", err)
	}
	if got := f.Item("t", k("TOPICS", "x"))["parts"].(*types.AttributeValueMemberN).Value; got != "4" {
		t.Fatalf("parts %s", got)
	}
	// Unsupported or malformed input is a loud error.
	for name, in := range map[string]*dynamodb.UpdateItemInput{
		"verb":       upd(k("a", "b"), "REMOVE x", "", nil),
		"set shape":  upd(k("a", "b"), "SET x", "", nil),
		"add shape":  upd(k("a", "b"), "ADD x", "", nil),
		"set value":  upd(k("a", "b"), "SET x = :v", "", item{}),
		"add string": upd(k("a", "b"), "ADD x :v", "", item{":v": s("1")}),
		"cond shape": upd(k("a", "b"), "SET x = :v", "x > :v", item{":v": n(1)}),
		"cond value": upd(k("a", "b"), "SET x = :v", "x = :w", item{":v": n(1)}),
	} {
		if _, err := f.UpdateItem(ctx, in); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := f.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String("nope")}); err == nil {
		t.Error("missing table accepted")
	}
}

func TestCompare(t *testing.T) {
	for _, c := range []struct {
		a, b types.AttributeValue
		want int
		bad  bool
	}{
		{n(1), n(2), -1, false}, {n(2), n(1), 1, false}, {n(2), n(2), 0, false},
		{s("a"), s("b"), -1, false}, {n(1), s("1"), 0, true}, {s("1"), n(1), 0, true},
		{&types.AttributeValueMemberB{}, n(1), 0, true},
		{&types.AttributeValueMemberN{Value: "x"}, n(1), 0, true}, {n(1), &types.AttributeValueMemberN{Value: "x"}, 0, true},
	} {
		got, err := compare(c.a, c.b)
		if (err != nil) != c.bad || got != c.want {
			t.Errorf("compare(%v,%v) = %d, %v", c.a, c.b, got, err)
		}
	}
	if _, err := addNumbers(&types.AttributeValueMemberN{Value: "x"}, n(1)); err == nil {
		t.Error("ADD on a non-number accepted")
	}
}

func TestGet(t *testing.T) {
	f := fresh(t)
	cp := item{"pk": s("a"), "sk": s("b"), "blob": &types.AttributeValueMemberB{Value: []byte("xy")}}
	f.Put("t", cp)
	cp["blob"].(*types.AttributeValueMemberB).Value[0] = 'Z' // the store holds its own copy
	f.ReportCapacity = true
	g, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: k("a", "b")})
	if err != nil || string(g.Item["blob"].(*types.AttributeValueMemberB).Value) != "xy" || g.ConsumedCapacity == nil {
		t.Fatalf("get: %v %v", g, err)
	}
	if g, _ = f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: k("x", "y")}); g.Item != nil {
		t.Fatal("absent item returned")
	}
	if f.Count("t") != 1 {
		t.Fatalf("count %d", f.Count("t"))
	}
}

func query(f *FakeDynamoDB, expr string, vals item, fwd *bool, limit *int32, start item) (*dynamodb.QueryOutput, error) {
	return f.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("t"), KeyConditionExpression: aws.String(expr),
		ExpressionAttributeValues: vals, ScanIndexForward: fwd, Limit: limit, ExclusiveStartKey: start})
}

func sks(o *dynamodb.QueryOutput) []string {
	var out []string
	for _, it := range o.Items {
		out = append(out, stringOf(it, "sk"))
	}
	return out
}

func TestQuery(t *testing.T) {
	f := fresh(t)
	for _, sortKey := range []string{"1", "2", "3", "4", "5"} {
		f.Put("t", k("p", sortKey))
	}
	f.Put("t", k("q", "9"))
	all := item{":p": s("p")}
	rng := item{":p": s("p"), ":lo": s("2"), ":hi": s("4")}
	between := "pk = :p AND sk BETWEEN :lo AND :hi"
	o, err := query(f, "pk = :p", all, nil, nil, nil)
	if err != nil || len(sks(o)) != 5 || o.LastEvaluatedKey != nil {
		t.Fatalf("all: %v %v", sks(o), err)
	}
	o, _ = query(f, between, rng, nil, nil, nil)
	if got := sks(o); len(got) != 3 || got[0] != "2" || got[2] != "4" {
		t.Fatalf("between: %v", got)
	}
	back := aws.Bool(false)
	o, _ = query(f, between, rng, back, aws.Int32(1), nil)
	if got := sks(o); len(got) != 1 || got[0] != "4" || o.LastEvaluatedKey == nil {
		t.Fatalf("backward limit 1: %v", got)
	}
	o, _ = query(f, between, rng, back, aws.Int32(1), o.LastEvaluatedKey)
	if got := sks(o); len(got) != 1 || got[0] != "3" {
		t.Fatalf("backward page 2: %v", got)
	}
	f.PageSize = 2 // paging through the table
	var got []string
	var start item
	for pages := 0; ; pages++ {
		o, _ = query(f, "pk = :p", all, nil, nil, start)
		got = append(got, sks(o)...)
		if start = o.LastEvaluatedKey; start == nil {
			if pages != 2 {
				t.Fatalf("%d pages", pages+1)
			}
			break
		}
	}
	if len(got) != 5 || got[0] != "1" || got[4] != "5" {
		t.Fatalf("paged: %v", got)
	}
	for _, bad := range []string{"pk > :p", "pk = :p AND sk = :lo", "pk = :p AND sk BETWEEN :lo", "pk = :p AND sk BETWEEN :lo AND :nope"} {
		if _, err := query(f, bad, rng, nil, nil, nil); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := f.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("nope")}); err == nil {
		t.Error("missing table accepted")
	}
}

// The newest item of a partition: a descending Query with Limit 1 (the commit's
// end-offset read), also on an empty partition.
func TestQueryNewest(t *testing.T) {
	f := fresh(t)
	vals := item{":p": s("p")}
	back := aws.Bool(false)
	o, err := query(f, "pk = :p", vals, back, aws.Int32(1), nil)
	if err != nil || len(o.Items) != 0 || o.LastEvaluatedKey != nil {
		t.Fatalf("empty partition: %v %v", o, err)
	}
	for _, sortKey := range []string{"00000000000000000000", "00000000000000000007", "00000000000000000012"} {
		f.Put("t", k("p", sortKey))
	}
	f.Put("t", k("q", "99999999999999999999"))
	o, err = query(f, "pk = :p", vals, back, aws.Int32(1), nil)
	if got := sks(o); err != nil || len(got) != 1 || got[0] != "00000000000000000012" {
		t.Fatalf("newest: %v %v", got, err)
	}
}

// A put conditioned on attribute_not_exists(sk) is the compare-and-set of the
// run base: of many concurrent writers of one key exactly one wins.
func TestPutIfAbsentOneWinner(t *testing.T) {
	f := fresh(t)
	cas := func() error {
		_, err := f.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("t"), Item: k("p", "5"),
			ConditionExpression: aws.String("attribute_not_exists(sk)")})
		return err
	}
	var wins, losses atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := cas(); err == nil {
				wins.Add(1)
			} else if _, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
				losses.Add(1)
			} else {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 || losses.Load() != 15 {
		t.Fatalf("%d winners, %d losers", wins.Load(), losses.Load())
	}
}

func TestPutItem(t *testing.T) {
	f := fresh(t)
	put := func(sortKey, v string, old bool) error {
		in := &dynamodb.PutItemInput{TableName: aws.String("t"), Item: item{"pk": s("p"), "sk": s(sortKey), "v": s(v)},
			ConditionExpression: aws.String("attribute_not_exists(pk)")}
		if old {
			in.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
		}
		_, err := f.PutItem(ctx, in)
		return err
	}
	if err := put("1", "a", true); err != nil {
		t.Fatal(err)
	}
	var cc *types.ConditionalCheckFailedException
	err := put("1", "b", true)
	if !errors.As(err, &cc) || cc.Item["v"].(*types.AttributeValueMemberS).Value != "a" {
		t.Fatalf("second put: %v %+v", err, cc)
	}
	if err := put("1", "b", false); !errors.As(err, &cc) || cc.Item != nil {
		t.Fatalf("no old item expected: %v", err)
	}
	if got := f.Item("t", k("p", "1"))["v"].(*types.AttributeValueMemberS).Value; got != "a" {
		t.Fatalf("stored %q", got)
	}
	f.AfterPut = func() error { return errors.New("lost answer") }
	if err := put("2", "c", false); err == nil || f.Item("t", k("p", "2")) == nil {
		t.Fatalf("lost answer must still store: %v", err)
	}
	f.Fail = func(call string) error { return errors.New("boom " + call) }
	if err := put("3", "d", false); err == nil || f.Item("t", k("p", "3")) != nil {
		t.Fatalf("failed call must not store: %v", err)
	}
	f.Fail = nil
	bad := &dynamodb.PutItemInput{TableName: aws.String("nope"), Item: k("p", "9")}
	var nf *types.ResourceNotFoundException
	if _, err := f.PutItem(ctx, bad); !errors.As(err, &nf) {
		t.Fatalf("missing table: %v", err)
	}
	bad = &dynamodb.PutItemInput{TableName: aws.String("t"), Item: k("p", "9"), ConditionExpression: aws.String("size(x) > 1")}
	if _, err := f.PutItem(ctx, bad); err == nil {
		t.Fatal("unsupported condition must fail")
	}
}

func TestBatchWriteItem(t *testing.T) {
	f := fresh(t)
	put := func(sortKey string) types.WriteRequest {
		return types.WriteRequest{PutRequest: &types.PutRequest{Item: k("p", sortKey)}}
	}
	del := func(sortKey string) types.WriteRequest {
		return types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: k("p", sortKey)}}
	}
	call := func(reqs ...types.WriteRequest) (*dynamodb.BatchWriteItemOutput, error) {
		return f.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"t": reqs}})
	}
	if _, err := call(put("a"), put("b"), put("c")); err != nil || f.Count("t") != 3 {
		t.Fatalf("puts: %v %d", err, f.Count("t"))
	}
	f.UnprocessWrite = 1
	out, err := call(del("a"), del("b"))
	if err != nil || len(out.UnprocessedItems["t"]) != 1 || f.Count("t") != 2 {
		t.Fatalf("unprocessed: %v %+v %d", err, out, f.Count("t"))
	}
	if _, err = call(); err == nil {
		t.Fatal("empty batch accepted")
	}
	big := make([]types.WriteRequest, 26)
	for i := range big {
		big[i] = put(strconv.Itoa(i))
	}
	if _, err = call(big...); err == nil {
		t.Fatal("26 requests accepted")
	}
	if _, err = f.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"nope": {put("x")}}}); err == nil {
		t.Fatal("missing table accepted")
	}
	f.Fail = func(string) error { return errors.New("down") }
	if _, err = call(put("z")); err == nil {
		t.Fatal("injected failure ignored")
	}
}
