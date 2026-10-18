package fakedb

import (
	"context"
	"errors"
	"strconv"
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
func k(pk, sk string) item { return item{"pk": s(pk), "sk": s(sk)} }

func fresh(t *testing.T) *Fake {
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
			call == "BatchGetItem" || call == "Query" || call == "UpdateItem" || call == "TransactWriteItems" {
			return boom
		}
		return nil
	}
	tn := aws.String("t")
	for name, err := range map[string]error{
		"GetItem":   func() error { _, e := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: tn}); return e }(),
		"Batch":     func() error { _, e := f.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{}); return e }(),
		"Query":     func() error { _, e := f.Query(ctx, &dynamodb.QueryInput{TableName: tn}); return e }(),
		"Update":    func() error { _, e := f.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tn}); return e }(),
		"Tx":        func() error { _, e := f.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{}); return e }(),
		"Create":    func() error { _, e := f.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: tn}); return e }(),
		"Describe":  func() error { _, e := f.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: tn}); return e }(),
		"DeleteTbl": func() error { _, e := f.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: tn}); return e }(),
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
	if _, err := addNum(&types.AttributeValueMemberN{Value: "x"}, n(1)); err == nil {
		t.Error("ADD on a non-number accepted")
	}
}

func TestGetAndBatchGet(t *testing.T) {
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
	keys := []item{k("a", "b"), k("x", "y"), k("a", "c")}
	req := map[string]types.KeysAndAttributes{"t": {Keys: keys}}
	b, err := f.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
	if err != nil || len(b.Responses["t"]) != 1 || len(b.UnprocessedKeys) != 0 || len(b.ConsumedCapacity) != 1 {
		t.Fatalf("batch: %+v %v", b, err)
	}
	f.Unprocess = 1
	b, _ = f.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
	if len(b.UnprocessedKeys["t"].Keys) != 1 || idOf(b.UnprocessedKeys["t"].Keys[0]) != idOf(keys[2]) || f.Unprocess != 0 {
		t.Fatalf("unprocessed: %+v", b.UnprocessedKeys)
	}
	big := make([]item, 101)
	if _, err := f.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{"t": {Keys: big}}}); err == nil {
		t.Error("101 keys accepted")
	}
	if _, err := f.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{"nope": {}}}); err == nil {
		t.Error("missing table accepted")
	}
	if f.Count("t") != 1 {
		t.Fatalf("count %d", f.Count("t"))
	}
}

func query(f *Fake, expr string, vals item, fwd *bool, limit *int32, start item) (*dynamodb.QueryOutput, error) {
	return f.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("t"), KeyConditionExpression: aws.String(expr),
		ExpressionAttributeValues: vals, ScanIndexForward: fwd, Limit: limit, ExclusiveStartKey: start})
}

func sks(o *dynamodb.QueryOutput) (out []string) {
	for _, it := range o.Items {
		out = append(out, strOf(it, "sk"))
	}
	return
}

func TestQuery(t *testing.T) {
	f := fresh(t)
	for _, sk := range []string{"1", "2", "3", "4", "5"} {
		f.Put("t", k("p", sk))
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

func put(key item) types.TransactWriteItem {
	return types.TransactWriteItem{Put: &types.Put{TableName: aws.String("t"), Item: key, ConditionExpression: aws.String("attribute_not_exists(pk)")}}
}

func check(key item, cond string, vals item) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{TableName: aws.String("t"), Key: key, ConditionExpression: aws.String(cond), ExpressionAttributeValues: vals}}
}

func update(key item, expr string, vals item) types.TransactWriteItem {
	return types.TransactWriteItem{Update: &types.Update{TableName: aws.String("t"), Key: key, UpdateExpression: aws.String(expr), ExpressionAttributeValues: vals}}
}

func tx(f *Fake, items ...types.TransactWriteItem) error {
	_, err := f.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	return err
}

func TestTransactAtomic(t *testing.T) {
	f := fresh(t)
	f.Put("t", item{"pk": s("N"), "sk": s("-"), "epoch": n(2)})
	ok := check(k("N", "-"), "epoch = :e", item{":e": n(2)})
	if err := tx(f, ok, put(k("G", "-")), update(k("R", "!"), "SET hwm = :h", item{":h": n(7)})); err != nil {
		t.Fatal(err)
	}
	if f.Item("t", k("G", "-")) == nil || f.Item("t", k("R", "!"))["hwm"].(*types.AttributeValueMemberN).Value != "7" {
		t.Fatal("transaction not applied")
	}
	// The second item's condition fails (G exists): nothing else is written.
	stale := check(k("N", "-"), "epoch = :e", item{":e": n(1)})
	err := tx(f, put(k("G2", "-")), put(k("G", "-")), stale)
	var tc *types.TransactionCanceledException
	if !errors.As(err, &tc) || len(tc.CancellationReasons) != 3 {
		t.Fatalf("want cancellation, got %v", err)
	}
	want := []string{"None", "ConditionalCheckFailed", "ConditionalCheckFailed"}
	for i, r := range tc.CancellationReasons {
		if aws.ToString(r.Code) != want[i] {
			t.Errorf("reason %d: %s", i, aws.ToString(r.Code))
		}
	}
	if f.Item("t", k("G2", "-")) != nil {
		t.Fatal("cancelled transaction wrote an item")
	}
}

func TestTransactInvalid(t *testing.T) {
	f := fresh(t)
	for name, items := range map[string][]types.TransactWriteItem{
		"empty":     nil,
		"too many":  make([]types.TransactWriteItem, 101),
		"delete":    {{Delete: &types.Delete{}}},
		"duplicate": {put(k("a", "b")), update(k("a", "b"), "SET x = :v", item{":v": n(1)})},
		"bad cond":  {check(k("a", "b"), "x > :v", item{":v": n(1)})},
		"bad upd":   {update(k("a", "b"), "REMOVE x", nil)},
		"no table":  {{Put: &types.Put{TableName: aws.String("nope"), Item: k("a", "b")}}},
	} {
		if err := tx(f, items...); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if f.Count("t") != 0 {
		t.Fatal("an invalid transaction wrote")
	}
}

func TestBeforeTransactRace(t *testing.T) {
	f := fresh(t)
	f.BeforeTransact = func() { f.Put("t", k("G", "-")) }
	var tc *types.TransactionCanceledException
	if err := tx(f, put(k("G", "-"))); !errors.As(err, &tc) {
		t.Fatalf("race not lost: %v", err)
	}
}
