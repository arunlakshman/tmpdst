package fakedynamodb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func withNames(in *dynamodb.UpdateItemInput, names map[string]string) *dynamodb.UpdateItemInput {
	in.ExpressionAttributeNames = names
	return in
}

func TestUpdateItemAllOldOnConditionFailure(t *testing.T) {
	f := fresh(t)
	meta := k("META", "WRITER")
	setup := withNames(upd(meta, "SET #e = :e, #o = :o", "", item{":e": n(3), ":o": s("cp-0")}), map[string]string{"#e": "epoch", "#o": "owner"})
	if _, err := f.UpdateItem(ctx, setup); err != nil {
		t.Fatal(err)
	}
	in := withNames(upd(meta, "SET #e = :next", "#e = :expect", item{":next": n(3), ":expect": n(2)}), map[string]string{"#e": "epoch"})
	in.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
	_, err := f.UpdateItem(ctx, in)
	failed, ok := errors.AsType[*types.ConditionalCheckFailedException](err)
	if !ok {
		t.Fatalf("err = %v, want ConditionalCheckFailed", err)
	}
	if got := failed.Item["epoch"].(*types.AttributeValueMemberN).Value; got != "3" {
		t.Fatalf("old epoch = %s, want 3", got)
	}
	if got := failed.Item["owner"].(*types.AttributeValueMemberS).Value; got != "cp-0" {
		t.Fatalf("old owner = %s, want cp-0", got)
	}
}

func TestUpdateItemNoOldWithoutAllOld(t *testing.T) {
	f := fresh(t)
	meta := k("META", "WRITER")
	if _, err := f.UpdateItem(ctx, upd(meta, "SET epoch = :e", "", item{":e": n(3)})); err != nil {
		t.Fatal(err)
	}
	_, err := f.UpdateItem(ctx, upd(meta, "SET epoch = :e", "epoch = :x", item{":e": n(4), ":x": n(2)}))
	failed, ok := errors.AsType[*types.ConditionalCheckFailedException](err)
	if !ok || failed.Item != nil {
		t.Fatalf("err = %v, want ConditionalCheckFailed without an item", err)
	}
}

func TestUpdateItemAllOldMissingItem(t *testing.T) {
	f := fresh(t)
	in := upd(k("META", "WRITER"), "SET epoch = :e", "epoch = :x", item{":e": n(1), ":x": n(5)})
	in.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
	_, err := f.UpdateItem(ctx, in)
	failed, ok := errors.AsType[*types.ConditionalCheckFailedException](err)
	if !ok || failed.Item != nil {
		t.Fatalf("missing item: err = %v, want ConditionalCheckFailed with nil Item", err)
	}
}

func TestEpochConditionForms(t *testing.T) {
	// Each case sets epoch to stored (or leaves the item missing when stored < 0),
	// then runs one conditional update and checks whether it applied.
	cases := []struct {
		name   string
		cond   string
		stored int
		value  int
		wins   bool
	}{
		{"equal wins", "epoch = :v", 3, 3, true},
		{"different loses", "epoch = :v", 3, 2, false},
		{"missing loses for equal", "epoch = :v", -1, 3, false},
		{"missing wins for not exists", "attribute_not_exists(epoch) OR epoch = :v", -1, 0, true},
		{"present epoch equal", "attribute_not_exists(epoch) OR epoch = :v", 3, 3, true},
		{"present epoch different", "attribute_not_exists(epoch) OR epoch = :v", 3, 2, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := fresh(t)
			key := k("META", "WRITER")
			if c.stored >= 0 {
				if _, err := f.UpdateItem(ctx, upd(key, "SET epoch = :e", "", item{":e": n(c.stored)})); err != nil {
					t.Fatal(err)
				}
			}
			in := withNames(upd(key, "SET #e = :next", c.cond, item{":v": n(c.value), ":next": n(9)}), map[string]string{"#e": "epoch"})
			_, err := f.UpdateItem(ctx, in)
			if c.wins && err != nil {
				t.Fatalf("want win, got %v", err)
			}
			if !c.wins {
				if _, ok := errors.AsType[*types.ConditionalCheckFailedException](err); !ok {
					t.Fatalf("want ConditionalCheckFailed, got %v", err)
				}
			}
		})
	}
}

func TestAliasesResolveNames(t *testing.T) {
	f := fresh(t)
	meta := k("META", "WRITER")
	in := withNames(upd(meta, "SET #e = :e, #o = :o", "", item{":e": n(7), ":o": s("cp-7")}),
		map[string]string{"#e": "epoch", "#o": "owner"})
	out, err := f.UpdateItem(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Attributes["owner"].(*types.AttributeValueMemberS).Value != "cp-7" || out.Attributes["epoch"].(*types.AttributeValueMemberN).Value != "7" {
		t.Fatalf("UPDATED_NEW attributes = %v, want owner cp-7 and epoch 7", out.Attributes)
	}
	if got := f.Item("t", meta)["owner"].(*types.AttributeValueMemberS).Value; got != "cp-7" {
		t.Fatalf("stored owner = %s", got)
	}
}

func TestReservedWordsNeedAlias(t *testing.T) {
	f := fresh(t)
	key := k("META", "WRITER")
	bad := map[string]*dynamodb.UpdateItemInput{
		"reserved in SET":       upd(key, "SET owner = :o", "", item{":o": s("cp-0")}),
		"reserved in condition": upd(key, "SET epoch = :e", "owner = :o", item{":e": n(1), ":o": s("cp-0")}),
		"unknown alias in SET":  withNames(upd(key, "SET #x = :o", "", item{":o": s("cp-0")}), map[string]string{}),
		"unknown alias in cond": withNames(upd(key, "SET epoch = :e", "#x = :o", item{":e": n(1), ":o": s("cp-0")}), map[string]string{}),
	}
	for name, in := range bad {
		if _, err := f.UpdateItem(ctx, in); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestGetItemReadsOneItem(t *testing.T) {
	f := fresh(t)
	key := k("META", "FLOOR")
	if _, err := f.UpdateItem(ctx, upd(key, "SET floor = :v", "", item{":v": n(42)})); err != nil {
		t.Fatal(err)
	}
	out, err := f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil || out.Item["floor"].(*types.AttributeValueMemberN).Value != "42" {
		t.Fatalf("get: %v %v", out.Item, err)
	}
	out, err = f.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: k("META", "NONE"), ConsistentRead: aws.Bool(true)})
	if err != nil || out.Item != nil {
		t.Fatalf("missing: %v %v, want nil item and no error", out.Item, err)
	}
}
