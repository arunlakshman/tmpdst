package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// metaItems are the two small items the control plane keeps in the log table
// under pk=META: the writer (who holds the write role, and its epoch) and the
// floor (the highest slot that a trim may have deleted).
type metaItems struct {
	hasWriter bool
	epoch     int64 // stored writer epoch
	owner     string
	hasFloor  bool
	floor     int64
}

// readMeta reads both items with a consistent GetItem. A missing item is not an error.
func readMeta(ctx context.Context, db *dynamodb.Client, table string) metaItems {
	writer := getMetaItem(ctx, db, table, "WRITER")
	floor := getMetaItem(ctx, db, table, "FLOOR")
	m, err := metaFromItems(writer, floor)
	check(err)
	return m
}

func getMetaItem(ctx context.Context, db *dynamodb.Client, table, sortKey string) map[string]types.AttributeValue {
	out, err := db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "META"},
			"sk": &types.AttributeValueMemberS{Value: sortKey},
		},
	})
	check(err)
	return out.Item // nil when the item does not exist
}

// metaFromItems reads the WRITER and FLOOR items. A nil map means the item is missing.
func metaFromItems(writer, floor map[string]types.AttributeValue) (metaItems, error) {
	var m metaItems
	if writer != nil {
		epoch, err := numberAttr(writer, "epoch")
		if err != nil {
			return m, err
		}
		owner, ok := writer["owner"].(*types.AttributeValueMemberS)
		if !ok {
			return m, errors.New("META/WRITER: attribute owner is not a string")
		}
		m.hasWriter, m.epoch, m.owner = true, epoch, owner.Value
	}
	if floor != nil {
		n, err := numberAttr(floor, "floor")
		if err != nil {
			return m, err
		}
		m.hasFloor, m.floor = true, n
	}
	return m, nil
}

func numberAttr(item map[string]types.AttributeValue, name string) (int64, error) {
	v, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("META item: attribute %s is not a number", name)
	}
	return strconv.ParseInt(v.Value, 10, 64)
}

// printMeta prints the writer and the floor, the first lines of every report.
func printMeta(m metaItems) {
	fmt.Println("== control plane items (pk=META)")
	if m.hasWriter {
		fmt.Printf("   writer epoch=%d owner=%s\n", m.epoch, m.owner)
	} else {
		fmt.Println("   writer none (no META/WRITER item)")
	}
	if m.hasFloor {
		fmt.Printf("   floor=%d\n", m.floor)
	} else {
		fmt.Println("   floor=none")
	}
}
