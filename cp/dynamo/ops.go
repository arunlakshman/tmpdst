package dynamo

import (
	"context"
	"errors"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

// Register: the node's epoch item gets an atomic ADD 1.
func (s *Store) Register(ctx context.Context, op core.Register) (core.RegisterResp, error) {
	out, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.table, Key: nodeKey(op.Self.Node),
		UpdateExpression:          aws.String("ADD epoch :one"),
		ExpressionAttributeValues: item{":one": num(1)},
		ReturnValues:              types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return core.RegisterResp{}, infra(err)
	}
	e, ok := getNum(out.Attributes, "epoch")
	if !ok {
		return core.RegisterResp{}, errors.New("dynamo: register: no epoch returned")
	}
	return core.RegisterResp{Epoch: core.Epoch(e)}, nil
}

// InitProducer: an atomic counter of calls; the ID is firstProducer-1+count.
func (s *Store) InitProducer(ctx context.Context) (core.InitProducerResp, error) {
	out, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.table, Key: pidKey(),
		UpdateExpression:          aws.String("ADD n :one"),
		ExpressionAttributeValues: item{":one": num(1)},
		ReturnValues:              types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return core.InitProducerResp{}, infra(err)
	}
	n, ok := getNum(out.Attributes, "n")
	if !ok {
		return core.InitProducerResp{}, errors.New("dynamo: init producer: no counter returned")
	}
	return core.InitProducerResp{Producer: core.ProducerID(firstProducer - 1 + n)}, nil
}

// CreateTopic raises the partition count; it never shrinks one.
func (s *Store) CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error) {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid, nil
	}
	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.table, Key: topicKey(name),
		UpdateExpression:          aws.String("SET parts = :n"),
		ConditionExpression:       aws.String("attribute_not_exists(parts) OR parts < :n"),
		ExpressionAttributeValues: item{":n": num(int64(partitions))},
	})
	var cc *types.ConditionalCheckFailedException
	if err != nil && !errors.As(err, &cc) {
		return core.OK, infra(err)
	}
	return core.OK, nil
}

func (s *Store) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	var resp core.DescribeTopicsResp
	add := func(it item) {
		if n, ok := getNum(it, "parts"); ok {
			resp.Topics = append(resp.Topics, core.TopicInfo{Name: getStr(it, "sk"), Partitions: int32(n)})
		}
	}
	if len(op.Topics) == 0 {
		items, err := s.query(ctx, &dynamodb.QueryInput{
			KeyConditionExpression:    aws.String("pk = :p"),
			ExpressionAttributeValues: item{":p": str("TOPICS")},
		}, 0)
		for _, it := range items {
			add(it)
		}
		return resp, err
	}
	names := slices.Clone(op.Topics)
	slices.Sort(names)
	for i, n := range names {
		if i > 0 && names[i-1] == n {
			continue
		}
		it, err := s.getItem(ctx, topicKey(n))
		if err != nil {
			return core.DescribeTopicsResp{}, err
		}
		add(it)
	}
	return resp, nil
}

// query runs q (consistent) through all pages, or until limit items (0: all).
func (s *Store) query(ctx context.Context, q *dynamodb.QueryInput, limit int) ([]item, error) {
	q.TableName, q.ConsistentRead = &s.table, aws.Bool(true)
	var res []item
	for {
		out, err := s.db.Query(ctx, q)
		if err != nil {
			return nil, infra(err)
		}
		res = append(res, out.Items...)
		if out.LastEvaluatedKey == nil || (limit > 0 && len(res) >= limit) {
			return res, nil
		}
		q.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func rangeOf(tp core.TopicPartition, it item) core.BatchRange {
	base, _ := getNum(it, "base")
	cnt, _ := getNum(it, "cnt")
	off, _ := getNum(it, "lo")
	ln, _ := getNum(it, "ll")
	return core.BatchRange{TP: tp, Base: core.Offset(base), Count: cnt, Loc: core.SegmentRef{Key: getStr(it, "lk"), Off: off, Len: ln}}
}

// Lookup reads the HWM first, then the ranges below it: the answer is a
// snapshot even while commits run.
func (s *Store) Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	if op.From < 0 {
		return core.LookupResp{}, core.ErrInvalid, nil
	}
	h, err := s.getItem(ctx, hwmKey(op.TP))
	if err != nil {
		return core.LookupResp{}, 0, err
	}
	hwm, _ := getNum(h, "hwm")
	resp := core.LookupResp{HighWatermark: core.Offset(hwm)}
	if hwm == 0 {
		return resp, core.OK, nil
	}
	pk := partKey("R#", op.TP)
	// The range holding From, if any, starts at or before it.
	lower := int64(op.From)
	prev, err := s.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: item{":p": str(pk), ":lo": str(offSK(0)), ":hi": str(offSK(int64(op.From)))},
		ScanIndexForward:          aws.Bool(false),
		Limit:                     aws.Int32(1),
	}, 1)
	if err != nil {
		return core.LookupResp{}, 0, err
	}
	if len(prev) > 0 {
		if r := rangeOf(op.TP, prev[0]); int64(r.Base)+r.Count > int64(op.From) {
			lower = int64(r.Base)
		}
	}
	var bytes int64
	q := &dynamodb.QueryInput{
		TableName: &s.table, ConsistentRead: aws.Bool(true),
		KeyConditionExpression:    aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: item{":p": str(pk), ":lo": str(offSK(lower)), ":hi": str(offSK(hwm - 1))},
	}
	if op.MaxBytes > 0 {
		q.Limit = aws.Int32(64)
	}
	for {
		out, err := s.db.Query(ctx, q)
		if err != nil {
			return core.LookupResp{}, 0, infra(err)
		}
		for _, it := range out.Items {
			r := rangeOf(op.TP, it)
			resp.Ranges = append(resp.Ranges, r)
			bytes += r.Loc.Len
			if op.MaxBytes > 0 && bytes >= int64(op.MaxBytes) {
				return resp, core.OK, nil
			}
		}
		if out.LastEvaluatedKey == nil {
			return resp, core.OK, nil
		}
		q.ExclusiveStartKey = out.LastEvaluatedKey
	}
}
