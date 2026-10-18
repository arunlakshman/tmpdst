package dynamo

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

// CreateTopic raises the partition count; it never shrinks one.
func (s *Store) CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error) {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid, nil
	}
	_, err := s.updateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.table, Key: topicKey(name),
		UpdateExpression:          aws.String("SET parts = :n"),
		ConditionExpression:       aws.String("attribute_not_exists(parts) OR parts < :n"),
		ExpressionAttributeValues: item{":n": num(int64(partitions))},
	})
	var cc *types.ConditionalCheckFailedException
	if errors.As(err, &cc) {
		s.Obs.Retry("create-topic", "conditional_check")
	} else if err != nil {
		return core.OK, infra(err)
	}
	return core.OK, nil
}

func (s *Store) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	if len(op.Topics) == 0 {
		return s.describeAll(ctx)
	}
	return s.describeNamed(ctx, op.Topics)
}

// topicInfo adds the topic item it to resp; items without a count are skipped.
func topicInfo(resp *core.DescribeTopicsResp, it item) {
	if n, ok := getNum(it, "parts"); ok {
		resp.Topics = append(resp.Topics, core.TopicInfo{Name: getStr(it, "sk"), Partitions: int32(n)})
	}
}

func (s *Store) describeAll(ctx context.Context) (core.DescribeTopicsResp, error) {
	var resp core.DescribeTopicsResp
	items, err := s.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String("pk = :p"),
		ExpressionAttributeValues: item{":p": str("TOPICS")},
	}, 0)
	for _, it := range items {
		topicInfo(&resp, it)
	}
	return resp, err
}

func (s *Store) describeNamed(ctx context.Context, topics []string) (core.DescribeTopicsResp, error) {
	var resp core.DescribeTopicsResp
	names := slices.Compact(slices.Sorted(slices.Values(topics)))
	for _, n := range names {
		it, err := s.getItem(ctx, topicKey(n))
		if err != nil {
			return core.DescribeTopicsResp{}, err
		}
		topicInfo(&resp, it)
	}
	return resp, nil
}

// query runs q (consistent) through all pages, or until limit items (0: all).
func (s *Store) query(ctx context.Context, q *dynamodb.QueryInput, limit int) ([]item, error) {
	q.TableName, q.ConsistentRead = &s.table, aws.Bool(true)
	var res []item
	for {
		out, err := s.queryPage(ctx, q)
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

// Lookup reads the partition end (the newest run) first, then the runs below
// it: the answer is a snapshot even while commits run.
func (s *Store) Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	if op.From < 0 {
		return core.LookupResp{}, core.ErrInvalid, nil
	}
	end, err := s.endOf(ctx, op.TP)
	if err != nil {
		return core.LookupResp{}, 0, err
	}
	resp := core.LookupResp{End: core.Offset(end)}
	if int64(op.From) >= end {
		return resp, core.OK, nil
	}
	lower, err := s.runStart(ctx, op)
	if err != nil {
		return core.LookupResp{}, 0, err
	}
	resp.Ranges, err = s.scanRuns(ctx, op, lower, end)
	if err != nil {
		return core.LookupResp{}, 0, err
	}
	return resp, core.OK, nil
}

// runStart is the sort-key base to scan from: the start of the run holding
// op.From if there is one, else op.From.
func (s *Store) runStart(ctx context.Context, op core.Lookup) (int64, error) {
	prev, err := s.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: item{":p": str(partKey("R#", op.TP)),
			":lo": str(offSK(0)), ":hi": str(offSK(int64(op.From)))},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	}, 1)
	if err != nil || len(prev) == 0 {
		return int64(op.From), err
	}
	base, _ := getNum(prev[0], "base")
	if cnt, _ := getNum(prev[0], "cnt"); base+cnt > int64(op.From) {
		return base, nil
	}
	return int64(op.From), nil
}

// scanRuns reads the run items in [lower, end) page by page and returns the
// batches at or after op.From, stopping once op.MaxBytes is reached.
func (s *Store) scanRuns(ctx context.Context, op core.Lookup, lower, end int64) ([]core.BatchRange, error) {
	q := &dynamodb.QueryInput{
		TableName: &s.table, ConsistentRead: aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: item{":p": str(partKey("R#", op.TP)),
			":lo": str(offSK(lower)), ":hi": str(offSK(end - 1))},
	}
	if op.MaxBytes > 0 {
		q.Limit = aws.Int32(64)
	}
	var res []core.BatchRange
	var bytes int64
	for {
		out, err := s.queryPage(ctx, q)
		if err != nil {
			return nil, infra(err)
		}
		for _, it := range out.Items {
			rs, err := expandRun(op.TP, it)
			if err != nil {
				return nil, err
			}
			var full bool
			res, bytes, full = takeRuns(res, bytes, rs, op)
			if full {
				return res, nil
			}
		}
		if out.LastEvaluatedKey == nil {
			return res, nil
		}
		q.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// takeRuns appends the batches of rs that end after op.From; full reports that
// MaxBytes is reached.
func takeRuns(res []core.BatchRange, bytes int64, rs []core.BatchRange, op core.Lookup) ([]core.BatchRange, int64, bool) {
	for _, r := range rs {
		if int64(r.Base)+r.Count <= int64(op.From) {
			continue // an earlier batch of the run holding From
		}
		res = append(res, r)
		bytes += r.Loc.Len
		if op.MaxBytes > 0 && bytes >= int64(op.MaxBytes) {
			return res, bytes, true
		}
	}
	return res, bytes, false
}

// queryPage is one measured Query call.
func (s *Store) queryPage(ctx context.Context, q *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	q.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
	t0 := time.Now()
	out, err := s.db.Query(ctx, q)
	var cc []types.ConsumedCapacity
	if out != nil {
		cc = capacities(out.ConsumedCapacity)
	}
	s.Obs.CallCtx(ctx, "Query", t0, cc)
	if err != nil {
		s.note(ctx, "Query", err)
	}
	return out, err
}

// updateItem is one measured UpdateItem call.
func (s *Store) updateItem(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	in.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
	t0 := time.Now()
	out, err := s.db.UpdateItem(ctx, in)
	var cc []types.ConsumedCapacity
	if out != nil {
		cc = capacities(out.ConsumedCapacity)
	}
	s.Obs.CallCtx(ctx, "UpdateItem", t0, cc)
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if !errors.As(err, &ccf) {
			s.note(ctx, "UpdateItem", err)
		}
	}
	return out, err
}
