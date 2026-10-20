package logcp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp/applier"
	"manifester/cp/dynamo"
	"manifester/cp/logsm"
)

// The indexer writes the same R# rows and TOPICS items as cp/dynamo, after
// the ack, so the old store reads them and a rollback needs no conversion
// (decision 50, docs/log-cp-service.md "Index"). Its plan is a pure function
// of the log: the commands at slots [w, h), where w is the watermark in the
// IDX item (pk IDX, sk "-", ix_slot N: slots below it are indexed) and h the
// applied position. Each command's ranges come from the applier's key record
// (applier.Assigned), so any node derives byte-identical rows and a repeated
// or concurrent flush is harmless. For one partition the rows are written in
// offset order, one wave at a time, so the rows in the table are always a
// prefix of the partition's runs, never a run with a gap below it. The
// watermark moves after the rows.

const idxPK = "IDX"

type idxState struct {
	mu     sync.Mutex
	nextAt int64 // pacing: earliest start of the next batch write
}

type row struct {
	tp   core.TopicPartition
	item map[string]types.AttributeValue
}

// indexLoop flushes every IndexEvery on the active node; on a standby it only
// follows the watermark so its feed stays short.
func (s *Service) indexLoop(ctx context.Context) {
	if s.noIndex() {
		return
	}
	for s.env.sleep(ctx, s.cfg.IndexEvery) == nil {
		var err error
		if s.isActive() {
			err = s.FlushIndex(ctx)
		} else {
			err = s.followIndex(ctx)
		}
		if err != nil && ctx.Err() == nil {
			s.m.indexErr()
			s.cfg.Logger.Warn("log: index flush failed", "event", "log_index_failed", "err", err.Error())
		}
	}
}

func (s *Service) followIndex(ctx context.Context) error {
	w, err := s.watermark(ctx)
	if err != nil {
		return err
	}
	s.rep.dropFeed(w)
	s.m.indexLag(w, s.rep.appliedNow())
	return nil
}

// watermark is the first slot not yet indexed.
func (s *Service) watermark(ctx context.Context) (int64, error) {
	it, err := s.getPointer(ctx, idxPK)
	if err != nil || it == nil {
		return 0, err
	}
	return numAttr(it, "ix_slot"), nil
}

func (s *Service) setWatermark(ctx context.Context, h int64) error {
	_, err := s.cfg.DB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 &s.cfg.Table,
		Item:                      map[string]types.AttributeValue{"pk": sv(idxPK), "sk": sv("-"), "ix_slot": nv(h), "ix_ts": nv(s.env.Now() / 1e6)},
		ConditionExpression:       aws.String("attribute_not_exists(ix_slot) OR ix_slot < :s"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":s": nv(h)},
	})
	var cc *types.ConditionalCheckFailedException
	if asErr(err, &cc) {
		return nil // a newer watermark is already there
	}
	return err
}

// FlushIndex writes the rows of everything applied since the watermark and
// moves the watermark. Safe to call on any node at any time.
func (s *Service) FlushIndex(ctx context.Context) error {
	if s.noIndex() {
		return nil
	}
	t0 := time.Now()
	w, err := s.watermark(ctx)
	if err != nil {
		return err
	}
	h := s.rep.appliedNow()
	if h <= w {
		s.rep.dropFeed(w)
		s.m.indexLag(w, h)
		return nil
	}
	items, err := s.feedRange(ctx, w, h)
	if err != nil {
		return err
	}
	rows, topics, err := s.planIndex(items)
	if err != nil {
		return err
	}
	if err := s.writeRows(ctx, rows); err != nil {
		return err
	}
	if err := s.writeTopics(ctx, topics); err != nil {
		return err
	}
	if err := s.setWatermark(ctx, h); err != nil {
		return err
	}
	s.rep.dropFeed(h)
	s.m.indexed(h, s.rep.appliedNow(), len(rows), time.Since(t0))
	return nil
}

// feedRange returns the commands at slots [w, h) in log order: from the
// log for the part below the feed (after a restore from a checkpoint or a
// feed overflow), then the in-memory feed.
func (s *Service) feedRange(ctx context.Context, w, h int64) ([]feedItem, error) {
	r := s.rep
	r.mu.Lock()
	from := r.feedFrom
	var mem []feedItem
	for _, it := range r.feed {
		if it.slot >= w && it.slot < h {
			mem = append(mem, it)
		}
	}
	r.mu.Unlock()
	if from <= w {
		return mem, nil
	}
	gap, err := s.readFeed(ctx, w, min(from, h))
	return append(gap, mem...), err
}

// readFeed reads the commands at slots [lo, hi) from the log.
func (s *Service) readFeed(ctx context.Context, lo, hi int64) ([]feedItem, error) {
	var out []feedItem
	for lo < hi {
		slots, err := s.log.Read(ctx, lo, s.cfg.ReadMax)
		if err != nil {
			return nil, err
		}
		if len(slots) == 0 {
			break
		}
		for _, sl := range slots {
			if sl.N >= hi {
				return out, nil
			}
			for _, c := range sl.E.Cmds {
				out = append(out, feedItem{slot: sl.N, cmd: c})
			}
		}
		lo = slots[len(slots)-1].N + 1
	}
	return out, nil
}

// dropFeed forgets the feed below slot (it is indexed).
func (r *replica) dropFeed(below int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := 0
	for i < len(r.feed) && r.feed[i].slot < below {
		i++
	}
	r.feed = append([]feedItem(nil), r.feed[i:]...)
	r.feedFrom = max(r.feedFrom, below)
}

func validCommand(c core.CommitSegment) bool {
	if c.SegmentKey == "" || len(c.Batches) == 0 {
		return false
	}
	for _, b := range c.Batches {
		if b.Records <= 0 || b.TP.Topic == "" || b.TP.Partition < 0 {
			return false
		}
	}
	return true
}

// planIndex turns commands into rows (one per partition and commit, in log
// order) and the partition count each topic needs.
func (s *Service) planIndex(items []feedItem) (rows []row, topics map[string]int32, err error) {
	topics = map[string]int32{}
	lost := 0
	for _, it := range items {
		switch it.cmd.Kind {
		case logsm.KindCreateTopic:
			if it.cmd.Topic != "" && it.cmd.Parts > 0 {
				topics[it.cmd.Topic] = max(topics[it.cmd.Topic], it.cmd.Parts)
			}
		case logsm.KindCommit:
			rs, st, err := s.app.Assigned(it.slot, it.cmd.Commit)
			if err != nil {
				return nil, nil, err
			}
			if st == applier.AssignUnknown && validCommand(it.cmd.Commit) {
				lost++
			}
			rows = append(rows, rowsOf(rs, topics)...)
		}
	}
	s.m.indexLost(lost)
	return rows, topics, nil
}

// rowsOf groups the ranges of one command by partition (first use order),
// one row each, and raises the topics' partition counts.
func rowsOf(rs []core.BatchRange, topics map[string]int32) []row {
	var out []row
	at := map[core.TopicPartition]int{}
	var runs [][]core.BatchRange
	for _, r := range rs {
		i, ok := at[r.TP]
		if !ok {
			i = len(runs)
			at[r.TP] = i
			runs = append(runs, nil)
			topics[r.TP.Topic] = max(topics[r.TP.Topic], r.TP.Partition+1)
		}
		runs[i] = append(runs[i], r)
	}
	for _, run := range runs {
		out = append(out, row{tp: run[0].TP, item: dynamo.RunRow(run[0].TP, run)})
	}
	return out
}

// waves splits rows so that wave k holds the k-th row of every partition.
func waves(rows []row) [][]row {
	seen := map[core.TopicPartition]int{}
	var out [][]row
	for _, r := range rows {
		k := seen[r.tp]
		seen[r.tp] = k + 1
		if k == len(out) {
			out = append(out, nil)
		}
		out[k] = append(out[k], r)
	}
	return out
}

func (s *Service) hooks() *writeHooks {
	return &writeHooks{sleep: s.env.sleep, unprocessed: s.m.indexUnprocessed, throttled: s.m.indexThrottle}
}

// writeRows writes the waves in order; within a wave BatchWriteItem calls
// (25 rows) run IndexParallel at a time, paced to IndexRate rows a second.
func (s *Service) writeRows(ctx context.Context, rows []row) error {
	for _, w := range waves(rows) {
		var chunks [][]row
		for len(w) > 0 {
			n := min(len(w), 25)
			chunks, w = append(chunks, w[:n]), w[n:]
		}
		err := s.parallel(ctx, len(chunks), s.cfg.IndexParallel, func(ctx context.Context, i int) error {
			if err := s.pace(ctx, len(chunks[i])); err != nil {
				return err
			}
			reqs := make([]types.WriteRequest, len(chunks[i]))
			for k, r := range chunks[i] {
				reqs[k] = types.WriteRequest{PutRequest: &types.PutRequest{Item: r.item}}
			}
			return writeAll(ctx, s.cfg.DB, s.cfg.Table, reqs, s.hooks())
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// pace waits for n rows' worth of the index rate budget.
func (s *Service) pace(ctx context.Context, n int) error {
	if s.cfg.IndexRate <= 0 {
		return nil
	}
	now := s.env.Now()
	s.ix.mu.Lock()
	at := max(now, s.ix.nextAt)
	s.ix.nextAt = at + int64(float64(n)/s.cfg.IndexRate*float64(time.Second))
	s.ix.mu.Unlock()
	if at > now {
		return s.env.sleep(ctx, time.Duration(at-now))
	}
	return nil
}

// writeTopics raises the TOPICS items (never lowers one).
func (s *Service) writeTopics(ctx context.Context, topics map[string]int32) error {
	names := make([]string, 0, len(topics))
	for n := range topics {
		names = append(names, n)
	}
	return s.parallel(ctx, len(names), s.cfg.IndexParallel, func(ctx context.Context, i int) error {
		_, err := s.cfg.DB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:           &s.cfg.Table,
			Key:                 map[string]types.AttributeValue{"pk": sv(dynamo.TopicsPK), "sk": sv(names[i])},
			UpdateExpression:    aws.String("SET parts = :n"),
			ConditionExpression: aws.String("attribute_not_exists(parts) OR parts < :n"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":n": nv(int64(topics[names[i]])),
			},
		})
		var cc *types.ConditionalCheckFailedException
		if asErr(err, &cc) {
			return nil
		}
		return err
	})
}

// parallel runs fn(0..n-1) on up to workers goroutines (through Env.Go) and
// returns the first error; after an error the rest are skipped.
func (s *Service) parallel(ctx context.Context, n, workers int, fn func(ctx context.Context, i int) error) error {
	if n == 0 {
		return nil
	}
	workers = min(workers, n)
	var next atomic.Int64
	var failed atomic.Bool
	var first error
	var mu sync.Mutex
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		s.env.Go(func() {
			defer func() { done <- struct{}{} }()
			for !failed.Load() {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				if err := fn(ctx, i); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					failed.Store(true)
				}
			}
		})
	}
	for w := 0; w < workers; w++ {
		if err := s.env.Wait(context.WithoutCancel(ctx), done, 0); err != nil {
			return err
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return first
}
