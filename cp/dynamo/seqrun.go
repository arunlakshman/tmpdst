package dynamo

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"manifester/core"
	"manifester/cp"
)

// run commits a group: attempts until every request is answered or the retry
// budget is spent. Requests it cannot take (budget) are deferred, not failed.
func (s *Sequencer) run(g *seqGroup) {
	defer s.wg.Done()
	defer s.finishGroup(g)
	t0 := time.Now()
	pending := g.reqs
	for attempt := 0; len(pending) > 0; attempt++ {
		if attempt > 0 {
			if err := s.backoff(attempt, t0); err != nil {
				s.failAll(pending, err)
				return
			}
		}
		pending = s.attempt(g, pending)
	}
}

// backoff sleeps (jittered, capped) before a retry, or errors when the group's
// attempts or time budget is spent (or the sequencer closed).
func (s *Sequencer) backoff(attempt int, t0 time.Time) error {
	if attempt >= s.o.MaxAttempts || time.Since(t0) > s.o.MaxTime {
		return fmt.Errorf("%w: group lost the race or the table %d times in %s", cp.ErrUnavailable, attempt, time.Since(t0).Round(time.Millisecond))
	}
	d := time.Duration(rand.IntN(4<<min(attempt, 5))+1) * time.Millisecond
	if err := sleep(s.ctx, d); err != nil {
		return ErrSequencerClosed
	}
	return nil
}

// failAll answers every request with err.
func (s *Sequencer) failAll(reqs []*seqReq, err error) {
	for _, r := range reqs {
		r.answer(core.CommitSegmentResp{}, 0, err)
	}
}

// attempt is one round for the pending requests of a group: replays from
// memory, the reads that memory lacks, the build, the transaction. It returns
// the requests that must go round again.
func (s *Sequencer) attempt(g *seqGroup, pending []*seqReq) []*seqReq {
	for _, r := range pending {
		s.obs.AttemptCtx(r.ctx, "commit")
	}
	live := s.replayCached(pending)
	if len(live) == 0 {
		return nil
	}
	t0 := time.Now()
	segs, err := s.load(live)
	s.obs.Prep("reads", time.Since(t0))
	if err != nil {
		return s.readFailed(live, err)
	}
	live = s.replayRead(live, segs)
	t1 := time.Now()
	plan, deferred, tooLarge := s.build(live)
	s.obs.Prep("decide_encode", time.Since(t1))
	g.deferred = append(g.deferred, deferred...)
	for _, r := range tooLarge {
		r.answer(core.CommitSegmentResp{}, core.ErrTooLarge, nil)
	}
	if plan == nil {
		return nil
	}
	return s.transact(plan)
}

// replayCached answers the requests whose key is in the LRU (replay or
// conflict) and returns the others.
func (s *Sequencer) replayCached(pending []*seqReq) []*seqReq {
	var live []*seqReq
	for _, r := range pending {
		e, ok := s.state.seg(r.op.SegmentKey)
		s.obs.SeqCache("segment", ok)
		if !ok {
			live = append(live, r)
			continue
		}
		s.stats.segHits.Add(1)
		resp, code := e.answer(r.op)
		r.answer(resp, code, nil)
	}
	return live
}

// replayRead answers the requests whose G# item was read and exists.
func (s *Sequencer) replayRead(live []*seqReq, segs map[string]item) []*seqReq {
	var out []*seqReq
	for _, r := range live {
		if !r.needRead {
			out = append(out, r)
			continue
		}
		it := lookupKey(segs, segKey(r.op.SegmentKey))
		if it == nil {
			r.needRead = false
			out = append(out, r)
			continue
		}
		resp, code, err := replay(r.op, it)
		r.answer(resp, code, err)
	}
	return out
}

// callCtx bounds one DynamoDB call.
func (s *Sequencer) callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.ctx, s.o.CallTimeout)
}

// load reads what memory lacks for reqs: the ends of unknown partitions (one
// consistent Query each), the unknown topics, and the G# items of the requests
// flagged needRead, the keys in one BatchGetItem. The ends and topics go into
// the state; the G# items are returned.
func (s *Sequencer) load(reqs []*seqReq) (map[string]item, error) {
	tps, topics, keys := s.missing(reqs)
	if len(tps)+len(topics)+len(keys) == 0 {
		return nil, nil
	}
	ctx, cancel := s.callCtx()
	defer cancel()
	var got map[string]item
	var jobs []func(context.Context) error
	if len(topics)+len(keys) > 0 {
		jobs = append(jobs, func(ctx context.Context) (err error) {
			got, err = s.st.batchGet(ctx, append(keys, topicKeys(topics)...))
			return err
		})
	}
	for _, tp := range tps {
		jobs = append(jobs, func(ctx context.Context) error { return s.loadEnd(ctx, tp) })
	}
	if err := runAll(ctx, readParallel, jobs); err != nil {
		return nil, err
	}
	for _, t := range topics {
		n, _ := getNum(lookupKey(got, topicKey(t)), "parts")
		s.state.growTopic(t, int32(n))
	}
	s.stats.segReads.Add(int64(len(keys)))
	return got, nil
}

// loadEnd reads one partition's end into the state. Each success is kept even
// if another read of the round fails: the group holds the partition, so
// nobody else moves it meanwhile, and a retry only reads what is still missing.
func (s *Sequencer) loadEnd(ctx context.Context, tp core.TopicPartition) error {
	end, err := s.st.endOf(ctx, tp)
	if err == nil {
		s.state.setEnd(tp, end)
	}
	return err
}

func topicKeys(topics []string) []item {
	var out []item
	for _, t := range topics {
		out = append(out, topicKey(t))
	}
	return out
}

// missing lists the partitions and topics the state does not know and the G#
// keys to read for reqs.
func (s *Sequencer) missing(reqs []*seqReq) (tps []core.TopicPartition, topics []string, keys []item) {
	seenT := map[string]bool{}
	seenP := map[core.TopicPartition]bool{}
	for _, r := range reqs {
		if r.needRead {
			keys = append(keys, segKey(r.op.SegmentKey))
		}
		for _, tp := range distinctPartitions(r.op.Batches) {
			tps, topics = s.missingTP(tp, seenP, seenT, tps, topics)
		}
	}
	return
}

func (s *Sequencer) missingTP(tp core.TopicPartition, seenP map[core.TopicPartition]bool, seenT map[string]bool,
	tps []core.TopicPartition, topics []string) ([]core.TopicPartition, []string) {
	if !seenP[tp] {
		seenP[tp] = true
		_, ok := s.state.end(tp)
		s.obs.SeqCache("end", ok)
		if ok {
			s.stats.endHits.Add(1)
		} else {
			s.stats.endMisses.Add(1)
			tps = append(tps, tp)
		}
	}
	if !seenT[tp.Topic] {
		seenT[tp.Topic] = true
		_, ok := s.state.topic(tp.Topic)
		s.obs.SeqCache("topic", ok)
		if !ok {
			topics = append(topics, tp.Topic)
		}
	}
	return tps, topics
}

// readFailed: a failed read round is retried if the table is unavailable and
// fails the requests otherwise.
func (s *Sequencer) readFailed(reqs []*seqReq, err error) []*seqReq {
	if s.ctx.Err() != nil {
		s.failAll(reqs, ErrSequencerClosed)
		return nil
	}
	if outcomeOf(err) == outcomeFatal {
		s.failAll(reqs, infra(err))
		return nil
	}
	s.retry("read_failed", reqs, err)
	return reqs
}

type outcome int

const (
	outcomeNotApplied outcome = iota // rejected before it could apply: throttling, conflict
	outcomeUnknown                   // may or may not have applied: 5xx, timeout, connection lost
	outcomeFatal                     // a request error: retrying cannot help
)

var notApplied = []func(error) bool{
	isType[*types.ProvisionedThroughputExceededException],
	isType[*types.RequestLimitExceeded],
	isType[*types.ThrottlingException],
	isType[*types.TransactionInProgressException],
	isType[*types.TransactionConflictException],
}

var fatalCodes = map[string]bool{"ValidationException": true, "ResourceNotFoundException": true, "AccessDeniedException": true,
	"UnrecognizedClientException": true, "SerializationException": true, "ItemCollectionSizeLimitExceededException": true}

// outcomeOf classifies a failed call. Anything that is not clearly a
// rejection is treated as possibly applied (a lost answer): the sequencer
// then re-reads before it reuses what it believes.
func outcomeOf(err error) outcome {
	for _, is := range notApplied {
		if is(err) {
			return outcomeNotApplied
		}
	}
	var ae smithy.APIError
	if errors.As(err, &ae) && fatalCodes[ae.ErrorCode()] {
		return outcomeFatal
	}
	return outcomeUnknown
}

// retry counts and logs a group retry (one line per retry, at warn).
func (s *Sequencer) retry(reason string, reqs []*seqReq, err error) {
	s.stats.retries.Add(1)
	s.obs.SeqRetry(reason)
	for _, r := range reqs {
		s.obs.RetryCtx(r.ctx, "commit", reasonLegacy(reason), nil)
	}
	if s.obs != nil {
		s.obs.Logger().Warn("sequencer retry", "event", "seq_retry", "reason", reason, "segments", len(reqs), "err", fmt.Sprint(err))
	}
}

// reasonLegacy maps a sequencer reason to the retries_total reasons that
// already existed (dashboards keep working).
func reasonLegacy(reason string) string {
	if reason == "transaction_canceled" {
		return reason
	}
	return "unavailable"
}

// transact sends the group's transaction and handles the outcome. It returns
// the requests to retry.
func (s *Sequencer) transact(p *groupPlan) []*seqReq {
	ctx, cancel := s.callCtx()
	defer cancel()
	t0 := time.Now()
	out, err := s.st.db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: p.items,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
	d := time.Since(t0)
	var cc []types.ConsumedCapacity
	if out != nil {
		cc = out.ConsumedCapacity
	}
	s.obs.CallCtx(context.Background(), "TransactWriteItems", t0, cc)
	for _, r := range p.accepted {
		s.obs.TallyDynamo(r.ctx, d)
	}
	s.stats.txs.Add(1)
	s.stats.items.Add(int64(len(p.items)))
	s.obs.SeqGroup(len(p.accepted), len(p.items))
	if s.st.Obs != nil {
		s.st.Obs.Logger().Debug("sequencer group", "event", "seq_group", "segments", len(p.accepted), "items", len(p.items),
			"partitions", len(p.tps), "tx_ms", float64(d.Microseconds())/1000, "ok", err == nil)
	}
	if err == nil {
		s.obs.SeqTx("ok", d)
		s.applied(p)
		return nil
	}
	return s.failed(p, err, d)
}

// applied records a committed group in the state and answers its requests.
func (s *Sequencer) applied(p *groupPlan) {
	s.stats.segments.Add(int64(len(p.accepted)))
	for tp, end := range p.newEnds {
		s.state.setEnd(tp, end)
	}
	for t, n := range p.grown {
		s.state.growTopic(t, n)
	}
	for i, r := range p.accepted {
		s.state.putSeg(r.op.SegmentKey, r.op.Batches, p.ranges[i])
		r.answer(core.CommitSegmentResp{Assigned: p.ranges[i]}, core.OK, nil)
	}
}

// failed handles a transaction that did not report success.
func (s *Sequencer) failed(p *groupPlan, err error, d time.Duration) []*seqReq {
	var tc *types.TransactionCanceledException
	if errors.As(err, &tc) {
		s.obs.SeqTx("canceled", d)
		return s.canceled(p, tc, err)
	}
	if s.ctx.Err() != nil {
		s.failAll(p.accepted, ErrSequencerClosed)
		return nil
	}
	switch outcomeOf(err) {
	case outcomeNotApplied:
		s.obs.SeqTx("unavailable", d)
		s.stats.notApplied.Add(1)
		s.retry("unavailable", p.accepted, err)
	case outcomeFatal:
		s.obs.SeqTx("error", d)
		s.failAll(p.accepted, infra(err))
		return nil
	default:
		s.obs.SeqTx("unknown", d)
		s.stats.unknown.Add(1)
		s.unknownOutcome(p)
		s.retry("unknown_outcome", p.accepted, err)
	}
	return p.accepted
}

// unknownOutcome: the transaction may have applied. Forget what the state
// believes about the group's partitions and topics and read each G# before the
// next attempt (a key that exists is answered as a replay).
func (s *Sequencer) unknownOutcome(p *groupPlan) {
	for _, tp := range p.tps {
		s.state.dropEnd(tp)
	}
	for _, t := range p.topics {
		s.state.dropTopic(t)
	}
	for _, r := range p.accepted {
		r.needRead = true
	}
}

// canceled maps the per-item cancellation reasons back to what to refresh: a
// failed G# item means the key exists (read it: replay or conflict), a failed
// run or topic item means the state was wrong (read the partition or topic
// again). Items that were fine ("None") keep their cached state.
func (s *Sequencer) canceled(p *groupPlan, tc *types.TransactionCanceledException, err error) []*seqReq {
	s.stats.canceled.Add(1)
	codes, detail := cancelReasons(tc, p.items)
	for i, r := range p.accepted {
		var c []string
		if i == 0 {
			c = codes
		}
		s.obs.RetryCtx(r.ctx, "commit", "transaction_canceled", c)
	}
	s.stats.retries.Add(1)
	s.obs.SeqRetry("transaction_canceled")
	if s.obs != nil {
		s.obs.Logger().Warn("sequencer transaction canceled", "event", "seq_txn_canceled", "segments", len(p.accepted),
			"items", len(p.items), "reasons", detail, "err", err.Error())
	}
	if len(tc.CancellationReasons) == 0 {
		s.unknownOutcome(p) // no reasons: refresh everything
		return p.accepted
	}
	for i, r := range tc.CancellationReasons {
		if c := aws.ToString(r.Code); c != "" && c != "None" {
			s.refresh(p, i, c)
		}
	}
	return p.accepted
}

// refresh reacts to item i of the group's transaction having failed with code.
func (s *Sequencer) refresh(p *groupPlan, i int, code string) {
	n := len(p.accepted)
	switch {
	case i < n:
		if code == "ConditionalCheckFailed" {
			p.accepted[i].needRead = true
		}
	case i < n+len(p.tps):
		s.state.dropEnd(p.tps[i-n])
	case i < n+len(p.tps)+len(p.topics):
		s.state.dropTopic(p.topics[i-n-len(p.tps)])
	}
}
