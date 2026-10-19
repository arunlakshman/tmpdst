package dynamo

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/cpobs"
)

// SeqOptions tune a Sequencer. The zero value of a field means its default.
type SeqOptions struct {
	// Pipeline is the number of group transactions in flight at once (default
	// 4; 1 is a strictly serial loop). Groups in flight touch disjoint
	// partitions, so per-partition order is kept whatever the value.
	Pipeline int
	// MaxItems is the item budget of one transaction (default and ceiling
	// MaxTxItems).
	MaxItems int
	// CacheKeys is the capacity of the recent-segment-key LRU (default 4096;
	// negative disables it: every replay reads G#).
	CacheKeys int
	// MaxAttempts and MaxTime bound the internal retries of one group
	// (defaults 40 and 8 s); then its requests get cp.ErrUnavailable (503) and
	// the broker resends under the same key.
	MaxAttempts int
	MaxTime     time.Duration
	// CallTimeout bounds one DynamoDB call of the sequencer (default 20 s).
	CallTimeout time.Duration
}

func (o SeqOptions) withDefaults() SeqOptions {
	if o.Pipeline <= 0 {
		o.Pipeline = 4
	}
	if o.MaxItems <= 0 || o.MaxItems > MaxTxItems {
		o.MaxItems = MaxTxItems
	}
	if o.CacheKeys == 0 {
		o.CacheKeys = 4096
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 40
	}
	if o.MaxTime <= 0 {
		o.MaxTime = 8 * time.Second
	}
	if o.CallTimeout <= 0 {
		o.CallTimeout = 20 * time.Second
	}
	return o
}

// SeqStats are cumulative counters of a Sequencer (tests, benchmarks).
type SeqStats struct {
	Requests   int64 // commit requests that entered the queue
	Groups     int64 // groups formed
	Segments   int64 // segments committed by a transaction
	Items      int64 // items of the transactions that were sent (applied or not)
	Txs        int64 // TransactWriteItems calls
	Canceled   int64 // TransactionCanceled answers
	NotApplied int64 // throttling-type failures
	Unknown    int64 // answers that left the outcome unknown (5xx, timeout, lost)
	Retries    int64 // group retries
	SegHits    int64 // replays answered from the LRU
	SegReads   int64 // G# items read for a replay or an unknown outcome
	EndMisses  int64 // partition ends read from the table
	EndHits    int64 // partition ends served from memory
}

type seqCounters struct {
	requests, groups, segments, items, txs, canceled, notApplied, unknown, retries, segHits, segReads, endMisses, endHits atomic.Int64
}

func (c *seqCounters) snapshot() SeqStats {
	return SeqStats{c.requests.Load(), c.groups.Load(), c.segments.Load(), c.items.Load(), c.txs.Load(), c.canceled.Load(),
		c.notApplied.Load(), c.unknown.Load(), c.retries.Load(), c.segHits.Load(), c.segReads.Load(), c.endMisses.Load(), c.endHits.Load()}
}

// ErrSequencerClosed is returned for a commit that is queued or arrives after Close.
var ErrSequencerClosed = fmt.Errorf("%w: sequencer closed", cp.ErrUnavailable)

// Sequencer is a cp.Store that owns the sequencing of CommitSegment over a
// DynamoDB Store: requests enter a queue, ONE loop goroutine forms groups from
// it in order, and each group is committed as one TransactWriteItems whose
// offsets come from in-memory partition ends. Contention is retried here, not
// by the broker. See cp/doc.go ("Sequencer") and decision 46.
//
// Correctness never depends on being the only sequencer: the transaction keeps
// the compare-and-set on every run item and the attribute_not_exists on G#, so
// a second or stale Sequencer (or an old Store) only causes internal retries.
type Sequencer struct {
	st  *Store
	o   SeqOptions
	obs *cpobs.Metrics

	state *seqState
	stats seqCounters

	// limits, in-package tests lower them.
	maxGroupBytes, maxRunBytes int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	cond     *sync.Cond
	queue    []*seqReq
	seq      uint64
	busy     map[resource]struct{} // reserved by groups in flight
	inflight int
	closed   bool
}

var _ cp.Store = (*Sequencer)(nil)

// maxGroupBlob bounds the sum of the G# blobs of one group: the transaction
// limit is 4 MB in all (blobs plus runs, and a run is far smaller than the
// blobs it summarises).
const maxGroupBlob = 1 << 20

// NewSequencer starts the sequencing loop over st. Close stops it.
func NewSequencer(st *Store, o SeqOptions) *Sequencer {
	o = o.withDefaults()
	s := &Sequencer{st: st, o: o, obs: st.Obs, state: newSeqState(st.Obs, o.CacheKeys),
		busy: map[resource]struct{}{}, maxGroupBytes: maxGroupBlob, maxRunBytes: maxRun}
	s.cond = sync.NewCond(&s.mu)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go s.loop()
	return s
}

// Store is the underlying store (reads, topics).
func (s *Sequencer) Store() *Store { return s.st }

// Stats is a snapshot of the counters.
func (s *Sequencer) Stats() SeqStats { return s.stats.snapshot() }

// Close stops the loop, fails the queued requests with ErrSequencerClosed and
// waits for the groups in flight to end (their calls are cancelled).
func (s *Sequencer) Close() {
	s.mu.Lock()
	s.closed = true
	q := s.queue
	s.queue = nil
	s.cancel()
	s.cond.Broadcast()
	s.mu.Unlock()
	for _, r := range q {
		r.answer(core.CommitSegmentResp{}, 0, ErrSequencerClosed)
	}
	s.wg.Wait()
}

// seqReq is one CommitSegment waiting for or inside a group.
type seqReq struct {
	op   core.CommitSegment
	ctx  context.Context
	enq  time.Time
	num  uint64
	done chan struct{}
	res  []resource

	needRead bool // read G# before assuming the key is new (replay or unknown outcome)

	resp core.CommitSegmentResp
	code core.ErrCode
	err  error
}

// answer completes the request (once).
func (r *seqReq) answer(resp core.CommitSegmentResp, code core.ErrCode, err error) {
	r.resp, r.code, r.err = resp, code, err
	close(r.done)
}

// CommitSegment queues the commit and waits for its group. A malformed op can
// still replay or conflict: it reads G# only and never enters the queue. A
// context cancelled while queued withdraws the request; once its group was
// formed the commit may still land, and the broker's resend replays it.
func (s *Sequencer) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	cpobs.FromContext(ctx).SetSegment(op.SegmentKey)
	if !wellFormed(op) {
		return s.st.replayOnly(ctx, op)
	}
	r := &seqReq{op: op, ctx: ctx, enq: time.Now(), done: make(chan struct{}), res: resourcesOf(op)}
	if err := s.enqueue(r); err != nil {
		return core.CommitSegmentResp{}, 0, err
	}
	select {
	case <-r.done:
		return r.resp, r.code, r.err
	case <-ctx.Done():
		s.mu.Lock()
		s.cond.Broadcast() // the loop drops it if still queued
		s.mu.Unlock()
		return core.CommitSegmentResp{}, 0, ctx.Err()
	}
}

func (s *Sequencer) enqueue(r *seqReq) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSequencerClosed
	}
	s.seq++
	r.num = s.seq
	s.queue = append(s.queue, r)
	s.stats.requests.Add(1)
	s.obs.SeqQueueDepth(len(s.queue))
	s.cond.Broadcast()
	return nil
}

// Lookup, DescribeTopics read the table directly (the sequencer adds nothing).
func (s *Sequencer) Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	return s.st.Lookup(ctx, op)
}

func (s *Sequencer) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	return s.st.DescribeTopics(ctx, op)
}

// CreateTopic raises the count in the table and forgets the cached count.
func (s *Sequencer) CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error) {
	code, err := s.st.CreateTopic(ctx, name, partitions)
	if err == nil {
		s.state.dropTopic(name)
	}
	return code, err
}

// loop is THE sequencing thread: it alone forms groups, in queue order, and
// starts them. The transactions run on their own goroutines (at most Pipeline).
func (s *Sequencer) loop() {
	defer s.wg.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	for !s.closed {
		g := s.plan()
		if g == nil {
			s.cond.Wait()
			continue
		}
		s.inflight++
		s.obs.SeqInflight(s.inflight)
		s.wg.Add(1)
		go s.run(g)
	}
}

// finishGroup releases the group's reservations and puts back the requests the
// group could not take (in their original queue order).
func (s *Sequencer) finishGroup(g *seqGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for res := range g.use {
		delete(s.busy, res)
	}
	s.inflight--
	s.obs.SeqInflight(s.inflight)
	if len(g.deferred) > 0 {
		s.queue = append(s.queue, g.deferred...)
		slices.SortFunc(s.queue, func(a, b *seqReq) int { return int(a.num) - int(b.num) })
		if s.closed {
			for _, r := range s.queue {
				r.answer(core.CommitSegmentResp{}, 0, ErrSequencerClosed)
			}
			s.queue = nil
		}
	}
	s.obs.SeqQueueDepth(len(s.queue))
	s.cond.Broadcast()
}
