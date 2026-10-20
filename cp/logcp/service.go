package logcp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/applier"
	"manifester/cp/logsm"
)

// Service is one control-plane node over the shared log. It implements
// cp.Store; Handler serves it over HTTP.
type Service struct {
	cfg Config
	env Env
	log Log
	app *applier.Applier
	m   *Metrics

	ctx    context.Context
	cancel context.CancelFunc

	rep *replica
	q   *queue
	fr  fresher

	active atomic.Bool
	hb     atomic.Bool
	ready  atomic.Bool

	running atomic.Int64
	exited  chan struct{}
	exitMu  sync.Once

	ck ckptState
	ix idxState
}

var _ cp.Store = (*Service)(nil)

// New builds a service. Start runs it.
func New(cfg Config) (*Service, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	s := &Service{cfg: cfg, env: *cfg.Env, log: cfg.Log, app: applier.New(cfg.KV), m: cfg.Metrics, exited: make(chan struct{})}
	s.rep = newReplica(s)
	s.q = newQueue()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s, nil
}

// Applier is the node's state machine (read-only use: digests, tests).
func (s *Service) Applier() *applier.Applier { return s.app }

// Name is the node name stamped on its slots.
func (s *Service) Name() string { return s.cfg.Name }

// Applied is the number of slots applied: slots 0..Applied()-1.
func (s *Service) Applied() int64 { return s.rep.appliedNow() }

// Blocked is non-nil once the node cannot apply the log any further (a slot
// in a format newer than this build, or a failed store): it serves 503.
func (s *Service) Blocked() error {
	_, _, b := s.rep.position()
	return b
}

func (s *Service) stopped() bool { return s.ctx.Err() != nil }

func (s *Service) noIndex() bool { return s.cfg.NoIndex || s.cfg.DB == nil || s.cfg.Table == "" }

// Ready is true once Start has restored the checkpoint and caught up.
func (s *Service) Ready() bool { return s.ready.Load() }

// Start restores the newest checkpoint (if any), replays the log to its tail
// and starts the loops: tailing with the hole filler and role logic, K
// flushers, checkpointing and the indexer. Rank 0 starts as the active node.
// ctx bounds the startup only; Close stops the loops.
func (s *Service) Start(ctx context.Context) error {
	slot, err := s.restore(ctx)
	if err != nil {
		return err
	}
	if slot > 0 {
		s.rep.startAt(slot)
	}
	if err := s.catchUp(ctx); err != nil {
		return err
	}
	now := s.env.Now()
	s.rep.mu.Lock()
	s.rep.lastOtherAt, s.rep.lastPullOK = now, now
	s.rep.mu.Unlock()
	if s.cfg.Rank == 0 {
		s.setActive(true)
	} else {
		s.m.roleIs(false)
	}
	s.spawn(s.runLoop)
	for i := 0; i < s.cfg.K; i++ {
		s.spawn(s.flusher)
	}
	s.spawn(s.ckptLoop)
	s.spawn(s.indexLoop)
	s.ready.Store(true)
	return nil
}

// spawn runs a loop until Close.
func (s *Service) spawn(f func(ctx context.Context)) {
	s.running.Add(1)
	s.env.Go(func() {
		defer func() {
			if s.running.Add(-1) == 0 && s.stopped() {
				s.exitMu.Do(func() { close(s.exited) })
			}
		}()
		f(s.ctx)
	})
}

// Stop signals the loops to end and returns at once (a crash, for tests).
func (s *Service) Stop() { s.cancel() }

// Close stops the loops and waits for them. Commands in flight are failed
// with ErrStopped; the store is not closed.
func (s *Service) Close() error {
	s.cancel()
	if s.running.Load() > 0 {
		<-s.exited
	}
	return nil
}

// catchUp replays the log to its current tail.
func (s *Service) catchUp(ctx context.Context) error {
	for {
		tail, err := s.checkTail(ctx)
		if err != nil {
			if serr := s.env.sleep(ctx, s.cfg.TailPoll); serr != nil {
				return serr
			}
			continue
		}
		for {
			applied, _, blocked := s.rep.position()
			if blocked != nil {
				return blocked
			}
			if applied >= tail {
				return nil
			}
			s.rep.pull(ctx)
			s.rep.holeTick(ctx)
			if a, _, _ := s.rep.position(); a == applied {
				if err := s.env.sleep(ctx, s.cfg.TailPoll); err != nil {
					return err
				}
			}
		}
	}
}

// runLoop tails the log: it keeps the node close to the tail, fills holes
// and runs the role rules. An active node polls less often, since it learns
// its own slots as it writes them.
func (s *Service) runLoop(ctx context.Context) {
	for ctx.Err() == nil {
		s.rep.pull(ctx)
		s.rep.holeTick(ctx)
		s.roleTick(ctx)
		poll := s.cfg.TailPoll
		if s.isActive() {
			poll *= 4
		}
		if s.env.Wait(ctx, s.rep.kick, poll) != nil {
			return
		}
	}
}

// CommitSegment implements cp.Store: the command goes to the log and the
// answer is the applier's, once the slot is applied.
func (s *Service) CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error) {
	d, err := s.submit(ctx, logsm.Entry{Kind: logsm.KindCommit, Commit: op})
	if err != nil {
		return core.CommitSegmentResp{}, core.OK, err
	}
	resp, _ := d.Resp.(core.CommitSegmentResp)
	return resp, d.Err, nil
}

// CreateTopic implements cp.Store.
func (s *Service) CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error) {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid, nil
	}
	d, err := s.submit(ctx, logsm.Entry{Kind: logsm.KindCreateTopic, Topic: name, Parts: partitions})
	return d.Err, err
}

// Lookup implements cp.Store: fresh (a tail check shared with concurrent
// reads, apply through it), then answered from the applier.
func (s *Service) Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error) {
	if op.From < 0 {
		return core.LookupResp{}, core.ErrInvalid, nil
	}
	if err := s.fresh(ctx); err != nil {
		return core.LookupResp{}, core.OK, err
	}
	d, err := s.app.Lookup(op)
	resp, _ := d.Resp.(core.LookupResp)
	return resp, d.Err, err
}

// DescribeTopics implements cp.Store, fresh like Lookup.
func (s *Service) DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error) {
	if err := s.fresh(ctx); err != nil {
		return core.DescribeTopicsResp{}, err
	}
	d, err := s.app.Describe(op)
	resp, _ := d.Resp.(core.DescribeTopicsResp)
	return resp, err
}

// End is the next offset of tp, fresh like Lookup.
func (s *Service) End(ctx context.Context, tp core.TopicPartition) (core.Offset, error) {
	if err := s.fresh(ctx); err != nil {
		return 0, err
	}
	return s.app.End(tp), nil
}

// Healthy reports why the node should not take traffic, or nil.
func (s *Service) Healthy() error {
	switch {
	case s.stopped():
		return ErrStopped
	case !s.Ready():
		return errors.New("logcp: starting")
	}
	return s.Blocked()
}

// since is the age of a clock reading.
func (s *Service) since(t int64) time.Duration { return time.Duration(s.env.Now() - t) }

func asErr[T error](err error, target *T) bool { return errors.As(err, target) }
