package logsm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"manifester/core"
	"manifester/veritas/veritastest"
)

// X2: the command log under a seeded scheduler. The replicas, committers and
// hole-fillers are the real code; only the Log is wrapped. Each actor is a
// goroutine, but exactly one runs at a time: every log call and every Wait
// parks the actor, and the scheduler resumes one parked actor, chosen by the
// seeded stream. The same seed therefore gives the same interleaving, the same
// faults and the same trace hash. Time is the step counter.

type park struct{ ch chan struct{} }

type sched struct {
	rng     *veritastest.Rand
	mu      sync.Mutex
	cv      *sync.Cond
	running int
	parked  []*park
	step    int64
	limit   int64
	trace   uint64
}

func newSched(seed uint64, limit int64) *sched {
	s := &sched{rng: veritastest.NewRand(seed), limit: limit, trace: 14695981039346656037}
	s.cv = sync.NewCond(&s.mu)
	return s
}

// spawn starts an actor and waits until it first parks or ends, so the park
// order is deterministic.
func (s *sched) spawn(f func()) {
	s.mu.Lock()
	s.running++
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			s.running--
			s.cv.Broadcast()
			s.mu.Unlock()
		}()
		f()
	}()
	s.mu.Lock()
	for s.running > 0 {
		s.cv.Wait()
	}
	s.mu.Unlock()
}

// yield parks the calling actor until the scheduler resumes it.
func (s *sched) yield() {
	p := &park{ch: make(chan struct{})}
	s.mu.Lock()
	s.parked = append(s.parked, p)
	s.running--
	s.cv.Broadcast()
	s.mu.Unlock()
	<-p.ch
}

func (s *sched) run() {
	for {
		s.mu.Lock()
		for s.running > 0 {
			s.cv.Wait()
		}
		if len(s.parked) == 0 {
			s.mu.Unlock()
			return
		}
		i := s.rng.Intn(len(s.parked))
		p := s.parked[i]
		s.parked = slices.Delete(s.parked, i, i+1)
		s.running++
		s.step++
		s.trace = (s.trace ^ uint64(i+1)) * 1099511628211
		s.mu.Unlock()
		close(p.ch)
	}
}

func (s *sched) chance(percent int) bool { return s.rng.Intn(100) < percent }

var (
	errLost  = errors.New("injected: lost")
	errLimit = errors.New("step limit")
)

type crashKey struct{}

// simLog wraps the shared MemLog with faults and scheduling points.
type simLog struct {
	s       *sched
	inner   Log
	crashes atomic.Int64
}

func (l *simLog) pause() {
	for n := l.s.rng.Intn(3); n >= 0; n-- { // a random delay of 0..2 extra steps
		l.s.yield()
	}
}

func (l *simLog) Append(ctx context.Context, slot int64, e Entry) (Entry, bool, error) {
	l.pause()
	if cancel, ok := ctx.Value(crashKey{}).(context.CancelFunc); ok && l.s.chance(2) {
		l.crashes.Add(1)
		cancel() // the writer dies after allocating the slot, before writing it
		return Entry{}, false, ctx.Err()
	}
	if l.s.chance(5) {
		return Entry{}, false, errLost // the write never lands
	}
	cur, won, err := l.inner.Append(ctx, slot, e)
	l.pause()
	if err == nil && l.s.chance(5) {
		return Entry{}, false, errLost // it landed, the answer did not
	}
	return cur, won, err
}

func (l *simLog) Read(ctx context.Context, from int64, limit int) ([]Slot, error) {
	l.pause()
	if l.s.chance(5) {
		return nil, errLost
	}
	return l.inner.Read(ctx, from, limit)
}

func (l *simLog) CheckTail(ctx context.Context) (int64, error) {
	l.pause()
	if l.s.chance(5) {
		return 0, errLost
	}
	return l.inner.CheckTail(ctx)
}

// obs is what a writer saw: the command and its answer.
type obs struct {
	e    Entry
	done core.CPDone
}

type result struct {
	trace   uint64
	steps   int64
	crashes int64
	lost    int64
	fills   int64
	retries int64
	slots   int64
}

// genCommand draws a command: keys come from a small pool, so duplicates are
// common; a key's payload usually repeats and sometimes differs (a conflict);
// a few commands are invalid; a few create topics.
func genCommand(r *veritastest.Rand) Entry {
	switch r.Intn(40) {
	case 0:
		return Entry{Kind: KindCreateTopic, Topic: "t" + strconv.Itoa(r.Intn(3)), Parts: int32(r.Intn(4))}
	case 1:
		return Entry{Kind: KindCommit, Commit: core.CommitSegment{SegmentKey: "bad" + strconv.Itoa(r.Intn(2))}}
	}
	key := "k" + strconv.Itoa(r.Intn(8))
	variant := 0
	if r.Intn(6) == 0 {
		variant = 1 + r.Intn(2)
	}
	pr := veritastest.NewRand(uint64(key[1])*31 + uint64(variant))
	var bs []core.PendingBatch
	for i := 1 + pr.Intn(5); i > 0; i-- {
		bs = append(bs, core.PendingBatch{
			TP:      core.TP([]string{"a", "b"}[pr.Intn(2)], int32(pr.Intn(4))),
			Records: int64(1 + pr.Intn(3)),
			Loc:     core.SegmentRef{Key: key, Off: int64(pr.Intn(4)) * 100, Len: int64(1 + pr.Intn(3)*50)},
		})
	}
	if r.Intn(30) == 0 {
		bs[0].Records = 0 // invalid
	}
	return Entry{Kind: KindCommit, Commit: core.CommitSegment{SegmentKey: key, Batches: bs}}
}

// runSeed runs one X2 scenario and returns the first violation it finds.
func runSeed(seed uint64) (result, error) { return runSeedWith(seed, nil) }

// runSeedWith is runSeed with a state machine per replica from mk (nil: the
// veritas default).
func runSeedWith(seed uint64, mk func() Machine) (result, error) {
	const replicas, perWriter = 3, 8
	writersPer := 1 + int(seed%3) // K in flight per replica
	s := newSched(seed, 200000)
	mem := NewMemLog()
	log := &simLog{s: s, inner: mem}
	var rs []*Replica
	for i := 0; i < replicas; i++ {
		var m Machine
		if mk != nil {
			m = mk()
		}
		rs = append(rs, NewReplica(Config{
			Machine: m, Log: log, Writer: "r" + strconv.Itoa(i), Now: func() int64 { return s.step },
			Wait: func(ctx context.Context) error {
				s.yield()
				if s.step > s.limit {
					return errLimit
				}
				return ctx.Err()
			},
			HoleAfter: 5 + int64(seed%50), TailPoll: 3, ReadMax: 4,
		}))
	}
	var mu sync.Mutex
	var seen []obs
	var failures []error
	var writers atomic.Int64
	for i, r := range rs {
		c := NewCommitter(r, writersPer)
		for w := 0; w < writersPer; w++ {
			writers.Add(1)
			s.spawn(func() {
				defer writers.Add(-1)
				ctx, cancel := context.WithCancel(context.Background())
				ctx = context.WithValue(ctx, crashKey{}, cancel)
				defer cancel()
				for n := 0; n < perWriter; n++ {
					e := genCommand(s.rng)
					d, err := c.Do(ctx, e)
					mu.Lock()
					switch {
					case ctx.Err() != nil: // crashed: the command may or may not be in the log
					case err != nil:
						failures = append(failures, fmt.Errorf("replica %d: %w", i, err))
					default:
						seen = append(seen, obs{e, d})
					}
					mu.Unlock()
					if ctx.Err() != nil {
						return
					}
				}
			})
		}
		s.spawn(func() { // tailer, hole-filler, then the final sync
			ctx := context.Background()
			for writers.Load() > 0 && s.step <= s.limit {
				r.Tick(ctx)
				if r.cfg.Wait(ctx) != nil {
					break
				}
			}
			if _, err := r.Read(ctx, core.DescribeTopics{}); err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("final sync: %w", err))
				mu.Unlock()
			}
		})
	}
	s.run()
	res := result{trace: s.trace, steps: s.step, crashes: log.crashes.Load()}
	for _, r := range rs {
		st := r.Stats()
		res.lost, res.fills, res.retries = res.lost+st.Lost, res.fills+st.HoleFills, res.retries+st.Retries
	}
	if len(failures) > 0 {
		return res, failures[0]
	}
	return res, check(mem, rs, seen, &res)
}

// check holds every replica to the log and to an independent model of the
// rules (not veritas): first valid slot of a key wins, a later identical
// command is a replay, a different one is ErrConflict, offsets are dense per
// partition in slot order.
func check(mem *MemLog, rs []*Replica, seen []obs, res *result) error {
	slots, _ := mem.Read(context.Background(), 0, 1<<30)
	res.slots = int64(len(slots))
	for i, r := range rs {
		if r.Applied() != int64(len(slots)) || (len(slots) > 0 && slots[len(slots)-1].N != r.Applied()-1) {
			return fmt.Errorf("replica %d applied %d, log has %d slots", i, r.Applied(), len(slots))
		}
		if r.StateHash() != rs[0].StateHash() {
			return fmt.Errorf("replica %d state hash %x != replica 0 %x", i, r.StateHash(), rs[0].StateHash())
		}
	}
	m := buildModel(slots)
	if err := m.checkState(rs[0]); err != nil {
		return err
	}
	for _, o := range seen {
		if err := m.checkAnswer(o); err != nil {
			return err
		}
	}
	return nil
}

type model struct {
	first map[string]core.CommitSegment
	rng   map[string][]core.BatchRange
	runs  map[core.TopicPartition][]core.BatchRange
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

func buildModel(slots []Slot) *model {
	m := &model{first: map[string]core.CommitSegment{}, rng: map[string][]core.BatchRange{},
		runs: map[core.TopicPartition][]core.BatchRange{}}
	for _, s := range slots {
		c := s.E.Commit
		if s.E.Kind != KindCommit || !validCommand(c) {
			continue
		}
		if _, dup := m.first[c.SegmentKey]; dup {
			continue
		}
		m.first[c.SegmentKey] = c
		for _, b := range c.Batches {
			var base core.Offset
			if rs := m.runs[b.TP]; len(rs) > 0 {
				base = rs[len(rs)-1].Base + core.Offset(rs[len(rs)-1].Count)
			}
			r := core.BatchRange{TP: b.TP, Base: base, Count: b.Records, Loc: b.Loc}
			m.runs[b.TP] = append(m.runs[b.TP], r)
			m.rng[c.SegmentKey] = append(m.rng[c.SegmentKey], r)
		}
	}
	return m
}

func (m *model) checkState(r *Replica) error {
	for tp, want := range m.runs {
		got, _ := r.Local(core.Lookup{TP: tp}).Resp.(core.LookupResp)
		if !slices.Equal(got.Ranges, want) {
			return fmt.Errorf("partition %v runs %+v, model %+v", tp, got.Ranges, want)
		}
		var end core.Offset
		for _, x := range want { // dense, no overlap
			if x.Base != end {
				return fmt.Errorf("partition %v: run at %d, expected %d", tp, x.Base, end)
			}
			end += core.Offset(x.Count)
		}
		if got.End != end {
			return fmt.Errorf("partition %v end %d, model %d", tp, got.End, end)
		}
	}
	return nil
}

func (m *model) checkAnswer(o obs) error {
	if o.e.Kind != KindCommit {
		return nil
	}
	c := o.e.Commit
	first, committed := m.first[c.SegmentKey]
	switch o.done.Err {
	case core.OK:
		resp, _ := o.done.Resp.(core.CommitSegmentResp)
		got := resp.Assigned
		if !committed || !slices.Equal(first.Batches, c.Batches) || !slices.Equal(got, m.rng[c.SegmentKey]) {
			return fmt.Errorf("key %q answered %+v, model %+v (committed %v)", c.SegmentKey, got, m.rng[c.SegmentKey], committed)
		}
	case core.ErrConflict:
		if !committed || slices.Equal(first.Batches, c.Batches) {
			return fmt.Errorf("key %q ErrConflict but model says committed=%v same=%v", c.SegmentKey, committed, committed && slices.Equal(first.Batches, c.Batches))
		}
	case core.ErrInvalid:
		if validCommand(c) {
			return fmt.Errorf("key %q ErrInvalid for a valid command", c.SegmentKey)
		}
	default:
		return fmt.Errorf("key %q unexpected error %v", c.SegmentKey, o.done.Err)
	}
	return nil
}

func seedCount() int {
	if n, err := strconv.Atoi(os.Getenv("LOGSM_SEEDS")); err == nil && n > 0 {
		return n
	}
	return 300
}

func TestX2Seeds(t *testing.T) {
	n := seedCount()
	var total result
	var failed atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	next := make(chan uint64)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := range next {
				res, err := runSeed(seed)
				mu.Lock()
				total.steps += res.steps
				total.crashes += res.crashes
				total.lost += res.lost
				total.fills += res.fills
				total.retries += res.retries
				total.slots += res.slots
				mu.Unlock()
				if err != nil {
					failed.Add(1)
					t.Errorf("seed %d: %v (reproduce: LOGSM_SEED=%d go test ./cp/logsm -run TestX2Seed -v)", seed, err, seed)
				}
			}
		}()
	}
	for seed := uint64(1); seed <= uint64(n); seed++ {
		next <- seed
	}
	close(next)
	wg.Wait()
	t.Logf("seeds=%d failed=%d slots=%d steps=%d writer-crashes=%d lost-slots=%d unknown-outcome-retries=%d hole-fills=%d",
		n, failed.Load(), total.slots, total.steps, total.crashes, total.lost, total.retries, total.fills)
	if total.crashes == 0 || total.lost == 0 || total.retries == 0 || total.fills == 0 {
		t.Fatalf("the faults did not all happen: %+v", total)
	}
}

func TestX2Deterministic(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		a, errA := runSeed(seed)
		b, errB := runSeed(seed)
		if a != b || (errA == nil) != (errB == nil) {
			t.Fatalf("seed %d: %+v vs %+v", seed, a, b)
		}
	}
}

func TestX2Seed(t *testing.T) {
	v := os.Getenv("LOGSM_SEED")
	if v == "" {
		t.Skip("LOGSM_SEED not set")
	}
	seed, _ := strconv.ParseUint(v, 10, 64)
	if _, err := runSeed(seed); err != nil {
		t.Fatal(err)
	}
}
