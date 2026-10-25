package logcp_test

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"

	"manifester/core"
	"manifester/cp/applier"
	"manifester/cp/fakedb"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
	"manifester/cp/logsm"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

// X2 for the real service (stage 2): three logcp.Service nodes (rank 0 the
// preferred appender, two standbys) over one log, under the seeded scheduler,
// with lost and late log answers, read and tail-check failures, node crashes
// and restarts (from a checkpoint), takeover, failback and split brain
// (several nodes appending). Clients behave like brokers: they retry a
// command with the same key on the next node after a refusal or a crash.
//
// At the end of every seed, with the log quiescent:
//   - every live node has applied the whole log and all agree on the state;
//   - the state equals an independent model of the rules (first slot of a key
//     wins within the window, ErrConflict, dense offsets) and, when no key
//     leaves the window, the veritas oracle's query digest;
//   - every answer a client got equals the model's answer for some slot that
//     holds that command;
//   - a Lookup or Describe on ANY node after an ack sees the acknowledged data.

type simMode int

const (
	modeSteady   simMode = iota // rank 0 active, two standbys
	modeKill                    // the active node crashes; a standby takes over
	modeFailback                // ...and restarts; the preferred node takes over again
	modeSplit                   // three nodes of equal rank all append
	modeWindow                  // small window, trim, a standby restarts from a checkpoint
	numModes
)

type simNode struct {
	idx   int
	svc   *logcp.Service
	dead  bool
	incar int
	reg   *prometheus.Registry
}

type obs struct {
	cmd  logsm.Entry
	resp core.CommitSegmentResp
	code core.ErrCode
}

type simRun struct {
	seed uint64
	mode simMode
	s    *sched
	mem  *logcp.MemLog
	rec  *recLog
	db   *fakedb.Fake
	objs *logcptest.Objects

	mu       sync.Mutex
	nodes    [3]*simNode
	seen     []obs
	fails    []error
	clients  atomic.Int64
	finished atomic.Bool

	settled  core.LogSlot // the log tail when the nodes were last seen quiescent
	acks     int
	regs     []*prometheus.Registry
	window   int64
	stride   int64
	keep     int64
	trimmedN int
	raced    int // restarts overtaken by a trim
}

type simResult struct {
	trace uint64
	steps int64
	slots int64
	acks  int
	lost  float64

	cov map[string]float64 // what the seed exercised (metric sums, restarts)
}

func (r *simRun) fail(format string, a ...any) {
	r.mu.Lock()
	r.fails = append(r.fails, fmt.Errorf(format, a...))
	r.mu.Unlock()
}

// recLog keeps every slot ever written, so the model sees the whole history
// even after the log has been trimmed.
type recLog struct {
	logcp.Log
	mu   sync.Mutex
	hist map[int64]logcp.Entry
	viol []error
}

func (l *recLog) Append(ctx context.Context, slot int64, e logcp.Entry) (logcp.Entry, bool, error) {
	cur, won, err := l.Log.Append(ctx, slot, e)
	if err == nil {
		l.mu.Lock()
		if old, had := l.hist[slot]; had && won {
			l.viol = append(l.viol, fmt.Errorf("slot %d was written again after it was gone: %q replaced %q", slot, e.ID, old.ID))
		}
		l.hist[slot] = cur
		l.mu.Unlock()
	}
	return cur, won, err
}

func (l *recLog) slots() []logcp.Slot {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []logcp.Slot
	for n, e := range l.hist {
		out = append(out, logcp.Slot{N: core.LogSlot(n), E: e})
	}
	slices.SortFunc(out, func(a, b logcp.Slot) int { return int(a.N - b.N) })
	return out
}

// faultLog wraps the shared MemLog with faults and scheduling points.
type faultLog struct {
	s     *sched
	inner logcp.Log
}

func (l *faultLog) pause() {
	for n := l.s.rng.Intn(3); n >= 0; n-- {
		l.s.yield()
	}
}

var errInjected = errors.New("injected")

func (l *faultLog) Append(ctx context.Context, slot int64, e logcp.Entry) (logcp.Entry, bool, error) {
	l.pause()
	if l.s.chance(5) {
		return logcp.Entry{}, false, errInjected // never lands
	}
	cur, won, err := l.inner.Append(ctx, slot, e)
	l.pause()
	if err == nil && l.s.chance(5) {
		return logcp.Entry{}, false, errInjected // landed, the answer was lost
	}
	return cur, won, err
}

func (l *faultLog) Read(ctx context.Context, from int64, limit int) ([]logcp.Slot, error) {
	l.pause()
	if l.s.chance(5) {
		return nil, errInjected
	}
	return l.inner.Read(ctx, from, limit)
}

func (l *faultLog) CheckTail(ctx context.Context) (int64, error) {
	l.pause()
	if l.s.chance(5) {
		return 0, errInjected
	}
	return l.inner.CheckTail(ctx)
}

func (l *faultLog) TrimBelow(ctx context.Context, slot int64) (int, error) {
	l.pause()
	return l.inner.(logcp.Trimmer).TrimBelow(ctx, slot)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func (r *simRun) nodeConfig(i int, incar int) (logcp.Config, *prometheus.Registry) {
	env := r.s.env()
	reg := prometheus.NewRegistry()
	rank := i
	if r.mode == modeSplit {
		rank = 0
	}
	takeover := time.Duration(25+r.seed%20) * time.Millisecond
	switch r.mode {
	case modeSplit:
		takeover = 0
	case modeSteady:
		takeover = 5 * time.Second // nothing fails: no standby may ever take over
	}
	return logcp.Config{
		Name: "n" + strconv.Itoa(i), Rank: rank, Incarnation: strconv.Itoa(incar),
		Log: &faultLog{s: r.s, inner: r.rec}, KV: applier.NewMemKV(),
		DB: r.db, Table: "cp", Objects: r.objs, Env: &env, Metrics: logcp.NewMetrics(reg), Logger: quiet(),
		K: 1 + int(r.seed%3), MaxSlotRate: 500, MaxCmdsPerSlot: 1 + int(r.seed%7)*3,
		HoleAfter: time.Duration(5+r.seed%50) * time.Millisecond, TailPoll: 2 * time.Millisecond, TailBatch: 2 * time.Millisecond,
		ReadMax: 4 + int(r.seed%5), Heartbeat: 10 * time.Millisecond, TakeoverAfter: takeover,
		WindowSlots: r.window, TrimStride: r.stride, KeepSlots: r.keep, TrimDelete: r.mode == modeWindow,
		CkptEvery: 40 * time.Millisecond, CkptMinSlots: 3,
	}, reg
}

// startNode creates node i and starts it as an actor (launch is s.spawn
// before the run and s.goFrom during it).
func (r *simRun) startNode(i int, launch func(func())) {
	r.mu.Lock()
	incar := 1
	if old := r.nodes[i]; old != nil {
		incar = old.incar + 1
	}
	cfg, reg := r.nodeConfig(i, incar)
	svc, err := logcp.New(cfg)
	if err != nil {
		r.mu.Unlock()
		r.fail("new node %d: %v", i, err)
		return
	}
	n := &simNode{idx: i, svc: svc, incar: incar, reg: reg}
	r.nodes[i] = n
	r.regs = append(r.regs, reg)
	r.mu.Unlock()
	launch(func() {
		if err := svc.Start(context.Background()); err != nil && !n.dead {
			if errors.Is(err, logcp.ErrTrimmed) {
				// The checkpoint it restored was overtaken by a newer checkpoint
				// and a trim during the restore (KeepSlots is a handful of slots
				// here): the node cannot continue and the supervisor starts it again.
				r.mu.Lock()
				n.dead = true
				r.raced++
				r.mu.Unlock()
				return
			}
			r.fail("start node %d: %v", i, err)
			return
		}
		if r.mode == modeSplit && i > 0 {
			svc.Promote()
		}
	})
}

func (r *simRun) kill(i int) {
	r.mu.Lock()
	n := r.nodes[i]
	n.dead = true
	r.mu.Unlock()
	n.svc.Stop()
}

// live returns the nodes that can take traffic.
func (r *simRun) live() []*simNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*simNode
	for _, n := range r.nodes {
		if n != nil && !n.dead && n.svc.Ready() {
			out = append(out, n)
		}
	}
	return out
}

func (r *simRun) alive(n *simNode) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !n.dead
}

// genOp draws a command: keys come from a small pool, so duplicates are
// common; a key's payload usually repeats and sometimes differs (a conflict);
// a few commands are invalid; a few create topics.
func genOp(r *veritastest.Rand) logsm.Entry {
	switch r.Intn(40) {
	case 0:
		return logsm.Entry{Kind: logsm.KindCreateTopic, Topic: "t" + strconv.Itoa(r.Intn(3)), Parts: int32(1 + r.Intn(4))}
	case 1:
		return logsm.Entry{Kind: logsm.KindCommit, Commit: core.CommitSegment{SegmentKey: "bad" + strconv.Itoa(r.Intn(2))}}
	}
	key := "k" + strconv.Itoa(r.Intn(16))
	variant := 0
	if r.Intn(8) == 0 {
		variant = 1 + r.Intn(2)
	}
	pr := veritastest.NewRand(uint64(key[1])*31 + uint64(len(key)) + uint64(variant))
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
	return logsm.Entry{Kind: logsm.KindCommit, Commit: core.CommitSegment{SegmentKey: key, Batches: bs}}
}

// call runs cmd on n. A refusal or an error is not a failure: the client goes
// on to the next node.
func (r *simRun) call(ctx context.Context, n *simNode, cmd logsm.Entry) (core.CommitSegmentResp, core.ErrCode, error) {
	if cmd.Kind == logsm.KindCreateTopic {
		code, err := n.svc.CreateTopic(ctx, cmd.Topic, cmd.Parts)
		return core.CommitSegmentResp{}, code, err
	}
	return n.svc.CommitSegment(ctx, cmd.Commit)
}

// client issues ops like a broker: sticky to the node that worked, the next
// one after a failure, the same key every time.
func (r *simRun) client(id, ops int) {
	defer r.clients.Add(-1)
	at := id % 3
	for range ops {
		cmd := genOp(r.s.rng)
		r.doOp(id, &at, cmd)
	}
}

func (r *simRun) doOp(id int, at *int, cmd logsm.Entry) {
	for range 400 {
		n := r.pick(*at)
		if n == nil {
			r.s.yield()
			*at = (*at + 1) % 3
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		if r.s.chance(3) { // the caller gives up while the command may be in flight
			k := 1 + r.s.rng.Intn(40)
			r.s.goFrom(func() {
				for ; k > 0; k-- {
					r.s.yield()
				}
				cancel()
			})
		}
		resp, code, err := r.call(ctx, n, cmd)
		cancel()
		if err == nil {
			r.record(obs{cmd, resp, code})
			r.readYourWrites(n, cmd, resp, code)
			return
		}
		*at = (*at + 1) % 3
		r.s.yield()
	}
}

func (r *simRun) pick(at int) *simNode {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.nodes[at]
	if n == nil || n.dead || !n.svc.Ready() {
		return nil
	}
	return n
}

func (r *simRun) record(o obs) {
	r.mu.Lock()
	r.seen = append(r.seen, o)
	r.acks++
	r.mu.Unlock()
}

// readYourWrites: after an ack, a fresh read on any live node sees the data.
func (r *simRun) readYourWrites(from *simNode, cmd logsm.Entry, resp core.CommitSegmentResp, code core.ErrCode) {
	if code != core.OK {
		return
	}
	live := r.live()
	if len(live) == 0 {
		return
	}
	n := live[r.s.rng.Intn(len(live))]
	ctx := context.Background()
	if cmd.Kind == logsm.KindCreateTopic {
		d, err := n.svc.DescribeTopics(ctx, core.DescribeTopics{Topics: []string{cmd.Topic}})
		if err == nil && (len(d.Topics) != 1 || d.Topics[0].Partitions < cmd.Parts) {
			r.fail("create %s/%d acked by n%d, describe on n%d says %+v", cmd.Topic, cmd.Parts, from.idx, n.idx, d.Topics)
		}
		return
	}
	for _, a := range resp.Assigned {
		l, code, err := n.svc.Lookup(ctx, core.Lookup{TP: a.TP, From: a.Base})
		if err != nil || code != core.OK {
			continue
		}
		if l.End < a.Base+core.Offset(a.Count) || !slices.Contains(l.Ranges, a) {
			r.fail("commit %q acked by n%d with %+v, lookup on n%d: end %d ranges %+v", cmd.Commit.SegmentKey, from.idx, a, n.idx, l.End, l.Ranges)
		}
	}
}

// chaos crashes and restarts nodes as the mode says, at points of the log
// (the number of slots written) so it lands mid-run whatever the time scale.
func (r *simRun) chaos() {
	grow := func(by int64) { // wait until the log has grown by `by` slots
		start, _ := r.mem.CheckTail(context.Background())
		for !r.finished.Load() {
			if tail, _ := r.mem.CheckTail(context.Background()); tail >= start+by {
				return
			}
			r.s.yield()
		}
	}
	switch r.mode {
	case modeKill:
		grow(int64(10 + r.s.rng.Intn(30)))
		r.killIfRunning(0)
	case modeFailback:
		grow(int64(10 + r.s.rng.Intn(30)))
		r.killIfRunning(0)
		grow(int64(5 + r.s.rng.Intn(30)))
		r.restartIfRunning(0)
	case modeWindow:
		grow(int64(20 + r.s.rng.Intn(40)))
		i := 1 + r.s.rng.Intn(2)
		r.killIfRunning(i)
		grow(int64(5 + r.s.rng.Intn(40)))
		r.restartIfRunning(i)
	}
}

func (r *simRun) killIfRunning(i int) {
	if !r.finished.Load() {
		r.kill(i)
	}
}

func (r *simRun) restartIfRunning(i int) {
	if !r.finished.Load() {
		r.startNode(i, r.s.goFrom)
	}
}

// settle waits for every live node to apply the whole log.
func (r *simRun) settle() core.LogSlot {
	for i := 0; i < 20000 && r.s.step < r.s.limit; i++ {
		tail, _ := r.mem.CheckTail(context.Background())
		ok := true
		for _, n := range r.live() {
			if n.svc.Blocked() == nil && n.svc.Applied() < core.LogSlot(tail) {
				ok = false
			}
		}
		if ok && i > 50 {
			return core.LogSlot(tail)
		}
		r.s.yield()
	}
	return 0
}

func (r *simRun) coordinator() {
	for r.clients.Load() > 0 && r.s.step < r.s.limit {
		r.s.yield()
	}
	r.settled = r.settle()
	r.finished.Store(true)
	for _, n := range r.live() {
		n.svc.Stop()
	}
	for _, n := range r.nodes {
		if n != nil {
			n.svc.Stop()
		}
	}
}

func runSim(seed uint64) (simResult, error) {
	_, res, err := runSimRun(seed)
	return res, err
}

func runSimRun(seed uint64) (*simRun, simResult, error) {
	r := &simRun{seed: seed, mode: simMode(seed % uint64(numModes)), s: newSched(seed, 300000),
		db: fakedb.New(), objs: logcptest.NewObjects(), window: 1 << 20, stride: 1024, keep: 1 << 20}
	if seed/5%2 == 0 {
		r.s.scale = 20 // an actor runs about every 40 steps: this keeps timeouts longer than a loop's period
		r.s.limit *= 20
	}
	r.mem = logcp.NewMemLog()
	r.rec = &recLog{Log: r.mem, hist: map[int64]logcp.Entry{}}
	if r.mode == modeWindow {
		r.window, r.stride, r.keep = 24, 8, 12
	}
	if _, err := r.db.CreateTable(context.Background(), &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		return nil, simResult{}, err
	}
	for i := range 3 {
		r.startNode(i, r.s.spawn)
	}
	nc := 1 + int(seed/5%3)
	for c := 0; c < 3*nc; c++ {
		id := c
		r.clients.Add(1)
		r.s.spawn(func() { r.client(id, 10) })
	}
	r.s.spawn(r.chaos)
	r.s.spawn(r.coordinator)
	r.s.run()
	res := simResult{trace: r.s.trace, steps: r.s.step, acks: r.acks}
	if r.s.hung {
		return r, res, fmt.Errorf("did not finish within the step limit (%d steps)", r.s.step)
	}
	if len(r.fails) > 0 {
		return r, res, r.fails[0]
	}
	if len(r.rec.viol) > 0 {
		return r, res, r.rec.viol[0]
	}
	return r, res, r.check(&res)
}

// ---- the independent model ------------------------------------------------

type firstInfo struct {
	slot core.LogSlot
	cmd  core.CommitSegment
	rs   []core.BatchRange
}

type model struct {
	window, stride int64
	first          map[string]firstInfo
	runs           map[core.TopicPartition][]core.BatchRange
	topics         map[string]int32
	answers        map[string][]core.CPDone // by command content: what each slot holding it answered
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

func cmdID(c logsm.Entry) string {
	return fmt.Sprintf("%d|%+v|%s|%d", c.Kind, c.Commit, c.Topic, c.Parts)
}

func buildModel(slots []logcp.Slot, window, stride int64) *model {
	m := &model{window: window, stride: stride, first: map[string]firstInfo{},
		runs: map[core.TopicPartition][]core.BatchRange{}, topics: map[string]int32{}, answers: map[string][]core.CPDone{}}
	for _, sl := range slots {
		if int64(sl.N)%stride == 0 && int64(sl.N) >= window {
			for k, f := range m.first {
				if f.slot < sl.N-core.LogSlot(window) {
					delete(m.first, k)
				}
			}
		}
		for _, c := range sl.E.Cmds {
			d := m.apply(sl.N, c)
			m.answers[cmdID(c)] = append(m.answers[cmdID(c)], d)
		}
	}
	return m
}

func (m *model) grow(topic string, n int32) {
	m.topics[topic] = max(m.topics[topic], n)
}

func (m *model) apply(slot core.LogSlot, c logsm.Entry) core.CPDone {
	if c.Kind == logsm.KindCreateTopic {
		if c.Topic == "" || c.Parts <= 0 {
			return core.CPDone{Err: core.ErrInvalid}
		}
		m.grow(c.Topic, c.Parts)
		return core.CPDone{}
	}
	cs := c.Commit
	if f, known := m.first[cs.SegmentKey]; known {
		if !slices.Equal(f.cmd.Batches, cs.Batches) {
			return core.CPDone{Err: core.ErrConflict}
		}
		return core.CPDone{Resp: core.CommitSegmentResp{Assigned: f.rs}}
	}
	if !validCommand(cs) {
		return core.CPDone{Err: core.ErrInvalid}
	}
	var rs []core.BatchRange
	for _, b := range cs.Batches {
		var base core.Offset
		if old := m.runs[b.TP]; len(old) > 0 {
			base = old[len(old)-1].Base + core.Offset(old[len(old)-1].Count)
		}
		r := core.BatchRange{TP: b.TP, Base: base, Count: b.Records, Loc: b.Loc}
		m.runs[b.TP] = append(m.runs[b.TP], r)
		m.grow(b.TP.Topic, b.TP.Partition+1)
		rs = append(rs, r)
	}
	m.first[cs.SegmentKey] = firstInfo{slot: slot, cmd: cs, rs: rs}
	return core.CPDone{Resp: core.CommitSegmentResp{Assigned: rs}}
}

// coverage sums the interesting counters of every node incarnation.
func (r *simRun) coverage() map[string]float64 {
	cov := map[string]float64{"restarts": 0, "trimmed_blocked": float64(r.trimmedN), "restart_raced_by_trim": float64(r.raced)}
	want := map[string]string{"manicp_log_lost_slots_total": "lost_slots", "manicp_log_hole_fills_total": "hole_fills",
		"manicp_log_trimmed_slots_total": "trimmed", "manicp_log_slots_total": "won_slots", "manicp_log_role_changes_total": "role_changes",
		"manicp_log_checkpoint_seconds": "checkpoints"}
	for _, reg := range r.regs {
		mf, _ := reg.Gather()
		for _, f := range mf {
			key, ok := want[f.GetName()]
			if !ok {
				continue
			}
			for _, mm := range f.GetMetric() {
				cov[key] += mm.GetCounter().GetValue() + float64(mm.GetHistogram().GetSampleCount())
			}
		}
	}
	for _, n := range r.nodes {
		if n != nil && n.incar > 1 {
			cov["restarts"]++
		}
	}
	return cov
}

// activations counts how often a node became active.
func activations(reg *prometheus.Registry) float64 {
	mf, _ := reg.Gather()
	for _, f := range mf {
		if f.GetName() == "manicp_log_role_changes_total" {
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetValue() == "active" {
						return m.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return 0
}

func (r *simRun) check(res *simResult) error {
	res.cov = r.coverage()
	if r.mode == modeSteady {
		for _, n := range r.nodes[1:] {
			if a := activations(n.reg); a != 0 {
				return fmt.Errorf("steady state: standby n%d became active %v times", n.idx, a)
			}
		}
	}
	slots := r.rec.slots()
	res.slots = int64(len(slots))
	var nodes []*simNode
	for _, n := range r.nodes {
		if n == nil || n.dead {
			continue
		}
		if err := n.svc.Blocked(); err != nil {
			if errors.Is(err, logcp.ErrTrimmed) {
				r.trimmedN++ // a standby so far behind that the log below it is gone: it must restart
				continue
			}
			return fmt.Errorf("n%d blocked: %w", n.idx, err)
		}
		nodes = append(nodes, n)
	}
	if len(nodes) == 0 {
		return errors.New("no live node at the end")
	}
	byApplied := map[core.LogSlot]*simNode{}
	for _, n := range nodes {
		if n.svc.Applied() < r.settled {
			return fmt.Errorf("n%d applied %d, below the settled tail %d", n.idx, n.svc.Applied(), r.settled)
		}
		// A late append (a caller that gave up) may land after the nodes
		// stopped, so nodes can differ by a slot: each is held to the model
		// of exactly the prefix it applied, and nodes at the same slot agree.
		if o, same := byApplied[n.svc.Applied()]; same && o.svc.Applier().StateHash() != n.svc.Applier().StateHash() {
			return fmt.Errorf("n%d and n%d applied %d slots with different states", n.idx, o.idx, n.svc.Applied())
		}
		byApplied[n.svc.Applied()] = n
		if err := buildModel(prefix(slots, n.svc.Applied()), r.window, r.stride).checkState(n.svc.Applier()); err != nil {
			return fmt.Errorf("n%d: %w", n.idx, err)
		}
		if r.window >= 1<<20 {
			if err := checkOracle(prefix(slots, n.svc.Applied()), n.svc.Applier()); err != nil {
				return fmt.Errorf("n%d: %w", n.idx, err)
			}
		}
	}
	m := buildModel(slots, r.window, r.stride)
	for _, o := range r.seen {
		if err := m.checkAnswer(o); err != nil {
			return err
		}
	}
	return nil
}

func prefix(slots []logcp.Slot, n core.LogSlot) []logcp.Slot {
	i := 0
	for i < len(slots) && slots[i].N < n {
		i++
	}
	return slots[:i]
}

func (m *model) checkState(a *applier.Applier) error {
	for tp, want := range m.runs {
		d, _ := a.Lookup(core.Lookup{TP: tp})
		got, _ := d.Resp.(core.LookupResp)
		var end core.Offset
		for _, x := range want {
			if x.Base != end {
				return fmt.Errorf("model: partition %v run at %d, expected %d", tp, x.Base, end)
			}
			end += core.Offset(x.Count)
		}
		if !slices.Equal(got.Ranges, want) || got.End != end {
			return fmt.Errorf("partition %v: runs %+v end %d, model %+v end %d", tp, got.Ranges, got.End, want, end)
		}
	}
	d, _ := a.Describe(core.DescribeTopics{})
	got := map[string]int32{}
	for _, t := range d.Resp.(core.DescribeTopicsResp).Topics {
		got[t.Name] = t.Partitions
	}
	if !reflect.DeepEqual(got, m.topics) && (len(got) != 0 || len(m.topics) != 0) {
		return fmt.Errorf("topics %v, model %v", got, m.topics)
	}
	return nil
}

// checkAnswer: the client's answer is what the model says for SOME slot that
// holds this very command (a retry after a crash can land in several).
func (m *model) checkAnswer(o obs) error {
	for _, w := range m.answers[cmdID(o.cmd)] {
		if o.cmd.Kind == logsm.KindCreateTopic && o.code == w.Err {
			return nil
		}
		got, _ := w.Resp.(core.CommitSegmentResp)
		if o.cmd.Kind == logsm.KindCommit && o.code == w.Err && slices.Equal(o.resp.Assigned, got.Assigned) {
			return nil
		}
	}
	return fmt.Errorf("answer %v %+v to %+v matches no slot holding it (model has %d)", o.code, o.resp, o.cmd, len(m.answers[cmdID(o.cmd)]))
}

// checkOracle applies the log to veritas and compares the query digests.
func checkOracle(slots []logcp.Slot, a *applier.Applier) error {
	cp := veritas.NewControlPlane()
	for _, sl := range slots {
		for _, c := range sl.E.Cmds {
			switch c.Kind {
			case logsm.KindCommit:
				cp.Exec(core.CPCall{Req: core.ReqID(sl.N), Op: c.Commit})
			case logsm.KindCreateTopic:
				cp.CreateTopic(c.Topic, c.Parts)
			}
		}
	}
	got := queryDigest(func(op core.CPOp) core.CPDone { return a.Exec(core.CPCall{Op: op}).(core.CPDone) })
	want := queryDigest(func(op core.CPOp) core.CPDone { return cp.Exec(core.CPCall{Op: op}).(core.CPDone) })
	if got != want {
		return fmt.Errorf("query digest: applier %x, veritas oracle %x", got, want)
	}
	return nil
}

func queryDigest(exec func(core.CPOp) core.CPDone) uint64 {
	h := fnv.New64a()
	d := exec(core.DescribeTopics{}).Resp.(core.DescribeTopicsResp)
	for _, t := range d.Topics {
		_, _ = fmt.Fprintf(h, "T%s/%d;", t.Name, t.Partitions)
		for p := int32(0); p < t.Partitions; p++ {
			l := exec(core.Lookup{TP: core.TP(t.Name, p)}).Resp.(core.LookupResp)
			_, _ = fmt.Fprintf(h, "P%d/%d:%+v;", p, l.End, l.Ranges)
		}
	}
	return h.Sum64()
}

// ---- tests ------------------------------------------------------------------

func simSeeds() int {
	if v, err := strconv.Atoi(os.Getenv("LOGCP_SEEDS")); err == nil && v > 0 {
		return v
	}
	return 300
}

func TestSimSeeds(t *testing.T) {
	n := simSeeds()
	var failed, slots, acks atomic.Int64
	var lost atomic.Int64
	var covMu sync.Mutex
	cov := map[string]float64{}
	var wg sync.WaitGroup
	next := make(chan uint64)
	workers := max(2, runtime.GOMAXPROCS(0)-1)
	for range workers {
		wg.Go(func() {
			for seed := range next {
				res, err := runSim(seed)
				slots.Add(res.slots)
				acks.Add(int64(res.acks))
				lost.Add(int64(res.lost))
				covMu.Lock()
				for k, v := range res.cov {
					cov[k] += v
				}
				covMu.Unlock()
				if err != nil {
					failed.Add(1)
					t.Errorf("seed %d (mode %d): %v", seed, seed%uint64(numModes), err)
				}
			}
		})
	}
	for seed := uint64(1); seed <= uint64(n); seed++ {
		next <- seed
	}
	close(next)
	wg.Wait()
	t.Logf("seeds=%d failed=%d slots=%d acks=%d runs_lost_to_window=%d", n, failed.Load(), slots.Load(), acks.Load(), lost.Load())
	t.Logf("coverage: %v", cov)
	for _, k := range []string{"lost_slots", "hole_fills", "role_changes", "restarts", "trimmed", "checkpoints"} {
		if n >= 300 && cov[k] == 0 {
			t.Errorf("the seeds never exercised %s", k)
		}
	}
}

func TestSimIsDeterministic(t *testing.T) {
	for _, seed := range []uint64{3, 4, 7, 12} {
		a, errA := runSim(seed)
		b, errB := runSim(seed)
		if errA != nil || errB != nil || a.trace != b.trace || a.steps != b.steps || a.slots != b.slots {
			t.Fatalf("seed %d: %+v %v / %+v %v", seed, a, errA, b, errB)
		}
	}
}

func (l *recLog) TrimBelow(ctx context.Context, slot int64) (int, error) {
	return l.Log.(logcp.Trimmer).TrimBelow(ctx, slot)
}
