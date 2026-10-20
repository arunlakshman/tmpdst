package logcp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/applier"
	"manifester/cp/dynamo/fakedb"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
	"manifester/cp/logsm"
	"manifester/sys"
)

var bg = context.Background()

func tp(t string, p int32) core.TopicPartition { return core.TopicPartition{Topic: t, Partition: p} }

func seg(key string, tps ...core.TopicPartition) core.CommitSegment {
	c := core.CommitSegment{SegmentKey: key}
	for i, p := range tps {
		c.Batches = append(c.Batches, core.PendingBatch{TP: p, Records: int64(i + 2), Loc: core.SegmentRef{Key: key, Off: int64(i) * 10, Len: 10}})
	}
	return c
}

func commit(t testing.TB, s *logcp.Service, c core.CommitSegment) core.CommitSegmentResp {
	t.Helper()
	r, code, err := s.CommitSegment(bg, c)
	if err != nil || code != core.OK {
		t.Fatalf("commit %s: %v %v", c.SegmentKey, code, err)
	}
	return r
}

func newDB(t testing.TB) *fakedb.Fake {
	t.Helper()
	db := fakedb.New()
	if _, err := db.CreateTable(bg, &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	return db
}

// node builds a started node on a shared log, with the pointer parts on db.
func node(t testing.TB, name string, rank int, l logcp.Log, db *fakedb.Fake, objs sys.ObjectStore, mod func(*logcp.Config)) *logcp.Service {
	t.Helper()
	cfg := logcptest.FastConfig(name, rank, l)
	cfg.Incarnation = name + "-" + time.Now().Format("150405.000000000")
	if db != nil {
		cfg.DB, cfg.Table, cfg.Objects = db, "cp", objs
	}
	if mod != nil {
		mod(&cfg)
	}
	return logcptest.Start(t, cfg)
}

func TestCommitLookupReplayConflict(t *testing.T) {
	s := logcptest.NewMem(t)
	a := tp("t", 0)
	if code, err := s.CreateTopic(bg, "t", 2); err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	r1 := commit(t, s, seg("k1", a, tp("t", 1), a))
	if len(r1.Assigned) != 3 || r1.Assigned[0].Base != 0 || r1.Assigned[2].Base != 2 || r1.Assigned[1].Base != 0 {
		t.Fatalf("assigned %+v", r1.Assigned)
	}
	if r2 := commit(t, s, seg("k1", a, tp("t", 1), a)); len(r2.Assigned) != 3 || r2.Assigned[2] != r1.Assigned[2] {
		t.Fatalf("replay %+v", r2.Assigned)
	}
	if _, code, err := s.CommitSegment(bg, seg("k1", a)); err != nil || code != core.ErrConflict {
		t.Fatalf("conflict: %v %v", code, err)
	}
	if _, code, _ := s.CommitSegment(bg, core.CommitSegment{SegmentKey: "bad"}); code != core.ErrInvalid {
		t.Fatalf("invalid: %v", code)
	}
	l, code, err := s.Lookup(bg, core.Lookup{TP: a, From: 0})
	if err != nil || code != core.OK || l.End != 6 || len(l.Ranges) != 2 {
		t.Fatalf("lookup %+v %v %v", l, code, err)
	}
	if _, code, _ := s.Lookup(bg, core.Lookup{TP: a, From: -1}); code != core.ErrInvalid {
		t.Fatalf("negative from: %v", code)
	}
	if d, err := s.DescribeTopics(bg, core.DescribeTopics{Topics: []string{"t"}}); err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 2 {
		t.Fatalf("describe %+v %v", d, err)
	}
	if end, err := s.End(bg, a); err != nil || end != 6 {
		t.Fatalf("end %d %v", end, err)
	}
	if code, _ := s.CreateTopic(bg, "", 1); code != core.ErrInvalid {
		t.Fatalf("create invalid: %v", code)
	}
	if err := s.Healthy(); err != nil {
		t.Fatal(err)
	}
}

// slowLog adds latency to appends and counts them.
type slowLog struct {
	logcp.Log
	delay   time.Duration
	appends atomic.Int64
	gate    chan struct{} // if set, appends wait for it to close
}

func (l *slowLog) Append(ctx context.Context, slot int64, e logcp.Entry) (logcp.Entry, bool, error) {
	l.appends.Add(1)
	if l.gate != nil {
		select {
		case <-l.gate:
		case <-ctx.Done():
			return logcp.Entry{}, false, ctx.Err()
		}
	}
	time.Sleep(l.delay)
	return l.Log.Append(ctx, slot, e)
}

func TestGroupCommitFewerSlotsThanCommands(t *testing.T) {
	log := &slowLog{Log: logcp.NewMemLog(), delay: 3 * time.Millisecond}
	reg := prometheus.NewRegistry()
	s := node(t, "n0", 0, log, nil, nil, func(c *logcp.Config) { c.Metrics = logcp.NewMetrics(reg); c.K = 2 })
	const n = 200
	var wg sync.WaitGroup
	resps := make([]core.CommitSegmentResp, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i] = commit(t, s, seg("g"+string(rune('A'+i/26))+string(rune('a'+i%26)), tp("t", int32(i%3))))
		}()
	}
	wg.Wait()
	slots := s.Applied()
	if slots >= n/2 {
		t.Fatalf("%d commands took %d slots: no grouping", n, slots)
	}
	// dense offsets per partition, whatever the grouping
	next := map[core.TopicPartition]core.Offset{}
	for _, r := range resps {
		for _, a := range r.Assigned {
			next[a.TP] += core.Offset(a.Count)
		}
	}
	for p, want := range next {
		if end, _ := s.End(bg, p); end != want {
			t.Fatalf("%v end %d, acked %d", p, end, want)
		}
	}
	if v := metricSum(t, reg, "manicp_log_entries_per_slot"); v < n {
		t.Fatalf("entries_per_slot observed %v commands, want %d", v, n)
	}
}

func metricSum(t testing.TB, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mf, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, f := range mf {
		if f.GetName() == name {
			for _, m := range f.GetMetric() {
				sum += m.GetCounter().GetValue() + m.GetGauge().GetValue() + m.GetHistogram().GetSampleSum()
			}
		}
	}
	return sum
}

func TestAdmissionControl(t *testing.T) {
	log := &slowLog{Log: logcp.NewMemLog(), gate: make(chan struct{})}
	reg := prometheus.NewRegistry()
	s := node(t, "n0", 0, log, nil, nil, func(c *logcp.Config) {
		c.Metrics = logcp.NewMetrics(reg)
		c.K, c.MaxCmdsPerSlot, c.QueueMax = 1, 1, 3
	})
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ { // one in the stuck append, three queued
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.CommitSegment(ctx, seg("q"+string(rune('a'+i)), tp("t", 0)))
		}()
		for deadline := time.Now().Add(2 * time.Second); log.appends.Load() < 1 && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, _, err := s.CommitSegment(bg, seg("over", tp("t", 0))); !errors.Is(err, logcp.ErrOverloaded) || !errors.Is(err, cp.ErrUnavailable) {
		t.Fatalf("a full queue must refuse with a 503-class error: %v", err)
	}
	close(log.gate)
	cancel()
	wg.Wait()
	// a command too large for one slot is refused with ErrTooLarge and never queued
	var big core.CommitSegment
	big.SegmentKey = "big"
	for i := 0; i < 20000; i++ {
		big.Batches = append(big.Batches, core.PendingBatch{TP: tp(strings.Repeat("x", 30)+string(rune('a'+i%26))+strings.Repeat("y", i%50), int32(i)), Records: 1, Loc: core.SegmentRef{Key: strings.Repeat("z", 20) + string(rune(i)), Len: 1}})
	}
	if _, code, err := s.CommitSegment(bg, big); err != nil || code != core.ErrTooLarge {
		t.Fatalf("too large: %v %v", code, err)
	}
	if metricSum(t, reg, "manicp_log_rejected_total") < 2 {
		t.Fatal("rejections not counted")
	}
}

func TestStandbyRefusesCommitsAndServesFreshReads(t *testing.T) {
	log := logcp.NewMemLog()
	active := node(t, "n0", 0, log, nil, nil, nil)
	standby := node(t, "n1", 1, log, nil, nil, nil)
	if !active.IsActive() || standby.IsActive() {
		t.Fatal("rank 0 starts active, others as standbys")
	}
	if _, _, err := standby.CommitSegment(bg, seg("k", tp("t", 0))); !errors.Is(err, logcp.ErrNotActive) || !errors.Is(err, cp.ErrUnavailable) {
		t.Fatalf("standby commit: %v", err)
	}
	if _, err := standby.CreateTopic(bg, "t", 1); !errors.Is(err, logcp.ErrNotActive) {
		t.Fatalf("standby create: %v", err)
	}
	for i := 0; i < 20; i++ { // a read on the standby sees every ack of the active node, at once
		r := commit(t, active, seg("k"+string(rune('a'+i)), tp("t", 0)))
		l, _, err := standby.Lookup(bg, core.Lookup{TP: tp("t", 0), From: r.Assigned[0].Base})
		if err != nil || l.End != r.Assigned[0].Base+core.Offset(r.Assigned[0].Count) {
			t.Fatalf("standby lookup after ack %d: %+v %v", i, l, err)
		}
	}
	srv := httptest.NewServer(standby.Handler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/commit-segment", "application/json", strings.NewReader(`{"segment_key":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" || resp.Header.Get(logcp.RoleHeader) != "standby" {
		t.Fatalf("standby http: %d %v", resp.StatusCode, resp.Header)
	}
	if resp, _ = http.Post(srv.URL+"/v1/describe-topics", "application/json", strings.NewReader(`{}`)); resp.StatusCode != 200 {
		t.Fatalf("describe on a standby: %d", resp.StatusCode)
	}
	if resp, _ = http.Get(srv.URL + "/healthz"); resp.StatusCode != 200 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
}

func TestFailoverNoLossNoDuplicate(t *testing.T) {
	log := logcp.NewMemLog()
	mod := func(c *logcp.Config) { c.TakeoverAfter = 80 * time.Millisecond }
	a := node(t, "n0", 0, log, nil, nil, mod)
	b := node(t, "n1", 1, log, nil, nil, mod)
	acked := map[string]core.CommitSegmentResp{}
	for i := 0; i < 10; i++ {
		k := "pre" + string(rune('a'+i))
		acked[k] = commit(t, a, seg(k, tp("t", 0)))
	}
	a.Stop() // the appender dies
	var late core.CommitSegmentResp
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, code, err := b.CommitSegment(bg, seg("post", tp("t", 0)))
		if err == nil && code == core.OK {
			late = r
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no takeover: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !b.IsActive() {
		t.Fatal("b committed without being active")
	}
	// a retry of an old key (the broker never saw the ack) gets the original range
	if r := commit(t, b, seg("prea", tp("t", 0))); r.Assigned[0] != acked["prea"].Assigned[0] {
		t.Fatalf("replay after failover: %+v vs %+v", r.Assigned, acked["prea"].Assigned)
	}
	end, _ := b.End(bg, tp("t", 0))
	if want := late.Assigned[0].Base + core.Offset(late.Assigned[0].Count); end != want || end != 11*2 {
		t.Fatalf("end %d, want %d (11 commits of 2 records, none lost, none twice)", end, want)
	}
}

func TestSplitBrainTwoAppenders(t *testing.T) {
	log := logcp.NewMemLog()
	mod := func(c *logcp.Config) { c.Rank = 0 } // equal ranks: neither steps down
	a := node(t, "n0", 0, log, nil, nil, mod)
	b := node(t, "n1", 0, log, nil, nil, mod)
	b.Promote()
	var wg sync.WaitGroup
	var mu sync.Mutex
	next := map[core.TopicPartition]core.Offset{}
	for w := 0; w < 4; w++ {
		for _, s := range []*logcp.Service{a, b} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					r := commit(t, s, seg("sb"+s.Name()+itoa2(w, i), tp("t", int32(i%2))))
					mu.Lock()
					for _, x := range r.Assigned {
						next[x.TP] += core.Offset(x.Count)
					}
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	for _, s := range []*logcp.Service{a, b} {
		for p, want := range next {
			if end, err := s.End(bg, p); err != nil || end != want {
				t.Fatalf("%v end %d, acked %d (%v)", p, end, want, err)
			}
		}
	}
	waitApplied(t, a, b)
	if a.Applier().StateHash() != b.Applier().StateHash() {
		t.Fatal("the two appenders disagree")
	}
}

func waitApplied(t testing.TB, ss ...*logcp.Service) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		same := true
		for _, s := range ss[1:] {
			same = same && s.Applied() == ss[0].Applied() && s.Applier().StateHash() == ss[0].Applier().StateHash()
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("nodes did not converge")
		}
	}
}

func TestHoleIsFilledAfterHoleAfter(t *testing.T) {
	log := logcp.NewMemLog()
	reg := prometheus.NewRegistry()
	// another writer took slot 0 and 2 and died before writing slot 1
	log.Append(bg, 0, logcp.Entry{Writer: "dead", ID: "d/1"})
	cmd := logsm.Entry{Kind: logsm.KindCommit, Commit: seg("late", tp("t", 0))}
	log.Append(bg, 2, logcp.Entry{Writer: "dead", ID: "d/2", Cmds: []logsm.Entry{cmd}})
	s := node(t, "n0", 0, log, nil, nil, func(c *logcp.Config) { c.Metrics = logcp.NewMetrics(reg) })
	d, code, err := s.Lookup(bg, core.Lookup{TP: tp("t", 0)})
	if err != nil || code != core.OK || d.End != 2 {
		t.Fatalf("slot 2 must be applied once slot 1 is filled: %+v %v %v", d, code, err)
	}
	if got, _ := log.Read(bg, 1, 1); len(got) != 1 || got[0].N != 1 || len(got[0].E.Cmds) != 0 {
		t.Fatalf("slot 1 should hold a no-op: %+v", got)
	}
	if metricSum(t, reg, "manicp_log_hole_fills_total") != 1 {
		t.Fatal("hole fill not counted")
	}
}

func TestUnsupportedSlotStopsTheNode(t *testing.T) {
	log := logcp.NewMemLog()
	log.Append(bg, 0, logcp.Entry{Writer: "w", ID: "i"})
	log.Put(1, []byte{logcp.Version + 1, 0, 0}) // written by a newer build
	s, err := logcp.New(logcptest.FastConfig("n0", 0, log))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	err = s.Start(ctx)
	var un *logcp.UnsupportedError
	if !errors.As(err, &un) || un.Version != logcp.Version+1 {
		t.Fatalf("start: %v", err)
	}
	if s.Applied() != 1 || s.Blocked() == nil {
		t.Fatalf("applied %d blocked %v", s.Applied(), s.Blocked())
	}
	if _, _, err := s.CommitSegment(bg, seg("k", tp("t", 0))); !errors.Is(err, logcp.ErrBlocked) {
		t.Fatalf("commit on a blocked node: %v", err)
	}
	if _, _, err := s.Lookup(bg, core.Lookup{TP: tp("t", 0)}); !errors.Is(err, logcp.ErrBlocked) {
		t.Fatalf("lookup on a blocked node: %v", err)
	}
	if s.Healthy() == nil {
		t.Fatal("blocked node reports healthy")
	}
	s.Close()
}

func TestRestartFromCheckpointAndTrim(t *testing.T) {
	db := newDB(t)
	objs := logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{})
	reg := prometheus.NewRegistry()
	small := func(c *logcp.Config) { c.WindowSlots, c.TrimStride, c.KeepSlots, c.TrimDelete = 40, 8, 10, true }
	a := node(t, "n0", 0, log, db, objs, func(c *logcp.Config) { small(c); c.Metrics = logcp.NewMetrics(reg) })
	acked := map[string]core.CommitSegmentResp{}
	for i := 0; i < 60; i++ {
		k := "c" + string(rune('A'+i/26)) + string(rune('a'+i%26))
		acked[k] = commit(t, a, seg(k, tp("t", int32(i%4))))
		if i == 29 {
			if slot, err := a.Checkpoint(bg); err != nil || slot == 0 {
				t.Fatalf("checkpoint: %d %v", slot, err)
			}
		}
	}
	if err := a.FlushIndex(bg); err != nil { // the log is trimmed only behind the index watermark
		t.Fatal(err)
	}
	slot, err := a.Checkpoint(bg)
	if err != nil {
		t.Fatal(err)
	}
	if keys, _ := objs.List(bg, "ckpt/"); len(keys) > 2 {
		t.Fatalf("old checkpoints are not pruned: %v", keys)
	}
	if metricSum(t, reg, "manicp_log_trimmed_slots_total") == 0 {
		t.Fatal("nothing trimmed")
	}
	if _, err := log.Read(bg, 0, 1); !errors.Is(err, logcp.ErrTrimmed) {
		t.Fatalf("the start of the log should be tombstones: %v", err)
	}
	// more commits after the checkpoint, then a node that restores and replays the tail
	for i := 0; i < 5; i++ {
		commit(t, a, seg("tail"+string(rune('a'+i)), tp("t", 0)))
	}
	b := node(t, "n1", 1, log, db, objs, small)
	waitApplied(t, a, b)
	if b.Applied() <= slot {
		t.Fatalf("restored node applied %d, checkpoint at %d", b.Applied(), slot)
	}
	// the recent window survived the restore: a replay of a recent key gets its original range
	last := "c" + string(rune('A'+59/26)) + string(rune('a'+59%26))
	if r := commit(t, a, seg(last, tp("t", 3))); r.Assigned[0] != acked[last].Assigned[0] {
		t.Fatalf("replay on the original: %+v vs %+v", r.Assigned, acked[last].Assigned)
	}
	// a node that cannot find its checkpoint and whose log is trimmed refuses to start
	for _, k := range mustList(objs) {
		objs.Delete(bg, k)
	}
	cfg := logcptest.FastConfig("n2", 2, log)
	cfg.DB, cfg.Table, cfg.Objects = db, "cp", objs
	s, _ := logcp.New(cfg)
	if err := s.Start(bg); !errors.Is(err, logcp.ErrNoCheckpoint) {
		t.Fatalf("no checkpoint and a trimmed log: %v", err)
	}
}

func mustList(o *logcptest.Objects) []string {
	k, _ := o.List(bg, "")
	return k
}

func TestCheckpointFallsBackToTheWholeLog(t *testing.T) {
	db := newDB(t)
	objs := logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{})
	a := node(t, "n0", 0, log, db, objs, nil)
	for i := 0; i < 8; i++ {
		commit(t, a, seg("f"+string(rune('a'+i)), tp("t", 0)))
	}
	if _, err := a.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	keys := mustList(objs)
	objs.Put(bg, keys[0], []byte("garbage")) // the object is damaged; the log still starts at slot 0
	b := node(t, "n1", 1, log, db, objs, nil)
	waitApplied(t, a, b)
	if end, _ := b.End(bg, tp("t", 0)); end != 16 {
		t.Fatalf("end %d", end)
	}
}

func TestLaggingNodeStopsInsteadOfWritingIntoTrimmedSlots(t *testing.T) {
	db := newDB(t)
	objs := logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{})
	small := func(c *logcp.Config) { c.WindowSlots, c.TrimStride, c.KeepSlots, c.TrimDelete = 24, 8, 4, true }
	a := node(t, "n0", 0, log, db, objs, small)
	// b is a node that stopped reading early: a stale view of the log
	b := node(t, "n1", 1, log, db, objs, func(c *logcp.Config) { small(c); c.TailPoll = time.Hour })
	for i := 0; i < 50; i++ {
		commit(t, a, seg("l"+string(rune('a'+i%26))+string(rune('a'+i/26)), tp("t", 0)))
	}
	if err := a.FlushIndex(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	b.Promote()
	_, _, err := b.CommitSegment(bg, seg("stale", tp("t", 0)))
	if err == nil || b.Blocked() == nil {
		for deadline := time.Now().Add(3 * time.Second); b.Blocked() == nil && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		}
	}
	if !errors.Is(b.Blocked(), logcp.ErrTrimmed) {
		t.Fatalf("a node behind the trim point must stop: %v (commit err %v)", b.Blocked(), err)
	}
	// and the log of the others is untouched: no entry of b is in it
	got, _ := log.Read(bg, 40, 100)
	for _, s := range got {
		if s.E.Writer == "n1" && len(s.E.Cmds) > 0 {
			t.Fatalf("n1 wrote a command into slot %d", s.N)
		}
	}
}

func TestMetricsAreExported(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := node(t, "n0", 0, logcp.NewMemLog(), nil, nil, func(c *logcp.Config) { c.Metrics = logcp.NewMetrics(reg) })
	commit(t, s, seg("m", tp("t", 0)))
	s.Lookup(bg, core.Lookup{TP: tp("t", 0)})
	want := []string{"slot_attempts_total", "slots_total", "append_seconds", "entries_per_slot", "entry_bytes", "applied_slot", "apply_lag_slots",
		"apply_seconds", "active", "role_changes_total", "tail_checks_total", "fresh_reads_total", "fresh_wait_seconds", "queue_wait_seconds", "tail_reads_total"}
	mf, _ := reg.Gather()
	have := map[string]bool{}
	for _, f := range mf {
		have[f.GetName()] = true
	}
	for _, n := range want {
		if !have["manicp_log_"+n] {
			t.Errorf("manicp_log_%s missing", n)
		}
	}
	if metricSum(t, reg, "manicp_log_fresh_reads_total") < 1 || metricSum(t, reg, "manicp_log_active") != 1 {
		t.Fatal("values")
	}
}

func TestFreshReadsShareTailChecks(t *testing.T) {
	log := &countTail{Log: logcp.NewMemLog()}
	reg := prometheus.NewRegistry()
	s := node(t, "n0", 0, log, nil, nil, func(c *logcp.Config) { c.Metrics = logcp.NewMetrics(reg); c.TailBatch = 20 * time.Millisecond })
	commit(t, s, seg("k", tp("t", 0)))
	before := log.tails.Load()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l, _, err := s.Lookup(bg, core.Lookup{TP: tp("t", 0)}); err != nil || l.End != 2 {
				t.Errorf("lookup %+v %v", l, err)
			}
		}()
	}
	wg.Wait()
	if n := log.tails.Load() - before; n > 5 {
		t.Fatalf("200 concurrent reads made %d tail checks", n)
	}
}

type countTail struct {
	logcp.Log
	tails atomic.Int64
}

func (c *countTail) CheckTail(ctx context.Context) (int64, error) {
	c.tails.Add(1)
	return c.Log.CheckTail(ctx)
}

func TestRealGoroutinesRace(t *testing.T) {
	db := newDB(t)
	db.Latency = func(call string) time.Duration {
		if call == "PutItem" {
			return 500 * time.Microsecond
		}
		return 200 * time.Microsecond
	}
	var n atomic.Int64
	var failing atomic.Bool
	failing.Store(true)
	db.Fail = func(call string) error { // every 40th call fails: an unknown outcome or a failed read
		if failing.Load() && n.Add(1)%40 == 0 && (call == "PutItem" || call == "Query") {
			return errors.New("injected")
		}
		return nil
	}
	objs := logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{})
	mod := func(c *logcp.Config) {
		c.CkptEvery, c.CkptMinSlots, c.IndexEvery, c.TakeoverAfter = 30*time.Millisecond, 3, 20*time.Millisecond, 200*time.Millisecond
		c.WindowSlots, c.TrimStride, c.KeepSlots, c.TrimDelete = 4096, 64, 256, true
	}
	nodes := []*logcp.Service{node(t, "n0", 0, log, db, objs, mod), node(t, "n1", 1, log, db, objs, mod), node(t, "n2", 2, log, db, objs, mod)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	ends := map[core.TopicPartition]core.Offset{}
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			at := 0
			for i := 0; i < 40; i++ {
				cmd := seg("r"+string(rune('a'+c))+string(rune('A'+i)), tp("t", int32(i%3)), tp("u", int32(c%2)))
				for {
					r, code, err := nodes[at].CommitSegment(bg, cmd)
					if err != nil {
						at = (at + 1) % 3
						time.Sleep(time.Millisecond)
						continue
					}
					if code != core.OK {
						t.Errorf("code %v", code)
					}
					mu.Lock()
					for _, a := range r.Assigned {
						ends[a.TP] += core.Offset(a.Count)
					}
					mu.Unlock()
					_, _, _ = nodes[(at+1)%3].Lookup(bg, core.Lookup{TP: r.Assigned[0].TP, From: r.Assigned[0].Base})
					break
				}
			}
		}()
	}
	wg.Wait()
	failing.Store(false)
	waitApplied(t, nodes...)
	for _, s := range nodes {
		for p, want := range ends {
			if end, err := s.End(bg, p); err != nil || end != want {
				t.Fatalf("%v end %d acked %d (%v)", p, end, want, err)
			}
		}
	}
	for _, s := range nodes {
		if err := s.FlushIndex(bg); err != nil {
			t.Fatal(err)
		}
	}
}

// guard: the applier type is part of the API of Service.Applier.
var _ *applier.Applier

func itoa2(w, i int) string { return string(rune('a'+w)) + string(rune('A'+i)) }
