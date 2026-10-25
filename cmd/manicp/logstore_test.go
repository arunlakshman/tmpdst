package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/core"
	"manifester/cp/cpclient"
	"manifester/cp/cpobs"
	"manifester/cp/fakedb"
	"manifester/cp/logcp/logcptest"
	"manifester/sys"
)

// syncBuf is a log sink safe for the goroutines of several run calls.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// logNode is one run of manicp -store dynamolog.
type logNode struct {
	addr   string
	cancel context.CancelFunc
	done   chan int
	logs   *syncBuf
}

func fakeDeps(db *fakedb.Fake, objs sys.ObjectStore) deps {
	return deps{
		db:      func(context.Context, cpFlags, *cpobs.Metrics) (logDB, error) { return db, nil },
		objects: func(context.Context, cpFlags) (sys.ObjectStore, error) { return objs, nil },
	}
}

func startLogNode(t *testing.T, d deps, name string, rank int, extra ...string) *logNode {
	t.Helper()
	n := &logNode{addr: freeAddr(t), done: make(chan int, 1), logs: &syncBuf{}}
	args := append([]string{"-store", kindLog, "-addr", n.addr, "-metrics-addr", "", "-node-name", name, "-rank", fmt.Sprint(rank),
		"-ckpt-bucket", "b", "-takeover-after", "600ms", "-heartbeat", "50ms", "-hole-after", "30ms", "-slot-rate", "5000",
		"-pebble-dir", filepath.Join(t.TempDir(), "pebble"), "-final-checkpoint=false"}, extra...)
	var ctx context.Context
	ctx, n.cancel = context.WithCancel(context.Background())
	t.Cleanup(n.cancel)
	go func() { n.done <- runWith(ctx, args, n.logs, d) }()
	waitHealthy(t, n)
	return n
}

func waitHealthy(t *testing.T, n *logNode) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		select {
		case code := <-n.done:
			t.Fatalf("exited %d before it was healthy\n%s", code, n.logs.String())
		default:
		}
		if resp, err := http.Get("http://" + n.addr + "/healthz"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
	}
	t.Fatalf("never healthy\n%s", n.logs.String())
}

func (n *logNode) stop(t *testing.T) int {
	t.Helper()
	n.cancel()
	select {
	case code := <-n.done:
		return code
	case <-time.After(20 * time.Second):
		t.Fatal("did not stop\n" + n.logs.String())
	}
	return -1
}

func role(t *testing.T, n *logNode) string {
	t.Helper()
	resp, err := http.Get("http://" + n.addr + "/healthz")
	if err != nil {
		return "down"
	}
	_ = resp.Body.Close()
	return resp.Header.Get("X-CP-Role")
}

func newFake(t *testing.T) *fakedb.Fake {
	t.Helper()
	db := fakedb.New()
	if _, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{TableName: aws.String("manifester_cp")}); err != nil {
		t.Fatal(err)
	}
	return db
}

// acks records what the control plane acknowledged.
type acks struct {
	mu   sync.Mutex
	rngs map[string][]core.BatchRange // segment key -> assigned
}

func commitLoop(ctx context.Context, c *cpclient.Client, w int, a *acks, n *atomic.Int64, stop <-chan struct{}) error {
	for i := 0; ; i++ {
		select {
		case <-stop:
			return nil
		default:
		}
		key := fmt.Sprintf("seg/w%d-%06d", w, i)
		op := core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{
			{TP: core.TopicPartition{Topic: "t", Partition: int32(i % 3)}, Records: 2 + int64(w), Loc: core.SegmentRef{Key: key, Off: 0, Len: 10}}}}
		resp, err := commitRetrying(ctx, c, op)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		a.mu.Lock()
		a.rngs[key] = resp.(core.CommitSegmentResp).Assigned
		a.mu.Unlock()
		n.Add(1)
	}
}

// commitRetrying is the broker's rule: the same segment key is retried with a
// short backoff on any transport failure or 503 until a deadline (the real
// deadline is 120 s; a takeover takes seconds).
func commitRetrying(ctx context.Context, c *cpclient.Client, op core.CommitSegment) (core.CPResp, error) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		resp, err := c.Call(cctx, op)
		var ce *cpclient.CallError
		if err == nil || !errors.As(err, &ce) || cctx.Err() != nil {
			return resp, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// checkPartitions reads every partition back through c and requires exactly the
// acked ranges: contiguous offsets from 0, no overlap, none lost or twice.
func checkPartitions(t *testing.T, c *cpclient.Client, a *acks) {
	t.Helper()
	type run struct{ base, cnt int64 }
	want := map[core.TopicPartition]map[run]string{}
	for key, rs := range a.rngs {
		for _, r := range rs {
			if want[r.TP] == nil {
				want[r.TP] = map[run]string{}
			}
			rn := run{int64(r.Base), r.Count}
			if prev, dup := want[r.TP][rn]; dup {
				t.Fatalf("%v: %s and %s were both acked offsets %v", r.TP, prev, key, rn)
			}
			want[r.TP][rn] = key
		}
	}
	for tp, runs := range want {
		var next int64
		for next < int64(len(runs))*100 {
			resp, err := c.Call(context.Background(), core.Lookup{TP: tp, From: core.Offset(next), MaxBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			lr := resp.(core.LookupResp)
			for _, r := range lr.Ranges {
				if int64(r.Base) != next {
					t.Fatalf("%v: gap or overlap, range at %d, expected %d", tp, r.Base, next)
				}
				if _, ok := runs[run{int64(r.Base), r.Count}]; !ok {
					t.Fatalf("%v: stored range [%d,+%d) was never acked (duplicated or invented)", tp, r.Base, r.Count)
				}
				delete(runs, run{int64(r.Base), r.Count})
				next += r.Count
			}
			if int64(lr.End) == next {
				break
			}
		}
		if len(runs) != 0 {
			t.Fatalf("%v: %d acked ranges are lost: %v", tp, len(runs), runs)
		}
	}
}

func TestLogStoreThreeNodesTakeover(t *testing.T) {
	db, objs := newFake(t), logcptest.NewObjects()
	d := fakeDeps(db, objs)
	n0 := startLogNode(t, d, "n0", 0)
	n1 := startLogNode(t, d, "n1", 1)
	n2 := startLogNode(t, d, "n2", 2)
	if r := []string{role(t, n0), role(t, n1), role(t, n2)}; r[0] != "active" || r[1] != "standby" || r[2] != "standby" {
		t.Fatalf("roles %v", r)
	}
	// Brokers' URL list: preferred node first.
	c := cpclient.New("http://"+n0.addr+",http://"+n1.addr+",http://"+n2.addr, nil)
	ctx := context.Background()
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	a := &acks{rngs: map[string][]core.BatchRange{}}
	var n atomic.Int64
	stop := make(chan struct{})
	errs := make(chan error, 3)
	for w := 0; w < 3; w++ {
		go func() { errs <- commitLoop(ctx, c, w, a, &n, stop) }()
	}
	waitCount(t, &n, 100)
	before := n.Load()
	n0.stop(t) // the appender dies mid-stream
	waitCount(t, &n, before+150)
	// Either standby may win the takeover race; commits only succeed through an active node.
	if r1, r2 := role(t, n1), role(t, n2); r1 != "active" && r2 != "active" {
		t.Fatalf("no standby took over: roles %q %q\n%s", r1, r2, n1.logs.String())
	}
	close(stop)
	for w := 0; w < 3; w++ {
		if err := <-errs; err != nil {
			t.Fatalf("commit failed across the takeover: %v", err)
		}
	}
	checkPartitions(t, cpclient.New("http://"+n2.addr, nil), a)
	checkPartitions(t, cpclient.New("http://"+n1.addr, nil), a)
	if n1.stop(t) != 0 || n2.stop(t) != 0 {
		t.Fatal("nonzero exit")
	}
}

func waitCount(t *testing.T, n *atomic.Int64, want int64) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if n.Load() >= want {
			return
		}
	}
	t.Fatalf("only %d acked commits, want %d", n.Load(), want)
}

func TestLogStoreRestartFromFinalCheckpoint(t *testing.T) {
	db, objs := newFake(t), logcptest.NewObjects()
	d := fakeDeps(db, objs)
	n := startLogNode(t, d, "n0", 0, "-final-checkpoint=true", "-ckpt-every", "-1s")
	c := cpclient.New("http://"+n.addr, nil)
	if err := c.CreateTopic(context.Background(), "t", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("seg/%d", i)
		_, err := c.Call(context.Background(), core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{
			{TP: core.TopicPartition{Topic: "t"}, Records: 3, Loc: core.SegmentRef{Key: key, Len: 9}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if code := n.stop(t); code != 0 || !strings.Contains(n.logs.String(), "log_final_ckpt") {
		t.Fatalf("code %d, no final checkpoint\n%s", code, n.logs.String())
	}
	if keys, _ := objs.List(context.Background(), "ckpt/"); len(keys) == 0 {
		t.Fatalf("no checkpoint object: %v", keys)
	}
	n2 := startLogNode(t, d, "n0", 0)
	resp, err := cpclient.New("http://"+n2.addr, nil).Call(context.Background(), core.Lookup{TP: core.TopicPartition{Topic: "t"}, MaxBytes: 1 << 20})
	if err != nil || resp.(core.LookupResp).End != 15 {
		t.Fatalf("after restart: %v %+v", err, resp)
	}
	n2.stop(t)
}

func TestLogStoreStartupErrors(t *testing.T) {
	foreign := t.TempDir()
	_ = os.WriteFile(filepath.Join(foreign, "precious.txt"), []byte("x"), 0o644)
	cases := map[string][]string{
		"foreign pebble dir": {"-pebble-dir", foreign},
		"table is missing":   {"-table", "missing"},
	}
	for name, extra := range cases {
		d := fakeDeps(newFake(t), logcptest.NewObjects())
		args := append([]string{"-store", kindLog, "-addr", freeAddr(t), "-metrics-addr", "", "-takeover-after", "1s", "-start-timeout", "500ms"}, extra...)
		var logs syncBuf
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if code := runWith(ctx, args, &logs, d); code != 1 {
			t.Errorf("%s: code %d\n%s", name, code, logs.String())
		}
		cancel()
	}
	if _, err := os.Stat(filepath.Join(foreign, "precious.txt")); err != nil {
		t.Fatal("a foreign directory was wiped")
	}
}

func TestLogStoreWithoutBucketWarns(t *testing.T) {
	d := fakeDeps(newFake(t), logcptest.NewObjects())
	n := startLogNode(t, d, "n0", 0, "-ckpt-bucket", "")
	if code := n.stop(t); code != 0 || !strings.Contains(n.logs.String(), "log_no_checkpoint") {
		t.Fatalf("code %d\n%s", code, n.logs.String())
	}
}

func TestLogFlagsDefaults(t *testing.T) {
	f, _, err := parseFlags([]string{"-store", kindLog}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	l := f.log
	if l.takeoverAfter != 5*time.Second || l.k != 4 || l.slotRate != 200 || l.holeAfter != 250*time.Millisecond ||
		l.ckptEvery != 5*time.Minute || l.windowSlots != 131072 ||
		l.heartbeat != time.Second || l.tableName(f.prefix) != "manifester_cp" || nodeName(l) == "" {
		t.Fatalf("defaults: %+v", l)
	}
	f.log.table = "x"
	if f.log.tableName(f.prefix) != "x" || logConfig(f, "n", nil).WindowSlots != 131072 {
		t.Fatal("table override or config")
	}
}

func TestRealDepsBuild(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	f, _, _ := parseFlags([]string{"-store", kindLog, "-dynamo-endpoint", "http://127.0.0.1:1", "-ckpt-bucket", "b", "-s3-endpoint", "http://127.0.0.1:1"}, &bytes.Buffer{})
	d := realDeps()
	if db, err := d.db(context.Background(), f, cpobs.New()); err != nil || db == nil {
		t.Fatal(err)
	}
	if o, err := d.objects(context.Background(), f); err != nil || o == nil {
		t.Fatal(err)
	}
}

func TestPlainStoresStillServe(t *testing.T) {
	// The rollback kinds go through the same handler path and are unchanged.
	f, _, _ := parseFlags([]string{"-store", "mem"}, &bytes.Buffer{})
	h, closeStore, err := openHandler(context.Background(), realDeps(), f, cpobs.New(), nil)
	if err != nil || h == nil {
		t.Fatal(err)
	}
	closeStore()
	f.kind = "nope"
	if _, _, err := openHandler(context.Background(), realDeps(), f, cpobs.New(), nil); err == nil {
		t.Fatal("unknown store accepted")
	}
}

var _ = net.Dial
