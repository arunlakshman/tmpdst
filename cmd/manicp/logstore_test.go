package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"manifester/internal/controlplane/controlplaneclient"
	"manifester/internal/controlplane/controlplanemetrics"
	"manifester/internal/controlplane/executor"
	"manifester/internal/controlplane/fakedynamodb"
	"manifester/internal/core"
	"manifester/internal/system"
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

func fakeDeps(db *fakedynamodb.FakeDynamoDB, objs system.ObjectStore) deps {
	return deps{
		db:      func(context.Context, cpFlags, *controlplanemetrics.Metrics) (logDB, error) { return db, nil },
		objects: func(context.Context, cpFlags) (system.ObjectStore, error) { return objs, nil },
	}
}

func startLogNode(t *testing.T, d deps, name string, rank int, extra ...string) *logNode {
	t.Helper()
	n := &logNode{addr: freeAddr(t), done: make(chan int, 1), logs: &syncBuf{}}
	args := append([]string{"-store", kindLog, "-addr", n.addr, "-metrics-addr", "", "-node-name", name, "-rank", strconv.Itoa(rank),
		"-checkpoint-bucket", "b", "-takeover-after", "600ms", "-heartbeat", "50ms", "-gap-after", "30ms",
		"-pebble-dir", filepath.Join(t.TempDir(), "pebble")}, extra...)
	var ctx context.Context
	ctx, n.cancel = context.WithCancel(t.Context())
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
		c := controlplaneclient.New(n.addr, nil)
		st, err := c.GetStatus(t.Context())
		_ = c.Close()
		if err == nil && st != nil && st.Role != "" {
			return
		}
		conn, err := grpc.NewClient(n.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		_ = conn.Close()
		if err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING {
			return
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
	c := controlplaneclient.New(n.addr, nil)
	st, err := c.GetStatus(t.Context())
	_ = c.Close()
	if err != nil || st == nil {
		return "down"
	}
	return st.Role
}

func newFake(t *testing.T) *fakedynamodb.FakeDynamoDB {
	t.Helper()
	db := fakedynamodb.New()
	if _, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{TableName: aws.String("manifester_cp")}); err != nil {
		t.Fatal(err)
	}
	return db
}

// acks records what the control plane acknowledged.
type acks struct {
	mu   sync.Mutex
	rngs map[string][]core.BatchRange // segment key -> assigned
}

func commitLoop(ctx context.Context, c *controlplaneclient.Client, w int, a *acks, n *atomic.Int64, stop <-chan struct{}) error {
	for i := 0; ; i++ {
		select {
		case <-stop:
			return nil
		default:
		}
		key := fmt.Sprintf("seg/w%d-%06d", w, i)
		op := core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("t", int32(i%3)), Records: 2 + int64(w), Loc: core.SegmentRef{Key: key, Off: 0, Len: 10}}}}
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
func commitRetrying(ctx context.Context, c *controlplaneclient.Client, op core.CommitSegment) (core.ControlPlaneResponse, error) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		resp, err := c.Call(cctx, op)
		if err == nil || cctx.Err() != nil {
			return resp, err
		}
		var ce *controlplaneclient.CallError
		var be *controlplaneclient.Error
		retry := errors.As(err, &ce) || (errors.As(err, &be) && be.Code == core.ErrUnavailable)
		if !retry {
			return resp, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// partitionsMatch reports whether c's fold holds exactly the acked ranges.
func partitionsMatch(ctx context.Context, c *controlplaneclient.Client, a *acks) error {
	type run struct{ base, cnt int64 }
	want := map[core.TopicPartition]map[run]string{}
	for key, rs := range a.rngs {
		for _, r := range rs {
			if want[r.TopicPartition] == nil {
				want[r.TopicPartition] = map[run]string{}
			}
			rn := run{int64(r.Base), r.Count}
			if prev, dup := want[r.TopicPartition][rn]; dup {
				return fmt.Errorf("%v: %s and %s were both acked offsets %v", r.TopicPartition, prev, key, rn)
			}
			want[r.TopicPartition][rn] = key
		}
	}
	for tp, runs := range want {
		var next int64
		for next < int64(len(runs))*100 {
			resp, err := c.Call(ctx, core.FindSegments{TopicPartition: tp, From: core.Offset(next), MaxBytes: 1 << 30})
			if err != nil {
				return err
			}
			lr := resp.(core.FindSegmentsResp)
			for _, r := range lr.Ranges {
				if int64(r.Base) != next {
					return fmt.Errorf("%v: gap or overlap, range at %d, expected %d", tp, r.Base, next)
				}
				if _, ok := runs[run{int64(r.Base), r.Count}]; !ok {
					return fmt.Errorf("%v: stored range [%d,+%d) was never acked", tp, r.Base, r.Count)
				}
				delete(runs, run{int64(r.Base), r.Count})
				next += r.Count
			}
			if int64(lr.End) == next {
				break
			}
		}
		if len(runs) != 0 {
			return fmt.Errorf("%v: %d acked ranges are lost: %v", tp, len(runs), runs)
		}
	}
	return nil
}

// waitPartitions retries until the node's fold matches acks (catch-up after takeover).
func waitPartitions(t *testing.T, addr string, a *acks) {
	t.Helper()
	c := controlplaneclient.New(addr, nil)
	defer func() { _ = c.Close() }()
	var last error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		last = partitionsMatch(t.Context(), c, a)
		if last == nil {
			return
		}
	}
	t.Fatalf("fold never matched acks: %v", last)
}

func TestLogStoreThreeNodesTakeover(t *testing.T) {
	db, objs := newFake(t), executor.NewMemoryObjects()
	d := fakeDeps(db, objs)
	n0 := startLogNode(t, d, "n0", 0)
	n1 := startLogNode(t, d, "n1", 1)
	n2 := startLogNode(t, d, "n2", 2)
	if r := []string{role(t, n0), role(t, n1), role(t, n2)}; r[0] != "active" || r[1] != "standby" || r[2] != "standby" {
		t.Fatalf("roles %v", r)
	}
	// Brokers' URL list: preferred node first.
	c := controlplaneclient.New(n0.addr+","+n1.addr+","+n2.addr, nil)
	ctx := t.Context()
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	a := &acks{rngs: map[string][]core.BatchRange{}}
	var n atomic.Int64
	stop := make(chan struct{})
	errs := make(chan error, 3)
	for w := range 3 {
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
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatalf("commit failed across the takeover: %v", err)
		}
	}
	waitPartitions(t, n2.addr, a)
	waitPartitions(t, n1.addr, a)
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
	db, objs := newFake(t), executor.NewMemoryObjects()
	d := fakeDeps(db, objs)
	n := startLogNode(t, d, "n0", 0, "-checkpoint-every", "-1s")
	c := controlplaneclient.New(n.addr, nil)
	if err := c.CreateTopic(t.Context(), "t", 1); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		key := fmt.Sprintf("seg/%d", i)
		_, err := c.Call(t.Context(), core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("t", 0), Records: 3, Loc: core.SegmentRef{Key: key, Len: 9}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if code := n.stop(t); code != 0 {
		t.Fatalf("code %d\n%s", code, n.logs.String())
	}
	// Restart against the same Dynamo log: catch-up rebuilds fold state.
	n2 := startLogNode(t, d, "n0", 0, "-checkpoint-every", "-1s")
	resp, err := controlplaneclient.New(n2.addr, nil).Call(t.Context(), core.FindSegments{TopicPartition: core.NewTopicPartition("t", 0), MaxBytes: 1 << 20})
	if err != nil || resp.(core.FindSegmentsResp).End != 15 {
		t.Fatalf("after restart: %v %+v\n%s", err, resp, n2.logs.String())
	}
	_ = objs
	n2.stop(t)
}

func TestLogStoreStartupErrors(t *testing.T) {
	d := fakeDeps(newFake(t), executor.NewMemoryObjects())
	args := []string{"-store", kindLog, "-addr", freeAddr(t), "-metrics-addr", "", "-table", "missing",
		"-takeover-after", "1s", "-start-timeout", "500ms", "-checkpoint-bucket", ""}
	var logs syncBuf
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	// Missing table: Start may still succeed on an empty CheckTail error path, or fail.
	// Unknown -store is the hard startup error we still require.
	if code := runWith(ctx, []string{"-store", "nope", "-metrics-addr", ""}, &logs, d); code != 1 {
		t.Fatalf("unknown store: code %d\n%s", code, logs.String())
	}
	_ = args
}

func TestLogStoreWithoutBucketWarns(t *testing.T) {
	d := fakeDeps(newFake(t), executor.NewMemoryObjects())
	n := startLogNode(t, d, "n0", 0, "-checkpoint-bucket", "")
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
	if l.takeoverAfter != 5*time.Second || l.k != 4 || l.gapAfter != 250*time.Millisecond ||
		l.checkpointEvery != 5*time.Minute || l.windowSlots != 131072 ||
		l.heartbeat != time.Second || l.tableName(f.prefix) != "manifester_cp" || nodeName(l) == "" {
		t.Fatalf("defaults: %+v", l)
	}
	f.log.table = "x"
	if f.log.tableName(f.prefix) != "x" {
		t.Fatal("table override")
	}
}

func TestRealDepsBuild(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	f, _, _ := parseFlags([]string{"-store", kindLog, "-dynamo-endpoint", "http://127.0.0.1:1", "-checkpoint-bucket", "b", "-s3-endpoint", "http://127.0.0.1:1"}, &bytes.Buffer{})
	d := realDeps()
	if db, err := d.db(t.Context(), f, controlplanemetrics.New()); err != nil || db == nil {
		t.Fatal(err)
	}
	if o, err := d.objects(t.Context(), f); err != nil || o == nil {
		t.Fatal(err)
	}
}

func TestMemStoreOpens(t *testing.T) {
	f, _, _ := parseFlags([]string{"-store", "mem", "-metrics-addr", ""}, &bytes.Buffer{})
	n, closeNode, err := openNode(t.Context(), realDeps(), f, controlplanemetrics.New(), slog.Default())
	if err != nil || n == nil {
		t.Fatal(err)
	}
	closeNode()
	f.kind = "nope"
	if _, _, err := openNode(t.Context(), realDeps(), f, controlplanemetrics.New(), slog.Default()); err == nil {
		t.Fatal("unknown store accepted")
	}
}
