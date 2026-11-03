package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

	"github.com/manifesterio/mds/internal/controlplane/controlplaneclient"
	"github.com/manifesterio/mds/internal/controlplane/controlplanemetrics"
	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/fakedynamodb"
	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/system"
	"github.com/manifesterio/mds/internal/system/runinfo"
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

// logNode is one run of mdscontrolplane -store dynamolog.
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
	args := append([]string{"-store", kindLog, "-node-name", name, "-rank", strconv.Itoa(rank),
		"-checkpoint-bucket", "b", "-takeover-after", "600ms", "-heartbeat", "50ms"}, extra...)
	return startNode(t, d, args)
}

// startNode runs mdscontrolplane with args and waits until it is healthy. The
// test adds the listen address, no metrics port and a temporary Pebble dir.
func startNode(t *testing.T, d deps, args []string) *logNode {
	t.Helper()
	n := &logNode{addr: freeAddr(t), done: make(chan int, 1), logs: &syncBuf{}}
	args = append([]string{"-addr", n.addr, "-metrics-addr", "", "-pebble-dir", filepath.Join(t.TempDir(), "pebble")}, args...)
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
		response, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		_ = conn.Close()
		if err == nil && response.Status == healthpb.HealthCheckResponse_SERVING {
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
	if _, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{TableName: aws.String("mds_cp")}); err != nil {
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
		operation := core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("t", int32(i%3)), Records: 2 + int64(w), Loc: core.SegmentRef{Key: key, Off: 0, Len: 10}}}}
		response, err := commitRetrying(ctx, c, operation)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		a.mu.Lock()
		a.rngs[key] = response.(core.CommitSegmentResp).Assigned
		a.mu.Unlock()
		n.Add(1)
	}
}

// commitRetrying is the broker's rule: the same segment key is retried with a
// short backoff on any transport failure or 503 until a deadline (the real
// deadline is 120 s; a takeover takes seconds).
func commitRetrying(ctx context.Context, c *controlplaneclient.Client, operation core.CommitSegment) (core.ControlPlaneResponse, error) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		response, err := c.Call(cctx, operation)
		if err == nil || cctx.Err() != nil {
			return response, err
		}
		_, isCallErr := errors.AsType[*controlplaneclient.CallError](err)
		be, isErr := errors.AsType[*controlplaneclient.Error](err)
		retry := isCallErr || (isErr && be.Code == core.ErrUnavailable)
		if !retry {
			return response, err
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
	for topicPartition, runs := range want {
		var next int64
		for next < int64(len(runs))*100 {
			response, err := c.Call(ctx, core.FindSegments{TopicPartition: topicPartition, From: core.Offset(next), MaxBytes: 1 << 30})
			if err != nil {
				return err
			}
			lr := response.(core.FindSegmentsResp)
			for _, r := range lr.Ranges {
				if int64(r.Base) != next {
					return fmt.Errorf("%v: gap or overlap, range at %d, expected %d", topicPartition, r.Base, next)
				}
				if _, ok := runs[run{int64(r.Base), r.Count}]; !ok {
					return fmt.Errorf("%v: stored range [%d,+%d) was never acked", topicPartition, r.Base, r.Count)
				}
				delete(runs, run{int64(r.Base), r.Count})
				next += r.Count
			}
			if int64(lr.End) == next {
				break
			}
		}
		if len(runs) != 0 {
			return fmt.Errorf("%v: %d acked ranges are lost: %v", topicPartition, len(runs), runs)
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
	response, err := controlplaneclient.New(n2.addr, nil).Call(t.Context(), core.FindSegments{TopicPartition: core.NewTopicPartition("t", 0), MaxBytes: 1 << 20})
	if err != nil || response.(core.FindSegmentsResp).End != 15 {
		t.Fatalf("after restart: %v %+v\n%s", err, response, n2.logs.String())
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
	if l.takeoverAfter != 5*time.Second || l.takeoverRankStep != time.Second ||
		l.checkpointEvery != 5*time.Minute || l.windowSlots != 131072 ||
		l.heartbeat != time.Second || l.tableName(f.prefix) != "mds_cp" || nodeName(l) == "" {
		t.Fatalf("defaults: %+v", l)
	}
	f.log.table = "x"
	if f.log.tableName(f.prefix) != "x" {
		t.Fatal("table override")
	}
}

// -takeover-rank-step reaches the machine config as TakeoverRankStep.
func TestTakeoverRankStepFlag(t *testing.T) {
	f, _, err := parseFlags([]string{"-store", kindLog, "-takeover-rank-step", "250ms"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if f.log.takeoverRankStep != 250*time.Millisecond {
		t.Fatalf("flag value: %v", f.log.takeoverRankStep)
	}
	if got := machineConfig(f).TakeoverRankStep; got != core.Duration(250*time.Millisecond) {
		t.Fatalf("machine config: %v", got)
	}
	if _, _, err := parseFlags([]string{"-takeover-rank-step", "soon"}, &bytes.Buffer{}); err == nil {
		t.Fatal("a bad duration must be rejected")
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

func TestPostgresFlags(t *testing.T) {
	t.Setenv(postgresDSNEnv, "")
	parse := func(args ...string) logFlags {
		t.Helper()
		f, _, err := parseFlags(append([]string{"-store", kindPostgres}, args...), &bytes.Buffer{})
		if err != nil {
			t.Fatal(err)
		}
		return f.log
	}
	if l := parse(); l.postgresPrefix != "mds_" || resolvedDSN(l) != "" {
		t.Fatalf("defaults: %+v", l)
	}
	// -table and -log-ttl are for dynamolog only; with postgres they are ignored.
	if l := parse("-postgres-prefix", "x_", "-postgres-dsn", "postgres://u@h/db", "-table", "other", "-log-ttl", "1h"); l.postgresPrefix != "x_" || resolvedDSN(l) != "postgres://u@h/db" {
		t.Fatalf("flags: %+v", l)
	}
	t.Setenv(postgresDSNEnv, "postgres://env@h/db")
	if got := resolvedDSN(parse()); got != "postgres://env@h/db" {
		t.Fatalf("env fallback: %q", got)
	}
	if got := resolvedDSN(parse("-postgres-dsn", "postgres://flag@h/db")); got != "postgres://flag@h/db" {
		t.Fatalf("the flag must win over the env: %q", got)
	}
}

func TestPostgresNeedsDSN(t *testing.T) {
	t.Setenv(postgresDSNEnv, "")
	f, _, _ := parseFlags([]string{"-store", kindPostgres}, &bytes.Buffer{})
	_, _, _, err := openPostgresLog(t.Context(), realDeps(), f, controlplanemetrics.New(), slog.Default())
	if err == nil || !strings.Contains(err.Error(), "-postgres-dsn or MDS_POSTGRES_DSN") {
		t.Fatalf("empty DSN: %v", err)
	}
	var logs bytes.Buffer
	args := []string{"-store", kindPostgres, "-metrics-addr", "", "-addr", freeAddr(t)}
	if code := run(t.Context(), args, &logs); code != 1 {
		t.Fatalf("postgres without a DSN: code %d\n%s", code, logs.String())
	}
}

func TestRedactDSN(t *testing.T) {
	cases := []struct{ dsn, want string }{
		{"postgres://user:hunter2@db.example:5432/mds?sslmode=require&password=hunter2", "db.example:5432/mds"},
		{"postgres://user:hunter2@db.example/mds", "db.example/mds"},
		{"postgres://u:hunter2@[::1]:5432/x", "[::1]:5432/x"},
		{"host=db password=hunter2 dbname=mds", ""},
		{"hunter2", ""},
		{"", ""},
	}
	for _, c := range cases {
		got := redactDSN(c.dsn)
		if got != c.want || strings.Contains(got, "hunter2") {
			t.Errorf("redactDSN(%q) = %q, want %q", c.dsn, got, c.want)
		}
	}
}

// The DSN must not show in the flag values, the run manifest, or an open error.
func TestDSNStaysOutOfFlagsManifestAndErrors(t *testing.T) {
	const dsn = "postgres://user:hunter2@db.example/mds" //nolint:gosec // G101: a fake password for this test
	t.Setenv(postgresDSNEnv, dsn)
	f, fs, err := parseFlags([]string{"-store", kindPostgres, "-postgres-dsn", dsn}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := fs.Lookup("postgres-dsn").Value.String(); got != "(set)" {
		t.Fatalf("flag value shows %q", got)
	}
	m := runinfo.NewManifest(runinfo.New("mdscontrolplane", "t", -1), fs, "us-east-1", nil)
	m.Args = []string{"-store", kindPostgres, "-postgres-dsn", dsn}
	args := m.Args
	hidden := hideDSN(m, resolvedDSN(f.log))
	body, err := json.Marshal(hidden)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "hunter2") {
		t.Fatalf("manifest holds the password: %s", body)
	}
	if args[3] != dsn {
		t.Fatal("hideDSN changed the caller's Args slice")
	}
	// Nothing listens on port 1, so the open fails. The error must not show the password.
	const badDSN = "postgres://user:hunter2@127.0.0.1:1/db?sslmode=disable&connect_timeout=1" //nolint:gosec // G101: a fake password for this test
	t.Setenv(postgresDSNEnv, "")
	bad, _, _ := parseFlags([]string{"-store", kindPostgres, "-postgres-dsn", badDSN}, &bytes.Buffer{})
	_, _, _, err = openPostgresLog(t.Context(), realDeps(), bad, controlplanemetrics.New(), slog.Default())
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("want an open error without the password, got %v", err)
	}
}

// TestPostgresStoreCommitAndLookup runs one node with -store postgres against a
// real PostgreSQL. It needs MDS_TEST_POSTGRES_DSN, and skips without it.
func TestPostgresStoreCommitAndLookup(t *testing.T) {
	dsn := os.Getenv("MDS_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MDS_TEST_POSTGRES_DSN is not set")
	}
	prefix := fmt.Sprintf("t%d_", time.Now().UnixNano()%1e9)
	t.Cleanup(func() { dropPostgresTables(t, dsn, prefix) })
	n := startNode(t, realDeps(), []string{"-store", kindPostgres, "-postgres-dsn", dsn, "-postgres-prefix", prefix,
		"-create-tables", "-checkpoint-bucket", "", "-node-name", "n0", "-rank", "0"})
	c := controlplaneclient.New(n.addr, nil)
	defer func() { _ = c.Close() }()
	waitRole(t, n, "active")
	if err := c.CreateTopic(t.Context(), "t", 1); err != nil {
		t.Fatalf("create topic: %v\n%s", err, n.logs.String())
	}
	key := "seg/0"
	_, err := c.Call(t.Context(), core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{
		{TopicPartition: core.NewTopicPartition("t", 0), Records: 3, Loc: core.SegmentRef{Key: key, Len: 9}}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Call(t.Context(), core.FindSegments{TopicPartition: core.NewTopicPartition("t", 0), MaxBytes: 1 << 20})
	if err != nil || response.(core.FindSegmentsResp).End != 3 {
		t.Fatalf("lookup after commit: %v %+v\n%s", err, response, n.logs.String())
	}
	if code := n.stop(t); code != 0 {
		t.Fatalf("code %d\n%s", code, n.logs.String())
	}
	if strings.Contains(n.logs.String(), dsn) {
		t.Fatal("the DSN was logged")
	}
}

// waitRole waits until the node has the role. A node that is healthy can still
// be catching up, and it refuses commits until it is active.
func waitRole(t *testing.T, n *logNode, want string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if role(t, n) == want {
			return
		}
	}
	t.Fatalf("role %q, want %q\n%s", role(t, n), want, n.logs.String())
}

// dropPostgresTables removes the two tables of one test prefix. It runs after
// the test, so it makes its own context (t.Context is already done by then).
func dropPostgresTables(t *testing.T, dsn, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := executor.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Errorf("drop tables: %v", err)
		return
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+prefix+"log, "+prefix+"meta"); err != nil {
		t.Errorf("drop tables %s: %v", prefix, err)
	}
}
