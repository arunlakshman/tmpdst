//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/sqllog"
	"github.com/manifesterio/mds/controlplane/internal/testinfra"
)

// backend is one log store (with or without S3 checkpoints) for a group of
// nodes, and a way to look at what the nodes left in it.
type backend struct {
	args   []string // flags for every node
	create []string // flags for the first node only
	secret string   // must never show in the output (the PostgreSQL DSN)
	// epochAndFloor reads the writer epoch and the trim floor from the store.
	epochAndFloor func(t *testing.T) (epoch, floor int64)
	// checkpoints counts the checkpoint objects in S3 (0 without a bucket).
	checkpoints func(t *testing.T) int
}

var stores = []string{"dynamolog", "postgres"}

// newBackend makes a fresh table or schema for store, and a fresh bucket when
// withBucket is set.
func newBackend(t *testing.T, store string, withBucket bool) backend {
	t.Helper()
	var b backend
	switch store {
	case "dynamolog":
		d := testinfra.DynamoDB(t)
		// A table that does not exist yet, so -create-tables really creates it.
		table := d.Table + "_cp"
		t.Cleanup(func() {
			// t.Context is done when cleanups run.
			_, _ = d.Client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &table})
		})
		b.args = []string{"-store", "dynamolog", "-dynamo-endpoint", d.Endpoint, "-region", "us-east-1", "-table", table}
		b.epochAndFloor = func(t *testing.T) (int64, int64) {
			return dynamoMeta(t, d.Client, table, "WRITER", "epoch"), dynamoMeta(t, d.Client, table, "FLOOR", "floor")
		}
	case "postgres":
		p := testinfra.Postgres(t)
		db, err := sqllog.OpenPostgres(t.Context(), p.DSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		b.args = []string{"-store", "postgres", "-postgres-dsn", p.DSN}
		b.secret = p.DSN
		b.epochAndFloor = func(t *testing.T) (int64, int64) { return postgresMeta(t, db) }
	default:
		t.Fatalf("unknown store %s", store)
	}
	b.create = []string{"-create-tables"}
	b.checkpoints = func(*testing.T) int { return 0 }
	if withBucket {
		s := testinfra.S3(t)
		b.args = append(b.args, "-checkpoint-bucket", s.Bucket, "-s3-endpoint", s.Endpoint, "-s3-path-style")
		b.checkpoints = func(t *testing.T) int {
			out, err := s.Client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: &s.Bucket, Prefix: aws.String("cp/checkpoint/")})
			if err != nil {
				t.Fatal(err)
			}
			return len(out.Contents)
		}
	}
	return b
}

// dynamoMeta reads one number of the META partition (0 if not there yet).
func dynamoMeta(t *testing.T, db *dynamodb.Client, table, sortKey, attr string) int64 {
	t.Helper()
	out, err := db.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: &table, ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "META"},
			"sk": &types.AttributeValueMemberS{Value: sortKey},
		}})
	if err != nil {
		t.Fatal(err)
	}
	n, ok := out.Item[attr].(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	v, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// postgresMeta reads the two meta rows (0 if not there yet).
func postgresMeta(t *testing.T, db *sql.DB) (int64, int64) {
	t.Helper()
	const query = "SELECT COALESCE((SELECT num FROM mds_meta WHERE name = 'epoch'), 0), " +
		"COALESCE((SELECT num FROM mds_meta WHERE name = 'floor'), 0)"
	var epoch, floor int64
	if err := db.QueryRowContext(t.Context(), query).Scan(&epoch, &floor); err != nil {
		t.Fatal(err)
	}
	return epoch, floor
}

// trimArgs make checkpoints and log trims happen within a second: a checkpoint
// every 100 ms, and a 64 slot window, so 8 slots are kept behind a checkpoint.
var trimArgs = []string{"-window-slots", "64", "-checkpoint-every", "100ms", "-checkpoint-min-slots", "5"}

func flags(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// Three nodes share one log and one bucket. Commits stream in from three
// writers while the writer process is killed (SIGKILL) and a standby takes
// over, and checkpoints and trims run all the time. Every acknowledged commit
// must be in the state that both surviving nodes replay, with no gap, no
// overlap and nothing that was never acknowledged.
func TestThreeNodesTakeover(t *testing.T) {
	for _, store := range stores {
		t.Run(store, func(t *testing.T) {
			b := newBackend(t, store, true)
			n0 := startNode(t, "n0", flags(b.args, b.create, rankArgs(0), trimArgs))
			n1 := startNode(t, "n1", flags(b.args, rankArgs(1), trimArgs))
			n2 := startNode(t, "n2", flags(b.args, rankArgs(2), trimArgs))
			n0.waitRole(t, "active")
			if r := []string{n0.role(t), n1.role(t), n2.role(t)}; r[0] != "active" || r[1] != "standby" || r[2] != "standby" {
				t.Fatalf("roles %v", r)
			}

			c := newClient(t, n0, n1, n2) // the brokers' list: preferred node first
			c.createTopic(t, "t", 3)
			a := &acks{ranges: map[string][]*sharedv1.BatchRange{}}
			var count atomic.Int64
			stop := make(chan struct{})
			errs := make(chan error, 3)
			for w := range 3 {
				go func() { errs <- commitLoop(t, c, w, a, &count, stop) }()
			}
			waitCount(t, &count, 100)
			before := count.Load()
			n0.kill() // the writer dies in the middle of the stream
			waitCount(t, &count, before+150)
			// Either standby may win the takeover race.
			if r1, r2 := n1.role(t), n2.role(t); r1 != "active" && r2 != "active" {
				t.Fatalf("no standby took over: roles %q %q", r1, r2)
			}
			close(stop)
			for range 3 {
				if err := <-errs; err != nil {
					t.Fatalf("commit failed across the takeover: %v", err)
				}
			}
			waitPartitions(t, n2, a)
			waitPartitions(t, n1, a)
			if c1, c2 := n1.stop(t), n2.stop(t); c1 != 0 || c2 != 0 {
				t.Fatalf("exit codes %d %d", c1, c2)
			}
			// The run really covered a takeover, trims and checkpoints in S3.
			// Without this a quiet run would pass for nothing.
			epoch, floor := b.epochAndFloor(t)
			cps := b.checkpoints(t)
			if epoch < 2 || floor < 1 || cps < 1 {
				t.Fatalf("the run did not cover a takeover, a trim and a checkpoint: epoch %d, floor %d, checkpoints %d", epoch, floor, cps)
			}
			t.Logf("covered: writer epoch %d, floor %d, %d checkpoints, %d acknowledged commits", epoch, floor, cps, count.Load())
		})
	}
}

// A node is stopped after its log was trimmed below a checkpoint, and started
// again. The topic was created in a trimmed slot, so it can only come back
// from the checkpoint in S3; the later commits come from the log.
func TestRestartFromCheckpoint(t *testing.T) {
	for _, store := range stores {
		t.Run(store, func(t *testing.T) {
			b := newBackend(t, store, true)
			n := startNode(t, "n0", flags(b.args, b.create, rankArgs(0), trimArgs))
			n.waitRole(t, "active")
			c := newClient(t, n)
			c.createTopic(t, "t", 1)
			st, err := n.status(t)
			if err != nil {
				t.Fatal(err)
			}
			topicSlot := st.GetAppliedSlot() // the topic is at or below this slot
			const segments = 30
			for i := range segments {
				if _, err := c.commit(t, fmt.Sprintf("seg/%d", i), "t", 0, 3); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool {
				_, floor := b.epochAndFloor(t)
				return floor >= topicSlot && b.checkpoints(t) > 0
			}, "the log trimmed past the topic, below a checkpoint")
			if code := n.stop(t); code != 0 {
				t.Fatalf("exit code %d", code)
			}

			again := startNode(t, "n0", flags(b.args, rankArgs(0), trimArgs))
			c = newClient(t, again)
			response, err := c.findSegments(t, "t", 0, 0)
			if err != nil || response.GetEnd() != 3*segments {
				t.Fatalf("after restart: %v %v", err, response)
			}
			topics, err := call(t, c, cpv1.ControlPlaneClient.DescribeTopics, &cpv1.DescribeTopicsRequest{})
			if err != nil || len(topics.GetTopics()) != 1 || topics.GetTopics()[0].GetName() != "t" || topics.GetTopics()[0].GetPartitions() != 1 {
				t.Fatalf("topics after restart: %v %v", err, topics)
			}
			if code := again.stop(t); code != 0 {
				t.Fatalf("exit code %d", code)
			}
		})
	}
}

// One node with no checkpoint bucket: it warns, commits a segment and finds it
// again, stops cleanly, and never logs the DSN.
func TestStoreAndLookup(t *testing.T) {
	for _, store := range stores {
		t.Run(store, func(t *testing.T) {
			b := newBackend(t, store, false)
			n := startNode(t, "n0", flags(b.args, b.create, rankArgs(0)))
			n.waitRole(t, "active")
			c := newClient(t, n)
			c.createTopic(t, "t", 1)
			if _, err := c.commit(t, "seg/0", "t", 0, 3); err != nil {
				t.Fatal(err)
			}
			response, err := c.findSegments(t, "t", 0, 0)
			if err != nil || response.GetEnd() != 3 || len(response.GetRanges()) != 1 || response.GetRanges()[0].GetLoc().GetKey() != "seg/0" {
				t.Fatalf("lookup after commit: %v %v", err, response)
			}
			if code := n.stop(t); code != 0 {
				t.Fatalf("exit code %d", code)
			}
			logs := n.logs.String()
			if !strings.Contains(logs, "log_no_checkpoint") {
				t.Fatal("no warning about the missing -checkpoint-bucket")
			}
			if b.secret != "" && strings.Contains(logs, b.secret) {
				t.Fatal("the DSN was logged")
			}
		})
	}
}

// acks records what the control plane acknowledged: segment key -> ranges.
type acks struct {
	mu     sync.Mutex
	ranges map[string][]*sharedv1.BatchRange
}

// commitLoop commits new segments until stop is closed. Each commit is retried
// with the same segment key until it is acknowledged, as a broker does.
func commitLoop(t *testing.T, c *client, w int, a *acks, count *atomic.Int64, stop <-chan struct{}) error {
	for i := 0; ; i++ {
		select {
		case <-stop:
			return nil
		default:
		}
		key := fmt.Sprintf("seg/w%d-%06d", w, i)
		response, err := c.commit(t, key, "t", int32(i%3), 2+int64(w))
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		a.mu.Lock()
		a.ranges[key] = response.GetAssigned()
		a.mu.Unlock()
		count.Add(1)
	}
}

func waitCount(t *testing.T, count *atomic.Int64, want int64) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if count.Load() >= want {
			return
		}
	}
	t.Fatalf("only %d acknowledged commits, want %d", count.Load(), want)
}

func waitFor(t *testing.T, ok func() bool, what string) {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

type partitionKey struct {
	topic     string
	partition int32
}

type run struct{ base, count int64 }

// partitionsMatch reports whether the node's state holds exactly the
// acknowledged ranges: no gap, no overlap, nothing lost, nothing extra.
func partitionsMatch(t *testing.T, c *client, a *acks) error {
	a.mu.Lock()
	want := map[partitionKey]map[run]string{}
	for key, rs := range a.ranges {
		for _, r := range rs {
			p := partitionKey{r.GetTp().GetTopic(), r.GetTp().GetPartition()}
			if want[p] == nil {
				want[p] = map[run]string{}
			}
			rn := run{r.GetBase(), r.GetCount()}
			if prev, dup := want[p][rn]; dup {
				a.mu.Unlock()
				return fmt.Errorf("%v: %s and %s were both acknowledged offsets %v", p, prev, key, rn)
			}
			want[p][rn] = key
		}
	}
	a.mu.Unlock()
	for p, runs := range want {
		var next int64
		for range len(runs) + 1 { // every call returns at least one range, or the end

			response, err := c.findSegments(t, p.topic, p.partition, next)
			if err != nil {
				return err
			}
			for _, r := range response.GetRanges() {
				if r.GetBase() != next {
					return fmt.Errorf("%v: gap or overlap, range at %d, expected %d", p, r.GetBase(), next)
				}
				rn := run{r.GetBase(), r.GetCount()}
				if _, ok := runs[rn]; !ok {
					return fmt.Errorf("%v: stored range [%d,+%d) was never acknowledged", p, r.GetBase(), r.GetCount())
				}
				delete(runs, rn)
				next += r.GetCount()
			}
			if response.GetEnd() == next {
				break
			}
		}
		if len(runs) != 0 {
			return fmt.Errorf("%v: %d acknowledged ranges are lost: %v", p, len(runs), runs)
		}
	}
	return nil
}

// waitPartitions retries until the node's state matches the acknowledgements
// (a standby catches up a little after the takeover).
func waitPartitions(t *testing.T, n *node, a *acks) {
	t.Helper()
	c := newClient(t, n)
	var last error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if last = partitionsMatch(t, c, a); last == nil {
			return
		}
	}
	t.Fatalf("%s: state never matched the acknowledgements: %v", n.name, last)
}
