package main

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manifesterio/mds/internal/controlplane/controlplaneclient"
	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/controlplane/executor/sqllog"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/system"
)

// Three control plane nodes share one real PostgreSQL. Commits stream in while
// the writer is killed and a standby takes over, and a checkpoint and trim run
// every 100 ms (the window is 64 slots, so the keep window is 8). Every
// acknowledged commit must be in the state that the surviving nodes replay: the
// log, the epoch claim and the floor all work on PostgreSQL, not only on the fake.
// It skips unless MDS_TEST_POSTGRES_DSN is set (make it-postgres sets it).
func TestPostgresThreeNodesTakeoverWithTrim(t *testing.T) {
	dsn := os.Getenv("MDS_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MDS_TEST_POSTGRES_DSN is not set")
	}
	prefix := "f" + strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 10) + "_"
	t.Cleanup(func() { dropPostgresTables(t, dsn, prefix) })
	objs := executor.NewMemoryObjects()
	d := deps{objects: func(context.Context, cpFlags) (system.ObjectStore, error) { return objs, nil }}
	node := func(name string, rank int, extra ...string) *logNode {
		args := append([]string{"-store", kindPostgres, "-postgres-dsn", dsn, "-postgres-prefix", prefix,
			"-node-name", name, "-rank", strconv.Itoa(rank), "-checkpoint-bucket", "b",
			"-takeover-after", "600ms", "-heartbeat", "50ms",
			"-window-slots", "64", "-checkpoint-every", "100ms", "-checkpoint-min-slots", "5"}, extra...)
		return startNode(t, d, args)
	}
	n0 := node("n0", 0, "-create-tables")
	n1 := node("n1", 1)
	n2 := node("n2", 2)
	waitRole(t, n0, "active")
	if r := []string{role(t, n0), role(t, n1), role(t, n2)}; r[0] != "active" || r[1] != "standby" || r[2] != "standby" {
		t.Fatalf("roles %v", r)
	}

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
	n0.stop(t) // the writer dies in the middle of the stream
	waitCount(t, &n, before+150)
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
	// The run really covered a takeover and trims: the writer epoch moved past 1
	// and the floor moved up. Without this a quiet run would pass for nothing.
	epoch, floor := postgresEpochAndFloor(t, dsn, prefix)
	if epoch < 2 || floor < 1 {
		t.Fatalf("the run did not cover a takeover and a trim: epoch %d, floor %d", epoch, floor)
	}
	t.Logf("covered: writer epoch %d, floor %d, %d acknowledged commits", epoch, floor, n.Load())
}

// postgresEpochAndFloor reads the two meta rows of one test prefix.
func postgresEpochAndFloor(t *testing.T, dsn, prefix string) (int64, int64) {
	t.Helper()
	db, err := sqllog.OpenPostgres(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	query := "SELECT (SELECT num FROM " + prefix + "meta WHERE name = 'epoch'), (SELECT num FROM " + prefix + "meta WHERE name = 'floor')"
	var epoch, floor int64
	if err := db.QueryRowContext(t.Context(), query).Scan(&epoch, &floor); err != nil {
		t.Fatal(err)
	}
	return epoch, floor
}
