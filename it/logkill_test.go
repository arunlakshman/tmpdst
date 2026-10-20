package it

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/twmb/franz-go/pkg/kgo"

	"manifester/broker"
	"manifester/core"
	"manifester/cp/cpclient"
	"manifester/cp/dynamo/fakedb"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
	"manifester/sys/prod"
	"manifester/veritas/vsys"
)

// cpNode is one log-backed control-plane node served over real HTTP.
type cpNode struct {
	svc *logcp.Service
	srv *httptest.Server
}

// kill is a crash: connections drop mid-request, the loops stop, the port is
// closed. Nothing is flushed and no step-down is announced.
func (n *cpNode) kill() {
	n.srv.CloseClientConnections()
	n.svc.Stop()
	n.srv.Close()
}

// logStack is brokers over a three-node log control plane (rank 0 active, two
// standbys). The brokers get the URL list, preferred node first.
type logStack struct {
	*stack
	cps []*cpNode
}

func newLogStack(t *testing.T, brokers int) *logStack {
	t.Helper()
	db := fakedb.New()
	if _, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	objs := logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{})
	ls := &logStack{}
	var urls []string
	for rank := 0; rank < 3; rank++ {
		cfg := logcptest.FastConfig(fmt.Sprintf("cp%d", rank), rank, log)
		cfg.Incarnation = fmt.Sprintf("it%d", rank)
		cfg.DB, cfg.Table, cfg.Objects = db, "cp", objs
		// Decision 51: the takeover time is a few heartbeats here (seconds in production).
		cfg.TakeoverAfter, cfg.Heartbeat, cfg.HoleAfter = 1500*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond
		svc := logcptest.Start(t, cfg)
		n := &cpNode{svc: svc, srv: httptest.NewServer(svc.Handler())}
		t.Cleanup(n.srv.Close)
		ls.cps = append(ls.cps, n)
		urls = append(urls, n.srv.URL)
	}
	// The brokers' (and the test's) client: one pooled transport, so connections are
	// reused and a long run does not exhaust the ephemeral ports (macOS).
	st := &stack{t: t, cp: cpclient.New(strings.Join(urls, ","), nil), objs: vsys.NewObjectStore(), objCls: prod.MemClassifier{},
		brokers: broker.Config{BatchWindow: core.Duration(20 * time.Millisecond)}}
	t.Cleanup(func() {
		for _, n := range st.nodes {
			n.Kill()
			<-n.Done()
		}
	})
	ls.stack = st
	st.addBrokers(brokers)
	return ls
}

// retryDial retries f a bounded number of times, only when it failed to dial
// (the macOS ephemeral-port flake); any other error is final.
func retryDial(f func() error) error {
	var err error
	for i := 0; i < 5; i++ {
		if err = f(); err == nil || !isDialErr(err) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

func isDialErr(err error) bool {
	var op *net.OpError
	return (errors.As(err, &op) && op.Op == "dial") || errors.Is(err, syscall.EADDRNOTAVAIL)
}

var roleClient = &http.Client{Timeout: 5 * time.Second}

// roleOf reads X-CP-Role from /healthz ("" if the node is down).
func roleOf(n *cpNode) string {
	var role string
	err := retryDial(func() error {
		resp, err := roleClient.Get(n.srv.URL + "/healthz")
		if err != nil {
			return err
		}
		resp.Body.Close()
		role = resp.Header.Get(logcp.RoleHeader)
		return nil
	})
	if err != nil {
		return ""
	}
	return role
}

// Kill the active control-plane node while franz-go produces: a standby takes
// over, the brokers' commits (same segment keys) continue through the URL list,
// every acked record is stored once at the offset it was acked at, and the
// stall is far inside the 120 s commit deadline. Idempotence is off (decisions 41-44).
func TestKillTheAppenderMidProduce(t *testing.T) {
	s := newLogStack(t, 2)
	create(s.stack)
	if roleOf(s.cps[0]) != "active" || roleOf(s.cps[1]) != "standby" {
		t.Fatalf("roles %q %q", roleOf(s.cps[0]), roleOf(s.cps[1]))
	}
	cl := newClient(t, s.seeds(), kgo.RetryTimeout(60*time.Second), kgo.RecordDeliveryTimeout(60*time.Second))
	acked := newSent()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const n = 1000
	var lastAck atomic.Int64
	promise := func(r *kgo.Record, err error) {
		lastAck.Store(time.Now().UnixNano())
		acked.promise(r, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			for topic, parts := range topics {
				for p := int32(0); p < parts; p++ {
					cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: p, Value: value(topic, p, i)}, promise)
				}
			}
			if i%10 == 0 {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()
	waitAcked(t, acked, 600)
	killedAt := time.Now()
	s.cps[0].kill()
	// Acks resume through the standby; measure the stall as the first ack after the kill.
	var resumed time.Time
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if at := time.Unix(0, lastAck.Load()); at.After(killedAt) {
			resumed = at
			break
		}
	}
	if resumed.IsZero() {
		t.Fatal("no ack after the control-plane node was killed")
	}
	<-done
	if err := cl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(acked.fails) > 0 {
		t.Fatalf("produce failures despite a live standby: %v", acked.fails)
	}
	t.Logf("first ack %v after the kill", resumed.Sub(killedAt).Round(time.Millisecond))
	if gap := resumed.Sub(killedAt); gap > 20*time.Second {
		t.Fatalf("takeover stall %v, budget 20 s (commit deadline 120 s)", gap)
	}
	// Either standby may win the takeover race.
	if r1, r2 := roleOf(s.cps[1]), roleOf(s.cps[2]); r1 != "active" && r2 != "active" {
		t.Fatalf("no standby took over: %q %q", r1, r2)
	}
	got := consume(t, s.seeds(), wantCounts(topics, n))
	verify(t, acked, got, false)
	for k, recs := range got {
		if len(recs) != n {
			t.Fatalf("%v: %d records, want %d (all were acked)", k, len(recs), n)
		}
	}
	// Both control-plane survivors agree on the state.
	d1, d2 := s.cps[1].svc.Applier(), s.cps[2].svc.Applier()
	waitSameApplied(t, s.cps[1].svc, s.cps[2].svc)
	if d1.StateHash() != d2.StateHash() {
		t.Fatalf("survivors diverge: %v vs %v", d1.StateHash(), d2.StateHash())
	}
}

func waitSameApplied(t *testing.T, a, b *logcp.Service) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if a.Applied() == b.Applied() {
			return
		}
	}
	t.Fatalf("survivors never converged: %d vs %d", a.Applied(), b.Applied())
}
