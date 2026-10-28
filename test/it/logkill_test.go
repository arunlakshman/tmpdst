package it

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"

	"manifester/internal/broker"
	"manifester/internal/controlplane/cpclient"
	"manifester/internal/controlplane/exec"
	"manifester/internal/controlplane/fakedb"
	"manifester/internal/controlplane/machine"
	"manifester/internal/controlplane/server"
	"manifester/internal/core"
	"manifester/internal/sys/prod"
	"manifester/internal/verify/veritas/vsys"
)

// cpNode is one machine-backed control-plane node served over gRPC.
type cpNode struct {
	node *server.Node
	gs   *grpc.Server
	addr string
	cli  *cpclient.Client
}

// kill is a crash: connections drop mid-request, the loop stops, the port is
// closed. Nothing is flushed and no step-down is announced.
func (n *cpNode) kill() {
	n.gs.Stop()
	_ = n.cli.Close()
	_ = n.node.Close()
}

// logStack is brokers over a three-node log control plane (rank 0 active, two
// standbys). The brokers get the address list, preferred node first.
type logStack struct {
	*stack
	cps []*cpNode
}

func newLogStack(t *testing.T, brokers int) *logStack {
	t.Helper()
	db := fakedb.New()
	if _, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{TableName: aws.String("cp")}); err != nil {
		t.Fatal(err)
	}
	log := exec.NewDynLog(db, "cp", exec.DynLogOptions{})
	objs := exec.NewMemObjects()
	ls := &logStack{}
	var addrs []string
	for rank := range 3 {
		cfg := machine.Config{
			K: 4, CatchUpEvery: core.Duration(5 * time.Millisecond),
			Rank: rank, Heartbeat: core.Duration(100 * time.Millisecond),
			TakeoverAfter: core.Duration(1500 * time.Millisecond),
			HoleAfter:     core.Duration(100 * time.Millisecond),
			Writer:        fmt.Sprintf("cp%d", rank),
		}
		n := server.New(server.Config{
			Machine: cfg, Log: log, Objects: objs,
			Self: core.NodeRef{Node: core.NodeID(fmt.Sprintf("cp%d", rank))},
		})
		if err := n.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		gs := grpc.NewServer()
		n.Register(gs)
		go func() { _ = gs.Serve(ln) }()
		addr := ln.Addr().String()
		cli := cpclient.New(addr, nil)
		cn := &cpNode{node: n, gs: gs, addr: addr, cli: cli}
		t.Cleanup(cn.kill)
		ls.cps = append(ls.cps, cn)
		addrs = append(addrs, addr)
	}
	st := &stack{t: t, cp: cpclient.New(strings.Join(addrs, ","), nil), objs: vsys.NewObjectStore(), objCls: prod.MemClassifier{},
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

func retryDial(f func() error) error {
	var err error
	for range 5 {
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

func roleOf(t *testing.T, n *cpNode) string {
	t.Helper()
	var role string
	err := retryDial(func() error {
		st, err := n.cli.GetStatus(t.Context())
		if err != nil {
			return err
		}
		role = st.Role
		return nil
	})
	if err != nil {
		return ""
	}
	return role
}

// Kill the active control-plane node while franz-go produces: a standby takes
// over, the brokers' commits (same segment keys) continue through the address
// list, every acked record is stored once at the offset it was acked at, and
// the stall is far inside the 120 s commit deadline. Idempotence is off
// (decisions 41-44).
func TestKillTheAppenderMidProduce(t *testing.T) {
	s := newLogStack(t, 2)
	create(s.stack)
	if roleOf(t, s.cps[0]) != "active" || roleOf(t, s.cps[1]) != "standby" {
		t.Fatalf("roles %q %q", roleOf(t, s.cps[0]), roleOf(t, s.cps[1]))
	}
	cl := newClient(t, s.seeds(), kgo.RetryTimeout(60*time.Second), kgo.RecordDeliveryTimeout(60*time.Second))
	defer closeClient(cl)
	acked := newSent()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	const n = 40 // enough to cross a takeover; not a load test
	var lastAck atomic.Int64
	promise := func(r *kgo.Record, err error) {
		lastAck.Store(time.Now().UnixNano())
		acked.promise(r, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range n {
			for topic, parts := range topics {
				for p := range parts {
					cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: p, Value: value(topic, p, i)}, promise)
				}
			}
			if i%5 == 0 {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()
	waitAcked(t, acked, 15)
	killedAt := time.Now()
	s.cps[0].kill()
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
	if r1, r2 := roleOf(t, s.cps[1]), roleOf(t, s.cps[2]); r1 != "active" && r2 != "active" {
		t.Fatalf("no standby took over: %q %q", r1, r2)
	}
	closeClient(cl)
	got := consume(t, s.seeds(), wantCounts(topics, n))
	verify(t, acked, got, false)
	for k, recs := range got {
		if len(recs) != n {
			t.Fatalf("%v: %d records, want %d (all were acked)", k, len(recs), n)
		}
	}
}
