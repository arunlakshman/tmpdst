package it

import (
	"context"
	"net"
	"testing"
	"time"

	"manifester/internal/broker"
	"manifester/internal/controlplane/cpclient"
	"manifester/internal/controlplane/grpctest"
	"manifester/internal/core"
	"manifester/internal/sys"
	"manifester/internal/sys/prod"
	"manifester/internal/verify/veritas/vsys"
)

// stack is a control plane, an object store and any number of brokers.
type stack struct {
	t       *testing.T
	cp      *cpclient.Client
	objs    sys.ObjectStore
	objCls  sys.ErrorClassifier
	nodes   []*prod.Node
	brokers broker.Config
}

// newStack boots an in-process gRPC control plane. st is ignored (kept for call-site shape).
func newStack(t *testing.T, _ any, objs sys.ObjectStore, cls sys.ErrorClassifier) *stack {
	t.Helper()
	gn := grpctest.StartMem(t)
	if objs == nil {
		objs, cls = vsys.NewObjectStore(), prod.MemoryErrorClassifier{}
	}
	s := &stack{t: t, cp: gn.Client, objs: objs, objCls: cls,
		brokers: broker.Config{BatchWindow: core.Duration(20 * time.Millisecond)}}
	t.Cleanup(func() {
		for _, n := range s.nodes {
			n.Kill()
			<-n.Done()
		}
	})
	return s
}

// addBroker starts a broker on 127.0.0.1:0 and returns it with its address.
func (s *stack) addBroker() *prod.Node {
	s.t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(s.t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatal(err)
	}
	bc := s.brokers
	bc.BrokerID = int32(len(s.nodes) + 1)
	n, err := prod.Start(s.t.Context(), prod.NodeConfig{
		NodeID: "broker-" + string(rune('a'+len(s.nodes))), Broker: bc, Listener: ln,
		Objects: s.objs, ObjClassify: s.objCls,
		ControlPlane: s.cp, CPClassify: cpclient.Classifier{},
		StopGrace: 5 * time.Second, FlushGrace: 5 * time.Second,
	})
	if err != nil {
		s.t.Fatal(err)
	}
	s.nodes = append(s.nodes, n)
	return n
}

func (s *stack) addBrokers(n int) {
	for range n {
		s.addBroker()
	}
}

func (s *stack) seeds() []string {
	var out []string
	for _, n := range s.nodes {
		out = append(out, n.Addr().String())
	}
	return out
}

func (s *stack) createTopic(name string, parts int32) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), 10*time.Second)
	defer cancel()
	if err := s.cp.CreateTopic(ctx, name, parts); err != nil {
		s.t.Fatal(err)
	}
}
