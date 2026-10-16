package it

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"manifester/broker"
	"manifester/cp"
	"manifester/cp/cpclient"
	"manifester/sys"
	"manifester/sys/prod"
	"manifester/veritas/vsys"
)

// stack is a control plane, an object store and any number of brokers.
type stack struct {
	t       *testing.T
	cp      *cpclient.Client
	objs    sys.ObjectStore
	objCls  sys.Classifier
	nodes   []*prod.Node
	brokers broker.Config
}

// newStack serves st over HTTP and uses objs (nil: in memory) for segments.
func newStack(t *testing.T, st cp.Store, objs sys.ObjectStore, cls sys.Classifier) *stack {
	t.Helper()
	srv := httptest.NewServer(cp.NewHandler(st))
	t.Cleanup(srv.Close)
	if objs == nil {
		objs, cls = vsys.NewObjectStore(), prod.MemClassifier{}
	}
	s := &stack{t: t, cp: cpclient.New(srv.URL, srv.Client()), objs: objs, objCls: cls}
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatal(err)
	}
	bc := s.brokers
	bc.BrokerID = int32(len(s.nodes) + 1)
	n, err := prod.Start(context.Background(), prod.NodeConfig{
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
	for i := 0; i < n; i++ {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.cp.CreateTopic(ctx, name, parts); err != nil {
		s.t.Fatal(err)
	}
}
