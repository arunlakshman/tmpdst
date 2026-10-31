// Package grpctestserver starts an in-process manicp-shaped gRPC node for tests.
package grpctestserver

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/controlplaneclient"
	"manifester/internal/controlplane/executor"
	"manifester/internal/controlplane/server"
	"manifester/internal/controlplane/statemachine"
	"manifester/internal/core"
)

// Node is a started gRPC control-plane node.
type Node struct {
	Addr   string
	Client *controlplaneclient.Client
	API    cpv1.ControlPlaneClient
	stop   func()
}

// StartMem boots a rank-0 machine over an in-memory log.
func StartMem(t testing.TB) *Node {
	t.Helper()
	return Start(t, statemachine.Config{
		K: 4, CatchUpEvery: core.Duration(time.Millisecond),
		Rank: 0, Heartbeat: core.Duration(time.Second),
	}, executor.NewMemoryLog(), executor.NewMemoryObjects())
}

// Start boots a node with the given machine config and stores.
func Start(t testing.TB, cfg statemachine.Config, log executor.Log, objs executor.ObjectStore) *Node {
	t.Helper()
	n := server.New(server.Config{
		Machine: cfg, Log: log, Objects: objs,
		Self: core.NodeInstance{Node: "cp-0"},
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
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	cli := controlplaneclient.New(ln.Addr().String(), nil)
	stop := func() {
		_ = cli.Close()
		_ = conn.Close()
		gs.GracefulStop()
		_ = n.Close()
	}
	t.Cleanup(stop)
	return &Node{
		Addr: ln.Addr().String(), Client: cli,
		API: cpv1.NewControlPlaneClient(conn), stop: stop,
	}
}
