package prod

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"manifester/broker"
	"manifester/core"
	"manifester/sys"
)

// NodeConfig is everything one broker process needs.
type NodeConfig struct {
	NodeID   string
	Broker   broker.Config // Host/Port are the advertised address; Port 0 means the listener's
	Listener net.Listener

	Objects      sys.ObjectStore
	ObjClassify  sys.Classifier
	ControlPlane sys.ControlPlane
	CPClassify   sys.Classifier

	Shutdown   <-chan struct{} // optional; closing it is Node.Stop
	StopGrace  time.Duration   // time the broker gets to finish in-flight work (10s)
	FlushGrace time.Duration   // time writers get to flush after a graceful exit (5s)
	DataInbox  int             // data queue capacity (1024)
	Net        NetConfig
	Exec       ExecConfig
	Facts      func(core.Fact)
	Log        *slog.Logger
}

// Node is a running broker.
type Node struct {
	addr net.Addr
	net  *Net
	stop chan struct{}
	once sync.Once
	done chan struct{}
	err  error
	kill context.CancelFunc
}

// Start wires the broker to its adapters and runs it until Stop, Kill or a
// failure. The returned Node's Wait gives sys.Run's error.
func Start(ctx context.Context, cfg NodeConfig) (*Node, error) {
	if cfg.Listener == nil || cfg.Objects == nil || cfg.ControlPlane == nil {
		return nil, errors.New("prod.Start: Listener, Objects and ControlPlane are required")
	}
	if cfg.StopGrace <= 0 {
		cfg.StopGrace = 10 * time.Second
	}
	if cfg.FlushGrace <= 0 {
		cfg.FlushGrace = 5 * time.Second
	}
	if cfg.DataInbox <= 0 {
		cfg.DataInbox = 1024
	}
	bc := cfg.Broker
	if bc.Port == 0 {
		_, p, err := net.SplitHostPort(cfg.Listener.Addr().String())
		if err != nil {
			return nil, err
		}
		port, _ := strconv.Atoi(p)
		bc.Port = int32(port)
	}
	if bc.Host == "" {
		bc.Host = "127.0.0.1"
	}
	env := NewEnv(cfg.Log)
	rctx, kill := context.WithCancel(ctx)
	in := sys.NewInbox(cfg.DataInbox)
	nt := NewNet(cfg.Listener, in, cfg.Net)
	n := &Node{addr: cfg.Listener.Addr(), net: nt, stop: make(chan struct{}), done: make(chan struct{}), kill: kill}
	exec := NewExecutor(rctx, cfg.Exec, cfg.Objects, cfg.ControlPlane, nt,
		Classifier{Objects: cfg.ObjClassify, CP: cfg.CPClassify}, in)
	if cfg.Shutdown != nil {
		go func() {
			select {
			case <-cfg.Shutdown:
				n.Stop()
			case <-n.done:
			}
		}()
	}
	b := sys.Bundle{
		Clock: NewClock(), Objects: cfg.Objects, ControlPlane: cfg.ControlPlane, Conns: nt,
		Classifier: exec.class, Env: env, Inbox: in, Facts: cfg.Facts,
		Shutdown: n.stop, StopGrace: core.Duration(cfg.StopGrace),
	}
	self := core.NodeRef{Node: core.NodeID(cfg.NodeID), Inc: core.Incarnation(env.NewID())}
	go func() {
		defer close(n.done)
		err := sys.Run(rctx, b, exec, broker.New(bc), self)
		grace := time.Duration(0)
		if err == nil {
			grace = cfg.FlushGrace // graceful exit: let the last answers reach the clients
		}
		nt.Shutdown(grace)
		kill()
		n.err = err
	}()
	return n, nil
}

// Addr is the address the broker listens on.
func (n *Node) Addr() net.Addr { return n.addr }

// Stop asks for a graceful stop: in-flight produces finish and are answered.
func (n *Node) Stop() { n.once.Do(func() { close(n.stop) }) }

// Kill stops the broker at once, resetting every connection (a crash).
func (n *Node) Kill() { n.kill() }

// Done is closed when the broker has fully stopped.
func (n *Node) Done() <-chan struct{} { return n.done }

// Wait blocks until the broker has stopped and returns sys.Run's result: nil
// after a graceful exit, sys.ErrStopped after Kill, sys.ErrTripped or
// sys.ErrKilled otherwise.
func (n *Node) Wait() error {
	<-n.done
	return n.err
}

// String names the node for logs.
func (n *Node) String() string { return fmt.Sprintf("broker(%s)", n.addr) }
