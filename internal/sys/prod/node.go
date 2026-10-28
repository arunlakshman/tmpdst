package prod

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"manifester/internal/broker"
	"manifester/internal/core"
	"manifester/internal/sys"
)

// NodeConfig is everything one broker process needs.
type NodeConfig struct {
	NodeID   string
	Broker   broker.Config // Host/Port are the advertised address; Port 0 means the listener's
	Listener net.Listener

	Objects      sys.ObjectStore
	ObjClassify  sys.ErrorClassifier
	ControlPlane sys.ControlPlane
	CPClassify   sys.ErrorClassifier

	Shutdown   <-chan struct{} // optional; closing it is Node.Stop
	StopGrace  time.Duration   // time the broker gets to finish in-flight work (10s)
	FlushGrace time.Duration   // time writers get to flush after a graceful exit (5s)
	DataInbox  int             // data queue capacity (1024)
	Net        NetConfig
	Exec       ExecConfig
	Facts      func(core.Fact)
	// OnIO, if set, sees every I/O action the broker issues, on the broker's
	// loop goroutine, before it executes (used to pair segment keys with
	// segment_sealed facts, see SegmentLog).
	OnIO func(core.IOCall)
	Log  *slog.Logger
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
	cfg = withDefaults(cfg)
	bc, err := advertised(cfg.Broker, cfg.Listener)
	if err != nil {
		return nil, err
	}
	env, err := NewEnv(cfg.Log)
	if err != nil {
		return nil, err
	}
	rctx, kill := context.WithCancel(ctx)
	in := sys.NewInbox(cfg.DataInbox)
	nt := NewNet(cfg.Listener, in, cfg.Net)
	n := &Node{addr: cfg.Listener.Addr(), net: nt, stop: make(chan struct{}), done: make(chan struct{}), kill: kill}
	exec := NewExecutor(rctx, cfg.Exec, cfg.Objects, cfg.ControlPlane, nt,
		Classifier{Objects: cfg.ObjClassify, CP: cfg.CPClassify}, in)
	var ex sys.Executor = exec
	if cfg.OnIO != nil {
		ex = hookedExecutor{exec, cfg.OnIO}
	}
	if cfg.Shutdown != nil {
		go n.stopOn(cfg.Shutdown)
	}
	b := sys.Dependencies{
		Clock: NewClock(), Objects: cfg.Objects, ControlPlane: cfg.ControlPlane, Connections: nt,
		Classifier: exec.class, Env: env, Inbox: in, Facts: cfg.Facts,
		Shutdown: n.stop, StopGrace: core.Duration(cfg.StopGrace),
	}
	self := core.NodeInstance{Node: core.NodeID(cfg.NodeID), Inc: core.BootNumber(env.NewID())}
	go n.run(rctx, b, ex, broker.New(bc), self, cfg.FlushGrace)
	return n, nil
}

// withDefaults fills the zero grace periods and inbox size.
func withDefaults(cfg NodeConfig) NodeConfig {
	if cfg.StopGrace <= 0 {
		cfg.StopGrace = 10 * time.Second
	}
	if cfg.FlushGrace <= 0 {
		cfg.FlushGrace = 5 * time.Second
	}
	if cfg.DataInbox <= 0 {
		cfg.DataInbox = 1024
	}
	return cfg
}

// advertised fills the broker's advertised host and port from the listener.
func advertised(bc broker.Config, ln net.Listener) (broker.Config, error) {
	if bc.Port == 0 {
		_, p, err := net.SplitHostPort(ln.Addr().String())
		if err != nil {
			return bc, err
		}
		port, err := strconv.Atoi(p)
		if err != nil {
			return bc, err
		}
		bc.Port, err = checkedInt32(port)
		if err != nil {
			return bc, err
		}
	}
	if bc.Host == "" {
		bc.Host = "127.0.0.1"
	}
	return bc, nil
}

// checkedInt32 converts n to int32 after rejecting values that would overflow.
func checkedInt32(n int) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %d out of int32 range", n)
	}
	return int32(n), nil
}

// stopOn stops the node when shutdown closes, or returns once it is done.
func (n *Node) stopOn(shutdown <-chan struct{}) {
	select {
	case <-shutdown:
		n.Stop()
	case <-n.done:
	}
}

// run drives the broker to its end, then closes the network and marks done.
func (n *Node) run(ctx context.Context, b sys.Dependencies, ex sys.Executor, br core.MachineFactory, self core.NodeInstance, flush time.Duration) {
	defer close(n.done)
	err := sys.Run(ctx, b, ex, br, self)
	grace := time.Duration(0)
	if err == nil {
		grace = flush // graceful exit: let the last answers reach the clients
	}
	n.net.Shutdown(grace)
	n.kill()
	n.err = err
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
// after a graceful exit, sys.ErrStopped after Kill, sys.ErrStoppedForRestart or
// sys.ErrKilledByRuntime otherwise.
func (n *Node) Wait() error {
	<-n.done
	return n.err
}

// String names the node for logs.
func (n *Node) String() string { return fmt.Sprintf("broker(%s)", n.addr) }

// hookedExecutor shows each action to a hook, then executes it.
type hookedExecutor struct {
	sys.Executor
	tap func(core.IOCall)
}

func (t hookedExecutor) Execute(a core.IOCall) {
	t.tap(a)
	t.Executor.Execute(a)
}

// OpenConns is the number of open client connections.
func (n *Node) OpenConns() int { return n.net.OpenConns() }
