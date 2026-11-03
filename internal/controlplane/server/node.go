package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/controlplane/groups"
	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/controlplane/runloop"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/system/realclock"
)

// Config wires a Node.
type Config struct {
	Machine replica.Config
	Self    core.NodeInstance
	Env     core.Environment
	Clock   core.Clock
	Log     executor.Log
	Objects executor.ObjectStore
	// StopGrace is how long the machine gets after Shutdown to ShutdownDone.
	StopGrace core.Duration
	// BrokerLeaseTTL is how long a broker stays live after its last heartbeat
	// (default 5s). Brokers send one every interval, so keep it 3x the interval.
	BrokerLeaseTTL time.Duration
	// GroupInitialRebalanceDelay is how long a new consumer group waits for
	// more members before its first generation (default 3s).
	GroupInitialRebalanceDelay time.Duration
}

// Node owns the machine run loop and serves ControlPlane RPCs.
type Node struct {
	cpv1.UnimplementedControlPlaneServer

	config Config
	inbox  *runloop.Inbox
	inject chan runloop.InjectFunc
	m      atomic.Pointer[replica.Replica]
	health *health.Server
	leases *leaseSet           // run loop only; empty until brokers heartbeat
	groups *groups.Coordinator // serves group calls while this node is active

	mu        sync.Mutex
	waiters   map[core.RequestID]chan core.ControlPlaneDone
	nextReq   core.RequestID
	shutdown  chan struct{}
	loopDone  chan error
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error

	// epoch and stepDowns are copies of the machine's write role. The run loop
	// stores them after every step, so a metrics scrape never waits on the loop.
	epoch     atomic.Uint64
	stepDowns atomic.Uint64
}

// New builds a Node. Call Start then serve gRPC with Register.
func New(config Config) *Node {
	if config.Self.Node == "" {
		config.Self = core.NodeInstance{Node: "cp"}
	}
	if config.Clock == nil {
		config.Clock = realclock.New()
	}
	if config.Env == nil {
		config.Env = &productionEnvironment{}
	}
	if config.Log == nil {
		config.Log = executor.NewMemoryLog()
	}
	if config.StopGrace <= 0 {
		config.StopGrace = core.Duration(15e9) // 15s
	}
	if config.BrokerLeaseTTL <= 0 {
		config.BrokerLeaseTTL = defaultBrokerLeaseTTL
	}
	n := &Node{
		config:   config,
		inbox:    runloop.NewInbox(64),
		inject:   make(chan runloop.InjectFunc, 64),
		health:   health.NewServer(),
		leases:   newLeaseSet(config.BrokerLeaseTTL),
		waiters:  map[core.RequestID]chan core.ControlPlaneDone{},
		shutdown: make(chan struct{}),
		loopDone: make(chan error, 1),
	}
	n.groups = groups.New(groups.Config{
		InitialRebalanceDelay: config.GroupInitialRebalanceDelay,
		Clock:                 config.Clock,
	}, nodeGroups{n: n})
	return n
}

// Health returns the health server to register on the gRPC server.
func (n *Node) Health() *health.Server { return n.health }

// Start launches the run loop. Ready flips health to Serving.
// ctx bounds only the Ready wait; the loop runs until Close (or a StopForRestart/ShutdownDone).
func (n *Node) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.cancel = cancel
	ex := executor.New(runCtx, executor.Config{}, n.config.Log, n.config.Objects, n.inbox)
	b := runloop.Dependencies{
		Clock: n.config.Clock, Env: n.config.Env, Inbox: n.inbox,
		Shutdown: n.shutdown, StopGrace: n.config.StopGrace, Inject: n.inject,
		AfterStep: func(cm core.Machine) {
			if m, ok := cm.(*replica.Replica); ok {
				n.flushAnswers(m)
				n.epoch.Store(m.Epoch())
				n.stepDowns.Store(m.StepDowns())
			}
		},
	}
	f := replica.New(n.config.Machine)
	go n.watchGroups(runCtx)
	go func() {
		err := runloop.Run(runCtx, b, ex, func(env core.Environment, self core.NodeInstance) core.Machine {
			m := f(env, self).(*replica.Replica)
			n.m.Store(m)
			return m
		}, n.config.Self)
		n.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		n.loopDone <- err
	}()
	return n.waitReady(ctx, cancel)
}

// LogRole returns the writer epoch this node holds (0 when it is a standby) and
// how many times it stepped down. It reads copies, so it never waits on the run loop.
func (n *Node) LogRole() (uint64, uint64) {
	return n.epoch.Load(), n.stepDowns.Load()
}

// waitReady waits until the machine is ready, then sets health to SERVING.
// It gives up when ctx ends, when the loop exits, or after two minutes.
// On a give-up it calls cancel and waits for the loop to stop.
func (n *Node) waitReady(ctx context.Context, cancel context.CancelFunc) error {
	fail := func(err error) error {
		cancel()
		loopErr := <-n.loopDone
		if err != nil {
			return err
		}
		return loopErr
	}
	timedOut := make(chan struct{})
	deadline := n.config.Clock.AfterFunc(core.Duration(2*time.Minute), func() { close(timedOut) })
	defer deadline.Stop()
	for {
		done := make(chan struct{})
		var ready bool
		n.inject <- func() []core.Action {
			defer close(done)
			m := n.m.Load()
			ready = m != nil && m.Ready()
			return nil
		}
		select {
		case <-done:
			if ready {
				n.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			}
		case <-ctx.Done():
			return fail(ctx.Err())
		case err := <-n.loopDone:
			if err == nil {
				err = errors.New("controlplane/server: run exited before Ready")
			}
			return err
		case <-timedOut:
			return fail(errors.New("controlplane/server: timed out waiting for Ready"))
		}
	}
}

// Close asks the loop to stop and waits for it. Safe to call more than once.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		select {
		case <-n.shutdown:
		default:
			close(n.shutdown)
		}
		if n.cancel != nil {
			n.cancel()
		}
		n.closeErr = <-n.loopDone
		n.groups.Close()
	})
	return n.closeErr
}

func (n *Node) allocReq() core.RequestID {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextReq++
	return n.nextReq
}

func (n *Node) onLoop(ctx context.Context, fn func(m *replica.Replica) []core.Action) error {
	done := make(chan struct{})
	var runErr error
	select {
	case n.inject <- func() []core.Action {
		defer close(done)
		m := n.m.Load()
		if m == nil {
			runErr = errors.New("controlplane/server: machine not started")
			return nil
		}
		acts := fn(m)
		n.flushAnswers(m)
		return acts
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return runErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) flushAnswers(m *replica.Replica) {
	for _, a := range m.TakeAnswers() {
		n.mu.Lock()
		ch := n.waiters[a.Req]
		delete(n.waiters, a.Req)
		n.mu.Unlock()
		if ch != nil {
			ch <- a.Done
		}
	}
}

func (n *Node) offer(ctx context.Context, cmd metadata.Entry) (core.ControlPlaneDone, error) {
	request := n.allocReq()
	ch := make(chan core.ControlPlaneDone, 1)
	n.mu.Lock()
	n.waiters[request] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, request)
		n.mu.Unlock()
	}()

	err := n.onLoop(ctx, func(m *replica.Replica) []core.Action {
		return m.Offer(replica.ClientOperation{Req: request, Cmd: cmd})
	})
	if err != nil {
		return core.ControlPlaneDone{}, err
	}
	// Offer may have answered synchronously (standby / queue full).
	select {
	case d := <-ch:
		return d, nil
	default:
	}
	for {
		select {
		case d := <-ch:
			return d, nil
		case <-ctx.Done():
			return core.ControlPlaneDone{Req: request, Err: core.ErrTimeout, Outcome: core.MaybeApplied}, ctx.Err()
		case n.inject <- func() []core.Action {
			m := n.m.Load()
			if m != nil {
				n.flushAnswers(m)
			}
			return nil
		}:
		}
	}
}

func (n *Node) read(ctx context.Context, fn func(m *replica.Replica) (core.ControlPlaneDone, error)) (core.ControlPlaneDone, error) {
	type result struct {
		d   core.ControlPlaneDone
		err error
	}
	ch := make(chan result, 1)
	err := n.onLoop(ctx, func(m *replica.Replica) []core.Action {
		if !m.Ready() {
			ch <- result{d: core.ControlPlaneDone{Err: core.ErrUnavailable}}
			return nil
		}
		d, e := fn(m)
		ch <- result{d: d, err: e}
		return nil
	})
	if err != nil {
		return core.ControlPlaneDone{}, err
	}
	select {
	case r := <-ch:
		return r.d, r.err
	case <-ctx.Done():
		return core.ControlPlaneDone{Err: core.ErrTimeout, Outcome: core.NotApplied}, ctx.Err()
	}
}

// Register attaches the ControlPlane service and health to s.
func (n *Node) Register(s *grpc.Server) {
	cpv1.RegisterControlPlaneServer(s, n)
	healthpb.RegisterHealthServer(s, n.health)
}
