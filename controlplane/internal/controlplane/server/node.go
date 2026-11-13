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

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/broker"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/clock"
)

// Config wires a Node.
type Config struct {
	Machine replica.Config
	Self    machine.NodeInstance
	Env     runtime.Environment
	Clock   machine.Clock
	Log     executor.Log
	Objects executor.ObjectStore
	// StopGrace is how long the machine gets after Shutdown to exit.
	StopGrace machine.Duration
	// BrokerLeaseTTL is how long a broker stays live after its last heartbeat
	// (default 5s). Brokers send one every interval, so keep it 3x the interval.
	BrokerLeaseTTL time.Duration
	// GroupInitialRebalanceDelay is how long a new consumer group waits for
	// more members before its first generation (default 3s).
	GroupInitialRebalanceDelay time.Duration
}

// Node owns the run loops and serves ControlPlane RPCs. It runs two loops:
// the replica, and the group coordinator (the rebalance machine), each with
// its own inbox. The machine talks to the replica through the replica inbox,
// and the replica answers it through reply.
type Node struct {
	cpv1.UnimplementedControlPlaneServer

	config Config
	inbox  *live.Inbox[runtime.EventData]
	inject chan func() []runtime.Action
	m      atomic.Pointer[replica.Replica]
	health *health.Server
	leases *broker.Leases // run loop only; empty until brokers heartbeat

	// groups is the inbox of the rebalance machine. groupsDone is closed when
	// its loop has ended. active is the role bit: the replica is the active
	// appender and Ready. Only the replica loop writes it (in AfterStep), and
	// the machine hears of every change as a RoleChanged.
	groups     *live.Inbox[rebalance.Input]
	groupsDone chan struct{}
	groupsErr  error // the result of the rebalance loop; set before groupsDone is closed
	active     atomic.Bool

	mu        sync.Mutex
	waiters   map[machine.RequestID]chan runtime.ControlPlaneDone
	nextReq   machine.RequestID
	calls     map[rebalance.CallID]chan rebalance.Reply // group calls that wait for the machine
	nextCall  rebalance.CallID
	shutdown  chan struct{}
	started   bool          // Start launched the loops (guarded by mu)
	loopDone  chan struct{} // closed when the replica loop has ended
	loopErr   error         // the result of the replica loop; set before loopDone is closed
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
		config.Self = machine.NodeInstance{Node: "cp"}
	}
	if config.Clock == nil {
		config.Clock = clock.NewReal()
	}
	if config.Env == nil {
		config.Env = &productionEnvironment{}
	}
	if config.Log == nil {
		config.Log = executor.NewMemoryLog()
	}
	if config.StopGrace <= 0 {
		config.StopGrace = machine.Duration(15e9) // 15s
	}
	if config.BrokerLeaseTTL <= 0 {
		config.BrokerLeaseTTL = time.Duration(broker.DefaultTTL)
	}
	n := &Node{
		config:     config,
		inbox:      live.NewInbox[runtime.EventData](0),
		inject:     make(chan func() []runtime.Action, 64),
		health:     health.NewServer(),
		leases:     broker.New(machine.Duration(config.BrokerLeaseTTL)),
		waiters:    map[machine.RequestID]chan runtime.ControlPlaneDone{},
		calls:      map[rebalance.CallID]chan rebalance.Reply{},
		groups:     live.NewInbox[rebalance.Input](0),
		groupsDone: make(chan struct{}),
		shutdown:   make(chan struct{}),
		loopDone:   make(chan struct{}),
	}
	return n
}

// routeReplica runs one output of the replica on the loop. It never blocks: an IO call
// starts a goroutine in the executor, and the answer comes back to the inbox. Every case of
// runtime.Action is listed, so a new action without a case does not compile.
func (n *Node) routeReplica(ex *executor.Executor) func(l *live.Loop[runtime.EventData], out runtime.Action) error {
	return func(l *live.Loop[runtime.EventData], out runtime.Action) error {
		switch a := out.(type) {
		case runtime.SetTimer:
			l.SetTimer(a.ID, a.After, runtime.TimerFired{ID: a.ID, Gen: a.Gen})
		case runtime.CancelTimer:
			l.CancelTimer(a.ID)
		case runtime.StopForRestart:
			l.End(machine.Trip, a.Cause)
		case runtime.ShutdownDone:
			l.End(machine.Exit, "")
		case runtime.ControlPlaneReply:
			n.reply(a)
		case runtime.ObjectPut:
			ex.Execute(a)
		case runtime.ObjectGet:
			ex.Execute(a)
		case runtime.ObjectDelete:
			ex.Execute(a)
		case runtime.ObjectList:
			ex.Execute(a)
		case runtime.ControlPlaneCall:
			ex.Execute(a)
		case runtime.LogAppend:
			ex.Execute(a)
		case runtime.LogRead:
			ex.Execute(a)
		case runtime.LogGetEnd:
			ex.Execute(a)
		case runtime.LogDeleteOld:
			ex.Execute(a)
		case runtime.LogClaimEpoch:
			ex.Execute(a)
		}
		return nil
	}
}

// Health returns the health server to register on the gRPC server.
func (n *Node) Health() *health.Server { return n.health }

// Start launches the run loops. Ready flips health to Serving.
// ctx bounds only the Ready wait; the loops run until Close (or the replica returns StopForRestart or ShutdownDone).
func (n *Node) Start(ctx context.Context) error {
	groupsEnv, err := newRandomEnvironment()
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.cancel = cancel
	groupsCtx, stopGroups := context.WithCancel(runCtx)
	ex := executor.New(runCtx, executor.Config{}, n.config.Log, n.config.Objects, n.inbox)
	m := replica.New(n.config.Machine)(n.config.Env, n.config.Self)
	n.m.Store(m)
	cfg := live.Config[runtime.EventData, runtime.Action]{
		Self: n.config.Self,
		New: func(machine.Env, machine.NodeInstance) machine.Machine[runtime.EventData, runtime.Action] {
			return m
		},
		Env:       n.config.Env,
		Boot:      func(self machine.NodeInstance) runtime.EventData { return runtime.NodeStarted{Self: self} },
		StopMsg:   func(deadline machine.NodeTime) runtime.EventData { return runtime.StopRequest{Deadline: deadline} },
		StopGrace: n.config.StopGrace,
		Clock:     n.config.Clock,
		Inbox:     n.inbox,
		Route:     n.routeReplica(ex),
		Shutdown:  n.shutdown,
		Inject:    n.inject,
		AfterStep: func() {
			n.epoch.Store(m.Epoch())
			n.stepDowns.Store(m.StepDowns())
			n.setRole(m.IsActive() && m.Ready())
		},
	}
	n.mu.Lock()
	n.started = true
	n.mu.Unlock()
	go func() {
		err := live.Run(groupsCtx, n.groupsConfig(groupsEnv))
		n.groupsErr = err
		close(n.groupsDone)
	}()
	go func() {
		err := live.Run(runCtx, cfg)
		n.setRole(false)
		stopGroups() // the machine can not reach the log any more
		n.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		n.loopErr = err
		close(n.loopDone)
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
		<-n.loopDone
		if err != nil {
			return err
		}
		return n.loopErr
	}
	timedOut := make(chan struct{})
	deadline := n.config.Clock.AfterFunc(machine.Duration(2*time.Minute), func() { close(timedOut) })
	defer deadline.Stop()
	for {
		done := make(chan struct{})
		var ready bool
		n.inject <- func() []runtime.Action {
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
		case <-n.loopDone:
			if n.loopErr != nil {
				return n.loopErr
			}
			return errors.New("controlplane/server: run exited before Ready")
		case <-timedOut:
			return fail(errors.New("controlplane/server: timed out waiting for Ready"))
		}
	}
}

// Close asks the loops to stop and waits for them. Safe to call more than once,
// after a failed Start (the loops have ended then), and when Start was never
// called. Group calls that still wait are answered Unavailable.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		select {
		case <-n.shutdown:
		default:
			close(n.shutdown)
		}
		n.mu.Lock()
		started := n.started
		n.mu.Unlock()
		if started {
			if n.cancel != nil {
				n.cancel()
			}
			<-n.loopDone
			<-n.groupsDone
			n.closeErr = n.loopErr
			if n.groupsErr != nil && !errors.Is(n.groupsErr, live.ErrStopped) {
				n.closeErr = errors.Join(n.closeErr, n.groupsErr)
			}
		}
	})
	return n.closeErr
}

func (n *Node) allocReq() machine.RequestID {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextReq++
	return n.nextReq
}

func (n *Node) onLoop(ctx context.Context, fn func(m *replica.Replica) []runtime.Action) error {
	done := make(chan struct{})
	var runErr error
	select {
	case n.inject <- func() []runtime.Action {
		defer close(done)
		m := n.m.Load()
		if m == nil {
			runErr = errors.New("controlplane/server: machine not started")
			return nil
		}
		return fn(m)
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

// serverFrom is who the node is when it talks to its own replica: the reply to
// a request goes back to it, and the reply sink hands it to the waiting RPC.
var serverFrom = machine.NodeInstance{Node: "server"}

// ask sends one request to the replica as an inbox value and waits for its
// reply. A request that ends with its context may have been applied if it is a
// command (maybeApplied), so its answer says MaybeApplied.
func (n *Node) ask(ctx context.Context, maybeApplied bool, body func(request machine.RequestID) runtime.EventData) (runtime.ControlPlaneDone, error) {
	request := n.allocReq()
	ch := make(chan runtime.ControlPlaneDone, 1)
	n.mu.Lock()
	n.waiters[request] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, request)
		n.mu.Unlock()
	}()
	n.inbox.Deliver(machine.Data, body(request))
	select {
	case d := <-ch:
		return d, nil
	case <-ctx.Done():
		outcome := runtime.NotApplied
		if maybeApplied {
			outcome = runtime.MaybeApplied
		}
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrTimeout, Outcome: outcome}, ctx.Err()
	case <-n.loopDone:
		return runtime.ControlPlaneDone{}, errors.New("controlplane/server: the run loop has ended")
	}
}

// reply is the reply sink of the run loop: it gives the answer of the replica
// to the RPC that waits for it, or to the rebalance machine. The server and the
// machine count request ids apart, so the same id can wait in both: look at To
// first, and touch the waiters only for the server.
func (n *Node) reply(r runtime.ControlPlaneReply) {
	switch r.To {
	case rebalanceFrom:
		n.groups.Deliver(machine.Data, rebalance.ReplicaReply{Req: r.Done.Req, Done: r.Done})
	case serverFrom:
		n.mu.Lock()
		ch := n.waiters[r.Done.Req]
		delete(n.waiters, r.Done.Req)
		n.mu.Unlock()
		if ch != nil {
			ch <- r.Done
		}
	}
}

// Register attaches the ControlPlane service and health to s.
func (n *Node) Register(s *grpc.Server) {
	cpv1.RegisterControlPlaneServer(s, n)
	healthpb.RegisterHealthServer(s, n.health)
}
