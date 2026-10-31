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

	cpv1 "mds/internal/controlplane/api/v1"
	"mds/internal/controlplane/apiconvert"
	"mds/internal/controlplane/executor"
	"mds/internal/controlplane/groupcoordinator"
	"mds/internal/controlplane/livebrokers"
	"mds/internal/controlplane/runloop"
	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

// Config wires a Node.
type Config struct {
	Machine statemachine.Config
	Self    core.NodeInstance
	Env     core.Environment
	Clock   runloop.Clock
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

// defaultBrokerLeaseTTL is the lease length when Config leaves it zero.
const defaultBrokerLeaseTTL = 5 * time.Second

// Node owns the machine run loop and serves ControlPlane RPCs.
type Node struct {
	cpv1.UnimplementedControlPlaneServer

	cfg    Config
	inbox  *runloop.Inbox
	inject chan runloop.InjectFunc
	m      atomic.Pointer[statemachine.Machine]
	health *health.Server
	leases *livebrokers.Set              // run loop only; empty until brokers heartbeat
	groups *groupcoordinator.Coordinator // serves group calls while this node is active

	mu        sync.Mutex
	waiters   map[core.RequestID]chan core.ControlPlaneDone
	nextReq   core.RequestID
	shutdown  chan struct{}
	loopDone  chan error
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

// New builds a Node. Call Start then serve gRPC with Register.
func New(cfg Config) *Node {
	if cfg.Self.Node == "" {
		cfg.Self = core.NodeInstance{Node: "cp"}
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Env == nil {
		cfg.Env = &productionEnvironment{}
	}
	if cfg.Log == nil {
		cfg.Log = executor.NewMemoryLog()
	}
	if cfg.StopGrace <= 0 {
		cfg.StopGrace = core.Duration(15e9) // 15s
	}
	if cfg.BrokerLeaseTTL <= 0 {
		cfg.BrokerLeaseTTL = defaultBrokerLeaseTTL
	}
	n := &Node{
		cfg:      cfg,
		inbox:    runloop.NewInbox(64),
		inject:   make(chan runloop.InjectFunc, 64),
		health:   health.NewServer(),
		leases:   livebrokers.New(cfg.BrokerLeaseTTL),
		waiters:  map[core.RequestID]chan core.ControlPlaneDone{},
		shutdown: make(chan struct{}),
		loopDone: make(chan error, 1),
	}
	n.groups = groupcoordinator.New(groupcoordinator.Config{
		InitialRebalanceDelay: cfg.GroupInitialRebalanceDelay,
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
	ex := executor.New(runCtx, executor.Config{}, n.cfg.Log, n.cfg.Objects, n.inbox)
	b := runloop.Dependencies{
		Clock: n.cfg.Clock, Env: n.cfg.Env, Inbox: n.inbox,
		Shutdown: n.shutdown, StopGrace: n.cfg.StopGrace, Inject: n.inject,
		AfterStep: func(cm core.Machine) {
			if m, ok := cm.(*statemachine.Machine); ok {
				n.flushAnswers(m)
			}
		},
	}
	f := statemachine.New(n.cfg.Machine)
	go n.watchGroups(runCtx)
	go func() {
		err := runloop.Run(runCtx, b, ex, func(env core.Environment, self core.NodeInstance) core.Machine {
			m := f(env, self).(*statemachine.Machine)
			n.m.Store(m)
			return m
		}, n.cfg.Self)
		n.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		n.loopDone <- err
	}()
	fail := func(err error) error {
		cancel()
		loopErr := <-n.loopDone
		if err != nil {
			return err
		}
		return loopErr
	}
	deadline := time.NewTimer(2 * time.Minute)
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
		case <-deadline.C:
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

func (n *Node) onLoop(ctx context.Context, fn func(m *statemachine.Machine) []core.Action) error {
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

func (n *Node) flushAnswers(m *statemachine.Machine) {
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

func (n *Node) offer(ctx context.Context, cmd stateupdater.Entry) (core.ControlPlaneDone, error) {
	req := n.allocReq()
	ch := make(chan core.ControlPlaneDone, 1)
	n.mu.Lock()
	n.waiters[req] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, req)
		n.mu.Unlock()
	}()

	err := n.onLoop(ctx, func(m *statemachine.Machine) []core.Action {
		return m.Offer(statemachine.ClientOperation{Req: req, Cmd: cmd})
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
			return core.ControlPlaneDone{Req: req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}, ctx.Err()
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

func (n *Node) read(ctx context.Context, fn func(m *statemachine.Machine) (core.ControlPlaneDone, error)) (core.ControlPlaneDone, error) {
	type result struct {
		d   core.ControlPlaneDone
		err error
	}
	ch := make(chan result, 1)
	err := n.onLoop(ctx, func(m *statemachine.Machine) []core.Action {
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

// CommitSegment implements ControlPlane.
func (n *Node) CommitSegment(ctx context.Context, req *cpv1.CommitSegmentRequest) (*cpv1.CommitSegmentResponse, error) {
	op := apiconvert.CommitSegmentToCore(req)
	d, err := n.offer(ctx, stateupdater.Entry{Kind: stateupdater.KindCommit, Commit: op})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, toRPCError(core.ErrTimeout, core.MaybeApplied)
		}
		return nil, toRPCError(core.ErrIO, core.MaybeApplied)
	}
	if d.Err != core.OK {
		return nil, toRPCError(d.Err, d.Outcome)
	}
	return apiconvert.CommitSegmentRespFromCore(core.ResponseAs[core.CommitSegmentResp](d)), nil
}

// Lookup implements ControlPlane.
//
//nolint:dupl // mirror of DescribeTopics; each RPC keeps its own conversion path
func (n *Node) Lookup(ctx context.Context, req *cpv1.LookupRequest) (*cpv1.LookupResponse, error) {
	op := apiconvert.LookupToCore(req)
	d, err := n.read(ctx, func(m *statemachine.Machine) (core.ControlPlaneDone, error) {
		return m.Fold().Lookup(op)
	})
	if err != nil {
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	if d.Err != core.OK {
		return nil, toRPCError(d.Err, d.Outcome)
	}
	return apiconvert.LookupRespFromCore(core.ResponseAs[core.FindSegmentsResp](d)), nil
}

// DescribeTopics implements ControlPlane.
//
//nolint:dupl // mirror of Lookup; each RPC keeps its own conversion path
func (n *Node) DescribeTopics(ctx context.Context, req *cpv1.DescribeTopicsRequest) (*cpv1.DescribeTopicsResponse, error) {
	op := apiconvert.DescribeTopicsToCore(req)
	d, err := n.read(ctx, func(m *statemachine.Machine) (core.ControlPlaneDone, error) {
		return m.Fold().Describe(op)
	})
	if err != nil {
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	if d.Err != core.OK {
		return nil, toRPCError(d.Err, d.Outcome)
	}
	return apiconvert.DescribeTopicsRespFromCore(core.ResponseAs[core.DescribeTopicsResp](d)), nil
}

// CreateTopic implements ControlPlane.
func (n *Node) CreateTopic(ctx context.Context, req *cpv1.CreateTopicRequest) (*cpv1.CreateTopicResponse, error) {
	if req == nil {
		return nil, toRPCError(core.ErrInvalid, core.NotApplied)
	}
	d, err := n.offer(ctx, stateupdater.CreateEntry(core.CreateTopic{
		Name: req.Topic, Partitions: req.Partitions, FailIfExists: req.FailIfExists,
	}))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, toRPCError(core.ErrTimeout, core.MaybeApplied)
		}
		return nil, toRPCError(core.ErrIO, core.MaybeApplied)
	}
	if d.Err != core.OK {
		return nil, toRPCError(d.Err, d.Outcome)
	}
	return &cpv1.CreateTopicResponse{}, nil
}

// GetStatus implements ControlPlane.
func (n *Node) GetStatus(ctx context.Context, _ *cpv1.GetStatusRequest) (*cpv1.GetStatusResponse, error) {
	type st struct {
		role             string
		applied, logTail int64
		ready            bool
	}
	ch := make(chan st, 1)
	err := n.onLoop(ctx, func(m *statemachine.Machine) []core.Action {
		role := "standby"
		if m.IsActive() {
			role = "active"
		}
		ch <- st{
			role: role, applied: int64(m.Applied()), logTail: int64(m.Applied()),
			ready: m.Ready(),
		}
		return nil
	})
	if err != nil {
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	select {
	case s := <-ch:
		if !s.ready {
			return nil, toRPCError(core.ErrUnavailable, core.NotApplied)
		}
		return &cpv1.GetStatusResponse{Role: s.role, AppliedSlot: s.applied, LogTail: s.logTail}, nil
	case <-ctx.Done():
		return nil, toRPCError(core.ErrTimeout, core.NotApplied)
	}
}

// BrokerHeartbeat implements ControlPlane. It renews the caller's lease and
// returns the live broker set (Q4). Only the active node keeps the set, so a
// standby answers Unavailable and the broker client tries the next address.
func (n *Node) BrokerHeartbeat(ctx context.Context, req *cpv1.BrokerHeartbeatRequest) (*cpv1.BrokerHeartbeatResponse, error) {
	b := apiconvert.BrokerHeartbeatToCore(req)
	if b.NodeID <= 0 || b.Host == "" || b.Port <= 0 {
		return nil, toRPCError(core.ErrInvalid, core.NotApplied)
	}
	type result struct {
		live []core.BrokerAddr
		ok   bool
	}
	ch := make(chan result, 1)
	err := n.onLoop(ctx, func(m *statemachine.Machine) []core.Action {
		if !m.IsActive() || !m.Ready() {
			ch <- result{}
			return nil
		}
		now := time.Now()
		n.leases.Renew(b, now)
		ch <- result{live: n.leases.Live(now), ok: true}
		return nil
	})
	if err != nil {
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	r := <-ch
	if !r.ok {
		return nil, toRPCError(core.ErrUnavailable, core.NotApplied)
	}
	return apiconvert.BrokerHeartbeatRespFromCore(r.live), nil
}

// Register attaches the ControlPlane service and health to s.
func (n *Node) Register(s *grpc.Server) {
	cpv1.RegisterControlPlaneServer(s, n)
	healthpb.RegisterHealthServer(s, n.health)
}
