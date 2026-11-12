package conformance

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"

	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The gRPC target: the real server and replica in one process, on the memory
// log and object store, and a generated gRPC client on a local TCP port. Every
// operation goes over the wire, so the request and the answer cross the
// conversions of the API as well.
//
// The conversions here are written by hand and do not use the wire package of
// the code under test. If they did, a bug that is the same in both directions
// would cancel out.

// grpcDisk is what survives a restart.
type grpcDisk struct {
	log  *executor.MemoryLog
	objs *executor.MemoryObjects
}

// grpcProcess is what one run of the node sees of the disk. A crash kills it:
// from then on its storage calls fail and change nothing.
type grpcProcess struct {
	disk  *grpcDisk
	dead  atomic.Bool
	mu    sync.Mutex
	armed map[int]int // failures still to inject, by kind
}

var (
	_ executor.Log         = (*grpcProcess)(nil)
	_ executor.ObjectStore = (*grpcProcess)(nil)
)

type injectedError struct{}

func (injectedError) Error() string { return "injected failure" }

func (p *grpcProcess) take(kind int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.armed[kind] == 0 {
		return false
	}
	p.armed[kind]--
	return true
}

func (p *grpcProcess) down() error {
	if p.dead.Load() {
		return injectedError{}
	}
	return nil
}

func (p *grpcProcess) Append(ctx context.Context, slot runtime.LogPosition, data []byte) ([]byte, bool, error) {
	if err := p.down(); err != nil || p.take(failAppend) {
		return nil, false, injectedError{}
	}
	existing, taken, err := p.disk.log.Append(ctx, slot, data)
	if err == nil && p.take(failAppendLate) {
		return nil, false, injectedError{}
	}
	return existing, taken, err
}

func (p *grpcProcess) Read(ctx context.Context, from runtime.LogPosition, limit int) ([]runtime.LogEntry, error) {
	if err := p.down(); err != nil || p.take(failRead) {
		return nil, injectedError{}
	}
	return p.disk.log.Read(ctx, from, limit)
}

func (p *grpcProcess) CheckTail(ctx context.Context) (runtime.LogPosition, error) {
	if err := p.down(); err != nil || p.take(failRead) {
		return 0, injectedError{}
	}
	return p.disk.log.CheckTail(ctx)
}

func (p *grpcProcess) TrimThrough(ctx context.Context, through runtime.LogPosition) error {
	if err := p.down(); err != nil {
		return err
	}
	return p.disk.log.TrimThrough(ctx, through)
}

func (p *grpcProcess) ClaimEpoch(ctx context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	if err := p.down(); err != nil {
		return 0, "", false, err
	}
	return p.disk.log.ClaimEpoch(ctx, expect, owner)
}

func (p *grpcProcess) Put(ctx context.Context, key string, data []byte) error {
	if err := p.down(); err != nil || p.take(failPut) {
		return injectedError{}
	}
	return p.disk.objs.Put(ctx, key, data)
}

func (p *grpcProcess) Get(ctx context.Context, ref runtime.SegmentRef) ([]byte, error) {
	if err := p.down(); err != nil {
		return nil, err
	}
	return p.disk.objs.Get(ctx, ref)
}

func (p *grpcProcess) Delete(ctx context.Context, key string) error {
	if err := p.down(); err != nil {
		return err
	}
	return p.disk.objs.Delete(ctx, key)
}

func (p *grpcProcess) List(ctx context.Context, prefix string) ([]string, error) {
	if err := p.down(); err != nil {
		return nil, err
	}
	return p.disk.objs.List(ctx, prefix)
}

const grpcCheckpointPrefix = "checkpoint/"

// grpcT is what a grpcNode needs of a test: a *rapid.T or a *testing.T.
type grpcT interface {
	Context() context.Context
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// grpcNode is a server.Node behind a gRPC server, with a client, that can be
// restarted on the same disk.
type grpcNode struct {
	t          grpcT
	cfg        replica.Config
	groupDelay time.Duration // the initial rebalance delay of a new group; 0 is the default
	disk       *grpcDisk
	proc       *grpcProcess
	node       *server.Node
	gs         *grpc.Server
	conn       *grpc.ClientConn
	api        cpv1.ControlPlaneClient
	ended      atomic.Int64 // the calls whose handler has returned, over every run
}

// newGRPCNode starts a node. The log is cut behind each checkpoint.
func newGRPCNode(rt grpcT) *grpcNode {
	return newGRPCNodeDelay(rt, 0)
}

// newGRPCNodeDelay is newGRPCNode with an initial rebalance delay.
func newGRPCNodeDelay(rt grpcT, groupDelay time.Duration) *grpcNode {
	cfg := replica.Config{
		CatchUpEvery: runtime.Duration(time.Millisecond), Rank: 0, Writer: "cp-0",
		Heartbeat:       runtime.Duration(time.Second),
		TakeoverAfter:   runtime.Duration(20 * time.Millisecond), // a restarted node claims the write role after this
		CheckpointEvery: runtime.Duration(3 * time.Millisecond), CheckpointMinSlots: 1, CheckpointPrefix: grpcCheckpointPrefix,
		KeepSlots: 2,
	}
	n := &grpcNode{t: rt, cfg: cfg, groupDelay: groupDelay, disk: &grpcDisk{log: executor.NewMemoryLog(), objs: executor.NewMemoryObjects()}}
	n.start()
	rt.Cleanup(n.stop)
	return n
}

func (n *grpcNode) start() {
	n.proc = &grpcProcess{disk: n.disk, armed: map[int]int{}}
	n.node = server.New(server.Config{
		Machine: n.cfg, Log: n.proc, Objects: n.proc,
		Self: runtime.NodeInstance{Node: "cp-0"}, StopGrace: runtime.Duration(50 * time.Millisecond),
		GroupInitialRebalanceDelay: n.groupDelay,
	})
	ctx, cancel := context.WithTimeout(n.t.Context(), 10*time.Second)
	defer cancel()
	if err := n.node.Start(ctx); err != nil {
		n.node = nil // Start already stopped it, and Close would wait for ever
		n.t.Fatalf("the node did not start: %v", err)
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		n.t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(n.count))
	n.gs = gs
	n.node.Register(gs)
	go func() { _ = gs.Serve(ln) }() // not n.gs: a reboot replaces it while this runs
	n.conn, err = grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		n.t.Fatalf("dial: %v", err)
	}
	n.api = cpv1.NewControlPlaneClient(n.conn)
}

// count runs one handler and then counts it in ended. A handler that has
// returned has given the node all it will: an abandoned group call has told
// the machine.
func (n *grpcNode) count(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	defer n.ended.Add(1)
	return handler(ctx, req)
}

// stop closes the client, the gRPC server and the node. A crash stops the
// server at once; a clean stop lets calls in flight end.
func (n *grpcNode) stop() {
	if n.node == nil {
		return
	}
	_ = n.conn.Close()
	n.gs.Stop()
	_ = n.node.Close()
	n.node = nil
}

// Exec asks again while the node says ErrUnavailable (it is not the writer
// yet, or not ready), as a real client does. That answer means "not accepted,
// try later".
//
// DescribeGroups gives the state of the group coordinator in memory (a Kafka
// state), and the metadata model the state of the log. This target never
// installs a group, so the coordinator never has one: it says Dead, which is
// the empty state of the model for a group that does not exist. Any other
// state of DescribeGroups is checked against the rebalance model in
// TestGRPCRebalance.
func (n *grpcNode) Exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	for {
		d, err := n.send(ctx, op)
		if err != nil || d.Err != runtime.ErrUnavailable || ctx.Err() != nil {
			if r, ok := d.Resp.(contract.DescribeGroupResp); ok && !r.Exists && r.State == "Dead" {
				r.State = ""
				d.Resp = r
			}
			return d, err
		}
		time.Sleep(time.Millisecond)
	}
}

// DirtyReboot is a crash. The process dies first (its storage calls stop at
// once, so nothing it had in flight lands later), and a new one starts on the
// same disk. The log is trimmed behind the checkpoints and cannot rebuild the
// state without them, so the crash leaves all of them (whatever keep says).
func (n *grpcNode) DirtyReboot(int) {
	n.proc.dead.Store(true)
	n.stop()
	n.start()
}

// Checkpoint waits (a little) until a checkpoint covers the whole log.
func (n *grpcNode) Checkpoint() {
	tail, _ := n.disk.log.CheckTail(n.t.Context())
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		keys, _ := n.disk.objs.List(n.t.Context(), grpcCheckpointPrefix)
		slices.Sort(keys)
		if len(keys) == 0 {
			continue
		}
		if slot, err := strconv.ParseInt(strings.TrimPrefix(keys[len(keys)-1], grpcCheckpointPrefix), 10, 64); err == nil && slot >= int64(tail) {
			return
		}
	}
}

// Inject arms one failure of a kind for the next matching call. -1 disarms all.
func (n *grpcNode) Inject(kind int) {
	p := n.proc
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind < 0 {
		clear(p.armed)
		return
	}
	p.armed[kind]++
}

// ---- The calls ----

// send makes one call over the wire. The API has no call for a group install,
// assign or remove, so the target lacks those operations (see grpcLacks).
func (n *grpcNode) send(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	var resp runtime.ControlPlaneResponse
	var err error
	switch op := op.(type) {
	case contract.CommitSegment:
		req := &cpv1.CommitSegmentRequest{SegmentKey: op.SegmentKey}
		for _, b := range op.Batches {
			req.Batches = append(req.Batches, &sharedv1.PendingBatch{Tp: tpToWire(b.TopicPartition), Records: b.Records, Loc: refToWire(b.Loc)})
		}
		var r *cpv1.CommitSegmentResponse
		if r, err = n.api.CommitSegment(ctx, req); err == nil {
			out := contract.CommitSegmentResp{}
			for _, a := range r.GetAssigned() {
				out.Assigned = append(out.Assigned, rangeFromWire(a))
			}
			resp = out
		}
	case contract.FindSegments:
		var r *cpv1.FindSegmentsResponse
		if r, err = n.api.FindSegments(ctx, &cpv1.FindSegmentsRequest{Tp: tpToWire(op.TopicPartition), From: int64(op.From), MaxBytes: op.MaxBytes}); err == nil {
			out := contract.FindSegmentsResp{End: contract.Offset(r.GetEnd())}
			for _, x := range r.GetRanges() {
				out.Ranges = append(out.Ranges, rangeFromWire(x))
			}
			resp = out
		}
	case contract.DescribeTopics:
		var r *cpv1.DescribeTopicsResponse
		if r, err = n.api.DescribeTopics(ctx, &cpv1.DescribeTopicsRequest{Topics: op.Topics}); err == nil {
			out := contract.DescribeTopicsResp{}
			for _, x := range r.GetTopics() {
				out.Topics = append(out.Topics, contract.TopicInfo{Name: x.GetName(), Partitions: x.GetPartitions()})
			}
			resp = out
		}
	case contract.CreateTopic:
		_, err = n.api.CreateTopic(ctx, &cpv1.CreateTopicRequest{Topic: op.Name, Partitions: op.Partitions, FailIfExists: op.FailIfExists})
	case contract.OffsetCommit:
		req := &cpv1.OffsetCommitRequest{Group: op.Group, MemberId: op.Member, Generation: op.Generation}
		for _, o := range op.Offsets {
			req.Offsets = append(req.Offsets, &cpv1.PartitionOffsetCommit{Tp: tpToWire(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata})
		}
		var r *cpv1.OffsetCommitResponse
		if r, err = n.api.OffsetCommit(ctx, req); err == nil {
			out := contract.OffsetCommitResp{}
			for _, c := range r.GetErrs() {
				out.Errs = append(out.Errs, codeFromWire(c))
			}
			resp = out
		}
	case contract.OffsetFetch:
		req := &cpv1.OffsetFetchRequest{Group: op.Group}
		for _, p := range op.Partitions {
			req.Partitions = append(req.Partitions, tpToWire(p))
		}
		var r *cpv1.OffsetFetchResponse
		if r, err = n.api.OffsetFetch(ctx, req); err == nil {
			out := contract.OffsetFetchResp{}
			for _, o := range r.GetOffsets() {
				out.Offsets = append(out.Offsets, contract.PartitionOffset{
					TopicPartition: contract.NewTopicPartition(o.GetTp().GetTopic(), o.GetTp().GetPartition()),
					Offset:         o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata()})
			}
			resp = out
		}
	case contract.DescribeGroup:
		var r *cpv1.DescribeGroupsResponse
		if r, err = n.api.DescribeGroups(ctx, &cpv1.DescribeGroupsRequest{Groups: []string{op.Group}}); err == nil {
			if len(r.GetGroups()) != 1 {
				return runtime.ControlPlaneDone{}, status.Errorf(codes.Internal, "DescribeGroups of one group returned %d groups", len(r.GetGroups()))
			}
			g := r.GetGroups()[0].GetDetails()
			out := contract.DescribeGroupResp{Exists: g.GetExists(), Generation: g.GetGeneration(), ProtocolType: g.GetProtocolType(),
				Protocol: g.GetProtocol(), Leader: g.GetLeader(), AssignedGeneration: g.GetAssignedGeneration(), State: g.GetState()}
			for _, m := range g.GetMembers() {
				j := m.GetMember()
				out.Members = append(out.Members, contract.GroupMemberInfo{
					GroupMember: contract.GroupMember{MemberID: j.GetMemberId(), ClientID: j.GetClientId(), ClientHost: j.GetClientHost(),
						GroupInstanceID: j.GetGroupInstanceId(), SessionTimeoutMs: j.GetSessionTimeoutMs(),
						RebalanceTimeoutMs: j.GetRebalanceTimeoutMs(), Metadata: j.GetMetadata()},
					JoinedTimeMs: m.GetJoinedTimeMs(), Assignment: m.GetAssignment()})
			}
			resp = out
		}
	case contract.ListGroups:
		var r *cpv1.ListGroupsResponse
		if r, err = n.api.ListGroups(ctx, &cpv1.ListGroupsRequest{}); err == nil {
			out := contract.ListGroupsResp{}
			for _, g := range r.GetGroups() {
				out.Groups = append(out.Groups, contract.GroupSummary{Name: g.GetName(), ProtocolType: g.GetProtocolType(), State: g.GetState()})
			}
			resp = out
		}
	default:
		return runtime.ControlPlaneDone{}, status.Errorf(codes.Unimplemented, "the gRPC target has no call for %T", op)
	}
	if err != nil {
		return answerOf(err)
	}
	return runtime.ControlPlaneDone{Resp: resp}, nil
}

// answerOf turns a failed call into an answer. A business error carries its
// code in an ErrorDetail. A call that did not reach the node (unavailable) is
// ErrUnavailable. Anything else did not get an answer: it is an error.
func answerOf(err error) (runtime.ControlPlaneDone, error) {
	st, ok := status.FromError(err)
	if !ok {
		return runtime.ControlPlaneDone{}, err
	}
	for _, d := range st.Details() {
		if ed, ok := d.(*cpv1.ErrorDetail); ok {
			return runtime.ControlPlaneDone{Err: codeFromWire(ed.GetCode())}, nil
		}
	}
	if st.Code() == codes.Unavailable {
		return runtime.ControlPlaneDone{Err: runtime.ErrUnavailable}, nil
	}
	return runtime.ControlPlaneDone{}, err
}

func tpToWire(tp contract.TopicPartition) *sharedv1.TopicPartition {
	return &sharedv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func refToWire(r runtime.SegmentRef) *sharedv1.SegmentRef {
	return &sharedv1.SegmentRef{Key: r.Key, Off: r.Off, Len: r.Len}
}

func rangeFromWire(b *sharedv1.BatchRange) contract.BatchRange {
	return contract.BatchRange{
		TopicPartition: contract.NewTopicPartition(b.GetTp().GetTopic(), b.GetTp().GetPartition()),
		Base:           contract.Offset(b.GetBase()), Count: b.GetCount(),
		Loc: runtime.SegmentRef{Key: b.GetLoc().GetKey(), Off: b.GetLoc().GetOff(), Len: b.GetLoc().GetLen()},
	}
}

// codeFromWire is the error code of the API. A code it does not know is ErrIO,
// which the harness takes as "try again", so a missing case shows as a failure
// of the test and not as a pass.
func codeFromWire(c cpv1.ErrCode) runtime.ErrCode {
	switch c {
	case cpv1.ErrCode_ERR_CODE_OK:
		return runtime.OK
	case cpv1.ErrCode_ERR_CODE_TIMEOUT:
		return runtime.ErrTimeout
	case cpv1.ErrCode_ERR_CODE_UNAVAILABLE:
		return runtime.ErrUnavailable
	case cpv1.ErrCode_ERR_CODE_NOT_FOUND:
		return runtime.ErrNotFound
	case cpv1.ErrCode_ERR_CODE_CONFLICT:
		return runtime.ErrConflict
	case cpv1.ErrCode_ERR_CODE_CONN_CLOSED:
		return runtime.ErrConnClosed
	case cpv1.ErrCode_ERR_CODE_TOO_LARGE:
		return runtime.ErrTooLarge
	case cpv1.ErrCode_ERR_CODE_CORRUPT:
		return runtime.ErrCorrupt
	case cpv1.ErrCode_ERR_CODE_INVALID:
		return runtime.ErrInvalid
	case cpv1.ErrCode_ERR_CODE_IO:
		return runtime.ErrIO
	case cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER:
		return runtime.ErrUnknownMember
	case cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION:
		return runtime.ErrIllegalGeneration
	case cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION:
		return runtime.ErrUnknownPartition
	case cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS:
		return runtime.ErrRebalanceInProgress
	case cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL:
		return runtime.ErrInconsistentGroupProtocol
	case cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS:
		return runtime.ErrTopicExists
	}
	return runtime.ErrIO
}

// grpcLacks are the operations that the API has no call for: they are log
// commands with a generation and members that the caller chooses. Over the
// wire a group is installed, assigned and changed only by JoinGroup,
// SyncGroup, Heartbeat and LeaveGroup, which the rebalance machine turns into
// these commands. TestGRPCRebalance checks those calls against the rebalance
// model.
var grpcLacks = []string{"groupInstall", "groupAssign", "groupRemove", "groupAssignLarge"}

// TestGRPC is the real replica behind a gRPC server, with crash restarts,
// checkpoints and injected storage failures, on a log that is trimmed behind
// each checkpoint. It has no clean restart and no variant without trim: those
// belong to the replica, and TestReplica has them. The wire does not change
// with them. After a restart the group calls are Unavailable until the group
// coordinator has read the groups of the log again.
func TestGRPC(t *testing.T) {
	fewerChecks(t, 10, 40)
	Run(t, func(rt *rapid.T) Target {
		n := newGRPCNode(rt)
		return Target{Exec: n.Exec, DirtyReboot: n.DirtyReboot, Checkpoint: n.Checkpoint, Inject: n.Inject, Lacks: grpcLacks}
	})
}
