package server

import (
	"context"
	"fmt"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/wire"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/system/live"
	"github.com/manifesterio/mds/controlplane/internal/system/simadapter"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sim"
)

// The host (docs/rebalance.md section 11). The machine runs in a second
// live.Run with its own inbox (Node.groups). Its SubmitCommand goes to the
// replica inbox as a ControlPlaneDeliver; its SubmitRead as a LocalRead, or as
// a ControlPlaneDeliver when Fresh. The replica answers to rebalanceFrom, and
// Node.reply gives that answer to the machine as a ReplicaReply. The replica
// loop sends RoleChanged when "active appender and Ready" changes. A gRPC
// handler gives its call an id, delivers the input and waits for the Reply.

// rebalanceFrom is who the machine is when it talks to the replica: the
// replica answers to it, and Node.reply hands the answer to the machine.
var rebalanceFrom = machine.NodeInstance{Node: "rebalance"}

// groupsProcessID is the process id of the rebalance machine. The machine sends to itself
// with this id.
const groupsProcessID sim.ProcessID = "rebalance"

// groupsConfig is the run of the rebalance machine. It has no boot, no stop
// request and no inject: Close ends it with the context. Its timers and the
// sends to itself are handled by live.
func (n *Node) groupsConfig(env machine.Env) live.Config {
	machineConfig := rebalance.Config{InitialDelay: machine.Duration(n.config.GroupInitialRebalanceDelay)}
	return live.Config{
		Process: simadapter.NewRebalance(groupsProcessID, rebalance.New(machineConfig, env)),
		Clock:   liveClock{n.config.Clock},
		Inbox:   n.groups,
		Exec:    n.groupsExec,
	}
}

// groupsExec runs one output of the machine. It never blocks.
func (n *Node) groupsExec(out any) error {
	switch v := out.(type) {
	case rebalance.SubmitCommand:
		n.inbox.Deliver(runtime.ControlPlaneDeliver{From: rebalanceFrom, Req: v.Req, Op: v.Op})
	case rebalance.SubmitRead:
		if v.Fresh {
			n.inbox.Deliver(runtime.ControlPlaneDeliver{From: rebalanceFrom, Req: v.Req, Op: v.Op})
		} else {
			n.inbox.Deliver(runtime.LocalRead{From: rebalanceFrom, Req: v.Req, Op: v.Op})
		}
	case rebalance.Reply:
		n.answer(v)
	default:
		return fmt.Errorf("controlplane/server: the rebalance machine returned %T, which nothing takes", out)
	}
	return nil
}

// setRole keeps the role bit and tells the machine when it changes. It runs
// on the replica loop (and once more when that loop has ended).
func (n *Node) setRole(active bool) {
	if n.active.Swap(active) != active {
		n.groups.Deliver(rebalance.RoleChanged{Active: active})
	}
}

// call gives one group call an id, delivers its input to the machine and
// waits for the reply. When ctx ends first, the machine is told that the call
// is abandoned, and a reply that comes later is dropped. When the node stops,
// the call is Unavailable. A failed answer is an RPC error; read says that the
// call writes nothing.
func (n *Node) call(ctx context.Context, read bool, input func(rebalance.CallID) any) (rebalance.Reply, error) {
	ch := make(chan rebalance.Reply, 1)
	n.mu.Lock()
	n.nextCall++
	call := n.nextCall
	n.calls[call] = ch
	n.mu.Unlock()
	n.groups.Deliver(input(call))
	select {
	case r := <-ch:
		if r.Err != runtime.OK {
			return r, wire.ToRPCError(r.Err, groupOutcome(read, r.Err))
		}
		return r, nil
	case <-ctx.Done():
		if n.forget(call) {
			n.groups.Deliver(rebalance.CallAbandoned{Call: call})
		}
		return rebalance.Reply{}, wire.ToRPCError(runtime.ErrTimeout, groupOutcome(read, runtime.ErrTimeout))
	case <-n.groupsDone:
	case <-n.shutdown:
	}
	n.forget(call)
	return rebalance.Reply{}, wire.ToRPCError(runtime.ErrUnavailable, groupOutcome(read, runtime.ErrUnavailable))
}

// forget stops waiting for call. It reports whether the call still waited.
func (n *Node) forget(call rebalance.CallID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.calls[call]
	delete(n.calls, call)
	return ok
}

// answer gives a reply of the machine to the call that waits for it. A call
// that no longer waits (it was abandoned) gets nothing.
func (n *Node) answer(r rebalance.Reply) {
	n.mu.Lock()
	ch := n.calls[r.Call]
	delete(n.calls, r.Call)
	n.mu.Unlock()
	if ch != nil {
		ch <- r
	}
}

// groupOutcome is the outcome of a group call that failed. A read changed
// nothing. A write that is unavailable or timed out may have been logged.
func groupOutcome(read bool, code runtime.ErrCode) runtime.Outcome {
	if !read && (code == runtime.ErrUnavailable || code == runtime.ErrTimeout) {
		return runtime.MaybeApplied
	}
	return runtime.NotApplied
}

// groupRPC runs one group call on the machine and converts its answer, an A,
// to the wire type Out.
func groupRPC[Req any, A rebalance.Answer, Out any](ctx context.Context, n *Node, read bool, request Req,
	input func(rebalance.CallID, Req) any, convert func(A) Out) (Out, error) {
	var zero Out
	r, err := n.call(ctx, read, func(call rebalance.CallID) any { return input(call, request) })
	if err != nil {
		return zero, err
	}
	a, _ := r.Body.(A) // a heartbeat has no body
	return convert(a), nil
}

// JoinGroup implements ControlPlane.
func (n *Node) JoinGroup(ctx context.Context, request *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.JoinToMachine), wire.JoinGroupResponseFromCore)
}

// SyncGroup implements ControlPlane.
func (n *Node) SyncGroup(ctx context.Context, request *cpv1.SyncGroupRequest) (*cpv1.SyncGroupResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.SyncToMachine), wire.SyncGroupResponseFromCore)
}

// Heartbeat implements ControlPlane.
func (n *Node) Heartbeat(ctx context.Context, request *cpv1.HeartbeatRequest) (*cpv1.HeartbeatResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.HeartbeatToMachine), wire.HeartbeatAnswerToWire)
}

// LeaveGroup implements ControlPlane. The answer has one code per member.
func (n *Node) LeaveGroup(ctx context.Context, request *cpv1.LeaveGroupRequest) (*cpv1.LeaveGroupResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.LeaveToMachine), wire.LeaveGroupResponseFromCore)
}

// OffsetCommit implements ControlPlane. The answer has one code per partition.
func (n *Node) OffsetCommit(ctx context.Context, request *cpv1.OffsetCommitRequest) (*cpv1.OffsetCommitResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.OffsetCommitToMachine), wire.CommitAnswerToWire)
}

// DescribeGroups implements ControlPlane. Any group that fails fails the call.
func (n *Node) DescribeGroups(ctx context.Context, request *cpv1.DescribeGroupsRequest) (*cpv1.DescribeGroupsResponse, error) {
	return groupRPC(ctx, n, true, request, asInput(wire.DescribeToMachine), wire.DescribeGroupsResponseFromCore)
}

// ListGroups implements ControlPlane.
func (n *Node) ListGroups(ctx context.Context, request *cpv1.ListGroupsRequest) (*cpv1.ListGroupsResponse, error) {
	return groupRPC(ctx, n, true, request, asInput(wire.ListToMachine), wire.ListAnswerToWire)
}

// asInput turns a converter that returns one input type into one that returns any.
func asInput[Req, In any](convert func(rebalance.CallID, Req) In) func(rebalance.CallID, Req) any {
	return func(call rebalance.CallID, request Req) any { return convert(call, request) }
}

// OffsetFetch implements ControlPlane. It is not a call of the machine: it
// reads the log of this node at once, behind the role bit, so a node that does
// not serve groups now answers Unavailable and the client tries the next
// address.
func (n *Node) OffsetFetch(ctx context.Context, request *cpv1.OffsetFetchRequest) (*cpv1.OffsetFetchResponse, error) {
	if !n.active.Load() {
		return nil, wire.ToRPCError(runtime.ErrUnavailable, runtime.NotApplied)
	}
	d, err := n.Exec(ctx, wire.OffsetFetchRequestToCore(request))
	if err != nil || d.Err != runtime.OK {
		return nil, wire.ToRPCError(runtime.ErrUnavailable, runtime.NotApplied)
	}
	return wire.OffsetFetchResponseFromCore(runtime.ResponseAs[contract.OffsetFetchResp](d)), nil
}
