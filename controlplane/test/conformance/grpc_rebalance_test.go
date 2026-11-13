package conformance

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/dst/machine"
)

// The group calls over the wire, against the rebalance model. The real node
// (server, rebalance machine and replica) runs behind a gRPC server, and a
// script of consumers joins, syncs, heartbeats, commits offsets, describes,
// lists and leaves, with a crash restart (a takeover) and an abandoned call in
// the middle. Every answer must be the one of spec.Rebalance.
//
// Time. The node runs on the real clock and the model on its own: each input
// is 1 ms after the one before, and the test moves the model past the initial
// delay where the node waits for it. Sessions and rebalance timeouts are 30 s,
// far longer than the test, so no other deadline fires on either side.
//
// New member ids. The node draws them at random, so the model learns each one
// from the answer of the node: inputs go to the model only at check points,
// when every call before has its answer.
//
// Order. Calls that park run on their own goroutine. Where the order of two
// calls changes an answer, the test waits before the second one: until the
// node shows the state that the first call made, or until the handler of an
// abandoned call has returned (it has told the machine then, and the inbox of
// the machine keeps the order). Elsewhere the order does not change any answer.

const (
	rbgDelay   = 200 * time.Millisecond
	rbgSession = 30000
	rbgGroup   = "g"
)

type rbgWorld struct {
	t     *testing.T
	n     *grpcNode
	model *spec.Rebalance
	now   machine.NodeTime

	queue []rbgInput // inputs for the model, given at the next check
	last  spec.CallID

	mu        sync.Mutex
	got       map[spec.CallID]string // answers of the node
	abandoned map[spec.CallID]bool
	ids       map[string]string // member id by test name
	wg        sync.WaitGroup
}

// rbgInput is one input of the model, at its time. newName names a new member
// whose id the answer of the node gives.
type rbgInput struct {
	at      machine.NodeTime
	in      any
	newName string
}

// TestGRPCRebalance runs the script.
func TestGRPCRebalance(t *testing.T) {
	w := &rbgWorld{t: t, n: newGRPCNodeDelay(t, rbgDelay), model: spec.NewRebalance(machine.Duration(rbgDelay)),
		got: map[spec.CallID]string{}, abandoned: map[spec.CallID]bool{}, ids: map[string]string{}}
	topic := contract.CreateTopic{Name: "t", Partitions: 2}
	if d, err := w.n.Exec(t.Context(), topic); err != nil || d.Err != runtime.OK {
		t.Fatalf("create topic: %v %v", d.Err, err)
	}
	w.model.Store.Apply(topic)
	w.model.Step(w.now, spec.RoleChanged{Active: true})
	w.serving()

	// A joins a new group alone: the join waits the initial delay, then A
	// leads generation 1.
	w.join("a", "", "consumer", "range")
	w.wait()
	w.tick(rbgDelay)
	w.check()
	w.describe()
	w.heartbeat("a", 1)                 // CompletingRebalance: OK
	w.commit("a", 1, 3)                 // CompletingRebalance: ErrRebalanceInProgress
	w.join("x", "", "connect", "range") // a group with members: ErrInconsistentGroupProtocol
	w.join("x", "", "consumer")         // no protocols: ErrInvalid
	w.wait()
	w.sync("a", 1, map[string]string{"a": "a1"})
	w.commit("a", 1, 5)
	w.commit("a", 0, 6) // another generation
	w.check()
	w.fetch()
	w.describe()

	// B joins the Stable group: a round opens at once and B waits for A.
	w.join("b", "", "consumer", "range", "rr")
	w.waitState("PreparingRebalance")
	w.heartbeat("a", 1)                               // ErrRebalanceInProgress
	w.join("a", w.id("a"), "consumer", "rr", "range") // the round is done: generation 2, protocol rr
	w.wait()
	// A follower's sync waits for the leader's assignment. If the leader's sync
	// gets there first, the follower's gets the stored assignment at once: the
	// same answer, so the test does not wait in between.
	w.sync("b", 2, nil)
	w.sync("a", 2, map[string]string{"a": "a2", "b": "b2"})
	w.wait()
	w.commit("b", 2, 7)
	w.check()
	w.fetch()
	w.describe()
	w.list()
	w.check()

	// A crash and a restart on the same log: the new run takes the role and
	// reads the groups again. The group is Stable, and the sessions start again.
	w.n.DirtyReboot(0)
	w.model.Step(w.now, spec.RoleChanged{Active: false})
	w.model.Step(w.now, spec.RoleChanged{Active: true})
	w.serving()
	w.describe()
	w.list()
	w.heartbeat("a", 2)
	w.heartbeat("b", 2)
	w.sync("b", 2, nil) // Stable: the stored assignment at once
	w.check()
	w.fetch()

	// B rejoins and gives up: its call is abandoned and gets no answer. The
	// round then waits for B; A's join waits with it, until B joins again (in
	// either order the second join ends the round).
	w.abandonedJoin("b")
	w.join("a", w.id("a"), "consumer", "range")
	w.join("b", w.id("b"), "consumer", "range")
	w.wait()
	w.check()
	w.describe()

	// B leaves (with a member that is not there), then A: the group is Empty.
	w.leave(w.id("b"), "nobody")
	w.heartbeat("a", 3) // a round is open: ErrRebalanceInProgress
	w.leave(w.id("a"))
	w.check()
	w.describe()
	w.list()
	w.fetch()
}

func (w *rbgWorld) tick(d time.Duration) { w.now += machine.NodeTime(d) }

// input queues one input of the model and returns its call id.
func (w *rbgWorld) input(newName string, in func(spec.CallID) any) spec.CallID {
	w.last++
	w.now += machine.NodeTime(machine.Millisecond)
	w.queue = append(w.queue, rbgInput{at: w.now, in: in(w.last), newName: newName})
	return w.last
}

// run makes the call of the node on its own goroutine and keeps its answer.
// The channel is closed when the answer is in.
func (w *rbgWorld) run(ctx context.Context, call spec.CallID, do func(context.Context) string) <-chan struct{} {
	done := make(chan struct{})
	w.wg.Go(func() {
		defer close(done)
		got := do(ctx)
		w.mu.Lock()
		w.got[call] = got
		w.mu.Unlock()
	})
	return done
}

// wait waits for every call in flight.
func (w *rbgWorld) wait() { w.wg.Wait() }

// id is the member id of a test name.
func (w *rbgWorld) id(name string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ids[name] == "" {
		w.t.Fatalf("member %s has no id yet", name)
	}
	return w.ids[name]
}

// join sends a JoinGroup of the member name; an empty id asks for a new
// member. It does not wait: a join may park.
func (w *rbgWorld) join(name, id, protocolType string, protocols ...string) {
	newName := ""
	if id == "" {
		newName = name
	}
	req := &cpv1.JoinGroupRequest{Group: rbgGroup, MemberId: id, ClientId: "c" + name, ClientHost: "h",
		SessionTimeoutMs: rbgSession, RebalanceTimeoutMs: rbgSession, ProtocolType: protocolType}
	model := spec.Join{Group: rbgGroup, MemberID: id, ClientID: req.ClientId, ClientHost: "h", ProtocolType: protocolType,
		SessionMs: rbgSession, RebalanceMs: rbgSession}
	for _, p := range protocols {
		req.Protocols = append(req.Protocols, &cpv1.GroupProtocol{Name: p, Metadata: []byte(name + "-" + p)})
		model.Protocols = append(model.Protocols, spec.Protocol{Name: p, Metadata: []byte(name + "-" + p)})
	}
	call := w.input(newName, func(c spec.CallID) any { model.Call = c; return model })
	ctx, cancel := context.WithTimeout(w.t.Context(), 10*time.Second)
	w.run(ctx, call, func(ctx context.Context) string {
		defer cancel()
		r, err := w.n.api.JoinGroup(ctx, req)
		if err != nil {
			return w.code(err)
		}
		if newName != "" {
			w.mu.Lock()
			w.ids[newName] = r.GetMemberId()
			w.mu.Unlock()
		}
		var ms []string
		for _, m := range r.GetMembers() {
			ms = append(ms, rbShowMember(m.GetMemberId(), m.GetGroupInstanceId(), m.GetMetadata()))
		}
		return rbShowJoin(runtime.OK, r.GetGeneration(), r.GetProtocol(), r.GetLeader(), r.GetMemberId(), ms)
	})
}

// abandonedJoin sends a rejoin of name that gives up while it waits.
func (w *rbgWorld) abandonedJoin(name string) {
	req := &cpv1.JoinGroupRequest{Group: rbgGroup, MemberId: w.id(name), ClientId: "c" + name, ClientHost: "h",
		SessionTimeoutMs: rbgSession, RebalanceTimeoutMs: rbgSession, ProtocolType: "consumer",
		Protocols: []*cpv1.GroupProtocol{{Name: "range", Metadata: []byte(name + "-range")}}}
	model := spec.Join{Group: rbgGroup, MemberID: req.MemberId, ClientID: req.ClientId, ClientHost: "h", ProtocolType: "consumer",
		SessionMs: rbgSession, RebalanceMs: rbgSession, Protocols: []spec.Protocol{{Name: "range", Metadata: []byte(name + "-range")}}}
	call := w.input("", func(c spec.CallID) any { model.Call = c; return model })
	w.mu.Lock()
	w.abandoned[call] = true
	w.mu.Unlock()
	ctx, cancel := context.WithCancel(w.t.Context())
	done := w.run(ctx, call, func(ctx context.Context) string {
		_, err := w.n.api.JoinGroup(ctx, req)
		if status.Code(err) != codes.Canceled {
			return fmt.Sprintf("an answer to an abandoned call: %v", err)
		}
		return "abandoned"
	})
	w.waitState("PreparingRebalance") // the join opened a round and waits
	// Every call but the join has ended: a handler returns before its client
	// gets the answer.
	ended := w.n.ended.Load()
	cancel()
	<-done
	w.input("", func(spec.CallID) any { return spec.Abandon{Call: call} })
	// The answer of the next call depends on the abandon, so wait until the
	// handler of the join has returned: it has told the machine then. No other
	// call is in flight.
	for end := time.Now().Add(10 * time.Second); w.n.ended.Load() == ended; time.Sleep(time.Millisecond) {
		if time.Now().After(end) {
			w.t.Fatal("the handler of the abandoned join does not return")
		}
	}
}

// sync sends a SyncGroup. The leader gives assign (member name to bytes).
func (w *rbgWorld) sync(name string, gen int32, assign map[string]string) {
	req := &cpv1.SyncGroupRequest{Group: rbgGroup, MemberId: w.id(name), Generation: gen}
	model := spec.Sync{Group: rbgGroup, MemberID: req.MemberId, Generation: gen}
	for _, member := range slices.Sorted(maps.Keys(assign)) {
		req.Assignments = append(req.Assignments, &cpv1.GroupAssignment{MemberId: w.id(member), Assignment: []byte(assign[member])})
		model.Assignments = append(model.Assignments, contract.GroupAssignment{Member: w.id(member), Bytes: []byte(assign[member])})
	}
	call := w.input("", func(c spec.CallID) any { model.Call = c; return model })
	w.run(w.t.Context(), call, func(ctx context.Context) string {
		r, err := w.n.api.SyncGroup(ctx, req)
		if err != nil {
			return w.code(err)
		}
		return rbShow(runtime.OK, rbBytes(r.GetAssignment()))
	})
	if len(assign) > 0 {
		w.wait() // the leader's sync answers at once
	}
}

func (w *rbgWorld) heartbeat(name string, gen int32) {
	req := &cpv1.HeartbeatRequest{Group: rbgGroup, MemberId: w.id(name), Generation: gen}
	call := w.input("", func(c spec.CallID) any {
		return spec.Heartbeat{Call: c, Group: rbgGroup, MemberID: req.MemberId, Generation: gen}
	})
	<-w.run(w.t.Context(), call, func(ctx context.Context) string {
		if _, err := w.n.api.Heartbeat(ctx, req); err != nil {
			return w.code(err)
		}
		return rbShow(runtime.OK, nil)
	})
}

func (w *rbgWorld) leave(ids ...string) {
	req := &cpv1.LeaveGroupRequest{Group: rbgGroup, Members: ids}
	call := w.input("", func(c spec.CallID) any { return spec.Leave{Call: c, Group: rbgGroup, Members: ids} })
	<-w.run(w.t.Context(), call, func(ctx context.Context) string {
		r, err := w.n.api.LeaveGroup(ctx, req)
		if err != nil {
			return w.code(err)
		}
		var errs []runtime.ErrCode
		for _, c := range r.GetErrs() {
			errs = append(errs, codeFromWire(c))
		}
		return rbShow(runtime.OK, errs)
	})
}

// commit commits offset on partition 0 and offset+1 on partition 5, which does not exist.
func (w *rbgWorld) commit(name string, gen int32, offset int64) {
	op := contract.OffsetCommit{Group: rbgGroup, Member: w.id(name), Generation: gen, Offsets: []contract.PartitionOffset{
		{TopicPartition: contract.NewTopicPartition("t", 0), Offset: offset, Metadata: name},
		{TopicPartition: contract.NewTopicPartition("t", 5), Offset: offset + 1}}}
	call := w.input("", func(c spec.CallID) any { return spec.Commit{Call: c, Op: op} })
	<-w.run(w.t.Context(), call, func(ctx context.Context) string {
		d, err := w.n.send(ctx, op)
		if err != nil {
			return err.Error()
		}
		if d.Err != runtime.OK {
			return d.Err.String()
		}
		return rbShow(runtime.OK, d.Resp.(contract.OffsetCommitResp).Errs)
	})
}

// describe and list are calls of the machine too. JoinedTimeMs is the wall
// clock of the node, and the model has its own, so it is not compared.
func (w *rbgWorld) describe() {
	call := w.input("", func(c spec.CallID) any { return spec.Describe{Call: c, Group: rbgGroup} })
	<-w.run(w.t.Context(), call, func(ctx context.Context) string {
		d, err := w.n.send(ctx, contract.DescribeGroup{Group: rbgGroup})
		if err != nil || d.Err != runtime.OK {
			return fmt.Sprintf("%v %v", d.Err, err)
		}
		return rbShow(runtime.OK, fmt.Sprintf("%+v", noJoinTimes(d.Resp.(contract.DescribeGroupResp))))
	})
}

func (w *rbgWorld) list() {
	call := w.input("", func(c spec.CallID) any { return spec.List{Call: c} })
	<-w.run(w.t.Context(), call, func(ctx context.Context) string {
		d, err := w.n.send(ctx, contract.ListGroups{})
		if err != nil || d.Err != runtime.OK {
			return fmt.Sprintf("%v %v", d.Err, err)
		}
		return rbShow(runtime.OK, fmt.Sprintf("%+v", d.Resp.(contract.ListGroupsResp).Groups))
	})
}

func noJoinTimes(d contract.DescribeGroupResp) contract.DescribeGroupResp {
	d.Members = slices.Clone(d.Members)
	for i := range d.Members {
		d.Members[i].JoinedTimeMs = 0
	}
	return d
}

// fetch compares OffsetFetch, which reads the log of the node at once, with
// the log of the model. Call it after check.
func (w *rbgWorld) fetch() {
	op := contract.OffsetFetch{Group: rbgGroup}
	d, err := w.n.send(w.t.Context(), op)
	want := w.model.Store.Apply(op)
	if err != nil || !sameAnswer(d.Answer(), want) {
		w.t.Fatalf("offset fetch\n  model: %s\n  node:  %s %v", show(want), show(d.Answer()), err)
	}
}

// code is the error code of a failed call.
func (w *rbgWorld) code(err error) string {
	d, err := answerOf(err)
	if err != nil {
		return err.Error()
	}
	return d.Err.String()
}

// check gives the queued inputs to the model, and compares every answer.
func (w *rbgWorld) check() {
	w.t.Helper()
	w.wait()
	want := map[spec.CallID]string{}
	for _, q := range w.queue {
		if j, ok := q.in.(spec.Join); ok && q.newName != "" {
			w.mu.Lock()
			id := w.ids[q.newName]
			w.mu.Unlock()
			j.NewID = strings.TrimPrefix(id, j.ClientID+"-")
			q.in = j
		}
		for _, r := range w.model.Step(q.at, q.in) {
			want[spec.CallID(r.Call)] = rbgShowModel(r)
		}
	}
	for _, r := range w.model.Step(w.now, nil) {
		want[spec.CallID(r.Call)] = rbgShowModel(r)
	}
	w.queue = nil
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, call := range slices.Sorted(maps.Keys(w.got)) {
		got := w.got[call]
		if w.abandoned[call] {
			if got != "abandoned" {
				w.t.Fatalf("call %d: %s", call, got)
			}
			if a, ok := want[call]; ok {
				w.t.Fatalf("call %d: the model answered an abandoned call: %s", call, a)
			}
			continue
		}
		if got != want[call] {
			w.t.Fatalf("call %d\n  model: %s\n  node:  %s", call, want[call], got)
		}
		delete(want, call)
	}
	for call, a := range want {
		w.t.Fatalf("call %d: the model answered %s, the node did not answer", call, a)
	}
	clear(w.got)
	clear(w.abandoned)
}

// rbgShowModel shows a reply of the model; a description without join times.
func rbgShowModel(r spec.Reply) string {
	if d, ok := r.Body.(contract.DescribeGroupResp); ok {
		r.Body = noJoinTimes(d)
	}
	return rbShowModel(r)
}

// serving waits until the node serves groups: it has the role and has read
// the groups of the log. These calls are not given to the model: a list
// changes nothing.
func (w *rbgWorld) serving() {
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if d, err := w.n.send(w.t.Context(), contract.ListGroups{}); err == nil && d.Err == runtime.OK {
			return
		}
	}
	w.t.Fatal("the node does not serve groups")
}

// waitState waits until the node describes the group in state. These calls
// are not given to the model: a describe changes nothing.
func (w *rbgWorld) waitState(state string) {
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		d, err := w.n.send(w.t.Context(), contract.DescribeGroup{Group: rbgGroup})
		if err == nil && d.Err == runtime.OK && d.Resp.(contract.DescribeGroupResp).State == state {
			return
		}
	}
	w.t.Fatalf("the group is never %s", state)
}
