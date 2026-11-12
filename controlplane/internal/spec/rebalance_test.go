package spec

import (
	"fmt"
	"slices"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// These tests check the rebalance model against the rules of
// docs/rebalance.md, so that the machine can later be checked against it.

const sec = runtime.NodeTime(runtime.Second)

type rb struct {
	t         *testing.T
	r         *Rebalance
	now       runtime.NodeTime
	calls     CallID
	ids       int
	session   int32
	rebalance int32
}

// newRB starts a serving model with the default initial delay of 3s.
func newRB(t *testing.T) *rb {
	h := &rb{t: t, r: NewRebalance(3 * runtime.Second), session: 10000}
	h.r.Step(0, RoleChanged{Active: true})
	return h
}

// step runs one input at the current time and returns the replies by call.
func (h *rb) step(in any) map[CallID]Reply {
	out := map[CallID]Reply{}
	for _, r := range h.r.Step(h.now, in) {
		if _, twice := out[r.Call]; twice {
			h.t.Fatalf("call %d answered twice", r.Call)
		}
		out[r.Call] = r
	}
	return out
}

func (h *rb) wait(d runtime.NodeTime) map[CallID]Reply { h.now += d; return h.step(nil) }

func (h *rb) call() CallID { h.calls++; return h.calls }

// join sends a join of member ("" for a new one, named m1, m2, ...) and
// returns its call and the member id.
func (h *rb) join(group, member string, protos ...string) (CallID, string, map[CallID]Reply) {
	in := Join{Call: h.call(), Group: group, MemberID: member, ProtocolType: "consumer", SessionMs: h.session, RebalanceMs: h.rebalance}
	if member == "" {
		h.ids++
		in.NewID = fmt.Sprintf("m%d", h.ids)
		member = in.NewID
	}
	if len(protos) == 0 {
		protos = []string{"range"}
	}
	for _, p := range protos {
		in.Protocols = append(in.Protocols, Protocol{Name: p, Metadata: []byte("meta-" + p)})
	}
	return in.Call, member, h.step(in)
}

func (h *rb) sync(group, member string, gen int32, assigns ...string) (CallID, map[CallID]Reply) {
	in := Sync{Call: h.call(), Group: group, MemberID: member, Generation: gen}
	for i := 0; i+1 < len(assigns); i += 2 {
		in.Assignments = append(in.Assignments, contract.GroupAssignment{Member: assigns[i], Bytes: []byte(assigns[i+1])})
	}
	return in.Call, h.step(in)
}

func (h *rb) heartbeat(group, member string, gen int32) runtime.ErrCode {
	c := h.call()
	return h.step(Heartbeat{Call: c, Group: group, MemberID: member, Generation: gen})[c].Err
}

func (h *rb) commit(group, member string, gen int32) runtime.ErrCode {
	c := h.call()
	op := contract.OffsetCommit{Group: group, Member: member, Generation: gen, Offsets: []contract.PartitionOffset{{TopicPartition: contract.NewTopicPartition("t", 0), Offset: 5}}}
	return h.step(Commit{Call: c, Op: op})[c].Err
}

// describe is the Describe call: the log's description with the memory state.
func (h *rb) describe(group string) contract.DescribeGroupResp {
	c := h.call()
	d, _ := h.step(Describe{Call: c, Group: group})[c].Body.(contract.DescribeGroupResp)
	return d
}

// logged is the members the log has for group.
func (h *rb) logged(group string) []string {
	d, _ := h.r.Store.Apply(contract.DescribeGroup{Group: group}).Resp.(contract.DescribeGroupResp)
	var ids []string
	for _, m := range d.Members {
		ids = append(ids, m.MemberID)
	}
	return ids
}

func joined(t *testing.T, r Reply, gen int32, leader string, members int) {
	t.Helper()
	j, _ := r.Body.(JoinResult)
	if r.Err != runtime.OK || j.Generation != gen || j.Leader != leader || len(j.Members) != members {
		t.Fatalf("join reply %+v, want generation %d, leader %s, %d members", r, gen, leader, members)
	}
}

// stableTwo makes group g Stable at generation 2 with members m1 (the leader) and m2.
func (h *rb) stableTwo() {
	ca, a, _ := h.join("g", "")
	joined(h.t, h.wait(3 * sec)[ca], 1, a, 1)
	h.sync("g", a, 1, a, "a1")
	cb, b, _ := h.join("g", "")
	ca, _, out := h.join("g", a)
	joined(h.t, out[ca], 2, a, 2)
	joined(h.t, out[cb], 2, a, 0)
	cs, out := h.sync("g", b, 2)
	if len(out) != 0 {
		h.t.Fatalf("follower sync answered before the leader: %+v", out)
	}
	cl, out := h.sync("g", a, 2, a, "a2", b, "b2")
	if string(out[cs].Body.([]byte)) != "b2" || string(out[cl].Body.([]byte)) != "a2" {
		h.t.Fatalf("leader sync: %+v", out)
	}
}

func TestRebalanceOneMemberJoinsAndSyncs(t *testing.T) {
	h := newRB(t)
	c, id, out := h.join("g", "")
	if len(out) != 0 || h.describe("g").State != PhaseJoining {
		t.Fatalf("a new group waits the initial delay: %+v %s", out, h.r.Phase("g"))
	}
	r := h.wait(3 * sec)[c]
	joined(t, r, 1, id, 1)
	if j := r.Body.(JoinResult); j.Protocol != "range" || string(j.Members[0].Metadata) != "meta-range" {
		t.Fatalf("protocol or metadata: %+v", j)
	}
	if d := h.describe("g"); d.State != PhaseSyncing || d.Generation != 1 {
		t.Fatalf("describe after the round: %+v", d)
	}
	if c, out := h.sync("g", id, 1, id, "all"); string(out[c].Body.([]byte)) != "all" {
		t.Fatalf("leader sync: %+v", out)
	}
	if d := h.describe("g"); d.State != PhaseStable || d.AssignedGeneration != 1 {
		t.Fatalf("describe: %+v", d)
	}
	if d := h.describe("nope"); d.State != PhaseDead || d.Exists {
		t.Fatalf("describe of an unknown group: %+v", d)
	}
}

func TestRebalanceNewMemberStartsARound(t *testing.T) {
	h := newRB(t)
	h.stableTwo()
	c3, m3, _ := h.join("g", "")
	c1, _, _ := h.join("g", "m1")
	c2, _, out := h.join("g", "m2")
	joined(t, out[c1], 3, "m1", 3)
	joined(t, out[c2], 3, "m1", 0)
	joined(t, out[c3], 3, "m1", 0)
	if got := h.logged("g"); len(got) != 3 || got[2] != m3 {
		t.Fatalf("logged members: %v", got)
	}
}

func TestRebalanceChecksMemberAndGeneration(t *testing.T) {
	h := newRB(t)
	h.stableTwo()
	for _, tc := range []struct {
		group, member string
		gen           int32
		want          runtime.ErrCode
	}{
		{"nope", "m1", 2, runtime.ErrUnknownMember},
		{"g", "zz", 2, runtime.ErrUnknownMember},
		{"g", "m1", 1, runtime.ErrIllegalGeneration},
		{"g", "m1", 2, runtime.OK},
	} {
		if got := h.heartbeat(tc.group, tc.member, tc.gen); got != tc.want {
			t.Errorf("heartbeat %+v: got %v", tc, got)
		}
		if c, out := h.sync(tc.group, tc.member, tc.gen); out[c].Err != tc.want {
			t.Errorf("sync %+v: got %v", tc, out[c].Err)
		}
	}
	// The round is open: sync is refused; the leader's assignment is checked.
	h.join("g", "m1")
	if c, out := h.sync("g", "m1", 2); out[c].Err != runtime.ErrRebalanceInProgress {
		t.Fatalf("sync in a round: %+v", out)
	}
	h.join("g", "m2")
	if c, out := h.sync("g", "m1", 3, "zz", "x"); out[c].Err != runtime.ErrUnknownMember {
		t.Fatalf("assignment to an unknown member: %+v", out)
	}
	if c, out := h.sync("g", "m1", 3, "m1", "x", "m1", "y"); out[c].Err != runtime.ErrInvalid {
		t.Fatalf("member assigned twice: %+v", out)
	}
}

// Kafka's heartbeat: PreparingRebalance is ErrRebalanceInProgress,
// CompletingRebalance and Stable are OK, and an Empty group has no members.
func TestRebalanceHeartbeatByState(t *testing.T) {
	h := newRB(t)
	h.stableTwo()
	if code := h.heartbeat("g", "m1", 2); code != runtime.OK {
		t.Fatalf("Stable: %v", code)
	}
	h.join("g", "m1")
	if code := h.heartbeat("g", "m2", 2); code != runtime.ErrRebalanceInProgress || h.r.Phase("g") != PhaseJoining {
		t.Fatalf("PreparingRebalance: %v", code)
	}
	h.join("g", "m2")
	if code := h.heartbeat("g", "m2", 3); code != runtime.OK || h.r.Phase("g") != PhaseSyncing {
		t.Fatalf("CompletingRebalance: %v", code)
	}
	h.step(Leave{Call: h.call(), Group: "g", Members: []string{"m1", "m2"}})
	if code := h.heartbeat("g", "m2", 3); code != runtime.ErrUnknownMember || h.r.Phase("g") != PhaseEmpty {
		t.Fatalf("Empty: %v", code)
	}
}

func TestRebalanceLeave(t *testing.T) {
	h := newRB(t)
	h.stableTwo()
	c := h.call()
	if codes := h.step(Leave{Call: c, Group: "g", Members: []string{"m2", "nobody"}})[c].Body.([]runtime.ErrCode); !slices.Equal(codes, []runtime.ErrCode{runtime.OK, runtime.ErrUnknownMember}) {
		t.Fatalf("leave codes: %v", codes)
	}
	c1, _, out := h.join("g", "m1")
	joined(t, out[c1], 3, "m1", 1)
	h.step(Leave{Call: h.call(), Group: "g", Members: []string{"m1"}})
	if d := h.describe("g"); d.Generation != 3 || d.State != PhaseEmpty {
		t.Fatalf("after the last leave: %+v", d)
	}
}

func TestRebalanceSessionExpiry(t *testing.T) {
	h := newRB(t)
	h.session = 5000
	h.stableTwo()
	for range 4 {
		h.wait(sec)
		h.heartbeat("g", "m1", 2)
	}
	h.wait(sec)
	if h.r.Phase("g") != PhaseJoining || len(h.logged("g")) != 1 || h.heartbeat("g", "m2", 2) != runtime.ErrUnknownMember {
		t.Fatalf("silent m2 not removed: %s %v", h.r.Phase("g"), h.logged("g"))
	}
}

// An existing member that waits in a join or a sync is not removed by its
// session timer (Kafka hasSatisfiedHeartbeat).
func TestRebalanceWaitingMemberDoesNotExpire(t *testing.T) {
	h := newRB(t)
	h.session, h.rebalance = 5000, 20000
	h.stableTwo()
	c1, _, _ := h.join("g", "m1")
	for range 8 { // m1 waits past its session; m2 beats but does not join yet
		h.wait(sec)
		h.heartbeat("g", "m2", 2)
	}
	c2, _, out := h.join("g", "m2")
	joined(t, out[c1], 3, "m1", 2)
	joined(t, out[c2], 3, "m1", 0)
	cs, _ := h.sync("g", "m2", 3)
	for range 8 { // m2 waits in its sync past its session; the leader beats
		h.wait(sec)
		h.heartbeat("g", "m1", 3)
	}
	if _, out := h.sync("g", "m1", 3, "m2", "b"); string(out[cs].Body.([]byte)) != "b" {
		t.Fatalf("waiting follower: %+v", out)
	}
}

// A new member can be removed while it waits in its first join, by the new
// member timeout. Its join gets ErrUnknownMember.
func TestRebalanceNewMemberExpires(t *testing.T) {
	h := newRB(t)
	h.rebalance = 20000
	h.stableTwo()
	h.r.NewMemberTimeout = 4 * runtime.Second
	c3, _, _ := h.join("g", "")
	c1, _, _ := h.join("g", "m1")
	var out map[CallID]Reply
	for range 4 { // m2 beats but does not join; m3 waits as a new member
		out = h.wait(sec)
		h.heartbeat("g", "m2", 2)
	}
	if out[c3].Err != runtime.ErrUnknownMember || h.r.Phase("g") != PhaseJoining {
		t.Fatalf("new member not removed: %+v", out)
	}
	c2, _, out := h.join("g", "m2")
	joined(t, out[c1], 3, "m1", 2)
	joined(t, out[c2], 3, "m1", 0)
}

// At the round end (the join timeout) the members that did not join again
// are removed, even when they beat.
func TestRebalanceJoinTimeoutRemovesSilentMembers(t *testing.T) {
	h := newRB(t)
	h.rebalance = 5000
	h.stableTwo()
	c1, _, _ := h.join("g", "m1")
	var out map[CallID]Reply
	for range 5 {
		out = h.wait(sec)
		h.heartbeat("g", "m2", 2)
	}
	joined(t, out[c1], 3, "m1", 1)
	if got := h.logged("g"); !slices.Equal(got, []string{"m1"}) || h.heartbeat("g", "m2", 2) != runtime.ErrUnknownMember {
		t.Fatalf("m2 kept: %v", got)
	}
}

// At the sync deadline the members that sent no sync are removed and a round
// opens; waiting syncs get ErrRebalanceInProgress.
func TestRebalanceSyncTimeout(t *testing.T) {
	h := newRB(t)
	h.rebalance = 5000
	h.join("g", "")
	h.join("g", "")
	h.wait(3 * sec)
	cs, _ := h.sync("g", "m2", 1)
	var out map[CallID]Reply
	for range 5 { // the leader m1 beats in CompletingRebalance but never syncs
		if code := h.heartbeat("g", "m1", 1); code != runtime.OK {
			t.Fatalf("leader heartbeat: %v", code)
		}
		out = h.wait(sec)
	}
	if out[cs].Err != runtime.ErrRebalanceInProgress || h.r.Phase("g") != PhaseJoining || !slices.Equal(h.logged("g"), []string{"m2"}) {
		t.Fatalf("leader that never synced: %+v %s %v", out, h.r.Phase("g"), h.logged("g"))
	}
	c2, _, out := h.join("g", "m2")
	joined(t, out[c2], 2, "m2", 1)
	// In Stable too: a follower that never syncs is removed.
	c4, m4, _ := h.join("g", "")
	c2, _, out = h.join("g", "m2")
	joined(t, out[c2], 3, "m2", 2)
	joined(t, out[c4], 3, "m2", 0)
	h.sync("g", "m2", 3)
	for range 5 {
		h.wait(sec)
		h.heartbeat("g", m4, 3)
	}
	if h.r.Phase("g") != PhaseJoining || h.heartbeat("g", m4, 3) != runtime.ErrUnknownMember {
		t.Fatalf("follower that never synced: %s %v", h.r.Phase("g"), h.logged("g"))
	}
}

func TestRebalanceAbandonedSync(t *testing.T) {
	h := newRB(t)
	h.join("g", "")
	h.join("g", "")
	h.wait(3 * sec)
	cs, _ := h.sync("g", "m2", 1)
	h.step(Abandon{Call: cs})
	if _, out := h.sync("g", "m1", 1, "m2", "x"); out[cs].Call != 0 {
		t.Fatalf("abandoned sync answered: %+v", out)
	}
}

// Offset commit: Kafka's checks on memory first, then the log's.
func TestRebalanceOffsetCommit(t *testing.T) {
	h := newRB(t)
	h.r.Store.Apply(contract.CreateTopic{Name: "t", Partitions: 2})
	h.stableTwo()
	h.join("g", "")
	h.join("g", "m1")
	h.join("g", "m2") // generation 3, CompletingRebalance
	for _, tc := range []struct {
		member string
		gen    int32
		want   runtime.ErrCode
	}{
		{"m1", 3, runtime.ErrRebalanceInProgress},
		{"zz", 3, runtime.ErrUnknownMember},
		{"m1", 2, runtime.ErrIllegalGeneration},
		{"", -1, runtime.ErrUnknownMember},
	} {
		if got := h.commit("g", tc.member, tc.gen); got != tc.want {
			t.Errorf("CompletingRebalance commit %+v: %v", tc, got)
		}
	}
	h.sync("g", "m1", 3)
	if got := h.commit("g", "m1", 3); got != runtime.OK {
		t.Fatalf("Stable commit: %v", got)
	}
	h.step(Leave{Call: h.call(), Group: "g", Members: []string{"m1", "m2", "m3"}})
	if got, gen := h.commit("g", "", -1), h.commit("g", "m1", 3); got != runtime.OK || gen != runtime.ErrUnknownMember {
		t.Fatalf("Empty group: generation -1 %v, member %v", got, gen)
	}
	if got := h.commit("fresh", "", -1); got != runtime.OK {
		t.Fatalf("a group with no memory: %v", got)
	}
	f, _ := h.r.Store.Apply(contract.OffsetFetch{Group: "fresh"}).Resp.(contract.OffsetFetchResp)
	c := h.call()
	l, _ := h.step(List{Call: c})[c].Body.(contract.ListGroupsResp)
	if len(f.Offsets) != 1 || f.Offsets[0].Offset != 5 || len(l.Groups) != 1 || l.Groups[0].State != PhaseEmpty {
		t.Fatalf("fetch %+v, list %+v", f, l)
	}
}

func TestRebalanceProtocolsAndBadJoins(t *testing.T) {
	h := newRB(t)
	ca, _, _ := h.join("q", "", "range")
	cb, _, _ := h.join("q", "", "roundrobin")
	if out := h.wait(3 * sec); out[ca].Err != runtime.ErrInconsistentGroupProtocol || out[cb].Err != runtime.ErrInconsistentGroupProtocol {
		t.Fatalf("no common protocol: %+v", out)
	}
	h.join("q", "m1", "range", "roundrobin")
	cb, _, out := h.join("q", "m2", "roundrobin")
	if j, _ := out[cb].Body.(JoinResult); j.Protocol != "roundrobin" || j.Generation != 1 {
		t.Fatalf("retry: %+v", out)
	}
	if _, _, out := h.join("q", "nobody"); out[h.calls].Err != runtime.ErrUnknownMember {
		t.Fatalf("unknown member id: %+v", out)
	}
	c := h.call()
	if out := h.step(Join{Call: c, Group: "q", ProtocolType: "connect", SessionMs: 1, Protocols: []Protocol{{Name: "x"}}}); out[c].Err != runtime.ErrInconsistentGroupProtocol {
		t.Fatalf("other protocol type: %+v", out)
	}
	if out := h.step(Join{Call: c + 1, Group: "q", ProtocolType: "consumer", Protocols: []Protocol{{Name: "x"}}}); out[c+1].Err != runtime.ErrInvalid {
		t.Fatalf("zero session: %+v", out)
	}
}

func TestRebalanceRoleChange(t *testing.T) {
	h := newRB(t)
	h.session = 5000
	h.stableTwo()
	c, _, _ := h.join("p", "")
	if out := h.step(RoleChanged{Active: false}); out[c].Err != runtime.ErrUnavailable {
		t.Fatalf("waiting join after the role is lost: %+v", out)
	}
	if code := h.heartbeat("g", "m1", 2); code != runtime.ErrUnavailable {
		t.Fatalf("call while not active: %v", code)
	}
	h.wait(60 * sec)
	h.step(RoleChanged{Active: true})
	if h.r.Phase("g") != PhaseStable || h.r.Phase("p") != PhaseDead {
		t.Fatalf("after takeover: g %s, p %s", h.r.Phase("g"), h.r.Phase("p"))
	}
	h.wait(4 * sec) // the sessions start at the takeover
	if h.heartbeat("g", "m1", 2) != runtime.OK {
		t.Fatal("m1 expired in the grace window")
	}
	h.wait(1500 * runtime.NodeTime(runtime.Millisecond))
	if h.r.Phase("g") != PhaseJoining || len(h.logged("g")) != 1 {
		t.Fatalf("silent m2 kept: %v", h.logged("g"))
	}
}
