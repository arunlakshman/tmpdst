package rebalance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Ports of the tests of the old coordinator (groups/rules_test.go, deleted in T6.6).

func TestProtocolMismatch(t *testing.T) {
	h := newHarness(t, Config{})
	other := h.join(2, "p", "")
	other.ProtocolType = "connect"
	h.run([]step{
		{0, h.join(1, "p", ""), nil},
		// A second member with another protocol type is refused at once.
		{0, other, []string{"2 ErrInconsistentGroupProtocol"}},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
	})

	// Two members with no protocol in common: both calls fail, and the group
	// stays in its round, so a retry can finish it.
	h2 := newHarness(t, Config{})
	a, b := h2.id(1), h2.id(2)
	h2.run([]step{
		{0, h2.join(1, "q", "", "range"), nil},
		{0, h2.join(2, "q", "", "roundrobin"), nil},
		{delay, nil, []string{"1 ErrInconsistentGroupProtocol", "2 ErrInconsistentGroupProtocol"}},
	})
	h2.check("after mismatch", []any{h2.phase("q"), h2.members("q")}, []any{stateJoining, []string{"m1", "m2"}})
	h2.run([]step{
		{delay, h2.join(3, "q", a, "range", "roundrobin"), nil},
		{delay, h2.join(4, "q", b, "roundrobin"), []string{
			"3 OK gen=1 proto=roundrobin leader=m1 id=m1 members=[m1:meta-roundrobin m2:meta-roundrobin]",
			"4 OK gen=1 proto=roundrobin leader=m1 id=m2",
		}},
	})
}

func TestEmptyMemberIDAndBadJoins(t *testing.T) {
	h := newHarness(t, Config{})
	withClient := h.join(1, "g", "")
	withClient.ClientID = "cli"
	h.run([]step{
		{0, withClient, nil},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=cli-m1 id=cli-m1 members=[cli-m1:meta-range]"}},
		{delay, h.join(2, "g", "nobody"), []string{"2 ErrUnknownMember"}},
	})
	// Rule 1, also before rule 2 (a node that does not serve still says ErrInvalid).
	bad := []func(*Join){
		func(j *Join) { j.SessionTimeoutMs = 0 },
		func(j *Join) { j.SessionTimeoutMs = -1 },
		func(j *Join) { j.Group = "" },
		func(j *Join) { j.ProtocolType = "" },
		func(j *Join) { j.Protocols = nil },
	}
	for i, change := range bad {
		j := h.join(CallID(10+i), "g", "")
		change(&j)
		want := fmt.Sprintf("%d ErrInvalid", 10+i)
		h.check(fmt.Sprintf("bad join %d", i), h.step(j), []string{want})
	}
	h.step(RoleChanged{Active: false})
	j := h.join(20, "g", "")
	j.SessionTimeoutMs = 0
	h.check("bad join on standby", h.step(j), []string{"20 ErrInvalid"})
}

func TestSyncRules(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, syncReq(2, "nope", a, 1), []string{"2 ErrUnknownMember"}},
		// The round is open: a sync is refused with a rebalance in progress.
		{0, syncReq(3, "g", a, 0), []string{"3 ErrRebalanceInProgress"}},
		{0, syncReq(4, "g", "zz", 0), []string{"4 ErrUnknownMember"}},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
		{delay, syncReq(5, "g", a, 9), []string{"5 ErrIllegalGeneration"}},
		{delay, syncReq(6, "g", "zz", 1), []string{"6 ErrUnknownMember"}},
		{delay, syncReq(7, "g", a, 1, as("zz", "")), []string{"7 ErrUnknownMember"}},
		{delay, syncReq(8, "g", a, 1, as(a, ""), as(a, "")), []string{"8 ErrInvalid"}},
		{delay, syncReq(9, "g", a, 1, as(a, "ok")), []string{"9 OK assign=ok"}},
		// Stable: the stored assignment comes back at once.
		{delay, syncReq(10, "g", a, 1), []string{"10 OK assign=ok"}},
	})
}

// TestOffsetCommitChecks checks rule 31. Changed on purpose (section 6):
// commit("", -1) on a group with members is ErrUnknownMember, as in Kafka.
// OffsetFetch is not a machine call any more: the log is read directly.
func TestOffsetCommitChecks(t *testing.T) {
	h := newHarness(t, Config{})
	if d := h.log.Apply(contract.CreateTopic{Name: "t", Partitions: 2}); d.Err != runtime.OK {
		t.Fatalf("topic: %v", d.Err)
	}
	a, b := h.stableTwo("g", 0, 1, 1)
	tp0 := contract.NewTopicPartition("t", 0)
	commit := func(call CallID, group, member string, gen int32, offs ...contract.PartitionOffset) OffsetCommit {
		return OffsetCommit{Call: call, Op: contract.OffsetCommit{Group: group, Member: member, Generation: gen, Offsets: offs}}
	}
	off := func(tp contract.TopicPartition, o int64) contract.PartitionOffset {
		return contract.PartitionOffset{TopicPartition: tp, Offset: o}
	}
	h.run([]step{
		{delay, commit(10, "g", a, 2, off(tp0, 5), off(contract.NewTopicPartition("t", 9), 1), off(tp0, -1)),
			[]string{"10 OK [OK ErrUnknownPartition ErrInvalid]"}},
		{delay, commit(11, "g", a, 1, off(tp0, 6)), []string{"11 ErrIllegalGeneration"}},
		{delay, commit(12, "g", "zz", 2, off(tp0, 6)), []string{"12 ErrUnknownMember"}},
		{delay, commit(13, "g", "", -1, off(tp0, 6)), []string{"13 ErrUnknownMember"}},
		{delay, commit(14, "fresh", "", -1, off(tp0, 3)), []string{"14 OK [OK]"}},
		// "fresh" is not in memory, so List does not show it.
		{delay, List{Call: 15}, []string{"15 OK g:consumer:Stable"}},
	})
	f := contract.ResponseAs[contract.OffsetFetchResp](h.log.Apply(contract.OffsetFetch{Group: "g", Partitions: []contract.TopicPartition{tp0}}))
	h.check("fetch", f.Offsets[0].Offset, 5)
	// New (rule 31): PreparingRebalance takes a commit, CompletingRebalance does not.
	h.run([]step{
		{delay, h.join(16, "g", ""), nil},
		{delay, commit(17, "g", a, 2, off(tp0, 7)), []string{"17 OK [OK]"}},
		{delay, h.join(18, "g", a), nil},
		{delay, h.join(19, "g", b), []string{
			"18 OK gen=3 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range m3:meta-range]",
			"19 OK gen=3 proto=range leader=m1 id=m2", "16 OK gen=3 proto=range leader=m1 id=m3",
		}},
		{delay, commit(20, "g", a, 3, off(tp0, 8)), []string{"20 ErrRebalanceInProgress"}},
	})
}

// TestLeaderNeverSyncs checks the sync deadline (rule 24). Changed on
// purpose (section 6): the leader that never syncs is removed at the
// deadline, so its rejoin is ErrUnknownMember and the next round has one
// member. The abandoned sync of the old test is TestParkedSyncAbandoned.
func TestLeaderNeverSyncs(t *testing.T) {
	h := newHarness(t, Config{})
	h.rebalance = 5000 // shorter than the session, so the deadline comes first
	a, b := h.id(1), h.id(2)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, h.join(2, "g", ""), nil},
		{delay, nil, []string{
			"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range]",
			"2 OK gen=1 proto=range leader=m1 id=m2",
		}},
		// B is a follower and parks. A never syncs.
		{delay, syncReq(3, "g", b, 1), nil},
		{delay + 5*sec - 1, nil, nil},
		{delay + 5*sec, nil, []string{"3 ErrRebalanceInProgress"}},
	})
	h.check("after the deadline", []any{h.phase("g"), h.members("g"), h.stored("g")}, []any{stateJoining, []string{"m2"}, []string{"m2"}})
	h.run([]step{
		{8 * sec, hb(4, "g", b, 1), []string{"4 ErrRebalanceInProgress"}},
		{8 * sec, h.join(5, "g", a), []string{"5 ErrUnknownMember"}},
		{8 * sec, h.join(6, "g", b), []string{"6 OK gen=2 proto=range leader=m2 id=m2 members=[m2:meta-range]"}},
	})
}

func TestRebuildKeepsMembersAndExpiresSilent(t *testing.T) {
	h := newHarness(t, Config{})
	h.session = 5000
	a, _ := h.stableTwo("g", 0, 1, 1)
	// A second group is joined but never synced: its generation is not assigned.
	h.session = 10000
	h.run([]step{
		{delay, h.join(10, "unsynced", ""), nil},
		{2 * delay, nil, []string{"10 OK gen=1 proto=range leader=m3 id=m3 members=[m3:meta-range]"}},
	})
	// Failover: the old machine loses the role, a new one takes over from the log.
	h.check("losing the role", h.step(RoleChanged{Active: false}), []string(nil))
	h.check("timers left", len(h.timers), 0)
	h.m = New(Config{}, h.env)
	h.step(RoleChanged{Active: true})
	h.check("rebuilt g", []any{h.phase("g"), h.group("g").generation, h.members("g")}, []any{stateStable, 2, []string{"m1", "m2"}})
	h.check("rebuilt unsynced", []any{h.phase("unsynced"), h.members("unsynced")}, []any{stateJoining, []string{"m3"}})
	// Grace: nobody expires before the session timeout from the takeover.
	h.run([]step{{2*delay + 4*sec, hb(11, "g", a, 2), []string{"11 OK"}}})
	h.check("during grace", []any{h.phase("g"), h.members("g")}, []any{stateStable, []string{"m1", "m2"}})
	h.run([]step{{2*delay + 5500*msec, nil, nil}})
	h.check("silent member", []any{h.phase("g"), h.members("g"), h.stored("g")}, []any{stateJoining, []string{"m1"}, []string{"m1"}})
}

// TestCloseWakesParkedCalls: losing the role answers parked calls and stops
// every timer (rule 34). The old Rebuild-after-Close check is dropped: the
// machine has no Close; its host stops it.
func TestCloseWakesParkedCalls(t *testing.T) {
	h := newHarness(t, Config{})
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, RoleChanged{Active: false}, []string{"1 ErrUnavailable"}},
		{0, h.join(2, "g", ""), []string{"2 ErrUnavailable"}},
	})
	h.check("timers left", len(h.timers), 0)
}

// TestStoreFailureBreaksTermAndRebuildRecovers: a failed install ends the
// term (rule 36). The machine reads the log again with fresh reads by itself;
// the old test needed Stop and Rebuild from the node.
func TestStoreFailureBreaksTermAndRebuildRecovers(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	h.run([]step{{0, h.join(1, "g", ""), nil}, {delay, nil, nil}})
	h.check("held install", heldOps(h), "[GroupInstall]")
	h.check("failed install", h.answer(0, runtime.ErrIO), []string{"1 ErrUnavailable"})
	h.check("phase", h.phase("g"), "loading")
	h.check("fresh read", heldOps(h), "[ListGroups fresh]")
	h.check("join on a broken term", h.step(h.join(2, "g", "")), []string{"2 ErrUnavailable"})
	h.answer(0, runtime.OK)
	h.hold = false
	h.run([]step{
		{delay, h.join(3, "g", ""), nil},
		{2 * delay, nil, []string{"3 OK gen=1 proto=range leader=m2 id=m2 members=[m2:meta-range]"}},
	})
}

// heldOps names the held submits.
func heldOps(h *harness) string {
	var out []string
	for _, o := range h.held {
		switch o := o.(type) {
		case SubmitCommand:
			out = append(out, strings.TrimPrefix(fmt.Sprintf("%T", o.Op), "contract."))
		case SubmitRead:
			name := strings.TrimPrefix(fmt.Sprintf("%T", o.Op), "contract.")
			if o.Fresh {
				name += " fresh"
			}
			out = append(out, name)
		case Reply, SetTimer, CancelTimer, SendSelf:
			// Not a submit: held holds only submits.
		}
	}
	return fmt.Sprint(out)
}

// TestConcurrentJoinsOneRound: many joins at once are just inputs in order
// for the machine (rule 37). They end in one round with one leader, and
// every member gets its own assignment.
func TestConcurrentJoinsOneRound(t *testing.T) {
	h := newHarness(t, Config{InitialDelay: 200 * msec})
	const n = 8
	h.session = 30000
	for i := range n {
		j := h.join(CallID(i+1), "g", "")
		j.ClientID = fmt.Sprintf("c%d", i)
		j.Protocols[0].Metadata = []byte(j.ClientID)
		h.check("parked", h.step(j), []string(nil))
	}
	h.check("before the delay", h.until(199*msec), []string(nil))
	h.until(200 * msec)
	replies := repliesIn(h.outs)
	if len(replies) != n {
		t.Fatalf("got %d join answers, want %d", len(replies), n)
	}
	var leader string
	var ids []string
	for _, r := range replies {
		ja, ok := r.Body.(JoinAnswer)
		if r.Err != runtime.OK || !ok || ja.Generation != 1 || ja.Protocol != "range" {
			t.Fatalf("join answer: %+v", r)
		}
		if ja.Members != nil {
			leader = ja.MemberID
			h.check("leader sees", len(ja.Members), n)
		}
		ids = append(ids, ja.MemberID)
	}
	h.check("leader", h.short(leader), "c0-m1")
	var assigns []Assignment
	for _, id := range ids {
		assigns = append(assigns, as(id, "as-"+id))
	}
	var got []string
	for i, id := range ids {
		got = append(got, h.step(syncReq(CallID(100+i), "g", id, 1, assigns...))...)
	}
	var want []string
	for i, id := range ids {
		want = append(want, h.short(fmt.Sprintf("%d OK assign=as-%s", 100+i, id)))
	}
	if !sameSet(got, want) {
		t.Fatalf("syncs:\n got  %q\n want %q", got, want)
	}
}

func repliesIn(outs []Output) []Reply {
	var out []Reply
	for _, o := range outs {
		if r, ok := o.(Reply); ok && r.Err == runtime.OK {
			if _, ok := r.Body.(JoinAnswer); ok {
				out = append(out, r)
			}
		}
	}
	return out
}
