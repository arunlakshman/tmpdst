package rebalance

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Ports of the tests of the old coordinator (groups/coordinator_test.go, deleted in T6.6).

func TestOneMemberJoinsAndSyncs(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay - 1, nil, nil},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
		{delay, syncReq(2, "g", a, 1, as(a, "all")), []string{"2 OK assign=all"}},
		{delay, hb(3, "g", a, 1), []string{"3 OK"}},
		{delay, Describe{Call: 4, Groups: []string{"g"}}, []string{"4 OK g:Stable gen=1 assigned=1 members=[m1]"}},
	})
}

func TestSecondMemberRebalancesStableGroup(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.stableTwo("g", 0, 1, 1)
	g := h.group("g")
	h.check("after two members", []any{g.phase.state(), g.generation, h.short(g.leader)}, []any{stateStable, 2, "m1"})
	c := h.id(3)
	h.run([]step{
		{delay, hb(10, "g", a, 2), []string{"10 OK"}},
		// A third member joins a Stable group: the others must rejoin.
		{delay, h.join(11, "g", ""), nil},
		{delay, hb(12, "g", a, 2), []string{"12 ErrRebalanceInProgress"}},
		{delay, h.join(13, "g", a), nil},
		{delay, h.join(14, "g", b), []string{
			"13 OK gen=3 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range m3:meta-range]",
			"14 OK gen=3 proto=range leader=m1 id=m2",
			"11 OK gen=3 proto=range leader=m1 id=m3",
		}},
	})
	h.check("stored members", h.stored("g"), []string{"m1", "m2", "m3"})
	h.run([]step{
		{delay, syncReq(15, "g", b, 3), nil},
		{delay, syncReq(16, "g", a, 3, as(a, "a3"), as(b, "b3"), as(c, "c3")), []string{"15 OK assign=b3", "16 OK assign=a3"}},
		{delay, syncReq(17, "g", c, 3), []string{"17 OK assign=c3"}},
	})
}

func TestLeaveRebalances(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.stableTwo("g", 0, 1, 1)
	h.run([]step{
		{delay, leave(10, "g", b, "nobody"), []string{"10 OK [OK ErrUnknownMember]"}},
		{delay, hb(11, "g", a, 2), []string{"11 ErrRebalanceInProgress"}},
		{delay, h.join(12, "g", a), []string{"12 OK gen=3 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
	})
	h.check("stored", h.stored("g"), []string{"m1"})
	// The last member leaves: the group is Empty and keeps its generation.
	h.run([]step{{delay, leave(13, "g", a), []string{"13 OK [OK]"}}})
	h.check("after last leave", []any{h.phase("g"), h.group("g").generation}, []any{stateEmpty, 3})
}

func TestSessionExpiryRemovesSilentMember(t *testing.T) {
	h := newHarness(t, Config{})
	h.session = 5000
	a, b := h.stableTwo("g", 0, 1, 1)
	// The leader's sync starts the sessions of the leader and of the parked
	// follower again at 3 s. A beats each second, B stays silent and expires at 8 s.
	h.run([]step{
		{4 * sec, hb(10, "g", a, 2), []string{"10 OK"}},
		{5 * sec, hb(11, "g", a, 2), []string{"11 OK"}},
		{6 * sec, hb(12, "g", a, 2), []string{"12 OK"}},
		{7 * sec, hb(13, "g", a, 2), []string{"13 OK"}},
		{8*sec - 1, nil, nil},
	})
	h.check("before expiry", h.members("g"), []string{"m1", "m2"})
	h.run([]step{{8 * sec, nil, nil}})
	h.check("after expiry", []any{h.phase("g"), h.members("g"), h.stored("g")}, []any{stateJoining, []string{"m1"}, []string{"m1"}})
	h.run([]step{
		{8 * sec, hb(14, "g", b, 2), []string{"14 ErrUnknownMember"}},
		{8 * sec, h.join(15, "g", a), []string{"15 OK gen=3 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
	})
}

// TestHeartbeatTable checks rule 25 in every state. Changed on purpose
// (docs/rebalance.md section 6): "completing and right" is OK, as in Kafka.
func TestHeartbeatTable(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.run([]step{{0, h.join(1, "g", ""), nil}, {delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}}})
	cases := []struct {
		name  string
		setup any // an input before the heartbeat, nil for none
		group string
		id    string
		gen   int32
		want  runtime.ErrCode
	}{
		{"unknown group", nil, "nope", a, 1, runtime.ErrUnknownMember},
		{"unknown member", nil, "g", "zz", 1, runtime.ErrUnknownMember},
		{"wrong generation", nil, "g", a, 0, runtime.ErrIllegalGeneration},
		{"completing and right", nil, "g", a, 1, runtime.OK},
		{"stable and right", syncReq(90, "g", a, 1, as(a, "x")), "g", a, 1, runtime.OK},
		{"preparing and right", h.join(91, "g", ""), "g", a, 1, runtime.ErrRebalanceInProgress},
		{"empty", leave(92, "g", a, h.id(2)), "g", a, 1, runtime.ErrUnknownMember},
	}
	for i, tc := range cases {
		if tc.setup != nil {
			h.step(tc.setup)
		}
		got := h.step(hb(CallID(i+1), tc.group, tc.id, tc.gen))
		want := h.show(Reply{Call: CallID(i + 1), Err: tc.want})
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s: got %q, want %q", tc.name, got, want)
		}
	}
	h.check("phase at the end", h.phase("g"), stateEmpty)
}
