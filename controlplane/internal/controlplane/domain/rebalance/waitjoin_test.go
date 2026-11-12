package rebalance

import "testing"

// Ports of the tests of the old coordinator (groups/waitjoin_test.go, deleted in T6.6), and the abandoned sync of the old
// TestLeaderNeverSyncs. An abandoned call gets no reply (rule 33).

// A join whose call is abandoned while it is parked gets no reply, and the
// member no longer waits for a round. The member stays.
func TestJoinCtxEndsWhileParked(t *testing.T) {
	h := newHarness(t, Config{})
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, CallAbandoned{Call: 1}, nil},
	})
	h.check("parked", h.parked("g"), []string(nil))
	h.check("members", h.members("g"), []string{"m1"})
}

// A round that ends after a parked join was abandoned does not answer that
// join. The other join gets the answer at the round end, and the member that
// did not rejoin is removed.
func TestJoinCanceledThenRoundCompletes(t *testing.T) {
	h := newHarness(t, Config{})
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, CallAbandoned{Call: 1}, nil},
		{0, h.join(2, "g", ""), nil},
		// The abandoned member did not rejoin, so the round waits for it until the rebalance timeout.
		{delay, nil, nil},
		{10*sec - 1, nil, nil},
		{10 * sec, nil, []string{"2 OK gen=1 proto=range leader=m2 id=m2 members=[m2:meta-range]"}},
	})
	h.check("after round", []any{h.members("g"), h.parked("g")}, []any{[]string{"m2"}, []string(nil)})
}

// A parked follower sync that is abandoned gets no reply; the member stays.
func TestParkedSyncAbandoned(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.id(1), h.id(2)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, h.join(2, "g", ""), nil},
		{delay, nil, []string{
			"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range]",
			"2 OK gen=1 proto=range leader=m1 id=m2",
		}},
		{delay, syncReq(3, "g", b, 1), nil},
		{delay, CallAbandoned{Call: 3}, nil},
	})
	h.check("parked", h.parked("g"), []string(nil))
	h.run([]step{
		{delay, syncReq(4, "g", a, 1, as(a, "x"), as(b, "y")), []string{"4 OK assign=x"}},
		{delay, syncReq(5, "g", b, 1), []string{"5 OK assign=y"}},
	})
}
