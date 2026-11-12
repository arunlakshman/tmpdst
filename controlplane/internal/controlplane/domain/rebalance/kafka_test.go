package rebalance

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Tests of the rules that are new or changed (docs/rebalance.md sections 5 and 6).

// Rule 30: an existing member with a parked join is not removed when its
// session timer fires. A member with no parked call is.
func TestWaitingMemberKeepsSession(t *testing.T) {
	h := newHarness(t, Config{})
	h.session, h.rebalance = 5000, 20000
	a, _ := h.stableTwo("g", 0, 1, 1) // both sessions start again at 3 s
	h.run([]step{
		{delay, h.join(10, "g", ""), nil}, // a new member: a round until 23 s
		{delay, h.join(11, "g", a), nil},  // A waits in the round; B does not rejoin
		// At 8 s both sessions end. A waits, so it stays. B is removed, and
		// then every member left has joined: the round is done.
		{8 * sec, nil, []string{
			"11 OK gen=3 proto=range leader=m1 id=m1 members=[m1:meta-range m3:meta-range]",
			"10 OK gen=3 proto=range leader=m1 id=m3",
		}},
	})
	h.check("stored", h.stored("g"), []string{"m1", "m3"})
}

// Rule 30: a new member has the new member timeout instead of its session,
// and is removed when it fires even with a parked join. It is not in the
// log, so nothing is written (section 13).
func TestNewMemberTimeout(t *testing.T) {
	h := newHarness(t, Config{NewMemberTimeout: 60 * sec})
	h.rebalance = 600000 // the round end is far away
	a := h.id(1)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
		{delay, syncReq(2, "g", a, 1, as(a, "x")), []string{"2 OK assign=x"}},
		{delay, h.join(3, "g", ""), nil}, // m2 waits; m1 does not rejoin, it only beats
	})
	call := CallID(4)
	for at := delay + 8*sec; at < delay+60*sec; at += 8 * sec {
		h.run([]step{{at, hb(call, "g", a, 1), []string{fmt.Sprintf("%d ErrRebalanceInProgress", call)}}})
		call++
	}
	h.run([]step{
		{delay + 60*sec - 1, nil, nil},
		{delay + 60*sec, nil, []string{"3 ErrUnknownMember"}},
	})
	h.check("members", []any{h.phase("g"), h.members("g")}, []any{stateJoining, []string{"m1"}})
	for _, o := range h.outs {
		if c, ok := o.(SubmitCommand); ok {
			if r, ok := c.Op.(contract.GroupRemove); ok {
				t.Fatalf("a removal of a new member was logged: %+v", r)
			}
		}
	}
}

// Rule 24: the sync deadline runs on in Stable while a member has not
// synced. When it fires, that member is removed and a round opens.
func TestSyncDeadlineInStable(t *testing.T) {
	h := newHarness(t, Config{})
	h.rebalance = 5000
	a, b := h.id(1), h.id(2)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, h.join(2, "g", ""), nil},
		{delay, nil, []string{
			"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range]",
			"2 OK gen=1 proto=range leader=m1 id=m2",
		}},
		{delay, syncReq(3, "g", a, 1, as(a, "x"), as(b, "y")), []string{"3 OK assign=x"}},
		{5 * sec, hb(4, "g", b, 1), []string{"4 OK"}}, // Stable: OK, but B owes a sync
		{8*sec - 1, nil, nil},
		{8 * sec, nil, nil},
	})
	h.check("after the deadline", []any{h.phase("g"), h.members("g"), h.stored("g")}, []any{stateJoining, []string{"m1"}, []string{"m1"}})
	h.run([]step{{8 * sec, hb(5, "g", a, 1), []string{"5 ErrRebalanceInProgress"}}})

	// When every member syncs, the deadline stops.
	h2 := newHarness(t, Config{})
	h2.rebalance = 5000
	a, b = h2.id(1), h2.id(2)
	h2.run([]step{
		{0, h2.join(1, "g", ""), nil},
		{0, h2.join(2, "g", ""), nil},
		{delay, nil, []string{
			"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range m2:meta-range]",
			"2 OK gen=1 proto=range leader=m1 id=m2",
		}},
		{delay, syncReq(3, "g", a, 1, as(a, "x"), as(b, "y")), []string{"3 OK assign=x"}},
		{4 * sec, syncReq(4, "g", b, 1), []string{"4 OK assign=y"}},
		{8 * sec, nil, nil},
	})
	h2.check("all synced", []any{h2.phase("g"), h2.members("g"), h2.group("g").round.on}, []any{stateStable, []string{"m1", "m2"}, false})
}

// Rule 29: an offset commit that passes the checks starts the session again.
func TestCommitKeepsSession(t *testing.T) {
	h := newHarness(t, Config{})
	h.session = 5000
	h.log.Apply(contract.CreateTopic{Name: "t", Partitions: 1})
	a := h.id(1)
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
		{delay, syncReq(2, "g", a, 1, as(a, "x")), []string{"2 OK assign=x"}},
	})
	call := CallID(3)
	for at := delay + 4*sec; at <= delay+20*sec; at += 4 * sec {
		c := OffsetCommit{Call: call, Op: contract.OffsetCommit{Group: "g", Member: a, Generation: 1,
			Offsets: []contract.PartitionOffset{{TopicPartition: contract.NewTopicPartition("t", 0), Offset: int64(call)}}}}
		h.run([]step{{at, c, []string{fmt.Sprintf("%d OK [OK]", call)}}})
		call++
	}
	h.check("still a member", []any{h.phase("g"), h.members("g")}, []any{stateStable, []string{"m1"}})
	h.run([]step{{delay + 25*sec, nil, nil}})
	h.check("expired after the last commit", h.phase("g"), stateEmpty)
}

// Rule 32: Describe is the log's description with the state from memory,
// Dead for a group the machine does not have. List is from memory.
func TestDescribeAndList(t *testing.T) {
	h := newHarness(t, Config{})
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{0, Describe{Call: 2, Groups: []string{"g", "nope"}}, []string{
			"2 OK g:PreparingRebalance gen=0 assigned=0 members=[] nope:Dead gen=0 assigned=0 members=[]",
		}},
		{0, List{Call: 3}, []string{"3 OK g:consumer:PreparingRebalance"}},
		{0, Describe{Call: 4}, []string{"4 OK"}},
		{delay, nil, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"}},
		{delay, Describe{Call: 5, Groups: []string{"g"}}, []string{"5 OK g:CompletingRebalance gen=1 assigned=0 members=[m1]"}},
		{delay, List{Call: 6}, []string{"6 OK g:consumer:CompletingRebalance"}},
	})
	// A read that fails fails the call; the term goes on.
	h.hold = true
	h.step(Describe{Call: 7, Groups: []string{"g", "g"}})
	h.check("first read fails", h.answer(0, runtime.ErrUnavailable), []string{"7 ErrUnavailable"})
	h.check("second read is ignored", h.answer(0, runtime.OK), []string(nil))
	h.check("serving", h.phase("g"), stateSyncing)
}
