package broker

import (
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// liveTrio is a live set with brokers 2 and 3. Broker 1 is the test broker
// itself, with the default address localhost:9092.
var liveTrio = []core.BrokerAddr{{NodeID: 2, Host: "h2", Port: 9092}, {NodeID: 3, Host: "h3", Port: 9093}}

func TestFNV1a32Vectors(t *testing.T) {
	// The standard FNV-1a 32 bit test vectors.
	for s, want := range map[string]uint32{"": 0x811c9dc5, "a": 0xe40c292c, "foobar": 0xbf9cf968} {
		if got := fnv1a32(s); got != want {
			t.Errorf("fnv1a32(%q) = %#x, want %#x", s, got, want)
		}
	}
}

func TestFindCoordinatorHashIsStableAndSpread(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	h.do(core.LiveBrokers{Brokers: liveTrio})
	count := map[int32]int{}
	for i := range 300 {
		name := "group-" + strconv.Itoa(i)
		addr := h.b.coordinatorOf(name)
		count[addr.NodeID]++
		if again := h.b.coordinatorOf(name); again != addr {
			t.Fatalf("%s: coordinator changed from %v to %v", name, addr, again)
		}
	}
	for _, id := range []int32{1, 2, 3} {
		if count[id] < 60 { // about 100 each is expected
			t.Errorf("broker %d coordinates %d of 300 groups", id, count[id])
		}
	}
	h.noViolations()
}

func findCoordinatorV0(key string) *kmsg.FindCoordinatorRequest {
	r := kmsg.NewPtrFindCoordinatorRequest()
	r.CoordinatorKey = key
	return r
}

func TestFindCoordinatorAnswersTheHashedBroker(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	h.do(core.LiveBrokers{Brokers: liveTrio})
	c := h.open(1)
	for _, name := range []string{"g1", "g2", "payments", "orders"} {
		want := h.b.coordinatorOf(name)
		r := h.call(c, 0, findCoordinatorV0(name)).(*kmsg.FindCoordinatorResponse)
		if r.ErrorCode != 0 || r.NodeID != want.NodeID || r.Host != want.Host || r.Port != want.Port {
			t.Errorf("%s: %+v, want node %d at %s:%d", name, r, want.NodeID, want.Host, want.Port)
		}
	}
	h.noViolations()
}

func TestFindCoordinatorFallsBackToSelf(t *testing.T) {
	// No LiveBrokers yet: every group is coordinated by this broker.
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	for _, name := range []string{"g1", "g2", "g3"} {
		r := h.call(c, 3, findCoordinatorV0(name)).(*kmsg.FindCoordinatorResponse)
		if r.ErrorCode != 0 || r.NodeID != 1 || r.Host != "localhost" || r.Port != 9092 {
			t.Fatalf("%s: %+v, want self", name, r)
		}
	}
	// An empty live set is the same as no live set.
	h.do(core.LiveBrokers{Brokers: liveTrio})
	h.do(core.LiveBrokers{})
	if r := h.call(c, 3, findCoordinatorV0("g1")).(*kmsg.FindCoordinatorResponse); r.NodeID != 1 {
		t.Fatalf("empty live set: node %d, want 1", r.NodeID)
	}
	h.noViolations()
}

func TestFindCoordinatorKeysAndTypes(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	// Version 4 asks for three keys: a group, an empty group id and a group.
	r4 := kmsg.NewPtrFindCoordinatorRequest()
	r4.CoordinatorKeys = []string{"g1", "", "g2"}
	r := h.call(c, 4, r4).(*kmsg.FindCoordinatorResponse)
	if len(r.Coordinators) != 3 {
		t.Fatalf("%d answers, want 3", len(r.Coordinators))
	}
	for i, want := range []kafkaErrorCode{errNone, errInvalidGroupID, errNone} {
		if got := kafkaErrorCode(r.Coordinators[i].ErrorCode); got != want {
			t.Errorf("key %d: error %d, want %d", i, got, want)
		}
	}
	if r.Coordinators[1].NodeID != -1 || r.Coordinators[0].Key != "g1" {
		t.Errorf("answers %+v", r.Coordinators)
	}

	// A transaction coordinator is not supported.
	transaction := kmsg.NewPtrFindCoordinatorRequest()
	transaction.CoordinatorKey, transaction.CoordinatorType = "t1", coordTransaction
	if r := h.call(c, 3, transaction).(*kmsg.FindCoordinatorResponse); kafkaErrorCode(r.ErrorCode) != errCoordNotAvailable {
		t.Errorf("transaction: error %d, want 15", r.ErrorCode)
	}
	// An empty group id on version 0 to 3.
	if r := h.call(c, 0, findCoordinatorV0("")).(*kmsg.FindCoordinatorResponse); kafkaErrorCode(r.ErrorCode) != errInvalidGroupID {
		t.Errorf("empty group: error %d, want 24", r.ErrorCode)
	}
	h.noViolations()
}

// listGroupsFrom asks broker id for its groups (version 4, no filter).
func listGroupsFrom(t *testing.T, id int32, live []core.BrokerAddr, groups []core.GroupSummary) map[string]int {
	t.Helper()
	config := DefaultConfig()
	config.BrokerID = id
	h, _ := withGroupCP(t, config, func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.ListGroupsResp{Groups: groups})
	})
	h.do(core.LiveBrokers{Brokers: live})
	r := h.call(h.open(1), 4, kmsg.NewPtrListGroupsRequest()).(*kmsg.ListGroupsResponse)
	out := map[string]int{}
	for _, g := range r.Groups {
		out[g.Group]++
	}
	h.noViolations()
	return out
}

func TestListGroupsEachBrokerListsItsOwn(t *testing.T) {
	var groups []core.GroupSummary
	for i := range 30 {
		groups = append(groups, core.GroupSummary{Name: "g" + strconv.Itoa(i), ProtocolType: "consumer", State: "Stable"})
	}
	live := []core.BrokerAddr{{NodeID: 1, Host: "localhost", Port: 9092}, {NodeID: 2, Host: "h2", Port: 9092}, {NodeID: 3, Host: "h3", Port: 9093}}
	seen := map[string]int{}
	for id := int32(1); id <= 3; id++ {
		for name, n := range listGroupsFrom(t, id, live, groups) {
			seen[name] += n
		}
	}
	if len(seen) != 30 {
		t.Fatalf("the three brokers list %d groups, want 30", len(seen))
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("group %s listed %d times, want once", name, n)
		}
	}
}

func TestListGroupsFallsBackToAllWithNoLiveSet(t *testing.T) {
	groups := []core.GroupSummary{{Name: "a", State: "Empty"}, {Name: "b", State: "Stable"}}
	got := listGroupsFrom(t, 1, nil, groups)
	if len(got) != 2 || got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("no live set: %v, want both groups", got)
	}
}

func TestListGroupsStatesFilter(t *testing.T) {
	groups := []core.GroupSummary{{Name: "a", State: "Empty"}, {Name: "b", State: "Stable"}, {Name: "c", State: "PreparingSync"}}
	h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.ListGroupsResp{Groups: groups})
	})
	c := h.open(1)
	r := kmsg.NewPtrListGroupsRequest()
	r.StatesFilter = []string{"CompletingRebalance", "Empty"}
	got := h.call(c, 4, r).(*kmsg.ListGroupsResponse)
	if len(got.Groups) != 2 || got.Groups[0].Group != "a" || got.Groups[1].Group != "c" || got.Groups[1].GroupState != "CompletingRebalance" {
		t.Fatalf("filtered list %+v", got.Groups)
	}
	h.noViolations()
}
