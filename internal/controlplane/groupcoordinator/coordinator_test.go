package groupcoordinator

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

// view is a read of one group's memory, taken under its mutex.
type view struct {
	phase   string
	gen     int32
	leader  string
	members []string
	joined  map[string]bool // parked JoinGroup
	synced  map[string]bool // parked SyncGroup
}

func peek(c *Coordinator, name string) view {
	t := c.enter()
	if t == nil {
		return view{phase: "down"}
	}
	g := c.group(t, name, false)
	if g == nil {
		return view{phase: "none"}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	v := view{phase: g.phase.String(), gen: g.generation, leader: g.leader, joined: map[string]bool{}, synced: map[string]bool{}}
	for id, m := range g.members {
		v.members = append(v.members, id)
		if m.joinWait != nil {
			v.joined[id] = true
		}
		if m.syncWait != nil {
			v.synced[id] = true
		}
	}
	slices.Sort(v.members)
	return v
}

type harness struct {
	t         *testing.T
	c         *Coordinator
	store     *stateupdater.Store
	clock     *fakeClock
	next      *atomic.Int64
	session   int32 // session timeout of the members joined by join
	rebalance int32 // rebalance timeout; 0 means the session timeout
}

// newHarness starts a coordinator over an empty in-memory log. Member ids
// are m1, m2, ... in the order they are made.
func newHarness(t *testing.T, config Config) *harness {
	t.Helper()
	h := &harness{t: t, clock: newFakeClock(time.Unix(1000, 0)), session: 10000, next: new(atomic.Int64)}
	h.store = &stateupdater.Store{A: stateupdater.NewMem()}
	if config.Clock == nil {
		config.Clock = h.clock
	}
	if config.NewID == nil {
		config.NewID = func() string { return fmt.Sprintf("m%d", h.next.Add(1)) }
	}
	h.c = New(config, h.store)
	t.Cleanup(h.c.Close)
	if err := h.c.Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
	return h
}

type joinResult struct {
	response JoinResp
	err      error
}

func (h *harness) joinReq(group, member string, protos ...string) JoinReq {
	r := JoinReq{
		Group: group, MemberID: member,
		SessionTimeoutMs: h.session, RebalanceTimeoutMs: h.session, ProtocolType: "consumer",
	}
	if h.rebalance > 0 {
		r.RebalanceTimeoutMs = h.rebalance
	}
	if len(protos) == 0 {
		protos = []string{"range"}
	}
	for _, p := range protos {
		r.Protocols = append(r.Protocols, Protocol{Name: p, Metadata: []byte("meta-" + p)})
	}
	return r
}

// joinAsync calls JoinGroup on its own goroutine.
func (h *harness) joinAsync(request JoinReq) <-chan joinResult {
	ch := make(chan joinResult, 1)
	go func() {
		response, err := h.c.JoinGroup(h.t.Context(), request)
		ch <- joinResult{response, err}
	}()
	return ch
}

// join starts a join with a new member and waits until it is parked. It
// returns the member id it will get.
func (h *harness) join(group string, protos ...string) (string, <-chan joinResult) {
	h.t.Helper()
	id := fmt.Sprintf("m%d", h.next.Load()+1)
	ch := h.joinAsync(h.joinReq(group, "", protos...))
	waitUntil(h.t, func() bool { return peek(h.c, group).joined[id] })
	return id, ch
}

// rejoin starts a join of a known member. It does not wait: the last member
// of a round completes the round at once, so it never parks.
func (h *harness) rejoin(group, id string, protos ...string) <-chan joinResult {
	h.t.Helper()
	return h.joinAsync(h.joinReq(group, id, protos...))
}

func (h *harness) recvJoin(ch <-chan joinResult) joinResult {
	h.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		h.t.Fatal("join did not return")
	}
	return joinResult{}
}

func (h *harness) sync(group, id string, gen int32, assigns ...Assignment) SyncResp {
	h.t.Helper()
	r, err := h.c.SyncGroup(h.t.Context(), SyncReq{Group: group, MemberID: id, Generation: gen, Assignments: assigns})
	if err != nil {
		h.t.Fatal(err)
	}
	return r
}

type syncResult struct {
	response SyncResp
	err      error
}

func (h *harness) syncAsync(group, id string, gen int32, assigns ...Assignment) <-chan syncResult {
	ch := make(chan syncResult, 1)
	go func() {
		r, err := h.c.SyncGroup(h.t.Context(), SyncReq{Group: group, MemberID: id, Generation: gen, Assignments: assigns})
		ch <- syncResult{r, err}
	}()
	return ch
}

func (h *harness) recvSync(ch <-chan syncResult) syncResult {
	h.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		h.t.Fatal("sync did not return")
	}
	return syncResult{}
}

// stableTwo makes group g Stable with two members, generation 2. The first
// member is the leader. It returns their ids.
func (h *harness) stableTwo(group string) (string, string) {
	h.t.Helper()
	a, chA := h.join(group)
	h.clock.Advance(defaultInitialRebalanceDelay)
	ra := h.recvJoin(chA)
	if ra.err != nil || ra.response.Err != core.OK || ra.response.Generation != 1 {
		h.t.Fatalf("first join: %+v %v", ra.response, ra.err)
	}
	if s := h.sync(group, a, 1, Assignment{Member: a, Bytes: []byte("a1")}); s.Err != core.OK {
		h.t.Fatalf("first sync: %v", s.Err)
	}
	b, chB := h.join(group)
	chA2 := h.rejoin(group, a)
	rb := h.recvJoin(chB)
	ra2 := h.recvJoin(chA2)
	if rb.response.Err != core.OK || ra2.response.Err != core.OK || rb.response.Generation != 2 || ra2.response.Generation != 2 {
		h.t.Fatalf("second round: %+v / %+v", rb.response, ra2.response)
	}
	chS := h.syncAsync(group, b, 2)
	waitUntil(h.t, func() bool { return peek(h.c, group).synced[b] })
	if s := h.sync(group, a, 2, Assignment{Member: a, Bytes: []byte("a2")}, Assignment{Member: b, Bytes: []byte("b2")}); s.Err != core.OK {
		h.t.Fatalf("leader sync: %v", s.Err)
	}
	if s := h.recvSync(chS); s.err != nil || s.response.Err != core.OK || string(s.response.Assignment) != "b2" {
		h.t.Fatalf("follower sync: %+v %v", s.response, s.err)
	}
	return a, b
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func storedMembers(t *testing.T, h *harness, group string) []string {
	t.Helper()
	d, err := h.store.DescribeGroup(t.Context(), core.DescribeGroup{Group: group})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range d.Members {
		out = append(out, m.MemberID)
	}
	slices.Sort(out)
	return out
}

func TestOneMemberJoinsAndSyncs(t *testing.T) {
	h := newHarness(t, Config{})
	id, ch := h.join("g")
	h.clock.Advance(defaultInitialRebalanceDelay)
	r := h.recvJoin(ch)
	if r.err != nil || r.response.Err != core.OK || r.response.Generation != 1 || r.response.MemberID != id {
		t.Fatalf("join: %+v %v", r.response, r.err)
	}
	if r.response.Leader != id || r.response.Protocol != "range" || len(r.response.Members) != 1 || r.response.Members[0].ID != id {
		t.Fatalf("leader answer: %+v", r.response)
	}
	if string(r.response.Members[0].Metadata) != "meta-range" {
		t.Fatalf("metadata: %q", r.response.Members[0].Metadata)
	}
	s := h.sync("g", id, 1, Assignment{Member: id, Bytes: []byte("all")})
	if s.Err != core.OK || string(s.Assignment) != "all" {
		t.Fatalf("sync: %+v", s)
	}
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: id, Generation: 1}); c != core.OK {
		t.Fatalf("heartbeat: %v", c)
	}
	d, _ := h.c.DescribeGroup(t.Context(), "g")
	if d.State != "Stable" || d.AssignedGeneration != 1 || d.Generation != 1 {
		t.Fatalf("describe: %+v", d)
	}
}

func TestSecondMemberRebalancesStableGroup(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.stableTwo("g")
	if v := peek(h.c, "g"); v.phase != "Stable" || v.gen != 2 || v.leader != a {
		t.Fatalf("after two members: %+v", v)
	}
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: a, Generation: 2}); c != core.OK {
		t.Fatalf("stable heartbeat: %v", c)
	}
	// A third member joins a Stable group: the first member must rejoin.
	c3, ch3 := h.join("g")
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: a, Generation: 2}); c != core.ErrRebalanceInProgress {
		t.Fatalf("heartbeat during rebalance: %v", c)
	}
	chA := h.rejoin("g", a)
	chB := h.rejoin("g", b)
	ra, rb, rc := h.recvJoin(chA), h.recvJoin(chB), h.recvJoin(ch3)
	for _, r := range []joinResult{ra, rb, rc} {
		if r.err != nil || r.response.Err != core.OK || r.response.Generation != 3 || r.response.Leader != a {
			t.Fatalf("round three: %+v %v", r.response, r.err)
		}
	}
	if len(ra.response.Members) != 3 || len(rb.response.Members) != 0 || len(rc.response.Members) != 0 {
		t.Fatalf("only the leader gets the list: %d %d %d", len(ra.response.Members), len(rb.response.Members), len(rc.response.Members))
	}
	if got := storedMembers(t, h, "g"); !slices.Equal(got, []string{a, b, c3}) {
		t.Fatalf("stored members: %v", got)
	}
	chS := h.syncAsync("g", b, 3)
	waitUntil(t, func() bool { return peek(h.c, "g").synced[b] })
	if s := h.sync("g", a, 3, Assignment{Member: a, Bytes: []byte("a3")}, Assignment{Member: b, Bytes: []byte("b3")}, Assignment{Member: c3, Bytes: []byte("c3")}); s.Err != core.OK {
		t.Fatalf("leader sync: %+v", s)
	}
	if s := h.recvSync(chS); s.response.Err != core.OK || string(s.response.Assignment) != "b3" {
		t.Fatalf("follower sync: %+v", s)
	}
	if s := h.sync("g", c3, 3); s.Err != core.OK || string(s.Assignment) != "c3" {
		t.Fatalf("third member sync: %+v", s)
	}
}

func TestLeaveRebalances(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.stableTwo("g")
	errs := h.c.LeaveGroup(t.Context(), LeaveReq{Group: "g", Members: []string{b, "nobody"}})
	if !slices.Equal(errs, []core.ErrCode{core.OK, core.ErrUnknownMember}) {
		t.Fatalf("leave errs: %v", errs)
	}
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: a, Generation: 2}); c != core.ErrRebalanceInProgress {
		t.Fatalf("heartbeat after leave: %v", c)
	}
	ch := h.rejoin("g", a)
	r := h.recvJoin(ch)
	if r.response.Err != core.OK || r.response.Generation != 3 || len(r.response.Members) != 1 {
		t.Fatalf("rejoin: %+v", r.response)
	}
	if got := storedMembers(t, h, "g"); !slices.Equal(got, []string{a}) {
		t.Fatalf("stored: %v", got)
	}
	// The last member leaves: the group is Empty and keeps its generation.
	h.c.LeaveGroup(t.Context(), LeaveReq{Group: "g", Members: []string{a}})
	if v := peek(h.c, "g"); v.phase != "Empty" || v.gen != 3 {
		t.Fatalf("after last leave: %+v", v)
	}
}

func TestSessionExpiryRemovesSilentMember(t *testing.T) {
	h := newHarness(t, Config{})
	h.session = 5000
	a, b := h.stableTwo("g")
	// The sync of the leader re-arms every session timer. A beats each second,
	// B stays silent and expires at the fifth second.
	for range 4 {
		h.clock.Advance(time.Second)
		if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: a, Generation: 2}); c != core.OK {
			t.Fatalf("heartbeat: %v", c)
		}
	}
	h.clock.Advance(time.Second)
	if v := peek(h.c, "g"); v.phase != "PreparingRebalance" || !slices.Equal(v.members, []string{a}) {
		t.Fatalf("after expiry: %+v", v)
	}
	if got := storedMembers(t, h, "g"); !slices.Equal(got, []string{a}) {
		t.Fatalf("stored after expiry: %v", got)
	}
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: b, Generation: 2}); c != core.ErrUnknownMember {
		t.Fatalf("expired member heartbeat: %v", c)
	}
	ch := h.rejoin("g", a)
	if r := h.recvJoin(ch); r.response.Err != core.OK || r.response.Generation != 3 {
		t.Fatalf("rejoin after expiry: %+v", r.response)
	}
}

func TestHeartbeatTable(t *testing.T) {
	h := newHarness(t, Config{})
	a, chA := h.join("g")
	h.clock.Advance(defaultInitialRebalanceDelay)
	h.recvJoin(chA)
	cases := []struct {
		name  string
		group string
		id    string
		gen   int32
		want  core.ErrCode
	}{
		{"unknown group", "nope", a, 1, core.ErrUnknownMember},
		{"unknown member", "g", "zz", 1, core.ErrUnknownMember},
		{"wrong generation", "g", a, 0, core.ErrIllegalGeneration},
		{"stable and right", "g", a, 1, core.ErrRebalanceInProgress},
	}
	for _, tc := range cases {
		got := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: tc.group, MemberID: tc.id, Generation: tc.gen})
		if got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
	// The group is still Completing (no sync yet): a heartbeat is refused.
	if v := peek(h.c, "g"); v.phase != "CompletingRebalance" {
		t.Fatalf("phase %s", v.phase)
	}
	if s := h.sync("g", a, 1, Assignment{Member: a, Bytes: []byte("x")}); s.Err != core.OK {
		t.Fatalf("sync: %v", s.Err)
	}
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: a, Generation: 1}); c != core.OK {
		t.Fatalf("stable: %v", c)
	}
}
