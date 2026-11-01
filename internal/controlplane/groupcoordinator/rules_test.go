package groupcoordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cp "manifester/internal/controlplane"
	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

func TestProtocolMismatch(t *testing.T) {
	h := newHarness(t, Config{})
	_, chA := h.join("p")
	// A second member with another protocol type is refused at once.
	other := h.joinReq("p", "")
	other.ProtocolType = "connect"
	if r, err := h.c.JoinGroup(t.Context(), other); err != nil || r.Err != core.ErrInconsistentGroupProtocol {
		t.Fatalf("wrong type: %+v %v", r, err)
	}
	h.clock.Advance(defaultInitialRebalanceDelay)
	h.recvJoin(chA)

	// Two members with no protocol in common: both calls fail, and the
	// group stays in a join round, so a retry can finish it.
	h2 := newHarness(t, Config{})
	a, chA2 := h2.join("q", "range")
	b, chB := h2.join("q", "roundrobin")
	h2.clock.Advance(defaultInitialRebalanceDelay)
	if r := h2.recvJoin(chA2); r.resp.Err != core.ErrInconsistentGroupProtocol {
		t.Fatalf("a: %+v", r.resp)
	}
	if r := h2.recvJoin(chB); r.resp.Err != core.ErrInconsistentGroupProtocol {
		t.Fatalf("b: %+v", r.resp)
	}
	if v := peek(h2.c, "q"); v.phase != "PreparingRebalance" || !slices.Equal(v.members, []string{a, b}) {
		t.Fatalf("after mismatch: %+v", v)
	}
	chA3 := h2.rejoin("q", a, "range", "roundrobin")
	chB2 := h2.rejoin("q", b, "roundrobin")
	ra, rb := h2.recvJoin(chA3), h2.recvJoin(chB2)
	if ra.resp.Err != core.OK || rb.resp.Err != core.OK || ra.resp.Protocol != "roundrobin" || rb.resp.Generation != 1 {
		t.Fatalf("retry: %+v / %+v", ra.resp, rb.resp)
	}
}

func TestEmptyMemberIDAndBadJoins(t *testing.T) {
	h := newHarness(t, Config{})
	h.next.Store(0)
	req := h.joinReq("g", "")
	req.ClientID = "cli"
	ch := h.joinAsync(req)
	waitUntil(t, func() bool { return peek(h.c, "g").joined["cli-m1"] })
	h.clock.Advance(defaultInitialRebalanceDelay)
	r := h.recvJoin(ch)
	if r.resp.Err != core.OK || r.resp.MemberID != "cli-m1" {
		t.Fatalf("new member id: %+v", r.resp)
	}
	if r, err := h.c.JoinGroup(t.Context(), h.joinReq("g", "nobody")); err != nil || r.Err != core.ErrUnknownMember {
		t.Fatalf("unknown member id: %+v %v", r, err)
	}
	bad := h.joinReq("g", "")
	bad.SessionTimeoutMs = 0
	if r, err := h.c.JoinGroup(t.Context(), bad); err != nil || r.Err != core.ErrInvalid {
		t.Fatalf("zero session: %+v %v", r, err)
	}
}

func TestSyncRules(t *testing.T) {
	h := newHarness(t, Config{})
	a, chA := h.join("g")
	if r, _ := h.c.SyncGroup(t.Context(), SyncReq{Group: "nope", MemberID: a, Generation: 1}); r.Err != core.ErrUnknownMember {
		t.Fatalf("unknown group: %v", r.Err)
	}
	// The round is open: a sync is refused with a rebalance in progress.
	if r := h.sync("g", a, 0); r.Err != core.ErrRebalanceInProgress {
		t.Fatalf("open round: %v", r.Err)
	}
	if r := h.sync("g", "zz", 0); r.Err != core.ErrUnknownMember {
		t.Fatalf("unknown member: %v", r.Err)
	}
	h.clock.Advance(defaultInitialRebalanceDelay)
	h.recvJoin(chA)
	if r := h.sync("g", a, 9); r.Err != core.ErrIllegalGeneration {
		t.Fatalf("wrong generation: %v", r.Err)
	}
	if r := h.sync("g", "zz", 1); r.Err != core.ErrUnknownMember {
		t.Fatalf("unknown sync member: %v", r.Err)
	}
	if r := h.sync("g", a, 1, Assignment{Member: "zz"}); r.Err != core.ErrUnknownMember {
		t.Fatalf("assignment to unknown member: %v", r.Err)
	}
	if r := h.sync("g", a, 1, Assignment{Member: a}, Assignment{Member: a}); r.Err != core.ErrInvalid {
		t.Fatalf("duplicate assignment: %v", r.Err)
	}
	if r := h.sync("g", a, 1, Assignment{Member: a, Bytes: []byte("ok")}); r.Err != core.OK {
		t.Fatalf("leader sync: %v", r.Err)
	}
	// Stable: the stored assignment comes back at once, to any member.
	if r := h.sync("g", a, 1); r.Err != core.OK || string(r.Assignment) != "ok" {
		t.Fatalf("stable sync: %+v", r)
	}
}

func TestOffsetCommitChecks(t *testing.T) {
	h := newHarness(t, Config{})
	ctx := t.Context()
	if code, err := h.store.CreateTopic(ctx, core.CreateTopic{Name: "t", Partitions: 2}); err != nil || code != core.OK {
		t.Fatalf("topic: %v %v", code, err)
	}
	a, _ := h.stableTwo("g")
	commit := func(member string, gen int32, offs ...core.PartitionOffset) (core.OffsetCommitResp, core.ErrCode) {
		return h.c.OffsetCommit(ctx, core.OffsetCommit{Group: "g", Member: member, Generation: gen, Offsets: offs})
	}
	tp0 := core.NewTopicPartition("t", 0)
	resp, code := commit(a, 2,
		core.PartitionOffset{TP: tp0, Offset: 5},
		core.PartitionOffset{TP: core.NewTopicPartition("t", 9), Offset: 1},
		core.PartitionOffset{TP: tp0, Offset: -1},
	)
	if code != core.OK || !slices.Equal(resp.Errs, []core.ErrCode{core.OK, core.ErrUnknownPartition, core.ErrInvalid}) {
		t.Fatalf("commit: %v %v", code, resp.Errs)
	}
	if _, code := commit(a, 1, core.PartitionOffset{TP: tp0, Offset: 6}); code != core.ErrIllegalGeneration {
		t.Fatalf("wrong generation: %v", code)
	}
	if _, code := commit("zz", 2, core.PartitionOffset{TP: tp0, Offset: 6}); code != core.ErrUnknownMember {
		t.Fatalf("unknown member: %v", code)
	}
	if _, code := commit("", -1, core.PartitionOffset{TP: tp0, Offset: 6}); code != core.ErrIllegalGeneration {
		t.Fatalf("manual commit with members: %v", code)
	}
	if _, code := h.c.OffsetCommit(ctx, core.OffsetCommit{Group: "fresh", Generation: -1, Offsets: []core.PartitionOffset{{TP: tp0, Offset: 3}}}); code != core.OK {
		t.Fatalf("manual commit, no members: %v", code)
	}
	f, code := h.c.OffsetFetch(ctx, core.OffsetFetch{Group: "g", Partitions: []core.TopicPartition{tp0}})
	if code != core.OK || len(f.Offsets) != 1 || f.Offsets[0].Offset != 5 {
		t.Fatalf("fetch: %v %+v", code, f)
	}
	lg, code := h.c.ListGroups(ctx)
	// "fresh" has no installed generation, so the log does not list it.
	if code != core.OK || len(lg) != 1 || lg[0].Name != "g" {
		t.Fatalf("list: %v %+v", code, lg)
	}
}

func TestLeaderNeverSyncs(t *testing.T) {
	h := newHarness(t, Config{})
	h.rebalance = 5000 // shorter than the session, so the round deadline comes first
	a, chA := h.join("g")
	b, chB := h.join("g")
	h.clock.Advance(defaultInitialRebalanceDelay)
	ra, rb := h.recvJoin(chA), h.recvJoin(chB)
	if ra.resp.Generation != 1 || rb.resp.Generation != 1 || ra.resp.Leader != a {
		t.Fatalf("round: %+v / %+v", ra.resp, rb.resp)
	}
	// B is a follower and parks. A never syncs.
	chS := h.syncAsync("g", b, 1)
	waitUntil(t, func() bool { return peek(h.c, "g").synced[b] })
	h.clock.Advance(5 * time.Second)
	if s := h.recvSync(chS); s.resp.Err != core.ErrRebalanceInProgress {
		t.Fatalf("parked sync after timeout: %+v %v", s.resp, s.err)
	}
	if v := peek(h.c, "g"); v.phase != "PreparingRebalance" {
		t.Fatalf("phase after timeout: %s", v.phase)
	}
	if c := h.c.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: b, Generation: 1}); c != core.ErrRebalanceInProgress {
		t.Fatalf("heartbeat: %v", c)
	}

	// A parked sync also ends when its own ctx ends.
	ra2 := h.rejoin("g", a)
	rb2 := h.rejoin("g", b)
	if h.recvJoin(ra2).resp.Generation != 2 || h.recvJoin(rb2).resp.Generation != 2 {
		t.Fatal("second round")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.c.SyncGroup(ctx, SyncReq{Group: "g", MemberID: b, Generation: 2})
		done <- err
	}()
	waitUntil(t, func() bool { return peek(h.c, "g").synced[b] })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if peek(h.c, "g").synced[b] {
		t.Fatal("canceled sync still parked")
	}
}

func TestRebuildKeepsMembersAndExpiresSilent(t *testing.T) {
	h := newHarness(t, Config{})
	h.session = 5000
	a, b := h.stableTwo("g")
	// A second group is joined but never synced: its generation is not assigned.
	h.session = 10000
	u, chU := h.join("unsynced")
	h.clock.Advance(defaultInitialRebalanceDelay)
	h.recvJoin(chU)

	// Failover: the old active stops, the new node rebuilds from the log.
	h.c.Stop()
	c2 := New(Config{Clock: h.clock, NewID: h.c.cfg.NewID}, h.store)
	t.Cleanup(c2.Close)
	if err := c2.Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
	if v := peek(c2, "g"); v.phase != "Stable" || v.gen != 2 || !slices.Equal(v.members, []string{a, b}) {
		t.Fatalf("rebuilt g: %+v", v)
	}
	if v := peek(c2, "unsynced"); v.phase != "PreparingRebalance" || !slices.Equal(v.members, []string{u}) {
		t.Fatalf("rebuilt unsynced: %+v", v)
	}
	// Grace: nobody expires before the session timeout from the rebuild.
	h.clock.Advance(4 * time.Second)
	if v := peek(c2, "g"); v.phase != "Stable" || len(v.members) != 2 {
		t.Fatalf("expired during grace: %+v", v)
	}
	if c := c2.Heartbeat(t.Context(), HeartbeatReq{Group: "g", MemberID: a, Generation: 2}); c != core.OK {
		t.Fatalf("live member after rebuild: %v", c)
	}
	h.clock.Advance(1500 * time.Millisecond)
	if v := peek(c2, "g"); v.phase != "PreparingRebalance" || !slices.Equal(v.members, []string{a}) {
		t.Fatalf("silent member not expired: %+v", v)
	}
	if got := storedMembers(t, h, "g"); !slices.Equal(got, []string{a}) {
		t.Fatalf("log after expiry: %v", got)
	}
}

func TestCloseWakesParkedCalls(t *testing.T) {
	h := newHarness(t, Config{})
	_, ch := h.join("g")
	h.c.Close()
	if r := h.recvJoin(ch); r.resp.Err != core.ErrUnavailable {
		t.Fatalf("parked join after close: %+v", r.resp)
	}
	if err := h.c.Rebuild(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("rebuild after close: %v", err)
	}
	if r, _ := h.c.JoinGroup(t.Context(), h.joinReq("g", "")); r.Err != core.ErrUnavailable {
		t.Fatalf("join after close: %+v", r)
	}
	if n := h.clock.pending(); n != 0 {
		t.Fatalf("timers left after close: %d", n)
	}
}

func TestStoreFailureBreaksTermAndRebuildRecovers(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	inner := &stateupdater.Store{A: stateupdater.NewMem()}
	flaky := &flakyStore{GroupStore: inner}
	next := new(atomic.Int64)
	c := New(Config{Clock: clock, NewID: func() string { return fmt.Sprintf("m%d", next.Add(1)) }}, flaky)
	t.Cleanup(c.Close)
	if err := c.Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, c: c, store: inner, clock: clock, session: 10000, next: next}
	_, ch := h.join("g")
	flaky.failInstall.Store(true)
	clock.Advance(defaultInitialRebalanceDelay)
	if r := h.recvJoin(ch); r.resp.Err != core.ErrUnavailable {
		t.Fatalf("failed install: %+v", r.resp)
	}
	if c.Serving() {
		t.Fatal("broken term still serves")
	}
	flaky.failInstall.Store(false)
	if r, err := c.JoinGroup(t.Context(), h.joinReq("g", "")); err != nil || r.Err != core.ErrUnavailable {
		t.Fatalf("join on broken term: %+v %v", r, err)
	}
	c.Stop()
	if err := c.Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !c.Serving() {
		t.Fatal("rebuilt term does not serve")
	}
	_, ch2 := h.join("g")
	clock.Advance(defaultInitialRebalanceDelay)
	if r := h.recvJoin(ch2); r.resp.Err != core.OK || r.resp.Generation != 1 {
		t.Fatalf("join after recovery: %+v", r.resp)
	}
}

// flakyStore fails GroupInstall on demand.
type flakyStore struct {
	cp.GroupStore
	failInstall atomic.Bool
}

func (f *flakyStore) GroupInstall(ctx context.Context, op core.GroupInstall) (core.ErrCode, error) {
	if f.failInstall.Load() {
		return core.OK, errors.New("log down")
	}
	return f.GroupStore.GroupInstall(ctx, op)
}

// TestConcurrentJoinsOneRound starts eight joins at once on the real clock.
// They must end in one round with one leader, and every member syncs.
func TestConcurrentJoinsOneRound(t *testing.T) {
	c := New(Config{InitialRebalanceDelay: 200 * time.Millisecond}, &stateupdater.Store{A: stateupdater.NewMem()})
	t.Cleanup(c.Close)
	if err := c.Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
	const n = 8
	var wg sync.WaitGroup
	res := make([]JoinResp, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			req := JoinReq{
				Group: "g", ClientID: fmt.Sprintf("c%d", i), SessionTimeoutMs: 30000,
				RebalanceTimeoutMs: 30000, ProtocolType: "consumer",
				Protocols: []Protocol{{Name: "range", Metadata: []byte{byte(i)}}},
			}
			res[i], errs[i] = c.JoinGroup(t.Context(), req)
		})
	}
	wg.Wait()
	leader := ""
	gen := int32(-1)
	ids := []string{}
	for i := range n {
		if errs[i] != nil || res[i].Err != core.OK {
			t.Fatalf("join %d: %+v %v", i, res[i], errs[i])
		}
		if gen == -1 {
			gen = res[i].Generation
		}
		if res[i].Generation != gen || res[i].Protocol != "range" {
			t.Fatalf("join %d: generation %d, want %d", i, res[i].Generation, gen)
		}
		if len(res[i].Members) > 0 {
			leader = res[i].MemberID
			if len(res[i].Members) != n {
				t.Fatalf("leader sees %d members", len(res[i].Members))
			}
		}
		ids = append(ids, res[i].MemberID)
	}
	if leader == "" || gen != 1 {
		t.Fatalf("no leader or generation %d", gen)
	}
	var assigns []Assignment
	for _, id := range ids {
		assigns = append(assigns, Assignment{Member: id, Bytes: []byte("as-" + id)})
	}
	for _, id := range ids {
		wg.Go(func() {
			r, err := c.SyncGroup(t.Context(), SyncReq{Group: "g", MemberID: id, Generation: gen, Assignments: assigns})
			if err != nil || r.Err != core.OK || string(r.Assignment) != "as-"+id {
				t.Errorf("sync %s: %+v %v", id, r, err)
			}
		})
	}
	wg.Wait()
}
