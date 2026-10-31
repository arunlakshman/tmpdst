package stateupdater_test

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

// tooBig is a byte field that makes a group command pass 256 KiB.
var tooBig = bytes.Repeat([]byte{'x'}, 300<<10)

func gm(id string) core.GroupMember {
	return core.GroupMember{
		MemberID: id, ClientID: "client-" + id, ClientHost: "/10.0.0.1",
		SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("sub-" + id),
	}
}

func install(group string, gen int32, leader string, ids ...string) core.GroupInstall {
	op := core.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, Leader: leader, TimeMs: 1000}
	for _, id := range ids {
		op.Members = append(op.Members, gm(id))
	}
	return op
}

func exec(t testing.TB, a *stateupdater.StateUpdater, op core.ControlPlaneOperation) core.ControlPlaneDone {
	t.Helper()
	return a.Exec(core.ControlPlaneCall{Req: 1, Op: op}).(core.ControlPlaneDone)
}

// step is one call and its expected error.
type step struct {
	name string
	op   core.ControlPlaneOperation
	want core.ErrCode
	// errs, when set, is the per-offset answer of an OffsetCommit.
	errs []core.ErrCode
}

func runSteps(t *testing.T, a *stateupdater.StateUpdater, steps []step) {
	t.Helper()
	for _, s := range steps {
		d := exec(t, a, s.op)
		if d.Err != s.want {
			t.Fatalf("%s: err %v, want %v", s.name, d.Err, s.want)
		}
		if s.errs != nil {
			got, _ := d.Resp.(core.OffsetCommitResp)
			if !reflect.DeepEqual(got.Errs, s.errs) {
				t.Fatalf("%s: per-offset errs %v, want %v", s.name, got.Errs, s.errs)
			}
		}
	}
}

// newGroupStore is a store with two topics, for every backend.
func newGroupStore(t *testing.T, mk func(testing.TB) stateupdater.KeyValueStore) *stateupdater.StateUpdater {
	t.Helper()
	a := stateupdater.New(mk(t))
	t.Cleanup(func() { _ = a.Close() })
	if code := a.CreateTopic("orders", 2); code != core.OK {
		t.Fatalf("create topic: %v", code)
	}
	return a
}

func describe(t *testing.T, a *stateupdater.StateUpdater, group string) core.DescribeGroupResp {
	t.Helper()
	return exec(t, a, core.DescribeGroup{Group: group}).Resp.(core.DescribeGroupResp)
}

func TestGroupInstallRules(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			noLeader := install("g", 1, "m9", "m1", "m2")
			dup := install("g", 2, "m1", "m1", "m1")
			runSteps(t, a, []step{
				{"gen 1 is the first generation", install("g", 1, "m1", "m1", "m2"), core.OK, nil},
				{"same generation again", install("g", 1, "m1", "m1", "m2"), core.ErrIllegalGeneration, nil},
				{"generation skips ahead", install("g", 3, "m1", "m1", "m2"), core.ErrIllegalGeneration, nil},
				{"no members", install("g", 2, ""), core.ErrInvalid, nil},
				{"leader is not a member", noLeader, core.ErrInvalid, nil},
				{"repeated member id", dup, core.ErrInvalid, nil},
				{"empty group name", install("", 1, "m1", "m1"), core.ErrInvalid, nil},
				{"too large", withMetadata(install("g", 2, "m1", "m1"), tooBig), core.ErrTooLarge, nil},
				{"next generation", install("g", 2, "m2", "m1", "m2"), core.OK, nil},
			})
			d := describe(t, a, "g")
			if !d.Exists || d.Generation != 2 || d.Leader != "m2" || d.State != "PreparingSync" || d.AssignedGeneration != 0 {
				t.Fatalf("describe after install: %+v", d)
			}
			if len(d.Members) != 2 || d.Members[0].MemberID != "m1" || d.Members[0].JoinedTimeMs != 1000 {
				t.Fatalf("members: %+v", d.Members)
			}
			if !bytes.Equal(d.Members[0].Metadata, []byte("sub-m1")) || d.Members[0].Assignment != nil {
				t.Fatalf("member m1: %+v", d.Members[0])
			}
		})
	}
}

func withMetadata(op core.GroupInstall, b []byte) core.GroupInstall {
	op.Members[0].Metadata = b
	return op
}

func TestGroupAssignRules(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			runSteps(t, a, []step{
				{"install", install("g", 1, "m1", "m1", "m2"), core.OK, nil},
				{"assign", assign("g", 1, "m1", "m1", "a", "m2", "b"), core.OK, nil},
				{"replay of the same assign", assign("g", 1, "m1", "m1", "a", "m2", "b"), core.OK, nil},
				{"wrong generation", assign("g", 2, "m1", "m1", "a"), core.ErrIllegalGeneration, nil},
				{"leader is not the leader", assign("g", 1, "m2", "m1", "a"), core.ErrUnknownMember, nil},
				{"unknown member", assign("g", 1, "m1", "zz", "a"), core.ErrUnknownMember, nil},
				{"group never installed", assign("nope", 0, "m1", "m1", "a"), core.ErrUnknownMember, nil},
				{"too large", withBytes(assign("g", 1, "m1", "m1", "a"), tooBig), core.ErrTooLarge, nil},
				{"assign again without m2", assign("g", 1, "m1", "m1", "x"), core.OK, nil},
			})
			d := describe(t, a, "g")
			if d.State != "Stable" || d.AssignedGeneration != 1 {
				t.Fatalf("describe after assign: %+v", d)
			}
			if len(d.Members) != 2 || !bytes.Equal(d.Members[0].Assignment, []byte("x")) || d.Members[1].Assignment != nil {
				t.Fatalf("assignments: %+v", d.Members)
			}
		})
	}
}

func assign(group string, gen int32, leader string, pairs ...string) core.GroupAssign {
	op := core.GroupAssign{Group: group, Generation: gen, Leader: leader, TimeMs: 2000}
	for i := 0; i+1 < len(pairs); i += 2 {
		op.Assignments = append(op.Assignments, core.GroupAssignment{Member: pairs[i], Bytes: []byte(pairs[i+1])})
	}
	return op
}

func withBytes(op core.GroupAssign, b []byte) core.GroupAssign {
	op.Assignments[0].Bytes = b
	return op
}

func TestGroupRemoveRules(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			runSteps(t, a, []step{
				{"install", install("g", 1, "m1", "m1", "m2", "m3"), core.OK, nil},
				{"assign", assign("g", 1, "m1", "m1", "a"), core.OK, nil},
				{"unknown member is ignored", remove("g", "zz"), core.OK, nil},
				{"remove m2", remove("g", "m2"), core.OK, nil},
				{"remove the leader", remove("g", "m1"), core.OK, nil},
				{"group never installed", remove("nope", "m1"), core.OK, nil},
				{"remove the last member", remove("g", "m3"), core.OK, nil},
				{"manual commit when empty", commitOp("g", "", -1, pair("orders", 0, 5)), core.OK, []core.ErrCode{core.OK}},
				{"managed commit of a removed member", commitOp("g", "m3", 1, pair("orders", 0, 6)), core.ErrUnknownMember, nil},
				{"next generation after empty", install("g", 2, "m3", "m3"), core.OK, nil},
				{"same generation again", install("g", 2, "m3", "m3"), core.ErrIllegalGeneration, nil},
			})
		})
	}
}

func TestGroupRemoveLeavesEmpty(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			runSteps(t, a, []step{
				{"install", install("g", 1, "m1", "m1", "m2"), core.OK, nil},
				{"assign", assign("g", 1, "m1", "m1", "a", "m2", "b"), core.OK, nil},
				{"remove leader", remove("g", "m1"), core.OK, nil},
				{"remove the rest", remove("g", "m2"), core.OK, nil},
			})
			d := describe(t, a, "g")
			want := core.DescribeGroupResp{Exists: true, Generation: 1, ProtocolType: "consumer", Protocol: "range", State: "Empty"}
			if !reflect.DeepEqual(d, want) {
				t.Fatalf("describe empty: %+v, want %+v", d, want)
			}
			ls := exec(t, a, core.ListGroups{}).Resp.(core.ListGroupsResp)
			if !reflect.DeepEqual(ls.Groups, []core.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Empty"}}) {
				t.Fatalf("list: %+v", ls.Groups)
			}
		})
	}
}

func remove(group string, ids ...string) core.GroupRemove {
	return core.GroupRemove{Group: group, Members: ids, TimeMs: 3000, Reason: "leave"}
}

func pair(topic string, p int32, off int64) core.PartitionOffset {
	return core.PartitionOffset{TP: core.NewTopicPartition(topic, p), Offset: off, LeaderEpoch: 7, Metadata: "m"}
}

func commitOp(group, member string, gen int32, offs ...core.PartitionOffset) core.OffsetCommit {
	return core.OffsetCommit{Group: group, Member: member, Generation: gen, TimeMs: 4000, Offsets: offs}
}

func TestOffsetCommitRules(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			ok, unk, bad := core.OK, core.ErrUnknownPartition, core.ErrInvalid
			runSteps(t, a, []step{
				{"install", install("g", 1, "m1", "m1", "m2"), core.OK, nil},
				{"managed commit", commitOp("g", "m1", 1, pair("orders", 0, 5)), core.OK, []core.ErrCode{ok}},
				{"unknown member", commitOp("g", "zz", 1, pair("orders", 0, 5)), core.ErrUnknownMember, nil},
				{"empty member with a generation", commitOp("g", "", 1, pair("orders", 0, 5)), core.ErrUnknownMember, nil},
				{"group never installed", commitOp("nope", "m1", 1, pair("orders", 0, 5)), core.ErrUnknownMember, nil},
				{"wrong generation", commitOp("g", "m1", 0, pair("orders", 0, 5)), core.ErrIllegalGeneration, nil},
				{"manual commit while members exist", commitOp("g", "", -1, pair("orders", 0, 5)), core.ErrIllegalGeneration, nil},
				{"generation -1 with a member", commitOp("g", "m1", -1, pair("orders", 0, 5)), core.ErrIllegalGeneration, nil},
				{"generation -2", commitOp("g", "", -2, pair("orders", 0, 5)), core.ErrIllegalGeneration, nil},
				{"manual commit of a group with no header", commitOp("solo", "", -1, pair("orders", 1, 9)), core.OK, []core.ErrCode{ok}},
				{"each offset alone", commitOp("g", "m1", 1,
					pair("orders", 0, 6), pair("orders", 9, 1), pair("orders", 1, -1),
					pair("nope", 0, 1), pair("orders", 1, 7), pair("orders", -1, 1)),
					core.OK, []core.ErrCode{ok, unk, bad, unk, ok, unk}},
				{"offset list is empty", commitOp("g", "m1", 1), core.OK, nil},
				{"too large", withMeta(commitOp("g", "m1", 1, pair("orders", 0, 5)), tooBig), core.ErrTooLarge, nil},
			})
			got := exec(t, a, core.OffsetFetch{Group: "g"}).Resp.(core.OffsetFetchResp)
			want := []core.PartitionOffset{
				{TP: core.NewTopicPartition("orders", 0), Offset: 6, LeaderEpoch: 7, Metadata: "m"},
				{TP: core.NewTopicPartition("orders", 1), Offset: 7, LeaderEpoch: 7, Metadata: "m"},
			}
			if !reflect.DeepEqual(got.Offsets, want) {
				t.Fatalf("offsets: %+v", got.Offsets)
			}
			solo := exec(t, a, core.OffsetFetch{Group: "solo"}).Resp.(core.OffsetFetchResp)
			if len(solo.Offsets) != 1 || solo.Offsets[0].Offset != 9 {
				t.Fatalf("solo: %+v", solo.Offsets)
			}
			if d := describe(t, a, "solo"); d.Exists {
				t.Fatalf("manual commit made a group: %+v", d)
			}
		})
	}
}

func withMeta(op core.OffsetCommit, b []byte) core.OffsetCommit {
	op.Offsets[0].Metadata = string(b)
	return op
}

func TestOffsetFetchReads(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			runSteps(t, a, []step{
				{"install", install("g", 1, "m1", "m1"), core.OK, nil},
				{"commit", commitOp("g", "m1", 1, pair("orders", 1, 3)), core.OK, []core.ErrCode{core.OK}},
			})
			got := exec(t, a, core.OffsetFetch{Group: "g", Partitions: []core.TopicPartition{
				core.NewTopicPartition("orders", 0), core.NewTopicPartition("orders", 1),
			}}).Resp.(core.OffsetFetchResp)
			want := []core.PartitionOffset{
				{TP: core.NewTopicPartition("orders", 0), Offset: -1, LeaderEpoch: -1},
				{TP: core.NewTopicPartition("orders", 1), Offset: 3, LeaderEpoch: 7, Metadata: "m"},
			}
			if !reflect.DeepEqual(got.Offsets, want) {
				t.Fatalf("asked: %+v", got.Offsets)
			}
			none := exec(t, a, core.OffsetFetch{Group: "nope"}).Resp.(core.OffsetFetchResp)
			if none.Offsets != nil {
				t.Fatalf("unknown group: %+v", none.Offsets)
			}
			if d := describe(t, a, "nope"); d.Exists || d.State != "" {
				t.Fatalf("unknown group describe: %+v", d)
			}
		})
	}
}

func TestGroupStatesAndList(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			runSteps(t, a, []step{
				{"b", install("b", 1, "m1", "m1"), core.OK, nil},
				{"a", install("a", 1, "m1", "m1", "m2"), core.OK, nil},
				{"assign a", assign("a", 1, "m1", "m1", "x", "m2", "y"), core.OK, nil},
			})
			ls := exec(t, a, core.ListGroups{}).Resp.(core.ListGroupsResp)
			want := []core.GroupSummary{
				{Name: "a", ProtocolType: "consumer", State: "Stable"},
				{Name: "b", ProtocolType: "consumer", State: "PreparingSync"},
			}
			if !reflect.DeepEqual(ls.Groups, want) {
				t.Fatalf("list: %+v", ls.Groups)
			}
		})
	}
}

// TestGroupLimitBoundary checks the 256 KiB rule at its edge. For this command
// the rule counts 101 bytes besides the metadata.
func TestGroupLimitBoundary(t *testing.T) {
	const fixed = 101
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := newGroupStore(t, mk)
			runSteps(t, a, []step{{"install", install("g", 1, "m1", "m1"), core.OK, nil}})
			fit := commitOp("g", "m1", 1, pair("orders", 0, 1))
			fit.Offsets[0].Metadata = strings.Repeat("m", 256<<10-fixed)
			runSteps(t, a, []step{{"at the limit", fit, core.OK, []core.ErrCode{core.OK}}})
			over := commitOp("g", "m1", 1, pair("orders", 0, 1))
			over.Offsets[0].Metadata = strings.Repeat("m", 256<<10-fixed+1)
			runSteps(t, a, []step{{"one byte over", over, core.ErrTooLarge, nil}})
		})
	}
}

// TestGroupTooLargeWritesNothing checks that a refused command leaves the state as it was.
func TestGroupTooLargeWritesNothing(t *testing.T) {
	a := newGroupStore(t, backends["mem"])
	runSteps(t, a, []step{
		{"install", install("g", 1, "m1", "m1", "m2"), core.OK, nil},
		{"assign", assign("g", 1, "m1", "m1", "a"), core.OK, nil},
	})
	before := snapshotBytes(t, a)
	runSteps(t, a, []step{
		{"too large assign", withBytes(assign("g", 1, "m1", "m1", "b"), tooBig), core.ErrTooLarge, nil},
		{"too large remove", core.GroupRemove{Group: "g", Members: []string{"m1", string(tooBig)}}, core.ErrTooLarge, nil},
	})
	if !bytes.Equal(before, snapshotBytes(t, a)) {
		t.Fatal("a refused command changed the state")
	}
}

func snapshotBytes(t *testing.T, a *stateupdater.StateUpdater) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 0); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// groupLog is a command list that uses every group command and a topic.
func groupLog() []stateupdater.Entry {
	return []stateupdater.Entry{
		{Kind: stateupdater.KindCreateTopic, Topic: "orders", Parts: 2},
		entryOf(install("g", 1, "m1", "m1", "m2")),
		entryOf(assign("g", 1, "m1", "m1", "a", "m2", "b")),
		entryOf(commitOp("g", "m1", 1, pair("orders", 0, 4), pair("orders", 1, 2))),
		entryOf(remove("g", "m2")),
		entryOf(install("g", 2, "m1", "m1", "m3")),
		entryOf(commitOp("solo", "", -1, pair("orders", 0, 1))),
		{Kind: stateupdater.KindCommit, Commit: core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{
			{TP: core.NewTopicPartition("orders", 0), Records: 3, Loc: core.SegmentRef{Key: "o1", Off: 0, Len: 9}},
		}}},
	}
}

func entryOf(op core.ControlPlaneOperation) stateupdater.Entry {
	e, ok := stateupdater.GroupEntry(op)
	if !ok {
		panic("not a group op")
	}
	return e
}

// TestGroupReplayIsPure applies the same log twice from scratch, through the
// entry codec, and checks that both stores hold the same bytes.
func TestGroupReplayIsPure(t *testing.T) {
	run := func() []byte {
		a := stateupdater.NewMem()
		defer func() { _ = a.Close() }()
		for i, e := range groupLog() {
			data := stateupdater.Encode(e)
			if _, err := a.ApplyBytes(int64(i), data); err != nil {
				t.Fatal(err)
			}
		}
		return snapshotBytes(t, a)
	}
	if first, second := run(), run(); !bytes.Equal(first, second) {
		t.Fatal("two replays of one log differ")
	}
}

// TestGroupCheckpointRoundTrip checks that the G and O keys survive a checkpoint.
func TestGroupCheckpointRoundTrip(t *testing.T) {
	a := stateupdater.NewMem()
	defer func() { _ = a.Close() }()
	for i, e := range groupLog() {
		a.Apply(int64(i), e)
	}
	data := snapshotBytes(t, a)
	b := stateupdater.NewMem()
	defer func() { _ = b.Close() }()
	if _, err := b.Restore(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, snapshotBytes(t, b)) {
		t.Fatal("restored checkpoint is not the same bytes")
	}
	for _, op := range []core.ControlPlaneOperation{
		core.DescribeGroup{Group: "g"}, core.OffsetFetch{Group: "g"}, core.ListGroups{}, core.OffsetFetch{Group: "solo"},
	} {
		if got, want := exec(t, b, op), exec(t, a, op); !reflect.DeepEqual(got, want) {
			t.Fatalf("%T after restore: %+v, want %+v", op, got, want)
		}
	}
}

func TestGroupEntryCodecRoundTrip(t *testing.T) {
	for _, e := range groupLog() {
		got, err := stateupdater.Decode(stateupdater.Encode(e))
		if err != nil {
			t.Fatalf("kind %d: %v", e.Kind, err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Fatalf("kind %d: decoded %+v, want %+v", e.Kind, got, e)
		}
	}
}

func TestGroupEntryNotAGroupOp(t *testing.T) {
	if _, ok := stateupdater.GroupEntry(core.CreateTopic{Name: "x", Partitions: 1}); ok {
		t.Fatal("CreateTopic is not a group entry")
	}
}

// TestGroupStoreAdapter runs the group calls through the Store adapter.
func TestGroupStoreAdapter(t *testing.T) {
	s := &stateupdater.Store{A: stateupdater.NewMem()}
	ctx := context.Background()
	if code, err := s.GroupInstall(ctx, install("g", 1, "m1", "m1")); code != core.OK || err != nil {
		t.Fatalf("install: %v %v", code, err)
	}
	if code, err := s.GroupAssign(ctx, assign("g", 1, "m1", "m1", "a")); code != core.OK || err != nil {
		t.Fatalf("assign: %v %v", code, err)
	}
	if code, err := s.GroupRemove(ctx, remove("g", "m1")); code != core.OK || err != nil {
		t.Fatalf("remove: %v %v", code, err)
	}
	if code, err := s.GroupAssign(ctx, assign("g", 1, "m1", "m1", "a")); code != core.ErrUnknownMember || err != nil {
		t.Fatalf("assign after remove: %v %v", code, err)
	}
	resp, code, err := s.OffsetCommit(ctx, commitOp("g", "", -1, pair("orders", 0, 3)))
	if code != core.OK || err != nil || len(resp.Errs) != 1 || resp.Errs[0] != core.ErrUnknownPartition {
		t.Fatalf("commit to a topic that does not exist: %+v %v %v", resp, code, err)
	}
	if _, err := s.OffsetFetch(ctx, core.OffsetFetch{Group: "g"}); err != nil {
		t.Fatal(err)
	}
	if d, err := s.DescribeGroup(ctx, core.DescribeGroup{Group: "g"}); err != nil || d.State != "Empty" {
		t.Fatalf("describe: %+v %v", d, err)
	}
	if ls, err := s.ListGroups(ctx, core.ListGroups{}); err != nil || len(ls.Groups) != 1 {
		t.Fatalf("list: %+v %v", ls, err)
	}
}
