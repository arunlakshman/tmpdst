package speccheck

import (
	"reflect"
	"strings"
	"testing"

	"mds/internal/core"
)

// gBig is a byte string that makes a group command pass 256 KiB.
var gBig = strings.Repeat("x", 300<<10)

func gMember(id string) core.GroupMember {
	return core.GroupMember{
		MemberID: id, ClientID: "client-" + id, ClientHost: "/10.0.0.1",
		SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("sub-" + id),
	}
}

func gInstall(group string, gen int32, leader string, ids ...string) core.GroupInstall {
	operation := core.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, Leader: leader, TimeMs: 1000}
	for _, id := range ids {
		operation.Members = append(operation.Members, gMember(id))
	}
	return operation
}

// gAssign takes pairs of member id and assignment bytes.
func gAssign(group string, gen int32, leader string, pairs ...string) core.GroupAssign {
	operation := core.GroupAssign{Group: group, Generation: gen, Leader: leader, TimeMs: 2000}
	for i := 0; i+1 < len(pairs); i += 2 {
		operation.Assignments = append(operation.Assignments, core.GroupAssignment{Member: pairs[i], Bytes: []byte(pairs[i+1])})
	}
	return operation
}

func gRemove(group, reason string, ids ...string) core.GroupRemove {
	return core.GroupRemove{Group: group, Members: ids, TimeMs: 3000, Reason: reason}
}

// gPair is one offset of topic partition topic-p, with epoch 7 and metadata "m".
func gPair(topic string, p int32, off int64) core.PartitionOffset {
	return core.PartitionOffset{TopicPartition: tp(topic, p), Offset: off, LeaderEpoch: 7, Metadata: "m"}
}

func gCommit(group, member string, gen int32, offs ...core.PartitionOffset) core.OffsetCommit {
	return core.OffsetCommit{Group: group, Member: member, Generation: gen, TimeMs: 4000, Offsets: offs}
}

// newGroupControl is a control plane with one topic, "orders" with 2 partitions.
func newGroupControl(t *testing.T) *ControlPlane {
	t.Helper()
	c := NewControlPlane()
	if d := call(c, core.CreateTopic{Name: "orders", Partitions: 2}); d.Err != core.OK {
		t.Fatalf("create topic: %+v", d)
	}
	return c
}

// gStep is one call and the answer it must give. errs, when set, is the
// per-offset answer of an OffsetCommit.
type gStep struct {
	name      string
	operation core.ControlPlaneOperation
	want      core.ErrCode
	errs      []core.ErrCode
}

func gRun(t *testing.T, c *ControlPlane, steps []gStep) {
	t.Helper()
	for _, s := range steps {
		d := call(c, s.operation)
		if d.Err != s.want {
			t.Fatalf("%s: err %v, want %v", s.name, d.Err, s.want)
		}
		if d.Err != core.OK && d.Outcome != core.NotApplied {
			t.Fatalf("%s: a refused call has outcome %v", s.name, d.Outcome)
		}
		if s.errs != nil {
			got, _ := d.Resp.(core.OffsetCommitResp)
			if !reflect.DeepEqual(got.Errs, s.errs) {
				t.Fatalf("%s: per-offset errs %v, want %v", s.name, got.Errs, s.errs)
			}
		}
	}
}

func gDescribe(t *testing.T, c *ControlPlane, group string) core.DescribeGroupResp {
	t.Helper()
	return call(c, core.DescribeGroup{Group: group}).Resp.(core.DescribeGroupResp)
}

func TestGroupInstallOracle(t *testing.T) {
	c := newGroupControl(t)
	big := gInstall("g", 2, "m1", "m1")
	big.Members[0].Metadata = []byte(gBig)
	gRun(t, c, []gStep{
		{name: "gen 1 is the first generation", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.OK},
		{name: "same generation again", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.ErrIllegalGeneration},
		{name: "generation skips ahead", operation: gInstall("g", 3, "m1", "m1", "m2"), want: core.ErrIllegalGeneration},
		{name: "no members", operation: gInstall("g", 2, "m1"), want: core.ErrInvalid},
		{name: "empty group name", operation: gInstall("", 1, "m1", "m1"), want: core.ErrInvalid},
		{name: "leader is not a member", operation: gInstall("g", 2, "m9", "m1", "m2"), want: core.ErrInvalid},
		{name: "repeated member id", operation: gInstall("g", 2, "m1", "m1", "m1"), want: core.ErrInvalid},
		{name: "empty member id", operation: gInstall("g", 2, "m1", "", "m1"), want: core.ErrInvalid},
		{name: "too large", operation: big, want: core.ErrTooLarge},
		{name: "next generation", operation: gInstall("g", 2, "m2", "m2", "m1"), want: core.OK},
	})
	d := gDescribe(t, c, "g")
	if !d.Exists || d.Generation != 2 || d.Leader != "m2" || d.State != "PreparingSync" || d.AssignedGeneration != 0 {
		t.Fatalf("describe after install: %+v", d)
	}
	if d.ProtocolType != "consumer" || d.Protocol != "range" {
		t.Fatalf("protocol: %+v", d)
	}
	if len(d.Members) != 2 || d.Members[0].MemberID != "m1" || d.Members[1].MemberID != "m2" {
		t.Fatalf("members not sorted by id: %+v", d.Members)
	}
	if d.Members[0].JoinedTimeMs != 1000 || d.Members[0].Assignment != nil {
		t.Fatalf("member m1: %+v", d.Members[0])
	}
	if string(d.Members[0].Metadata) != "sub-m1" || d.Members[0].ClientID != "client-m1" {
		t.Fatalf("member m1 fields: %+v", d.Members[0].GroupMember)
	}
}

func TestGroupAssignOracle(t *testing.T) {
	c := newGroupControl(t)
	big := gAssign("g", 1, "m1", "m1", "a")
	big.Assignments[0].Bytes = []byte(gBig)
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.OK},
		{name: "assign", operation: gAssign("g", 1, "m1", "m1", "a", "m2", "b"), want: core.OK},
		{name: "replay of the same assign", operation: gAssign("g", 1, "m1", "m1", "a", "m2", "b"), want: core.OK},
		{name: "wrong generation", operation: gAssign("g", 2, "m1", "m1", "a"), want: core.ErrIllegalGeneration},
		{name: "not the leader", operation: gAssign("g", 1, "m2", "m1", "a"), want: core.ErrUnknownMember},
		{name: "unknown member", operation: gAssign("g", 1, "m1", "zz", "a"), want: core.ErrUnknownMember},
		{name: "group never installed", operation: gAssign("nope", 0, "m1", "m1", "a"), want: core.ErrUnknownMember},
		{name: "member repeated", operation: gAssign("g", 1, "m1", "m1", "a", "m1", "b"), want: core.ErrInvalid},
		{name: "too large", operation: big, want: core.ErrTooLarge},
		{name: "assign again without m2", operation: gAssign("g", 1, "m1", "m1", "x"), want: core.OK},
	})
	d := gDescribe(t, c, "g")
	if d.State != "Stable" || d.AssignedGeneration != 1 {
		t.Fatalf("describe after assign: %+v", d)
	}
	if len(d.Members) != 2 || string(d.Members[0].Assignment) != "x" || d.Members[1].Assignment != nil {
		t.Fatalf("assignments: %+v", d.Members)
	}
}

func TestGroupAssignLeaderCleared(t *testing.T) {
	c := newGroupControl(t)
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.OK},
		{name: "leader leaves", operation: gRemove("g", "leave", "m1"), want: core.OK},
		{name: "assign with no leader", operation: gAssign("g", 1, "", "m2", "a"), want: core.ErrUnknownMember},
	})
}

func TestGroupRemoveOracle(t *testing.T) {
	c := newGroupControl(t)
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1", "m2", "m3"), want: core.OK},
		{name: "assign", operation: gAssign("g", 1, "m1", "m1", "a"), want: core.OK},
		{name: "unknown member is ignored", operation: gRemove("g", "leave", "zz"), want: core.OK},
		{name: "remove m2", operation: gRemove("g", "leave", "m2"), want: core.OK},
		{name: "remove the leader", operation: gRemove("g", "leave", "m1"), want: core.OK},
		{name: "group never installed", operation: gRemove("nope", "expire", "m1"), want: core.OK},
		{name: "too large", operation: gRemove("g", "leave", gBig), want: core.ErrTooLarge},
	})
	d := gDescribe(t, c, "g")
	if !d.Exists || d.Generation != 1 || d.Leader != "" || d.State != "Stable" || len(d.Members) != 1 {
		t.Fatalf("after removes: %+v", d)
	}
	gRun(t, c, []gStep{
		{name: "remove the last member", operation: gRemove("g", "leave", "m3"), want: core.OK},
		{name: "next generation after empty", operation: gInstall("g", 2, "m3", "m3"), want: core.OK},
	})
}

// TestGroupRemoveEmptiesGroup: removing every member keeps the generation and
// clears the assignment generation.
func TestGroupRemoveEmptiesGroup(t *testing.T) {
	c := newGroupControl(t)
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.OK},
		{name: "assign", operation: gAssign("g", 1, "m1", "m1", "a", "m2", "b"), want: core.OK},
		{name: "remove leader", operation: gRemove("g", "leave", "m1"), want: core.OK},
		{name: "remove the rest", operation: gRemove("g", "expire", "m2"), want: core.OK},
	})
	d := gDescribe(t, c, "g")
	want := core.DescribeGroupResp{Exists: true, Generation: 1, ProtocolType: "consumer", Protocol: "range", State: "Empty"}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("describe empty: %+v, want %+v", d, want)
	}
	ls := call(c, core.ListGroups{}).Resp.(core.ListGroupsResp)
	if !reflect.DeepEqual(ls.Groups, []core.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Empty"}}) {
		t.Fatalf("list: %+v", ls.Groups)
	}
}

// TestGroupExpireKeepsAssignment: an expired member that is not the leader
// leaves the rest of the group as it was.
func TestGroupExpireKeepsAssignment(t *testing.T) {
	c := newGroupControl(t)
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.OK},
		{name: "assign", operation: gAssign("g", 1, "m1", "m1", "a", "m2", "b"), want: core.OK},
		{name: "expire m2", operation: gRemove("g", "expire", "m2"), want: core.OK},
	})
	d := gDescribe(t, c, "g")
	if d.Leader != "m1" || d.State != "Stable" || d.AssignedGeneration != 1 || len(d.Members) != 1 {
		t.Fatalf("after expire: %+v", d)
	}
}

func TestOffsetCommitOracle(t *testing.T) {
	c := newGroupControl(t)
	big := gCommit("g", "m1", 1, gPair("orders", 0, 5))
	big.Offsets[0].Metadata = gBig
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1", "m2"), want: core.OK},
		{name: "managed commit", operation: gCommit("g", "m1", 1, gPair("orders", 0, 5)), want: core.OK, errs: []core.ErrCode{core.OK}},
		{name: "unknown group", operation: gCommit("nope", "m1", 1, gPair("orders", 0, 5)), want: core.ErrUnknownMember},
		{name: "unknown member", operation: gCommit("g", "zz", 1, gPair("orders", 0, 5)), want: core.ErrUnknownMember},
		{name: "empty member with a generation", operation: gCommit("g", "", 1, gPair("orders", 0, 5)), want: core.ErrUnknownMember},
		{name: "generation 0 is wrong", operation: gCommit("g", "m1", 0, gPair("orders", 0, 5)), want: core.ErrIllegalGeneration},
		{name: "generation 2 is wrong", operation: gCommit("g", "m1", 2, gPair("orders", 0, 5)), want: core.ErrIllegalGeneration},
		{name: "manual commit while members exist", operation: gCommit("g", "", -1, gPair("orders", 0, 5)), want: core.ErrIllegalGeneration},
		{name: "generation -1 with a member", operation: gCommit("g", "m1", -1, gPair("orders", 0, 5)), want: core.ErrIllegalGeneration},
		{name: "generation -2", operation: gCommit("g", "", -2, gPair("orders", 0, 5)), want: core.ErrIllegalGeneration},
		{name: "manual commit to a group with no header", operation: gCommit("solo", "", -1, gPair("orders", 1, 9)), want: core.OK, errs: []core.ErrCode{core.OK}},
		{name: "each offset is checked alone", operation: gCommit("g", "m1", 1,
			gPair("orders", 0, 6), gPair("orders", 9, 1), gPair("orders", 1, -1),
			gPair("nope", 0, 1), gPair("orders", 1, 7), gPair("orders", -1, 1)),
			want: core.OK, errs: []core.ErrCode{core.OK, core.ErrUnknownPartition, core.ErrInvalid, core.ErrUnknownPartition, core.OK, core.ErrUnknownPartition}},
		{name: "empty offset list", operation: gCommit("g", "m1", 1), want: core.OK},
		{name: "too large", operation: big, want: core.ErrTooLarge},
	})
	got := call(c, core.OffsetFetch{Group: "g"}).Resp.(core.OffsetFetchResp)
	want := []core.PartitionOffset{
		{TopicPartition: tp("orders", 0), Offset: 6, LeaderEpoch: 7, Metadata: "m"},
		{TopicPartition: tp("orders", 1), Offset: 7, LeaderEpoch: 7, Metadata: "m"},
	}
	if !reflect.DeepEqual(got.Offsets, want) {
		t.Fatalf("offsets: %+v", got.Offsets)
	}
	solo := call(c, core.OffsetFetch{Group: "solo"}).Resp.(core.OffsetFetchResp)
	if len(solo.Offsets) != 1 || solo.Offsets[0].Offset != 9 {
		t.Fatalf("solo: %+v", solo.Offsets)
	}
	if d := gDescribe(t, c, "solo"); d.Exists {
		t.Fatalf("manual commit made a group: %+v", d)
	}
}

// TestOffsetCommitManualWhenEmpty: a manual commit is allowed once the group
// has no members left.
func TestOffsetCommitManualWhenEmpty(t *testing.T) {
	c := newGroupControl(t)
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1"), want: core.OK},
		{name: "leave", operation: gRemove("g", "leave", "m1"), want: core.OK},
		{name: "manual commit when empty", operation: gCommit("g", "", -1, gPair("orders", 0, 5)), want: core.OK, errs: []core.ErrCode{core.OK}},
		{name: "managed commit by a left member", operation: gCommit("g", "m1", 1, gPair("orders", 0, 6)), want: core.ErrUnknownMember},
	})
}

// TestOffsetCommitOutcome: a commit with no good offset is not applied, and
// one good offset makes it applied.
func TestOffsetCommitOutcome(t *testing.T) {
	c := newGroupControl(t)
	if d := call(c, gCommit("g", "", -1, gPair("orders", 9, 1))); d.Err != core.OK || d.Outcome != core.NotApplied {
		t.Fatalf("all offsets bad: %+v", d)
	}
	if d := call(c, gCommit("g", "", -1, gPair("orders", 9, 1), gPair("orders", 0, 1))); d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatalf("one good offset: %+v", d)
	}
	if d := call(c, gCommit("g", "", -1)); d.Outcome != core.NotApplied {
		t.Fatalf("no offsets: %+v", d)
	}
}

func TestOffsetFetchOracle(t *testing.T) {
	c := newGroupControl(t)
	if code := call(c, core.CreateTopic{Name: "beta", Partitions: 1}).Err; code != core.OK {
		t.Fatalf("create beta: %v", code)
	}
	gRun(t, c, []gStep{
		{name: "install", operation: gInstall("g", 1, "m1", "m1"), want: core.OK},
		{name: "commit", operation: gCommit("g", "m1", 1, gPair("orders", 1, 3), gPair("orders", 0, 2), gPair("beta", 0, 4)), want: core.OK, errs: []core.ErrCode{core.OK, core.OK, core.OK}},
	})
	all := call(c, core.OffsetFetch{Group: "g"}).Resp.(core.OffsetFetchResp)
	wantAll := []core.PartitionOffset{
		{TopicPartition: tp("beta", 0), Offset: 4, LeaderEpoch: 7, Metadata: "m"},
		{TopicPartition: tp("orders", 0), Offset: 2, LeaderEpoch: 7, Metadata: "m"},
		{TopicPartition: tp("orders", 1), Offset: 3, LeaderEpoch: 7, Metadata: "m"},
	}
	if !reflect.DeepEqual(all.Offsets, wantAll) {
		t.Fatalf("all: %+v", all.Offsets)
	}
	listed := call(c, core.OffsetFetch{Group: "g", Partitions: []core.TopicPartition{tp("orders", 0), tp("zz", 0)}}).Resp.(core.OffsetFetchResp)
	wantListed := []core.PartitionOffset{
		{TopicPartition: tp("orders", 0), Offset: 2, LeaderEpoch: 7, Metadata: "m"},
		{TopicPartition: tp("zz", 0), Offset: -1, LeaderEpoch: -1},
	}
	if !reflect.DeepEqual(listed.Offsets, wantListed) {
		t.Fatalf("listed: %+v", listed.Offsets)
	}
	none := call(c, core.OffsetFetch{Group: "nope"}).Resp.(core.OffsetFetchResp)
	if none.Offsets != nil {
		t.Fatalf("unknown group, all: %+v", none.Offsets)
	}
	miss := call(c, core.OffsetFetch{Group: "nope", Partitions: []core.TopicPartition{tp("orders", 1)}}).Resp.(core.OffsetFetchResp)
	if !reflect.DeepEqual(miss.Offsets, []core.PartitionOffset{{TopicPartition: tp("orders", 1), Offset: -1, LeaderEpoch: -1}}) {
		t.Fatalf("unknown group, listed: %+v", miss.Offsets)
	}
}

func TestDescribeGroupStates(t *testing.T) {
	c := newGroupControl(t)
	if d := gDescribe(t, c, "never"); d.Exists || d.State != "" || d.Members != nil {
		t.Fatalf("never installed: %+v", d)
	}
	gRun(t, c, []gStep{
		{name: "b installed", operation: gInstall("b", 1, "m1", "m1"), want: core.OK},
		{name: "a installed, members out of order", operation: gInstall("a", 1, "m1", "m2", "m1"), want: core.OK},
		{name: "a assigned", operation: gAssign("a", 1, "m1", "m1", "x"), want: core.OK},
		{name: "c installed", operation: gInstall("c", 1, "m1", "m1"), want: core.OK},
		{name: "c empty", operation: gRemove("c", "leave", "m1"), want: core.OK},
	})
	a := gDescribe(t, c, "a")
	if !a.Exists || a.State != "Stable" || a.AssignedGeneration != 1 || len(a.Members) != 2 {
		t.Fatalf("a: %+v", a)
	}
	if a.Members[0].MemberID != "m1" || string(a.Members[0].Assignment) != "x" || a.Members[1].MemberID != "m2" || a.Members[1].Assignment != nil {
		t.Fatalf("a members: %+v", a.Members)
	}
	if a.Members[0].JoinedTimeMs != 1000 || string(a.Members[0].Metadata) != "sub-m1" {
		t.Fatalf("a member fields: %+v", a.Members[0])
	}
	if b := gDescribe(t, c, "b"); b.State != "PreparingSync" {
		t.Fatalf("b: %+v", b)
	}
	if cc := gDescribe(t, c, "c"); cc.State != "Empty" || !cc.Exists {
		t.Fatalf("c: %+v", cc)
	}
	ls := call(c, core.ListGroups{}).Resp.(core.ListGroupsResp)
	want := []core.GroupSummary{
		{Name: "a", ProtocolType: "consumer", State: "Stable"},
		{Name: "b", ProtocolType: "consumer", State: "PreparingSync"},
		{Name: "c", ProtocolType: "consumer", State: "Empty"},
	}
	if !reflect.DeepEqual(ls.Groups, want) {
		t.Fatalf("list: %+v", ls.Groups)
	}
}

func TestListGroupsEmpty(t *testing.T) {
	c := NewControlPlane()
	if ls := call(c, core.ListGroups{}).Resp.(core.ListGroupsResp); ls.Groups != nil {
		t.Fatalf("no groups: %+v", ls.Groups)
	}
}

// TestGroupLimitBoundaryOracle checks the 256 KiB rule at its edge for an
// offset commit. The fixed part of this command is 101 bytes.
func TestGroupLimitBoundaryOracle(t *testing.T) {
	const fixed = 101
	c := newGroupControl(t)
	gRun(t, c, []gStep{{name: "install", operation: gInstall("g", 1, "m1", "m1"), want: core.OK}})
	fit := gCommit("g", "m1", 1, gPair("orders", 0, 1))
	fit.Offsets[0].Metadata = strings.Repeat("m", 256<<10-fixed)
	over := gCommit("g", "m1", 1, gPair("orders", 0, 1))
	over.Offsets[0].Metadata = strings.Repeat("m", 256<<10-fixed+1)
	gRun(t, c, []gStep{
		{name: "at the limit", operation: fit, want: core.OK, errs: []core.ErrCode{core.OK}},
		{name: "one byte over", operation: over, want: core.ErrTooLarge},
	})
}

// TestCompareTP checks the order of topic partitions: by topic name first,
// then by partition number.
func TestCompareTP(t *testing.T) {
	cases := []struct {
		a, b core.TopicPartition
		want int
	}{
		{tp("a", 5), tp("b", 0), -1},
		{tp("b", 0), tp("a", 5), 1},
		{tp("a", 1), tp("a", 3), -2},
		{tp("a", 3), tp("a", 1), 2},
		{tp("a", 2), tp("a", 2), 0},
	}
	for _, tc := range cases {
		if got := compareTP(tc.a, tc.b); got != tc.want {
			t.Fatalf("compareTP(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
