package group

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// plainView is a View in plain maps, so the rules run with no store.
type plainView struct {
	headers    map[string]Header
	members    map[string][]contract.GroupMemberInfo // in id order
	partitions map[string]int64
}

func (v plainView) Header(group string) (Header, bool) {
	h, ok := v.headers[group]
	return h, ok
}

func (v plainView) HasMember(group, member string) bool {
	for _, m := range v.members[group] {
		if m.MemberID == member {
			return true
		}
	}
	return false
}

func (v plainView) MemberIDs(group string) []string {
	var ids []string
	for _, m := range v.members[group] {
		ids = append(ids, m.MemberID)
	}
	return ids
}

func (v plainView) Members(group string) []contract.GroupMemberInfo {
	return append([]contract.GroupMemberInfo(nil), v.members[group]...)
}

func (v plainView) PartitionCount(topic string) int64 { return v.partitions[topic] }

func member(id string) contract.GroupMember {
	return contract.GroupMember{MemberID: id, ClientID: "client-" + id, SessionTimeoutMs: 10000, Metadata: []byte("sub-" + id)}
}

func info(id string, joined int64, assignment string) contract.GroupMemberInfo {
	m := contract.GroupMemberInfo{GroupMember: member(id), JoinedTimeMs: joined}
	if assignment != "" {
		m.Assignment = []byte(assignment)
	}
	return m
}

func installOf(group string, gen int32, leader string, ids ...string) contract.GroupInstall {
	op := contract.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, Leader: leader, TimeMs: 1000}
	for _, id := range ids {
		op.Members = append(op.Members, member(id))
	}
	return op
}

func assignOf(group string, gen int32, leader string, pairs ...string) contract.GroupAssign {
	op := contract.GroupAssign{Group: group, Generation: gen, Leader: leader}
	for i := 0; i+1 < len(pairs); i += 2 {
		op.Assignments = append(op.Assignments, contract.GroupAssignment{Member: pairs[i], Bytes: []byte(pairs[i+1])})
	}
	return op
}

func offset(topic string, partition int32, off int64) contract.PartitionOffset {
	return contract.PartitionOffset{TopicPartition: contract.NewTopicPartition(topic, partition), Offset: off, LeaderEpoch: 7, Metadata: "m"}
}

// stable is a view with group g at generation 2 with members m1 (the leader)
// and m2, assigned for generation 2, and a topic "orders" of 2 partitions.
func stable() plainView {
	return plainView{
		headers:    map[string]Header{"g": {Generation: 2, ProtocolType: "consumer", Protocol: "range", Leader: "m1", AssignedGeneration: 2}},
		members:    map[string][]contract.GroupMemberInfo{"g": {info("m1", 1000, "a"), info("m2", 1000, "b")}},
		partitions: map[string]int64{"orders": 2},
	}
}

func TestDecideInstall(t *testing.T) {
	view := stable()
	next := Header{Generation: 3, ProtocolType: "consumer", Protocol: "range", Leader: "m2"}
	for _, c := range []struct {
		name      string
		operation contract.GroupInstall
		events    []Event
		code      runtime.ErrCode
	}{
		{
			name:      "the first generation of a new group",
			operation: installOf("new", 1, "m1", "m1", "m2"),
			events: []Event{GroupInstalled{Group: "new", Header: Header{Generation: 1, ProtocolType: "consumer", Protocol: "range", Leader: "m1"},
				Members: []contract.GroupMemberInfo{info("m1", 1000, ""), info("m2", 1000, "")}}},
			code: runtime.OK,
		},
		{
			name:      "the next generation replaces the members",
			operation: installOf("g", 3, "m2", "m2", "m3"),
			events:    []Event{GroupInstalled{Group: "g", Header: next, Members: []contract.GroupMemberInfo{info("m2", 1000, ""), info("m3", 1000, "")}, Replaced: []string{"m1", "m2"}}},
			code:      runtime.OK,
		},
		{name: "the same generation again", operation: installOf("g", 2, "m1", "m1"), code: runtime.ErrIllegalGeneration},
		{name: "a generation that skips ahead", operation: installOf("g", 4, "m1", "m1"), code: runtime.ErrIllegalGeneration},
		{name: "an old generation", operation: installOf("g", 1, "m1", "m1"), code: runtime.ErrIllegalGeneration},
		{name: "a new group must start at 1", operation: installOf("new", 2, "m1", "m1"), code: runtime.ErrIllegalGeneration},
		{name: "no members", operation: installOf("g", 3, ""), code: runtime.ErrInvalid},
		{name: "the leader is not a member", operation: installOf("g", 3, "m9", "m1", "m2"), code: runtime.ErrInvalid},
		{name: "a repeated member id", operation: installOf("g", 3, "m1", "m1", "m1"), code: runtime.ErrInvalid},
		{name: "an empty member id", operation: installOf("g", 3, "", ""), code: runtime.ErrInvalid},
		{name: "an empty group name", operation: installOf("", 1, "m1", "m1"), code: runtime.ErrInvalid},
		{name: "too large", operation: func() contract.GroupInstall {
			op := installOf("g", 3, "m1", "m1")
			op.Members[0].Metadata = bytes.Repeat([]byte{'x'}, 300<<10)
			return op
		}(), code: runtime.ErrTooLarge},
	} {
		events, code := DecideInstall(view, c.operation)
		if code != c.code || !reflect.DeepEqual(events, c.events) {
			t.Errorf("%s:\n got %v %+v\nwant %v %+v", c.name, code, events, c.code, c.events)
		}
	}
}

func TestDecideAssign(t *testing.T) {
	view := stable()
	header := Header{Generation: 2, ProtocolType: "consumer", Protocol: "range", Leader: "m1", AssignedGeneration: 2}
	for _, c := range []struct {
		name      string
		operation contract.GroupAssign
		events    []Event
		code      runtime.ErrCode
	}{
		{
			name:      "the leader assigns both members",
			operation: assignOf("g", 2, "m1", "m1", "x", "m2", "y"),
			events:    []Event{AssignmentsSet{Group: "g", Header: header, Members: []contract.GroupMemberInfo{info("m1", 1000, "x"), info("m2", 1000, "y")}}},
			code:      runtime.OK,
		},
		{
			name:      "a member with no bytes loses its assignment",
			operation: assignOf("g", 2, "m1", "m1", "x"),
			events:    []Event{AssignmentsSet{Group: "g", Header: header, Members: []contract.GroupMemberInfo{info("m1", 1000, "x"), info("m2", 1000, "")}}},
			code:      runtime.OK,
		},
		{
			name:      "no assignments clears every assignment",
			operation: assignOf("g", 2, "m1"),
			events:    []Event{AssignmentsSet{Group: "g", Header: header, Members: []contract.GroupMemberInfo{info("m1", 1000, ""), info("m2", 1000, "")}}},
			code:      runtime.OK,
		},
		{name: "a stale generation", operation: assignOf("g", 1, "m1", "m1", "x"), code: runtime.ErrIllegalGeneration},
		{name: "a generation ahead", operation: assignOf("g", 3, "m1", "m1", "x"), code: runtime.ErrIllegalGeneration},
		{name: "a sender that is not the leader", operation: assignOf("g", 2, "m2", "m1", "x"), code: runtime.ErrUnknownMember},
		{name: "a sender that is not a member", operation: assignOf("g", 2, "zz", "m1", "x"), code: runtime.ErrUnknownMember},
		{name: "an assignment for an unknown member", operation: assignOf("g", 2, "m1", "m1", "x", "zz", "y"), code: runtime.ErrUnknownMember},
		{name: "a member assigned twice", operation: assignOf("g", 2, "m1", "m1", "x", "m1", "y"), code: runtime.ErrInvalid},
		{name: "a group that was never installed", operation: assignOf("nope", 0, "m1", "m1", "x"), code: runtime.ErrUnknownMember},
		{name: "too large", operation: func() contract.GroupAssign {
			op := assignOf("g", 2, "m1", "m1", "x")
			op.Assignments[0].Bytes = bytes.Repeat([]byte{'x'}, 300<<10)
			return op
		}(), code: runtime.ErrTooLarge},
	} {
		events, code := DecideAssign(view, c.operation)
		if code != c.code || !reflect.DeepEqual(events, c.events) {
			t.Errorf("%s:\n got %v %+v\nwant %v %+v", c.name, code, events, c.code, c.events)
		}
	}
}

func TestDecideRemove(t *testing.T) {
	view := stable()
	view.headers["g"] = Header{Generation: 2, ProtocolType: "consumer", Protocol: "range", Leader: "m1", AssignedGeneration: 2}
	both := Header{Generation: 2, ProtocolType: "consumer", Protocol: "range", Leader: "m1", AssignedGeneration: 2}
	remove := func(ids ...string) contract.GroupRemove {
		return contract.GroupRemove{Group: "g", Members: ids, Reason: "leave"}
	}
	for _, c := range []struct {
		name      string
		operation contract.GroupRemove
		events    []Event
		code      runtime.ErrCode
	}{
		{name: "a group that was never installed", operation: contract.GroupRemove{Group: "nope", Members: []string{"m1"}}, code: runtime.OK},
		{
			name:      "a member that is not in the group is ignored",
			operation: remove("zz"),
			events:    []Event{MembersRemoved{Group: "g", Header: both}},
			code:      runtime.OK,
		},
		{
			name:      "a member that is not the leader leaves",
			operation: remove("m2"),
			events:    []Event{MembersRemoved{Group: "g", Members: []string{"m2"}, Header: both}},
			code:      runtime.OK,
		},
		{
			name:      "the leader leaves and the leader is cleared",
			operation: remove("m1"),
			events:    []Event{MembersRemoved{Group: "g", Members: []string{"m1"}, Header: Header{Generation: 2, ProtocolType: "consumer", Protocol: "range", AssignedGeneration: 2}}},
			code:      runtime.OK,
		},
		{
			name:      "the last member leaves and the leader and the assignment are cleared",
			operation: remove("m1", "m2"),
			events:    []Event{MembersRemoved{Group: "g", Members: []string{"m1", "m2"}, Header: Header{Generation: 2, ProtocolType: "consumer", Protocol: "range"}}},
			code:      runtime.OK,
		},
		{
			name:      "a member listed twice leaves once, and the group is not empty",
			operation: remove("m2", "m2"),
			events:    []Event{MembersRemoved{Group: "g", Members: []string{"m2"}, Header: both}},
			code:      runtime.OK,
		},
		{name: "too large", operation: remove("m1", strings.Repeat("x", 300<<10)), code: runtime.ErrTooLarge},
	} {
		events, code := DecideRemove(view, c.operation)
		if code != c.code || !reflect.DeepEqual(events, c.events) {
			t.Errorf("%s:\n got %v %+v\nwant %v %+v", c.name, code, events, c.code, c.events)
		}
	}
}

func commitOf(group, member string, gen int32, offs ...contract.PartitionOffset) contract.OffsetCommit {
	return contract.OffsetCommit{Group: group, Member: member, Generation: gen, TimeMs: 4000, Offsets: offs}
}

func TestDecideCommitOffsets(t *testing.T) {
	view := stable()
	view.members["empty"] = nil
	view.headers["empty"] = Header{Generation: 1}
	ok, unknown, invalid := runtime.OK, runtime.ErrUnknownPartition, runtime.ErrInvalid
	one := offset("orders", 0, 5)
	for _, c := range []struct {
		name      string
		operation contract.OffsetCommit
		events    []Event
		errs      []runtime.ErrCode
		code      runtime.ErrCode
	}{
		{
			name:      "a managed commit of a member of the current generation",
			operation: commitOf("g", "m1", 2, one),
			events:    []Event{OffsetsCommitted{Group: "g", TimeMs: 4000, Offsets: []contract.PartitionOffset{one}}},
			errs:      []runtime.ErrCode{ok},
		},
		{name: "a member that is not in the group", operation: commitOf("g", "zz", 2, one), code: runtime.ErrUnknownMember},
		{name: "an empty member with a generation", operation: commitOf("g", "", 2, one), code: runtime.ErrUnknownMember},
		{name: "a group that was never installed", operation: commitOf("nope", "m1", 2, one), code: runtime.ErrUnknownMember},
		{name: "a stale generation", operation: commitOf("g", "m1", 1, one), code: runtime.ErrIllegalGeneration},
		{name: "a generation ahead", operation: commitOf("g", "m1", 3, one), code: runtime.ErrIllegalGeneration},
		{name: "an unknown member is refused before the generation", operation: commitOf("g", "zz", 1, one), code: runtime.ErrUnknownMember},
		{name: "a manual commit while members exist", operation: commitOf("g", "", -1, one), code: runtime.ErrIllegalGeneration},
		{name: "generation -1 with a member", operation: commitOf("g", "m1", -1, one), code: runtime.ErrIllegalGeneration},
		{name: "generation -2", operation: commitOf("g", "", -2, one), code: runtime.ErrIllegalGeneration},
		{
			name:      "a manual commit of a group with no members",
			operation: commitOf("empty", "", -1, one),
			events:    []Event{OffsetsCommitted{Group: "empty", TimeMs: 4000, Offsets: []contract.PartitionOffset{one}}},
			errs:      []runtime.ErrCode{ok},
		},
		{
			name:      "a manual commit of a group with no header",
			operation: commitOf("solo", "", -1, one),
			events:    []Event{OffsetsCommitted{Group: "solo", TimeMs: 4000, Offsets: []contract.PartitionOffset{one}}},
			errs:      []runtime.ErrCode{ok},
		},
		{
			name: "each offset is checked alone and only the good ones are stored",
			operation: commitOf("g", "m1", 2, offset("orders", 0, 6), offset("orders", 9, 1), offset("orders", 1, -1),
				offset("nope", 0, 1), offset("orders", 1, 7), offset("orders", -1, 1)),
			events: []Event{OffsetsCommitted{Group: "g", TimeMs: 4000, Offsets: []contract.PartitionOffset{offset("orders", 0, 6), offset("orders", 1, 7)}}},
			errs:   []runtime.ErrCode{ok, unknown, invalid, unknown, ok, unknown},
		},
		{name: "no good offset makes no event", operation: commitOf("g", "m1", 2, offset("nope", 0, 1)), errs: []runtime.ErrCode{unknown}},
		{name: "an empty offset list makes no event", operation: commitOf("g", "m1", 2)},
	} {
		events, errs, code := DecideCommitOffsets(view, c.operation)
		if code != c.code || !reflect.DeepEqual(events, c.events) || !reflect.DeepEqual(errs, c.errs) {
			t.Errorf("%s:\n got %v %+v %v\nwant %v %+v %v", c.name, code, events, errs, c.code, c.events, c.errs)
		}
	}
}

// TestDecideCommitOffsetsLimit checks the 256 KiB rule at its edge. For this
// command the rule counts 101 bytes besides the metadata.
func TestDecideCommitOffsetsLimit(t *testing.T) {
	const fixed = 101
	view := stable()
	fit := commitOf("g", "m1", 2, offset("orders", 0, 1))
	fit.Offsets[0].Metadata = strings.Repeat("m", 256<<10-fixed)
	if _, errs, code := DecideCommitOffsets(view, fit); code != runtime.OK || !reflect.DeepEqual(errs, []runtime.ErrCode{runtime.OK}) {
		t.Errorf("at the limit: %v %v", code, errs)
	}
	over := commitOf("g", "m1", 2, offset("orders", 0, 1))
	over.Offsets[0].Metadata = strings.Repeat("m", 256<<10-fixed+1)
	if events, errs, code := DecideCommitOffsets(view, over); code != runtime.ErrTooLarge || events != nil || errs != nil {
		t.Errorf("one byte over: %v %+v %v", code, events, errs)
	}
}
