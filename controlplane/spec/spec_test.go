package spec

import (
	"bytes"
	"slices"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
)

// These tests check the rules of the model one by one, with small examples.
// The model is also compared with the real code by the tests in
// test/conformance.

func tp(topic string, p int32) contract.TopicPartition { return contract.NewTopicPartition(topic, p) }

// batch is a batch of n records with a segment location of 10 bytes a record.
func batch(topic string, p int32, n int64) contract.PendingBatch {
	return contract.PendingBatch{TopicPartition: tp(topic, p), Records: n, Loc: contract.SegmentRef{Key: "o", Len: n * 10}}
}

func commit(m *ControlPlane, key string, batches ...contract.PendingBatch) contract.Done {
	return m.Apply(contract.CommitSegment{SegmentKey: key, Batches: batches})
}

func bases(d contract.Done) []contract.Offset {
	var out []contract.Offset
	switch r := d.Resp.(type) {
	case contract.CommitSegmentResp:
		for _, x := range r.Assigned {
			out = append(out, x.Base)
		}
	case contract.FindSegmentsResp:
		for _, x := range r.Ranges {
			out = append(out, x.Base)
		}
	}
	return out
}

func TestCommitAssignsContiguousOffsets(t *testing.T) {
	m := New()
	if got := bases(commit(m, "s1", batch("t", 0, 3), batch("t", 1, 2), batch("t", 0, 4))); !slices.Equal(got, []contract.Offset{0, 0, 3}) {
		t.Fatalf("s1 bases %v", got)
	}
	if got := bases(commit(m, "s2", batch("t", 0, 1), batch("u", 0, 5))); !slices.Equal(got, []contract.Offset{7, 0}) {
		t.Fatalf("s2 bases %v", got)
	}
	if end := m.end(tp("t", 0)); end != 8 {
		t.Fatalf("end %d, want 8", end)
	}
}

func TestCommitReplayAndConflict(t *testing.T) {
	m := New()
	first := commit(m, "s1", batch("t", 0, 3))
	commit(m, "s2", batch("t", 0, 2)) // moves the end on
	if again := commit(m, "s1", batch("t", 0, 3)); again.Err != contract.OK || !slices.Equal(bases(again), bases(first)) || m.end(tp("t", 0)) != 5 {
		t.Fatalf("replay %+v changed the answer or the state", again)
	}
	for name, bs := range map[string][]contract.PendingBatch{
		"records": {batch("t", 0, 4)}, "partition": {batch("t", 1, 3)}, "extra": {batch("t", 0, 3), batch("t", 1, 1)}, "none": nil,
	} {
		if d := commit(m, "s1", bs...); d.Err != contract.ErrConflict || d.Outcome != contract.NotApplied {
			t.Fatalf("%s: %+v", name, d)
		}
	}
}

func TestCommitInvalidChangesNothing(t *testing.T) {
	m := New()
	for _, d := range []contract.Done{
		commit(m, ""), commit(m, "", batch("t", 0, 1)), commit(m, "k"),
		commit(m, "k", batch("t", 0, 0)), commit(m, "k", batch("", 0, 1)), commit(m, "k", batch("t", -1, 1)),
		commit(m, "k", batch("t", 0, 1), batch("t", 1, 0)), // the first batch is fine, the second is not
	} {
		if d.Err != contract.ErrInvalid {
			t.Fatalf("%+v", d)
		}
	}
	if len(m.topics) != 0 || len(m.runs) != 0 || len(m.segments) != 0 {
		t.Fatal("an invalid commit changed the state")
	}
	if d := commit(m, "k", batch("t", 0, 1)); d.Err != contract.OK {
		t.Fatalf("the key of a failed commit can be used again: %+v", d)
	}
}

func TestFindSegments(t *testing.T) {
	m := New()
	commit(m, "s1", batch("t", 0, 3))
	commit(m, "s2", batch("t", 0, 2), batch("t", 1, 1))
	commit(m, "s3", batch("t", 0, 4))
	find := func(p int32, from contract.Offset, maxBytes int32) contract.Done {
		return m.Apply(contract.FindSegments{TopicPartition: tp("t", p), From: from, MaxBytes: maxBytes})
	}
	for _, c := range []struct {
		name string
		got  contract.Done
		want []contract.Offset
	}{
		{"from 0", find(0, 0, 0), []contract.Offset{0, 3, 5}},
		{"from 4 is inside the run 3..4", find(0, 4, 0), []contract.Offset{3, 5}},
		{"from the end", find(0, 9, 0), nil},
		{"one run at least", find(0, 0, 1), []contract.Offset{0}},
		{"stop when the sum reaches MaxBytes", find(0, 0, 50), []contract.Offset{0, 3}},
		{"unknown partition", find(7, 0, 0), nil},
	} {
		if !slices.Equal(bases(c.got), c.want) {
			t.Errorf("%s: bases %v, want %v", c.name, bases(c.got), c.want)
		}
	}
	if r := find(0, 0, 0).Resp.(contract.FindSegmentsResp); r.End != 9 {
		t.Fatalf("end %d, want 9", r.End)
	}
	if d := find(0, -1, 0); d.Err != contract.ErrInvalid {
		t.Fatalf("negative From: %+v", d)
	}
}

func TestTopics(t *testing.T) {
	m := New()
	create := func(name string, parts int32, exclusive bool) contract.ErrCode {
		return m.Apply(contract.CreateTopic{Name: name, Partitions: parts, FailIfExists: exclusive}).Err
	}
	if create("b", 3, false) != contract.OK || create("", 1, false) != contract.ErrInvalid || create("x", 0, false) != contract.ErrInvalid {
		t.Fatal("create rules")
	}
	if create("b", 1, true) != contract.ErrTopicExists || create("c", 1, true) != contract.OK {
		t.Fatal("FailIfExists rule")
	}
	commit(m, "s1", batch("a", 4, 1), batch("b", 1, 1)) // a is made by the commit
	create("b", 2, false)                               // never shrinks
	describe := func(names ...string) []contract.TopicInfo {
		return m.Apply(contract.DescribeTopics{Topics: names}).Resp.(contract.DescribeTopicsResp).Topics
	}
	want := []contract.TopicInfo{{Name: "a", Partitions: 5}, {Name: "b", Partitions: 3}, {Name: "c", Partitions: 1}}
	if got := describe(); !slices.Equal(got, want) {
		t.Fatalf("all: %+v", got)
	}
	if got := describe("b", "nope"); !slices.Equal(got, want[1:2]) {
		t.Fatalf("some: %+v", got)
	}
}

func member(id string) contract.GroupMember {
	return contract.GroupMember{MemberID: id, ClientID: "c-" + id, SessionTimeoutMs: 1000, Metadata: []byte("m")}
}

func install(m *ControlPlane, group string, gen int32, ids ...string) contract.Done {
	op := contract.GroupInstall{Group: group, Generation: gen, Leader: ids[0], ProtocolType: "consumer", Protocol: "range", TimeMs: 5}
	for _, id := range ids {
		op.Members = append(op.Members, member(id))
	}
	return m.Apply(op)
}

func describeGroup(m *ControlPlane, group string) contract.DescribeGroupResp {
	return m.Apply(contract.DescribeGroup{Group: group}).Resp.(contract.DescribeGroupResp)
}

func TestGroupInstallAssignRemove(t *testing.T) {
	m := New()
	if d := install(m, "g", 2, "a"); d.Err != contract.ErrIllegalGeneration {
		t.Fatalf("a new group starts at generation 1: %+v", d)
	}
	if d := install(m, "g", 1, "a", "b"); d.Err != contract.OK || d.Outcome != contract.Applied {
		t.Fatalf("install: %+v", d)
	}
	if got := describeGroup(m, "g"); got.State != "PreparingSync" || got.Leader != "a" || len(got.Members) != 2 {
		t.Fatalf("after install: %+v", got)
	}
	assign := func(gen int32, leader string, members ...string) contract.ErrCode {
		op := contract.GroupAssign{Group: "g", Generation: gen, Leader: leader}
		for _, id := range members {
			op.Assignments = append(op.Assignments, contract.GroupAssignment{Member: id, Bytes: []byte("x-" + id)})
		}
		return m.Apply(op).Err
	}
	if assign(2, "a", "a") != contract.ErrIllegalGeneration || assign(1, "b", "a") != contract.ErrUnknownMember ||
		assign(1, "a", "z") != contract.ErrUnknownMember || assign(1, "a", "a", "a") != contract.ErrInvalid {
		t.Fatal("assign rules")
	}
	if assign(1, "a", "a") != contract.OK {
		t.Fatal("assign")
	}
	got := describeGroup(m, "g")
	if got.State != "Stable" || string(got.Members[0].Assignment) != "x-a" || got.Members[1].Assignment != nil {
		t.Fatalf("after assign: %+v", got)
	}
	if d := install(m, "g", 2, "b"); d.Err != contract.OK || describeGroup(m, "g").State != "PreparingSync" {
		t.Fatal("a new generation clears the assignments")
	}
	m.Apply(contract.GroupRemove{Group: "g", Members: []string{"b", "nobody"}})
	if got := describeGroup(m, "g"); got.State != "Empty" || got.Leader != "" || got.Generation != 2 || got.AssignedGeneration != 0 {
		t.Fatalf("after remove: %+v", got)
	}
	if l := m.Apply(contract.ListGroups{}).Resp.(contract.ListGroupsResp); len(l.Groups) != 1 || l.Groups[0].State != "Empty" {
		t.Fatalf("list: %+v", l)
	}
}

func TestGroupInstallInvalid(t *testing.T) {
	m := New()
	dup := contract.GroupInstall{Group: "g", Generation: 1, Leader: "a", Members: []contract.GroupMember{member("a"), member("a")}}
	for name, op := range map[string]contract.GroupInstall{
		"no group":               {Generation: 1, Leader: "a", Members: []contract.GroupMember{member("a")}},
		"no member":              {Group: "g", Generation: 1},
		"leader is not a member": {Group: "g", Generation: 1, Leader: "z", Members: []contract.GroupMember{member("a")}},
		"repeated member":        dup,
		"empty member id":        {Group: "g", Generation: 1, Leader: "", Members: []contract.GroupMember{member("")}},
	} {
		if d := m.Apply(op); d.Err != contract.ErrInvalid {
			t.Errorf("%s: %+v", name, d)
		}
	}
}

func TestOffsetCommitAndFetch(t *testing.T) {
	m := New()
	m.Apply(contract.CreateTopic{Name: "t", Partitions: 2})
	install(m, "g", 1, "a")
	offsets := func(topic string, p int32, off int64) []contract.PartitionOffset {
		return []contract.PartitionOffset{{TopicPartition: tp(topic, p), Offset: off, LeaderEpoch: 3, Metadata: "meta"}}
	}
	commitOffsets := func(gen int32, member string, offs []contract.PartitionOffset) contract.Done {
		return m.Apply(contract.OffsetCommit{Group: "g", Generation: gen, Member: member, Offsets: offs})
	}
	if commitOffsets(1, "z", offsets("t", 0, 5)).Err != contract.ErrUnknownMember ||
		commitOffsets(2, "a", offsets("t", 0, 5)).Err != contract.ErrIllegalGeneration ||
		commitOffsets(-1, "", offsets("t", 0, 5)).Err != contract.ErrIllegalGeneration { // the group has a member
		t.Fatal("offset commit checks")
	}
	d := commitOffsets(1, "a", append(offsets("t", 0, 5), append(offsets("t", 9, 1), offsets("t", 1, -1)...)...))
	if want := (contract.OffsetCommitResp{Errs: []contract.ErrCode{contract.OK, contract.ErrUnknownPartition, contract.ErrInvalid}}); !slices.Equal(d.Resp.(contract.OffsetCommitResp).Errs, want.Errs) || d.Outcome != contract.Applied {
		t.Fatalf("per offset codes: %+v", d)
	}
	fetch := func(ps ...contract.TopicPartition) []contract.PartitionOffset {
		return m.Apply(contract.OffsetFetch{Group: "g", Partitions: ps}).Resp.(contract.OffsetFetchResp).Offsets
	}
	if got := fetch(tp("t", 1), tp("t", 0)); got[0].Offset != -1 || got[0].LeaderEpoch != -1 || got[1].Offset != 5 || got[1].Metadata != "meta" {
		t.Fatalf("fetch: %+v", got)
	}
	if got := fetch(); len(got) != 1 || got[0].TopicPartition != tp("t", 0) {
		t.Fatalf("fetch all: %+v", got)
	}
	m.Apply(contract.GroupRemove{Group: "g", Members: []string{"a"}})
	if d := commitOffsets(-1, "", offsets("t", 1, 2)); d.Err != contract.OK {
		t.Fatalf("a manual commit is allowed when the group has no member: %+v", d)
	}
}

func TestGroupCommandTooLarge(t *testing.T) {
	m := New()
	big := bytes.Repeat([]byte("x"), maxGroupCommand)
	install(m, "g", 1, "a")
	if d := m.Apply(contract.GroupAssign{Group: "g", Generation: 1, Leader: "a", Assignments: []contract.GroupAssignment{{Member: "a", Bytes: big}}}); d.Err != contract.ErrTooLarge {
		t.Fatalf("assign: %+v", d)
	}
	if d := m.Apply(contract.GroupRemove{Group: "g", Reason: string(big)}); d.Err != contract.ErrTooLarge {
		t.Fatalf("remove: %+v", d)
	}
	// The limit is on the size: 16 + (4 + 1) group + (4 + 1) leader + 30 numbers + 4 + 1 member + 4 + n bytes.
	n := maxGroupCommand - (16 + 5 + 5 + 30 + 5 + 4)
	op := contract.GroupAssign{Group: "g", Generation: 1, Leader: "a", Assignments: []contract.GroupAssignment{{Member: "a", Bytes: bytes.Repeat([]byte("x"), n)}}}
	if d := m.Apply(op); d.Err != contract.OK {
		t.Fatalf("a command of exactly the limit is allowed: %+v", d)
	}
	op.Assignments[0].Bytes = append(op.Assignments[0].Bytes, 'x')
	if d := m.Apply(op); d.Err != contract.ErrTooLarge {
		t.Fatalf("one byte more is too large: %+v", d)
	}
}

func TestUnknownOperation(t *testing.T) {
	if d := New().Apply(nil); d.Err != contract.ErrInvalid {
		t.Fatalf("%+v", d)
	}
}
