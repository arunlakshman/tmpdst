package spec

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
)

// TestGroupCommandSizeAtTheLimit checks the Size rule at its edge for the four
// group commands that carry data. For each one the size is worked out by hand
// from the rule (16 for the command, 4 + length for a string or byte field, 10
// for a number or a list length), then a command of exactly 256 KiB must pass
// and one byte more must be ErrTooLarge.
func TestGroupCommandSizeAtTheLimit(t *testing.T) {
	// ready is a model with topic "t" and group "g" at generation 1, with member "a".
	ready := func() *ControlPlane {
		m := New()
		m.Apply(contract.CreateTopic{Name: "t", Partitions: 1})
		install(m, "g", 1, "a")
		return m
	}
	cases := []struct {
		name  string
		fixed int // the size of the command with an empty padding field
		setup func() *ControlPlane
		op    func(pad int) contract.Operation
	}{
		{
			// 16 + group 5 + type 5 + protocol 5 + leader 5 + 3 numbers 30
			// + member (id 5 + client id 4 + host 4 + instance id 4 + metadata 4 + 2 numbers 20).
			name: "install", fixed: 16 + 5 + 5 + 5 + 5 + 30 + 5 + 4 + 4 + 4 + 4 + 20, setup: New,
			op: func(pad int) contract.Operation {
				return contract.GroupInstall{
					Group: "g", ProtocolType: "p", Protocol: "r", Generation: 1, Leader: "a",
					Members: []contract.GroupMember{{MemberID: "a", Metadata: bytes.Repeat([]byte("x"), pad)}},
				}
			},
		},
		{
			// 16 + group 5 + leader 5 + 3 numbers 30 + assignment (member 5 + bytes 4).
			name: "assign", fixed: 16 + 5 + 5 + 30 + 5 + 4, setup: ready,
			op: func(pad int) contract.Operation {
				return contract.GroupAssign{
					Group: "g", Generation: 1, Leader: "a",
					Assignments: []contract.GroupAssignment{{Member: "a", Bytes: bytes.Repeat([]byte("x"), pad)}},
				}
			},
		},
		{
			// 16 + group 5 + reason 4 + 2 numbers 20.
			name: "remove", fixed: 16 + 5 + 4 + 20, setup: ready,
			op: func(pad int) contract.Operation {
				return contract.GroupRemove{Group: "g", Reason: strings.Repeat("x", pad)}
			},
		},
		{
			// 16 + group 5 + member 5 + 3 numbers 30 + offset (topic 5 + 3 numbers 30 + metadata 4).
			name: "offset commit", fixed: 16 + 5 + 5 + 30 + 5 + 30 + 4, setup: ready,
			op: func(pad int) contract.Operation {
				return contract.OffsetCommit{
					Group: "g", Member: "a", Generation: 1,
					Offsets: []contract.PartitionOffset{{TopicPartition: tp("t", 0), Offset: 1, Metadata: strings.Repeat("x", pad)}},
				}
			},
		},
	}
	for _, c := range cases {
		pad := maxGroupCommand - c.fixed
		if d := c.setup().Apply(c.op(pad)); d.Err != contract.OK {
			t.Errorf("%s: a command of exactly the limit must be allowed: %+v", c.name, d)
		}
		if d := c.setup().Apply(c.op(pad + 1)); d.Err != contract.ErrTooLarge {
			t.Errorf("%s: one byte over the limit must be ErrTooLarge: %+v", c.name, d)
		}
	}
}

// TestEmptyBytesAreReportedAsNil checks the rule "an empty byte field is
// reported as nil" for the metadata of a member and for an assignment.
func TestEmptyBytesAreReportedAsNil(t *testing.T) {
	m := New()
	op := contract.GroupInstall{
		Group: "g", Generation: 1, Leader: "a",
		Members: []contract.GroupMember{{MemberID: "a", Metadata: []byte{}}},
	}
	if d := m.Apply(op); d.Err != contract.OK {
		t.Fatalf("install: %+v", d)
	}
	m.Apply(contract.GroupAssign{Group: "g", Generation: 1, Leader: "a", Assignments: []contract.GroupAssignment{{Member: "a", Bytes: []byte{}}}})
	got := describeGroup(m, "g").Members[0]
	if got.Metadata != nil || got.Assignment != nil {
		t.Fatalf("empty fields must be nil: metadata %#v assignment %#v", got.Metadata, got.Assignment)
	}
}

// TestOffsetEdgesAndOrder checks the edges of the offset rules: the last
// partition of a topic is known and the next one is not, offset 0 is valid, a
// generation without a member name is refused with ErrIllegalGeneration, and a
// fetch of every offset is ordered by topic and then by partition.
func TestOffsetEdgesAndOrder(t *testing.T) {
	m := New()
	m.Apply(contract.CreateTopic{Name: "a", Partitions: 2})
	m.Apply(contract.CreateTopic{Name: "b", Partitions: 2})
	install(m, "g", 1, "x")
	var offsets []contract.PartitionOffset
	for _, p := range []contract.TopicPartition{tp("b", 1), tp("a", 1), tp("b", 0), tp("a", 0), tp("a", 2)} {
		offsets = append(offsets, contract.PartitionOffset{TopicPartition: p, Offset: 0})
	}
	d := m.Apply(contract.OffsetCommit{Group: "g", Member: "x", Generation: 1, Offsets: offsets})
	want := []contract.ErrCode{contract.OK, contract.OK, contract.OK, contract.OK, contract.ErrUnknownPartition}
	if got := d.Resp.(contract.OffsetCommitResp).Errs; !slices.Equal(got, want) {
		t.Fatalf("per offset codes %v, want %v", got, want)
	}
	var order []contract.TopicPartition
	for _, o := range m.Apply(contract.OffsetFetch{Group: "g"}).Resp.(contract.OffsetFetchResp).Offsets {
		order = append(order, o.TopicPartition)
	}
	if wantOrder := []contract.TopicPartition{tp("a", 0), tp("a", 1), tp("b", 0), tp("b", 1)}; !slices.Equal(order, wantOrder) {
		t.Fatalf("fetch order %v, want %v", order, wantOrder)
	}
	// Generation -1 with a member name is not a manual commit.
	d = m.Apply(contract.OffsetCommit{Group: "g", Member: "x", Generation: -1, Offsets: offsets[:1]})
	if d.Err != contract.ErrIllegalGeneration {
		t.Fatalf("generation -1 with a member: %+v", d)
	}
}

// TestRemoveFromUnknownGroupIsIgnored checks the GroupRemove rule: an unknown
// group is not an error and changes nothing.
func TestRemoveFromUnknownGroupIsIgnored(t *testing.T) {
	m := New()
	d := m.Apply(contract.GroupRemove{Group: "nope", Members: []string{"a"}})
	if d.Err != contract.OK {
		t.Fatalf("remove from an unknown group: %+v", d)
	}
	if describeGroup(m, "nope").Exists {
		t.Fatal("a remove must not make a group")
	}
}

// TestOffsetCommitStaleGeneration checks that an older generation is refused,
// not only a newer one.
func TestOffsetCommitStaleGeneration(t *testing.T) {
	m := New()
	m.Apply(contract.CreateTopic{Name: "t", Partitions: 1})
	install(m, "g", 1, "a")
	install(m, "g", 2, "a")
	commitAt := func(gen int32) contract.ErrCode {
		return m.Apply(contract.OffsetCommit{
			Group: "g", Member: "a", Generation: gen,
			Offsets: []contract.PartitionOffset{{TopicPartition: tp("t", 0), Offset: 1}},
		}).Err
	}
	if commitAt(1) != contract.ErrIllegalGeneration {
		t.Fatal("an older generation must be ErrIllegalGeneration")
	}
	if commitAt(3) != contract.ErrIllegalGeneration {
		t.Fatal("a newer generation must be ErrIllegalGeneration")
	}
	if commitAt(2) != contract.OK {
		t.Fatal("the current generation must be accepted")
	}
}
