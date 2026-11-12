package group

import (
	"bytes"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func dump(store engine.Engine) map[string][]byte {
	got := map[string][]byte{}
	_ = store.View(func(transaction engine.Txn) {
		transaction.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			got[string(k)] = bytes.Clone(v)
			return true
		})
	})
	return got
}

func sameState(t *testing.T, got, want map[string][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%d keys, want %d: %q", len(got), len(want), got)
	}
	for k, v := range want {
		if !bytes.Equal(got[k], v) {
			t.Errorf("key %q: %v, want %v", k, got[k], v)
		}
	}
}

// TestApplyWritesTheStateRecords runs the four commands on a store and pins
// the keys and bytes that Apply leaves: the state bytes must not change when
// the rules move.
func TestApplyWritesTheStateRecords(t *testing.T) {
	store := memory.New()
	partitions := func(_ engine.Txn, topic string) int64 {
		if topic == "orders" {
			return 2
		}
		return 0
	}
	commit := CommitOffsets(partitions)
	tp := contract.NewTopicPartition("orders", 1)
	_ = store.Update(func(transaction engine.Txn) {
		steps := []runtime.ControlPlaneDone{
			Install(transaction, 0, installOf("g", 1, "m1", "m1", "m2", "m3")),
			Assign(transaction, 0, assignOf("g", 1, "m1", "m1", "a", "m2", "b")),
			Remove(transaction, 0, contract.GroupRemove{Group: "g", Members: []string{"m3", "zz"}}),
			commit(transaction, 0, commitOf("g", "m1", 1, offset("orders", 1, 5), offset("orders", 9, 5))),
			Install(transaction, 0, installOf("g", 2, "m2", "m2")),
		}
		for i, d := range steps {
			if d.Err != runtime.OK {
				t.Errorf("step %d: %v", i, d.Err)
			}
		}
	})
	// The last install dropped m1 and m3 and kept the offset.
	sameState(t, dump(store), map[string][]byte{
		string(groupKey("g")):        Header{Generation: 2, ProtocolType: "consumer", Protocol: "range", Leader: "m2"}.encode(),
		string(memberKey("g", "m2")): encodeMember(contract.GroupMemberInfo{GroupMember: member("m2"), JoinedTimeMs: 1000}),
		string(offsetKey("g", tp)):   offsetRecord{offset: 5, epoch: 7, meta: "m", time: 4000}.encode(),
	})
}

// TestApplyAssignAndRemove: an assign writes the header and every member, and
// a remove of the last member clears the leader and the assigned generation.
func TestApplyAssignAndRemove(t *testing.T) {
	store := memory.New()
	_ = store.Update(func(transaction engine.Txn) {
		Install(transaction, 0, installOf("g", 1, "m1", "m1", "m2"))
		Assign(transaction, 0, assignOf("g", 1, "m1", "m1", "a"))
	})
	h := Header{Generation: 1, ProtocolType: "consumer", Protocol: "range", Leader: "m1", AssignedGeneration: 1}
	sameState(t, dump(store), map[string][]byte{
		string(groupKey("g")):        h.encode(),
		string(memberKey("g", "m1")): encodeMember(withAssignment(member("m1"), "a")),
		string(memberKey("g", "m2")): encodeMember(contract.GroupMemberInfo{GroupMember: member("m2"), JoinedTimeMs: 1000}),
	})
	_ = store.Update(func(transaction engine.Txn) {
		Remove(transaction, 0, contract.GroupRemove{Group: "g", Members: []string{"m1", "m2"}})
	})
	sameState(t, dump(store), map[string][]byte{
		string(groupKey("g")): Header{Generation: 1, ProtocolType: "consumer", Protocol: "range"}.encode(),
	})
}

func withAssignment(m contract.GroupMember, a string) contract.GroupMemberInfo {
	return contract.GroupMemberInfo{GroupMember: m, JoinedTimeMs: 1000, Assignment: []byte(a)}
}

func TestApplyPanicsOnAnUnknownEvent(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	Apply(nil, nil)
}

// Corrupt stored records stop the node, in a command and in a read.
func TestCorruptStoredRecordsPanic(t *testing.T) {
	store := memory.New()
	_ = store.Update(func(transaction engine.Txn) {
		transaction.Set(groupKey("bad"), []byte{0x80})
		transaction.Set(groupKey("g"), Header{Generation: 1, Leader: "m1"}.encode())
		transaction.Set(memberKey("g", "m1"), []byte{9})
		transaction.Set(offsetKey("g", contract.NewTopicPartition("t", 0)), []byte{0x80})
	})
	for name, f := range map[string]func(transaction engine.Txn){
		"install header": func(transaction engine.Txn) { Install(transaction, 0, installOf("bad", 1, "m1", "m1")) },
		"commit header":  func(transaction engine.Txn) { CommitOffsets(nil)(transaction, 0, commitOf("bad", "", -1)) },
		"assign member":  func(transaction engine.Txn) { Assign(transaction, 0, assignOf("g", 1, "m1", "m1", "a")) },
		"describe":       func(transaction engine.Txn) { DescribeGroup(transaction, contract.DescribeGroup{Group: "g"}) },
		"list":           func(transaction engine.Txn) { ListGroups(transaction, contract.ListGroups{}) },
		"offset fetch":   func(transaction engine.Txn) { OffsetFetch(transaction, contract.OffsetFetch{Group: "g"}) },
		"offset fetch one": func(transaction engine.Txn) {
			OffsetFetch(transaction, contract.OffsetFetch{Group: "g", Partitions: []contract.TopicPartition{contract.NewTopicPartition("t", 0)}})
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic on a corrupt record", name)
				}
			}()
			_ = store.Update(f)
		}()
	}
}
