// Package groupfix builds small consumer group commands for tests.
package groupfix

import "github.com/manifesterio/mds/internal/core"

// Member is a group member with fixed client fields.
func Member(id string) core.GroupMember {
	return core.GroupMember{
		MemberID: id, ClientID: "client-" + id, ClientHost: "/10.0.0.1",
		SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("sub-" + id),
	}
}

// Install is a GroupInstall of the members ids.
func Install(group string, gen int32, leader string, ids ...string) core.GroupInstall {
	operation := core.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, Leader: leader, TimeMs: 1000}
	for _, id := range ids {
		operation.Members = append(operation.Members, Member(id))
	}
	return operation
}

// Assign is a GroupAssign. pairs are member, bytes, member, bytes, and so on.
func Assign(group string, gen int32, leader string, pairs ...string) core.GroupAssign {
	operation := core.GroupAssign{Group: group, Generation: gen, Leader: leader, TimeMs: 2000}
	for i := 0; i+1 < len(pairs); i += 2 {
		operation.Assignments = append(operation.Assignments, core.GroupAssignment{Member: pairs[i], Bytes: []byte(pairs[i+1])})
	}
	return operation
}

// Remove is a GroupRemove of the members ids.
func Remove(group string, ids ...string) core.GroupRemove {
	return core.GroupRemove{Group: group, Members: ids, TimeMs: 3000, Reason: "leave"}
}

// Pair is one partition offset.
func Pair(topic string, p int32, off int64) core.PartitionOffset {
	return core.PartitionOffset{TopicPartition: core.NewTopicPartition(topic, p), Offset: off, LeaderEpoch: 7, Metadata: "m"}
}

// CommitOp is an OffsetCommit.
func CommitOp(group, member string, gen int32, offs ...core.PartitionOffset) core.OffsetCommit {
	return core.OffsetCommit{Group: group, Member: member, Generation: gen, TimeMs: 4000, Offsets: offs}
}
