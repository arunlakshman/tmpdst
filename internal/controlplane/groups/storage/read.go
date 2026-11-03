package storage

import (
	"cmp"
	"slices"
	"strings"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/core"
)

// Group reads (slice 4a). They change nothing, so they are not logged.

// offsetFetch answers an OffsetFetch. A partition with no commit is -1.
func offsetFetch(transaction kv.Transaction, operation core.OffsetFetch) core.ControlPlaneDone {
	var out []core.PartitionOffset
	if len(operation.Partitions) > 0 {
		for _, topicPartition := range operation.Partitions {
			v := transaction.Get(offsetKey(operation.Group, topicPartition))
			if v == nil {
				out = append(out, core.PartitionOffset{TopicPartition: topicPartition, Offset: -1, LeaderEpoch: -1})
				continue
			}
			out = append(out, offsetEntry(topicPartition, decodeOffset(operation.Group, v)))
		}
		return core.ControlPlaneDone{Resp: core.OffsetFetchResp{Offsets: out}}
	}
	lo := groupPrefix(prefixOffset, operation.Group)
	transaction.Ascend(lo, kv.PrefixEnd(lo), func(k, v []byte) bool {
		topicPartition, ok := kv.SplitTopicPartition(k[len(lo):])
		if !ok {
			corruptGroup(operation.Group)
		}
		out = append(out, offsetEntry(topicPartition, decodeOffset(operation.Group, v)))
		return true
	})
	slices.SortFunc(out, func(a, b core.PartitionOffset) int {
		return cmp.Or(strings.Compare(a.TopicPartition.Topic, b.TopicPartition.Topic), cmp.Compare(a.TopicPartition.Partition, b.TopicPartition.Partition))
	})
	return core.ControlPlaneDone{Resp: core.OffsetFetchResp{Offsets: out}}
}

func offsetEntry(topicPartition core.TopicPartition, o offsetRecord) core.PartitionOffset {
	return core.PartitionOffset{TopicPartition: topicPartition, Offset: o.offset, LeaderEpoch: o.epoch, Metadata: o.meta}
}

// describeGroup answers a DescribeGroup.
func describeGroup(transaction kv.Transaction, operation core.DescribeGroup) core.ControlPlaneDone {
	h, found := readHeader(transaction, operation.Group)
	if !found {
		return core.ControlPlaneDone{Resp: core.DescribeGroupResp{}}
	}
	members := readMembers(transaction, operation.Group)
	response := core.DescribeGroupResp{
		Exists:             true,
		Generation:         h.generation,
		ProtocolType:       h.protoType,
		Protocol:           h.protocol,
		Leader:             h.leader,
		AssignedGeneration: h.assignedGen,
		State:              groupState(h, len(members)),
		Members:            members,
	}
	return core.ControlPlaneDone{Resp: response}
}

// groupState is the state a group reports: "Empty" with no members, "Stable"
// when the stored assignment is for the current generation, else "PreparingSync".
func groupState(h groupHeader, members int) string {
	switch {
	case members == 0:
		return "Empty"
	case h.assignedGen == h.generation:
		return "Stable"
	}
	return "PreparingSync"
}

// listGroups answers a ListGroups: every installed group, sorted by name.
func listGroups(transaction kv.Transaction, _ core.ListGroups) core.ControlPlaneDone {
	type row struct {
		name string
		h    groupHeader
	}
	var rows []row
	lo := []byte{prefixGroup}
	transaction.Ascend(lo, kv.PrefixEnd(lo), func(k, v []byte) bool {
		name := string(k[1:])
		rows = append(rows, row{name: name, h: decodeHeader(name, v)})
		return true
	})
	var out []core.GroupSummary
	for _, r := range rows {
		n := len(memberKeys(transaction, r.name))
		out = append(out, core.GroupSummary{Name: r.name, ProtocolType: r.h.protoType, State: groupState(r.h, n)})
	}
	return core.ControlPlaneDone{Resp: core.ListGroupsResp{Groups: out}}
}
