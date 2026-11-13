package group

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The tables of the group context: its keys, the View over a transaction,
// Apply, which writes events, and the three reads. Layout (all keys are
// prefix-free per kind):
//
//	G group                        group header: generation, protocolType,
//	                               protocol, leader, assignedGeneration
//	M group member                 one member: clientId, clientHost,
//	                               groupInstanceId, sessionTimeoutMs,
//	                               rebalanceTimeoutMs, joinedTimeMs, metadata,
//	                               assignment (member id is the key tail)
//	O group tp                     committed offset: offset, leaderEpoch,
//	                               metadata, commitTimeMs
//
// A group in M and O keys is uvarint(len group), group, so the keys of one
// group are one prefix. The records are in codec.go.

const (
	prefixGroup  = 'G'
	prefixMember = 'M'
	prefixOffset = 'O'
)

// Partitions tells how many partitions a topic has (0 when it does not exist).
// The topic context has it; metadb gives it to the group schema at wiring, so
// that this package does not import the topic package.
type Partitions func(transaction engine.Txn, topic string) int64

// groupKey is the G key of a group: its header.
func groupKey(group string) []byte { return append([]byte{prefixGroup}, group...) }

// groupPrefix is the prefix-free key prefix of one group for kind p.
func groupPrefix(p byte, group string) []byte {
	b := binary.AppendUvarint([]byte{p}, uint64(len(group)))
	return append(b, group...)
}

// memberKey is the M key of one member of a group.
func memberKey(group, member string) []byte {
	return append(groupPrefix(prefixMember, group), member...)
}

// offsetKey is the O key of one committed partition of a group.
func offsetKey(group string, topicPartition contract.TopicPartition) []byte {
	return engine.AppendTopicPartition(groupPrefix(prefixOffset, group), topicPartition)
}

// txnView is the View over a transaction. partitions may be nil for the
// commands that never ask a partition count.
type txnView struct {
	transaction engine.Txn
	partitions  Partitions
}

// Header reads the G record of a group. A corrupt record stops the node
// (fail-stop on corrupt state).
func (v txnView) Header(group string) (Header, bool) {
	b := v.transaction.Get(groupKey(group))
	if b == nil {
		return Header{}, false
	}
	return decodeHeader(group, b), true
}

func (v txnView) HasMember(group, member string) bool {
	return v.transaction.Get(memberKey(group, member)) != nil
}

func (v txnView) MemberIDs(group string) []string {
	var ids []string
	lo := groupPrefix(prefixMember, group)
	v.transaction.Ascend(lo, engine.PrefixEnd(lo), func(k, _ []byte) bool {
		ids = append(ids, string(k[len(lo):]))
		return true
	})
	return ids
}

// Members reads the M records of a group. A corrupt record stops the node.
func (v txnView) Members(group string) []contract.GroupMemberInfo {
	var out []contract.GroupMemberInfo
	lo := groupPrefix(prefixMember, group)
	v.transaction.Ascend(lo, engine.PrefixEnd(lo), func(k, val []byte) bool {
		out = append(out, decodeMember(group, string(k[len(lo):]), val))
		return true
	})
	return out
}

func (v txnView) PartitionCount(topic string) int64 { return v.partitions(v.transaction, topic) }

// Apply writes event to the tables.
func Apply(transaction engine.Txn, event Event) {
	switch e := event.(type) {
	case GroupInstalled:
		for _, id := range e.Replaced {
			transaction.Delete(memberKey(e.Group, id))
		}
		writeMembers(transaction, e.Group, e.Members)
		transaction.Set(groupKey(e.Group), e.Header.encode())
	case AssignmentsSet:
		writeMembers(transaction, e.Group, e.Members)
		transaction.Set(groupKey(e.Group), e.Header.encode())
	case MembersRemoved:
		for _, id := range e.Members {
			transaction.Delete(memberKey(e.Group, id))
		}
		transaction.Set(groupKey(e.Group), e.Header.encode())
	case OffsetsCommitted:
		for _, o := range e.Offsets {
			rec := offsetRecord{offset: o.Offset, epoch: o.LeaderEpoch, meta: o.Metadata, time: e.TimeMs}
			transaction.Set(offsetKey(e.Group, o.TopicPartition), rec.encode())
		}
	default:
		panic(fmt.Sprintf("groups: unknown event %T", event))
	}
}

func writeMembers(transaction engine.Txn, group string, members []contract.GroupMemberInfo) {
	for _, m := range members {
		transaction.Set(memberKey(group, m.MemberID), encodeMember(m))
	}
}

// Group reads. They change nothing, so they are not logged.

// OffsetFetch answers an OffsetFetch. A partition with no commit is -1.
func OffsetFetch(transaction engine.Txn, operation contract.OffsetFetch) runtime.ControlPlaneDone {
	var out []contract.PartitionOffset
	if len(operation.Partitions) > 0 {
		for _, topicPartition := range operation.Partitions {
			v := transaction.Get(offsetKey(operation.Group, topicPartition))
			if v == nil {
				out = append(out, contract.PartitionOffset{TopicPartition: topicPartition, Offset: -1, LeaderEpoch: -1})
				continue
			}
			out = append(out, offsetEntry(topicPartition, decodeOffset(operation.Group, v)))
		}
		return runtime.ControlPlaneDone{Resp: contract.OffsetFetchResp{Offsets: out}}
	}
	lo := groupPrefix(prefixOffset, operation.Group)
	transaction.Ascend(lo, engine.PrefixEnd(lo), func(k, v []byte) bool {
		topicPartition, ok := engine.SplitTopicPartition(k[len(lo):])
		if !ok {
			corruptGroup(operation.Group)
		}
		out = append(out, offsetEntry(topicPartition, decodeOffset(operation.Group, v)))
		return true
	})
	slices.SortFunc(out, func(a, b contract.PartitionOffset) int {
		return cmp.Or(strings.Compare(a.TopicPartition.Topic, b.TopicPartition.Topic), cmp.Compare(a.TopicPartition.Partition, b.TopicPartition.Partition))
	})
	return runtime.ControlPlaneDone{Resp: contract.OffsetFetchResp{Offsets: out}}
}

func offsetEntry(topicPartition contract.TopicPartition, o offsetRecord) contract.PartitionOffset {
	return contract.PartitionOffset{TopicPartition: topicPartition, Offset: o.offset, LeaderEpoch: o.epoch, Metadata: o.meta}
}

// DescribeGroup answers a DescribeGroup.
func DescribeGroup(transaction engine.Txn, operation contract.DescribeGroup) runtime.ControlPlaneDone {
	view := txnView{transaction: transaction}
	h, found := view.Header(operation.Group)
	if !found {
		return runtime.ControlPlaneDone{Resp: contract.DescribeGroupResp{}}
	}
	members := view.Members(operation.Group)
	response := contract.DescribeGroupResp{
		Exists:             true,
		Generation:         h.Generation,
		ProtocolType:       h.ProtocolType,
		Protocol:           h.Protocol,
		Leader:             h.Leader,
		AssignedGeneration: h.AssignedGeneration,
		State:              groupState(h, len(members)),
		Members:            members,
	}
	return runtime.ControlPlaneDone{Resp: response}
}

// groupState is the state a group reports: "Empty" with no members, "Stable"
// when the stored assignment is for the current generation, else "PreparingSync".
func groupState(h Header, members int) string {
	switch {
	case members == 0:
		return "Empty"
	case h.AssignedGeneration == h.Generation:
		return "Stable"
	}
	return "PreparingSync"
}

// ListGroups answers a ListGroups: every installed group, sorted by name.
func ListGroups(transaction engine.Txn, _ contract.ListGroups) runtime.ControlPlaneDone {
	type row struct {
		name string
		h    Header
	}
	var rows []row
	lo := []byte{prefixGroup}
	transaction.Ascend(lo, engine.PrefixEnd(lo), func(k, v []byte) bool {
		name := string(k[1:])
		rows = append(rows, row{name: name, h: decodeHeader(name, v)})
		return true
	})
	view := txnView{transaction: transaction}
	var out []contract.GroupSummary
	for _, r := range rows {
		n := len(view.MemberIDs(r.name))
		out = append(out, contract.GroupSummary{Name: r.name, ProtocolType: r.h.ProtocolType, State: groupState(r.h, n)})
	}
	return runtime.ControlPlaneDone{Resp: contract.ListGroupsResp{Groups: out}}
}
