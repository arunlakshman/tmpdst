package stateupdater

import (
	"cmp"
	"slices"
	"strings"

	"manifester/internal/core"
)

// Group reads (slice 4a). They change nothing, so they are not logged.

// OffsetFetch answers an OffsetFetch. A partition with no commit is -1.
func (a *StateUpdater) OffsetFetch(op core.OffsetFetch) (core.ControlPlaneDone, error) {
	return a.view(func(tx Transaction) core.ControlPlaneDone { return offsetFetchTransaction(tx, op) })
}

func offsetFetchTransaction(tx Transaction, op core.OffsetFetch) core.ControlPlaneDone {
	var out []core.PartitionOffset
	if len(op.Partitions) > 0 {
		for _, tp := range op.Partitions {
			v := tx.Get(offsetKey(op.Group, tp))
			if v == nil {
				out = append(out, core.PartitionOffset{TP: tp, Offset: -1, LeaderEpoch: -1})
				continue
			}
			out = append(out, offsetEntry(tp, decodeOffset(op.Group, v)))
		}
		return core.ControlPlaneDone{Resp: core.OffsetFetchResp{Offsets: out}}
	}
	lo := groupPrefix(prefixOffset, op.Group)
	tx.Ascend(lo, prefixEnd(lo), func(k, v []byte) bool {
		tp, ok := splitTopicPartition(k[len(lo):])
		if !ok {
			corruptGroup(op.Group)
		}
		out = append(out, offsetEntry(tp, decodeOffset(op.Group, v)))
		return true
	})
	slices.SortFunc(out, func(a, b core.PartitionOffset) int {
		return cmp.Or(strings.Compare(a.TP.Topic, b.TP.Topic), cmp.Compare(a.TP.Partition, b.TP.Partition))
	})
	return core.ControlPlaneDone{Resp: core.OffsetFetchResp{Offsets: out}}
}

func offsetEntry(tp core.TopicPartition, o offsetRecord) core.PartitionOffset {
	return core.PartitionOffset{TP: tp, Offset: o.offset, LeaderEpoch: o.epoch, Metadata: o.meta}
}

// DescribeGroup answers a DescribeGroup.
func (a *StateUpdater) DescribeGroup(op core.DescribeGroup) (core.ControlPlaneDone, error) {
	return a.view(func(tx Transaction) core.ControlPlaneDone { return describeGroupTransaction(tx, op) })
}

func describeGroupTransaction(tx Transaction, op core.DescribeGroup) core.ControlPlaneDone {
	h, found := readHeader(tx, op.Group)
	if !found {
		return core.ControlPlaneDone{Resp: core.DescribeGroupResp{}}
	}
	members := readMembers(tx, op.Group)
	resp := core.DescribeGroupResp{
		Exists:             true,
		Generation:         h.generation,
		ProtocolType:       h.protoType,
		Protocol:           h.protocol,
		Leader:             h.leader,
		AssignedGeneration: h.assignedGen,
		State:              groupState(h, len(members)),
		Members:            members,
	}
	return core.ControlPlaneDone{Resp: resp}
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

// ListGroups answers a ListGroups: every installed group, sorted by name.
func (a *StateUpdater) ListGroups(op core.ListGroups) (core.ControlPlaneDone, error) {
	return a.view(listGroupsTransaction)
}

// view answers one read in a read transaction.
func (a *StateUpdater) view(fn func(Transaction) core.ControlPlaneDone) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.kv.View(func(tx Transaction) { d = fn(tx) })
	return d, err
}

func listGroupsTransaction(tx Transaction) core.ControlPlaneDone {
	type row struct {
		name string
		h    groupHeader
	}
	var rows []row
	lo := []byte{prefixGroup}
	tx.Ascend(lo, prefixEnd(lo), func(k, v []byte) bool {
		name := string(k[1:])
		rows = append(rows, row{name: name, h: decodeHeader(name, v)})
		return true
	})
	var out []core.GroupSummary
	for _, r := range rows {
		n := len(memberKeys(tx, r.name))
		out = append(out, core.GroupSummary{Name: r.name, ProtocolType: r.h.protoType, State: groupState(r.h, n)})
	}
	return core.ControlPlaneDone{Resp: core.ListGroupsResp{Groups: out}}
}
