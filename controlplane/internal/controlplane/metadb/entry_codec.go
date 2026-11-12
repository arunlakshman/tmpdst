package metadb

import (
	"fmt"

	logv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/log/v1"
	sharedv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/shared/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The conversion between the contract types and the log messages (logv1). It
// is the only one: the log package has no generated converters. A message
// field that is not in the bytes reads as its zero value, and an empty list or
// []byte reads as nil, so Decode(Encode(e)) is e for every e whose lists are nil
// when empty.

// kindOf is the kind of a logged operation, and KindNoop for any other.
func kindOf(operation runtime.ControlPlaneOperation) Kind {
	switch x := operation.(type) {
	case contract.CommitSegment:
		return KindCommit
	case contract.CreateTopic:
		if x.FailIfExists {
			return KindCreateTopicExclusive
		}
		return KindCreateTopic
	case contract.GroupInstall:
		return KindGroupInstall
	case contract.GroupAssign:
		return KindGroupAssign
	case contract.GroupRemove:
		return KindGroupRemove
	case contract.OffsetCommit:
		return KindOffsetCommit
	}
	return KindNoop
}

func entryToLog(en Entry) *logv1.Entry {
	m := &logv1.Entry{Id: en.ID}
	switch x := en.Op.(type) {
	case nil:
	case contract.CommitSegment:
		m.Command = &logv1.Entry_CommitSegment{CommitSegment: &logv1.CommitSegment{SegmentKey: x.SegmentKey, Batches: batchesToLog(x.Batches)}}
	case contract.CreateTopic:
		m.Command = &logv1.Entry_CreateTopic{CreateTopic: &logv1.CreateTopic{Name: x.Name, Partitions: x.Partitions, FailIfExists: x.FailIfExists}}
	case contract.GroupInstall:
		m.Command = &logv1.Entry_GroupInstall{GroupInstall: installToLog(x)}
	case contract.GroupAssign:
		m.Command = &logv1.Entry_GroupAssign{GroupAssign: assignToLog(x)}
	case contract.GroupRemove:
		m.Command = &logv1.Entry_GroupRemove{GroupRemove: &logv1.GroupRemove{Group: x.Group, Members: x.Members, TimeMs: x.TimeMs, Reason: x.Reason}}
	case contract.OffsetCommit:
		m.Command = &logv1.Entry_OffsetCommit{OffsetCommit: offsetCommitToLog(x)}
	default:
		panic(fmt.Sprintf("metadata: %T is not a logged operation", en.Op))
	}
	return m
}

func entryFromLog(m *logv1.Entry) Entry {
	en := Entry{ID: m.GetId()}
	switch x := m.GetCommand().(type) {
	case *logv1.Entry_CommitSegment:
		c := x.CommitSegment
		en.Op = contract.CommitSegment{SegmentKey: c.GetSegmentKey(), Batches: batchesFromLog(c.GetBatches())}
	case *logv1.Entry_CreateTopic:
		c := x.CreateTopic
		en.Op = contract.CreateTopic{Name: c.GetName(), Partitions: c.GetPartitions(), FailIfExists: c.GetFailIfExists()}
	case *logv1.Entry_GroupInstall:
		en.Op = installFromLog(x.GroupInstall)
	case *logv1.Entry_GroupAssign:
		en.Op = assignFromLog(x.GroupAssign)
	case *logv1.Entry_GroupRemove:
		r := x.GroupRemove
		en.Op = contract.GroupRemove{Group: r.GetGroup(), Members: listOrNil(r.GetMembers()), TimeMs: r.GetTimeMs(), Reason: r.GetReason()}
	case *logv1.Entry_OffsetCommit:
		en.Op = offsetCommitFromLog(x.OffsetCommit)
	}
	en.Kind = kindOf(en.Op)
	return en
}

func topicPartitionToLog(tp contract.TopicPartition) *sharedv1.TopicPartition {
	return &sharedv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionFromLog(tp *sharedv1.TopicPartition) contract.TopicPartition {
	return contract.TopicPartition{Topic: tp.GetTopic(), Partition: tp.GetPartition()}
}

func batchesToLog(batches []contract.PendingBatch) []*sharedv1.PendingBatch {
	var out []*sharedv1.PendingBatch
	for _, b := range batches {
		out = append(out, &sharedv1.PendingBatch{
			Tp:      topicPartitionToLog(b.TopicPartition),
			Records: b.Records,
			Loc:     &sharedv1.SegmentRef{Key: b.Loc.Key, Off: b.Loc.Off, Len: b.Loc.Len},
		})
	}
	return out
}

func batchesFromLog(batches []*sharedv1.PendingBatch) []contract.PendingBatch {
	var out []contract.PendingBatch
	for _, b := range batches {
		loc := b.GetLoc()
		out = append(out, contract.PendingBatch{
			TopicPartition: topicPartitionFromLog(b.GetTp()),
			Records:        b.GetRecords(),
			Loc:            runtime.SegmentRef{Key: loc.GetKey(), Off: loc.GetOff(), Len: loc.GetLen()},
		})
	}
	return out
}

func installToLog(x contract.GroupInstall) *logv1.GroupInstall {
	m := &logv1.GroupInstall{Group: x.Group, ProtocolType: x.ProtocolType, Protocol: x.Protocol, Generation: x.Generation, Leader: x.Leader, TimeMs: x.TimeMs}
	for _, g := range x.Members {
		m.Members = append(m.Members, &logv1.GroupMember{
			MemberId: g.MemberID, ClientId: g.ClientID, ClientHost: g.ClientHost, GroupInstanceId: g.GroupInstanceID,
			SessionTimeoutMs: g.SessionTimeoutMs, RebalanceTimeoutMs: g.RebalanceTimeoutMs, Metadata: g.Metadata,
		})
	}
	return m
}

func installFromLog(m *logv1.GroupInstall) contract.GroupInstall {
	x := contract.GroupInstall{Group: m.GetGroup(), ProtocolType: m.GetProtocolType(), Protocol: m.GetProtocol(), Generation: m.GetGeneration(), Leader: m.GetLeader(), TimeMs: m.GetTimeMs()}
	for _, g := range m.GetMembers() {
		x.Members = append(x.Members, contract.GroupMember{
			MemberID: g.GetMemberId(), ClientID: g.GetClientId(), ClientHost: g.GetClientHost(), GroupInstanceID: g.GetGroupInstanceId(),
			SessionTimeoutMs: g.GetSessionTimeoutMs(), RebalanceTimeoutMs: g.GetRebalanceTimeoutMs(), Metadata: listOrNil(g.GetMetadata()),
		})
	}
	return x
}

func assignToLog(x contract.GroupAssign) *logv1.GroupAssign {
	m := &logv1.GroupAssign{Group: x.Group, Generation: x.Generation, Leader: x.Leader, TimeMs: x.TimeMs}
	for _, a := range x.Assignments {
		m.Assignments = append(m.Assignments, &logv1.GroupAssignment{Member: a.Member, Data: a.Bytes})
	}
	return m
}

func assignFromLog(m *logv1.GroupAssign) contract.GroupAssign {
	x := contract.GroupAssign{Group: m.GetGroup(), Generation: m.GetGeneration(), Leader: m.GetLeader(), TimeMs: m.GetTimeMs()}
	for _, a := range m.GetAssignments() {
		x.Assignments = append(x.Assignments, contract.GroupAssignment{Member: a.GetMember(), Bytes: listOrNil(a.GetData())})
	}
	return x
}

func offsetCommitToLog(x contract.OffsetCommit) *logv1.OffsetCommit {
	m := &logv1.OffsetCommit{Group: x.Group, Member: x.Member, Generation: x.Generation, TimeMs: x.TimeMs}
	for _, o := range x.Offsets {
		m.Offsets = append(m.Offsets, &sharedv1.PartitionOffset{
			Tp: topicPartitionToLog(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
		})
	}
	return m
}

func offsetCommitFromLog(m *logv1.OffsetCommit) contract.OffsetCommit {
	x := contract.OffsetCommit{Group: m.GetGroup(), Member: m.GetMember(), Generation: m.GetGeneration(), TimeMs: m.GetTimeMs()}
	for _, o := range m.GetOffsets() {
		x.Offsets = append(x.Offsets, contract.PartitionOffset{
			TopicPartition: topicPartitionFromLog(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
		})
	}
	return x
}

// listOrNil is nil for an empty list, so an empty list and a nil list decode
// the same way (the bytes do not tell them apart).
func listOrNil[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return s
}
