package cpclient

import (
	sharedv1 "github.com/manifesterio/mds/broker/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/broker/api/controlplane/v1"
	"github.com/manifesterio/mds/broker/internal/core"
)

// Wire conversions between core ControlPlane types and the controlplane API
// messages. Shared message types live in sharedv1; ErrCode, Outcome, and
// RPC-specific messages live in cpv1.

// ---- Error and outcome codes ----

var errCodePairs = [...]struct {
	core core.ErrCode
	wire cpv1.ErrCode
}{
	{core.OK, cpv1.ErrCode_ERR_CODE_OK},
	{core.ErrTimeout, cpv1.ErrCode_ERR_CODE_TIMEOUT},
	{core.ErrUnavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
	{core.ErrNotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
	{core.ErrConflict, cpv1.ErrCode_ERR_CODE_CONFLICT},
	{core.ErrConnClosed, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
	{core.ErrTooLarge, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
	{core.ErrCorrupt, cpv1.ErrCode_ERR_CODE_CORRUPT},
	{core.ErrInvalid, cpv1.ErrCode_ERR_CODE_INVALID},
	{core.ErrIO, cpv1.ErrCode_ERR_CODE_IO},
	{core.ErrUnknownMember, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
	{core.ErrIllegalGeneration, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
	{core.ErrUnknownPartition, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
	{core.ErrRebalanceInProgress, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
	{core.ErrInconsistentGroupProtocol, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
	{core.ErrTopicExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
}

func toCoreErr(c cpv1.ErrCode) core.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return core.ErrIO
}

func fromCoreErr(c core.ErrCode) cpv1.ErrCode {
	for _, pair := range errCodePairs {
		if pair.core == c {
			return pair.wire
		}
	}
	return cpv1.ErrCode_ERR_CODE_IO
}

func toCoreOutcome(o cpv1.Outcome) core.Outcome {
	switch o {
	case cpv1.Outcome_OUTCOME_MAYBE_APPLIED:
		return core.MaybeApplied
	case cpv1.Outcome_OUTCOME_APPLIED:
		return core.Applied
	default:
		return core.NotApplied
	}
}

func fromCoreOutcome(o core.Outcome) cpv1.Outcome {
	switch o {
	case core.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case core.Applied:
		return cpv1.Outcome_OUTCOME_APPLIED
	default:
		return cpv1.Outcome_OUTCOME_NOT_APPLIED
	}
}

// mapSlice converts every element. An empty input gives nil.
func mapSlice[From, To any](in []From, convert func(From) To) []To {
	if len(in) == 0 {
		return nil
	}
	out := make([]To, len(in))
	for i, item := range in {
		out[i] = convert(item)
	}
	return out
}

func topicPartitionFromCore(tp core.TopicPartition) *sharedv1.TopicPartition {
	return &sharedv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionToCore(tp *sharedv1.TopicPartition) core.TopicPartition {
	return core.NewTopicPartition(tp.GetTopic(), tp.GetPartition())
}

func segmentRefFromCore(ref core.SegmentRef) *sharedv1.SegmentRef {
	return &sharedv1.SegmentRef{Key: ref.Key, Off: ref.Off, Len: ref.Len}
}

func segmentRefToCore(ref *sharedv1.SegmentRef) core.SegmentRef {
	return core.SegmentRef{Key: ref.GetKey(), Off: ref.GetOff(), Len: ref.GetLen()}
}

func pendingBatchFromCore(b core.PendingBatch) *sharedv1.PendingBatch {
	return &sharedv1.PendingBatch{
		Tp:      topicPartitionFromCore(b.TopicPartition),
		Records: b.Records,
		Loc:     segmentRefFromCore(b.Loc),
	}
}

func pendingBatchToCore(b *sharedv1.PendingBatch) core.PendingBatch {
	return core.PendingBatch{
		TopicPartition: topicPartitionToCore(b.GetTp()),
		Records:        b.GetRecords(),
		Loc:            segmentRefToCore(b.GetLoc()),
	}
}

func batchRangeFromCore(br core.BatchRange) *sharedv1.BatchRange {
	return &sharedv1.BatchRange{
		Tp:    topicPartitionFromCore(br.TopicPartition),
		Base:  int64(br.Base),
		Count: br.Count,
		Loc:   segmentRefFromCore(br.Loc),
	}
}

func batchRangeToCore(br *sharedv1.BatchRange) core.BatchRange {
	return core.BatchRange{
		TopicPartition: topicPartitionToCore(br.GetTp()),
		Base:           core.Offset(br.GetBase()),
		Count:          br.GetCount(),
		Loc:            segmentRefToCore(br.GetLoc()),
	}
}

func topicInfoFromCore(t core.TopicInfo) *sharedv1.TopicInfo {
	return &sharedv1.TopicInfo{Name: t.Name, Partitions: t.Partitions}
}

func topicInfoToCore(t *sharedv1.TopicInfo) core.TopicInfo {
	return core.TopicInfo{Name: t.GetName(), Partitions: t.GetPartitions()}
}

func brokerInfoFromCore(b core.BrokerAddr) *cpv1.BrokerInfo {
	return &cpv1.BrokerInfo{NodeId: b.NodeID, Host: b.Host, Port: b.Port}
}

func brokerInfoToCore(b *cpv1.BrokerInfo) core.BrokerAddr {
	return core.BrokerAddr{NodeID: b.GetNodeId(), Host: b.GetHost(), Port: b.GetPort()}
}
