package client

import (
	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

// Wire conversions between core ControlPlane types and the controlplane API
// messages. Shared message types live in sharedv1; ErrCode, Outcome, and
// RPC-specific messages live in cpv1.

// ---- Error and outcome codes ----

var errCodePairs = [...]struct {
	core contract.ErrCode
	wire cpv1.ErrCode
}{
	{contract.OK, cpv1.ErrCode_ERR_CODE_OK},
	{contract.ErrTimeout, cpv1.ErrCode_ERR_CODE_TIMEOUT},
	{contract.ErrUnavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
	{contract.ErrNotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
	{contract.ErrConflict, cpv1.ErrCode_ERR_CODE_CONFLICT},
	{contract.ErrConnClosed, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
	{contract.ErrTooLarge, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
	{contract.ErrCorrupt, cpv1.ErrCode_ERR_CODE_CORRUPT},
	{contract.ErrInvalid, cpv1.ErrCode_ERR_CODE_INVALID},
	{contract.ErrIO, cpv1.ErrCode_ERR_CODE_IO},
	{contract.ErrUnknownMember, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
	{contract.ErrIllegalGeneration, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
	{contract.ErrUnknownPartition, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
	{contract.ErrRebalanceInProgress, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
	{contract.ErrInconsistentGroupProtocol, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
	{contract.ErrTopicExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
}

func toContractErr(c cpv1.ErrCode) contract.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return contract.ErrIO
}

func fromContractErr(c contract.ErrCode) cpv1.ErrCode {
	for _, pair := range errCodePairs {
		if pair.core == c {
			return pair.wire
		}
	}
	return cpv1.ErrCode_ERR_CODE_IO
}

func toContractOutcome(o cpv1.Outcome) contract.Outcome {
	switch o {
	case cpv1.Outcome_OUTCOME_MAYBE_APPLIED:
		return contract.MaybeApplied
	case cpv1.Outcome_OUTCOME_APPLIED:
		return contract.Applied
	default:
		return contract.NotApplied
	}
}

func fromContractOutcome(o contract.Outcome) cpv1.Outcome {
	switch o {
	case contract.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case contract.Applied:
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

func topicPartitionFromContract(tp contract.TopicPartition) *sharedv1.TopicPartition {
	return &sharedv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionToContract(tp *sharedv1.TopicPartition) contract.TopicPartition {
	return contract.NewTopicPartition(tp.GetTopic(), tp.GetPartition())
}

func segmentRefFromContract(ref contract.SegmentRef) *sharedv1.SegmentRef {
	return &sharedv1.SegmentRef{Key: ref.Key, Off: ref.Off, Len: ref.Len}
}

func segmentRefToContract(ref *sharedv1.SegmentRef) contract.SegmentRef {
	return contract.SegmentRef{Key: ref.GetKey(), Off: ref.GetOff(), Len: ref.GetLen()}
}

func pendingBatchFromContract(b contract.PendingBatch) *sharedv1.PendingBatch {
	return &sharedv1.PendingBatch{
		Tp:      topicPartitionFromContract(b.TopicPartition),
		Records: b.Records,
		Loc:     segmentRefFromContract(b.Loc),
	}
}

func pendingBatchToContract(b *sharedv1.PendingBatch) contract.PendingBatch {
	return contract.PendingBatch{
		TopicPartition: topicPartitionToContract(b.GetTp()),
		Records:        b.GetRecords(),
		Loc:            segmentRefToContract(b.GetLoc()),
	}
}

func batchRangeFromContract(br contract.BatchRange) *sharedv1.BatchRange {
	return &sharedv1.BatchRange{
		Tp:    topicPartitionFromContract(br.TopicPartition),
		Base:  int64(br.Base),
		Count: br.Count,
		Loc:   segmentRefFromContract(br.Loc),
	}
}

func batchRangeToContract(br *sharedv1.BatchRange) contract.BatchRange {
	return contract.BatchRange{
		TopicPartition: topicPartitionToContract(br.GetTp()),
		Base:           contract.Offset(br.GetBase()),
		Count:          br.GetCount(),
		Loc:            segmentRefToContract(br.GetLoc()),
	}
}

func topicInfoFromContract(t contract.TopicInfo) *sharedv1.TopicInfo {
	return &sharedv1.TopicInfo{Name: t.Name, Partitions: t.Partitions}
}

func topicInfoToContract(t *sharedv1.TopicInfo) contract.TopicInfo {
	return contract.TopicInfo{Name: t.GetName(), Partitions: t.GetPartitions()}
}

func brokerInfoFromContract(b contract.BrokerAddr) *cpv1.BrokerInfo {
	return &cpv1.BrokerInfo{NodeId: b.NodeID, Host: b.Host, Port: b.Port}
}

func brokerInfoToContract(b *cpv1.BrokerInfo) contract.BrokerAddr {
	return contract.BrokerAddr{NodeID: b.GetNodeId(), Host: b.GetHost(), Port: b.GetPort()}
}
