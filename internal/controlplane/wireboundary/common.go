package wireboundary

import (
	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/core"
)

// Conversions shared by every context: error codes and outcomes, and the
// types that more than one context uses (topic partitions, segment references,
// batches and ranges).

// ---- Error and outcome codes ----

// errCodePairs is the one list that ties each core error code to its wire code.
// Both directions read it. To add a code, add one line here, plus the new
// constant in core and the new value in the proto enum. TestErrCodeTableIsComplete
// fails if a code is missing.
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

// ToCoreErr maps a wire ErrCode to core.ErrCode. Unknown values become ErrIO.
func ToCoreErr(c cpv1.ErrCode) core.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return core.ErrIO
}

// FromCoreErr maps core.ErrCode to the wire enum. Unknown values become ERR_CODE_IO.
func FromCoreErr(c core.ErrCode) cpv1.ErrCode {
	for _, pair := range errCodePairs {
		if pair.core == c {
			return pair.wire
		}
	}
	return cpv1.ErrCode_ERR_CODE_IO
}

// ToCoreOutcome maps a wire Outcome to core.Outcome. Unknown values become NotApplied.
func ToCoreOutcome(o cpv1.Outcome) core.Outcome {
	switch o {
	case cpv1.Outcome_OUTCOME_MAYBE_APPLIED:
		return core.MaybeApplied
	case cpv1.Outcome_OUTCOME_APPLIED:
		return core.Applied
	default:
		return core.NotApplied
	}
}

// FromCoreOutcome maps core.Outcome to the wire enum.
func FromCoreOutcome(o core.Outcome) cpv1.Outcome {
	switch o {
	case core.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case core.Applied:
		return cpv1.Outcome_OUTCOME_APPLIED
	default:
		return cpv1.Outcome_OUTCOME_NOT_APPLIED
	}
}

// ---- Shared pieces ----

// Rule for every "ToCore" function in this package: read the wire message with
// its Get methods. They accept a nil message and give the zero value, so a
// missing part becomes a zero value and nothing panics.

// mapSlice converts every element. An empty input gives nil, the same as a
// loop that appends to a nil slice.
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

func topicPartitionFromCore(tp core.TopicPartition) *cpv1.TopicPartition {
	return &cpv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionToCore(tp *cpv1.TopicPartition) core.TopicPartition {
	return core.NewTopicPartition(tp.GetTopic(), tp.GetPartition())
}

func segmentRefFromCore(ref core.SegmentRef) *cpv1.SegmentRef {
	return &cpv1.SegmentRef{Key: ref.Key, Off: ref.Off, Len: ref.Len}
}

func segmentRefToCore(ref *cpv1.SegmentRef) core.SegmentRef {
	return core.SegmentRef{Key: ref.GetKey(), Off: ref.GetOff(), Len: ref.GetLen()}
}

func pendingBatchFromCore(b core.PendingBatch) *cpv1.PendingBatch {
	return &cpv1.PendingBatch{
		Tp:      topicPartitionFromCore(b.TopicPartition),
		Records: b.Records,
		Loc:     segmentRefFromCore(b.Loc),
	}
}

func pendingBatchToCore(b *cpv1.PendingBatch) core.PendingBatch {
	return core.PendingBatch{
		TopicPartition: topicPartitionToCore(b.GetTp()),
		Records:        b.GetRecords(),
		Loc:            segmentRefToCore(b.GetLoc()),
	}
}

func batchRangeFromCore(br core.BatchRange) *cpv1.BatchRange {
	return &cpv1.BatchRange{
		Tp:    topicPartitionFromCore(br.TopicPartition),
		Base:  int64(br.Base),
		Count: br.Count,
		Loc:   segmentRefFromCore(br.Loc),
	}
}

func batchRangeToCore(br *cpv1.BatchRange) core.BatchRange {
	return core.BatchRange{
		TopicPartition: topicPartitionToCore(br.GetTp()),
		Base:           core.Offset(br.GetBase()),
		Count:          br.GetCount(),
		Loc:            segmentRefToCore(br.GetLoc()),
	}
}
