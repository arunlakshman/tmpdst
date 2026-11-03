package wireboundary

import (
	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/core"
	"slices"
)

// Conversions for the topics context: CommitSegment, Lookup and DescribeTopics.

func topicInfoFromCore(t core.TopicInfo) *cpv1.TopicInfo {
	return &cpv1.TopicInfo{Name: t.Name, Partitions: t.Partitions}
}

func topicInfoToCore(t *cpv1.TopicInfo) core.TopicInfo {
	return core.TopicInfo{Name: t.GetName(), Partitions: t.GetPartitions()}
}

// CommitSegmentFromCore builds a CommitSegmentRequest from a core op.
func CommitSegmentFromCore(operation core.CommitSegment) *cpv1.CommitSegmentRequest {
	return &cpv1.CommitSegmentRequest{
		SegmentKey: operation.SegmentKey,
		Batches:    mapSlice(operation.Batches, pendingBatchFromCore),
	}
}

// CommitSegmentToCore rebuilds a core.CommitSegment from a request.
func CommitSegmentToCore(request *cpv1.CommitSegmentRequest) core.CommitSegment {
	return core.CommitSegment{
		SegmentKey: request.GetSegmentKey(),
		Batches:    mapSlice(request.GetBatches(), pendingBatchToCore),
	}
}

// LookupFromCore builds a LookupRequest from a core op.
func LookupFromCore(operation core.FindSegments) *cpv1.LookupRequest {
	return &cpv1.LookupRequest{
		Tp:       topicPartitionFromCore(operation.TopicPartition),
		From:     int64(operation.From),
		MaxBytes: operation.MaxBytes,
	}
}

// LookupToCore rebuilds a core.FindSegments from a request.
func LookupToCore(request *cpv1.LookupRequest) core.FindSegments {
	return core.FindSegments{
		TopicPartition: topicPartitionToCore(request.GetTp()),
		From:           core.Offset(request.GetFrom()),
		MaxBytes:       request.GetMaxBytes(),
	}
}

// DescribeTopicsFromCore builds a DescribeTopicsRequest from a core op.
func DescribeTopicsFromCore(operation core.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: slices.Clone(operation.Topics)}
}

// DescribeTopicsToCore rebuilds a core.DescribeTopics from a request.
func DescribeTopicsToCore(request *cpv1.DescribeTopicsRequest) core.DescribeTopics {
	return core.DescribeTopics{Topics: slices.Clone(request.GetTopics())}
}

// CommitSegmentRespFromCore builds a CommitSegmentResponse from a core response.
func CommitSegmentRespFromCore(r core.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	return &cpv1.CommitSegmentResponse{Assigned: mapSlice(r.Assigned, batchRangeFromCore)}
}

// CommitSegmentRespToCore rebuilds a core.CommitSegmentResp from a response.
func CommitSegmentRespToCore(r *cpv1.CommitSegmentResponse) core.CommitSegmentResp {
	return core.CommitSegmentResp{Assigned: mapSlice(r.GetAssigned(), batchRangeToCore)}
}

// LookupRespFromCore builds a LookupResponse from a core response.
func LookupRespFromCore(r core.FindSegmentsResp) *cpv1.LookupResponse {
	return &cpv1.LookupResponse{End: int64(r.End), Ranges: mapSlice(r.Ranges, batchRangeFromCore)}
}

// LookupRespToCore rebuilds a core.FindSegmentsResp from a response.
func LookupRespToCore(r *cpv1.LookupResponse) core.FindSegmentsResp {
	return core.FindSegmentsResp{End: core.Offset(r.GetEnd()), Ranges: mapSlice(r.GetRanges(), batchRangeToCore)}
}

// DescribeTopicsRespFromCore builds a DescribeTopicsResponse from a core response.
func DescribeTopicsRespFromCore(r core.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	return &cpv1.DescribeTopicsResponse{Topics: mapSlice(r.Topics, topicInfoFromCore)}
}

// DescribeTopicsRespToCore rebuilds a core.DescribeTopicsResp from a response.
func DescribeTopicsRespToCore(r *cpv1.DescribeTopicsResponse) core.DescribeTopicsResp {
	return core.DescribeTopicsResp{Topics: mapSlice(r.GetTopics(), topicInfoToCore)}
}

// ---- Broker heartbeat ----
