package cpclient

import (
	"slices"

	cpv1 "github.com/manifesterio/mds/broker/api/controlplane/v1"
	"github.com/manifesterio/mds/broker/internal/core"
)

func commitSegmentFromCore(operation core.CommitSegment) *cpv1.CommitSegmentRequest {
	return &cpv1.CommitSegmentRequest{
		SegmentKey: operation.SegmentKey,
		Batches:    mapSlice(operation.Batches, pendingBatchFromCore),
	}
}

func commitSegmentToCore(request *cpv1.CommitSegmentRequest) core.CommitSegment {
	return core.CommitSegment{
		SegmentKey: request.GetSegmentKey(),
		Batches:    mapSlice(request.GetBatches(), pendingBatchToCore),
	}
}

func findSegmentsFromCore(operation core.FindSegments) *cpv1.FindSegmentsRequest {
	return &cpv1.FindSegmentsRequest{
		Tp:       topicPartitionFromCore(operation.TopicPartition),
		From:     int64(operation.From),
		MaxBytes: operation.MaxBytes,
	}
}

func findSegmentsToCore(request *cpv1.FindSegmentsRequest) core.FindSegments {
	return core.FindSegments{
		TopicPartition: topicPartitionToCore(request.GetTp()),
		From:           core.Offset(request.GetFrom()),
		MaxBytes:       request.GetMaxBytes(),
	}
}

func describeTopicsFromCore(operation core.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: slices.Clone(operation.Topics)}
}

func describeTopicsToCore(request *cpv1.DescribeTopicsRequest) core.DescribeTopics {
	return core.DescribeTopics{Topics: slices.Clone(request.GetTopics())}
}

func commitSegmentRespFromCore(r core.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	return &cpv1.CommitSegmentResponse{Assigned: mapSlice(r.Assigned, batchRangeFromCore)}
}

func commitSegmentRespToCore(r *cpv1.CommitSegmentResponse) core.CommitSegmentResp {
	return core.CommitSegmentResp{Assigned: mapSlice(r.GetAssigned(), batchRangeToCore)}
}

func findSegmentsRespFromCore(r core.FindSegmentsResp) *cpv1.FindSegmentsResponse {
	return &cpv1.FindSegmentsResponse{End: int64(r.End), Ranges: mapSlice(r.Ranges, batchRangeFromCore)}
}

func findSegmentsRespToCore(r *cpv1.FindSegmentsResponse) core.FindSegmentsResp {
	return core.FindSegmentsResp{End: core.Offset(r.GetEnd()), Ranges: mapSlice(r.GetRanges(), batchRangeToCore)}
}

func describeTopicsRespFromCore(r core.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	return &cpv1.DescribeTopicsResponse{Topics: mapSlice(r.Topics, topicInfoFromCore)}
}

func describeTopicsRespToCore(r *cpv1.DescribeTopicsResponse) core.DescribeTopicsResp {
	return core.DescribeTopicsResp{Topics: mapSlice(r.GetTopics(), topicInfoToCore)}
}

func brokerHeartbeatFromCore(b core.BrokerAddr) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: brokerInfoFromCore(b)}
}

func brokerHeartbeatToCore(request *cpv1.BrokerHeartbeatRequest) core.BrokerAddr {
	return brokerInfoToCore(request.GetBroker())
}

func brokerHeartbeatRespFromCore(live []core.BrokerAddr) *cpv1.BrokerHeartbeatResponse {
	return &cpv1.BrokerHeartbeatResponse{Brokers: mapSlice(live, brokerInfoFromCore)}
}

func brokerHeartbeatRespToCore(r *cpv1.BrokerHeartbeatResponse) []core.BrokerAddr {
	return mapSlice(r.GetBrokers(), brokerInfoToCore)
}
