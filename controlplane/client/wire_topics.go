package client

import (
	"slices"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

func commitSegmentFromContract(operation contract.CommitSegment) *cpv1.CommitSegmentRequest {
	return &cpv1.CommitSegmentRequest{
		SegmentKey: operation.SegmentKey,
		Batches:    mapSlice(operation.Batches, pendingBatchFromContract),
	}
}

func commitSegmentToContract(request *cpv1.CommitSegmentRequest) contract.CommitSegment {
	return contract.CommitSegment{
		SegmentKey: request.GetSegmentKey(),
		Batches:    mapSlice(request.GetBatches(), pendingBatchToContract),
	}
}

func findSegmentsFromContract(operation contract.FindSegments) *cpv1.FindSegmentsRequest {
	return &cpv1.FindSegmentsRequest{
		Tp:       topicPartitionFromContract(operation.TopicPartition),
		From:     int64(operation.From),
		MaxBytes: operation.MaxBytes,
	}
}

func findSegmentsToContract(request *cpv1.FindSegmentsRequest) contract.FindSegments {
	return contract.FindSegments{
		TopicPartition: topicPartitionToContract(request.GetTp()),
		From:           contract.Offset(request.GetFrom()),
		MaxBytes:       request.GetMaxBytes(),
	}
}

func describeTopicsFromContract(operation contract.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: slices.Clone(operation.Topics)}
}

func describeTopicsToContract(request *cpv1.DescribeTopicsRequest) contract.DescribeTopics {
	return contract.DescribeTopics{Topics: slices.Clone(request.GetTopics())}
}

func commitSegmentRespFromContract(r contract.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	return &cpv1.CommitSegmentResponse{Assigned: mapSlice(r.Assigned, batchRangeFromContract)}
}

func commitSegmentRespToContract(r *cpv1.CommitSegmentResponse) contract.CommitSegmentResp {
	return contract.CommitSegmentResp{Assigned: mapSlice(r.GetAssigned(), batchRangeToContract)}
}

func findSegmentsRespFromContract(r contract.FindSegmentsResp) *cpv1.FindSegmentsResponse {
	return &cpv1.FindSegmentsResponse{End: int64(r.End), Ranges: mapSlice(r.Ranges, batchRangeFromContract)}
}

func findSegmentsRespToContract(r *cpv1.FindSegmentsResponse) contract.FindSegmentsResp {
	return contract.FindSegmentsResp{End: contract.Offset(r.GetEnd()), Ranges: mapSlice(r.GetRanges(), batchRangeToContract)}
}

func describeTopicsRespFromContract(r contract.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	return &cpv1.DescribeTopicsResponse{Topics: mapSlice(r.Topics, topicInfoFromContract)}
}

func describeTopicsRespToContract(r *cpv1.DescribeTopicsResponse) contract.DescribeTopicsResp {
	return contract.DescribeTopicsResp{Topics: mapSlice(r.GetTopics(), topicInfoToContract)}
}

func brokerHeartbeatFromContract(b contract.BrokerAddr) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: brokerInfoFromContract(b)}
}

func brokerHeartbeatToContract(request *cpv1.BrokerHeartbeatRequest) contract.BrokerAddr {
	return brokerInfoToContract(request.GetBroker())
}

func brokerHeartbeatRespFromContract(live []contract.BrokerAddr) *cpv1.BrokerHeartbeatResponse {
	return &cpv1.BrokerHeartbeatResponse{Brokers: mapSlice(live, brokerInfoFromContract)}
}

func brokerHeartbeatRespToContract(r *cpv1.BrokerHeartbeatResponse) []contract.BrokerAddr {
	return mapSlice(r.GetBrokers(), brokerInfoToContract)
}
