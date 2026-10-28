// Package apiconv maps core ControlPlaneOperation/ControlPlaneResponse values to controlplane/api/v1 messages.
package apiconv

import (
	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/core"
)

// ToCoreErr maps a wire ErrCode to core.ErrCode. Unknown values become ErrIO.
//
//nolint:dupl // mirror of FromCoreErr; each direction must stay exhaustive
func ToCoreErr(c cpv1.ErrCode) core.ErrCode {
	switch c {
	case cpv1.ErrCode_ERR_CODE_OK:
		return core.OK
	case cpv1.ErrCode_ERR_CODE_TIMEOUT:
		return core.ErrTimeout
	case cpv1.ErrCode_ERR_CODE_UNAVAILABLE:
		return core.ErrUnavailable
	case cpv1.ErrCode_ERR_CODE_NOT_FOUND:
		return core.ErrNotFound
	case cpv1.ErrCode_ERR_CODE_CONFLICT:
		return core.ErrConflict
	case cpv1.ErrCode_ERR_CODE_CONN_CLOSED:
		return core.ErrConnClosed
	case cpv1.ErrCode_ERR_CODE_TOO_LARGE:
		return core.ErrTooLarge
	case cpv1.ErrCode_ERR_CODE_CORRUPT:
		return core.ErrCorrupt
	case cpv1.ErrCode_ERR_CODE_INVALID:
		return core.ErrInvalid
	case cpv1.ErrCode_ERR_CODE_IO:
		return core.ErrIO
	default:
		return core.ErrIO
	}
}

// FromCoreErr maps core.ErrCode to the wire enum.
//
//nolint:dupl // mirror of ToCoreErr; each direction must stay exhaustive
func FromCoreErr(c core.ErrCode) cpv1.ErrCode {
	switch c {
	case core.OK:
		return cpv1.ErrCode_ERR_CODE_OK
	case core.ErrTimeout:
		return cpv1.ErrCode_ERR_CODE_TIMEOUT
	case core.ErrUnavailable:
		return cpv1.ErrCode_ERR_CODE_UNAVAILABLE
	case core.ErrNotFound:
		return cpv1.ErrCode_ERR_CODE_NOT_FOUND
	case core.ErrConflict:
		return cpv1.ErrCode_ERR_CODE_CONFLICT
	case core.ErrConnClosed:
		return cpv1.ErrCode_ERR_CODE_CONN_CLOSED
	case core.ErrTooLarge:
		return cpv1.ErrCode_ERR_CODE_TOO_LARGE
	case core.ErrCorrupt:
		return cpv1.ErrCode_ERR_CODE_CORRUPT
	case core.ErrInvalid:
		return cpv1.ErrCode_ERR_CODE_INVALID
	case core.ErrIO:
		return cpv1.ErrCode_ERR_CODE_IO
	default:
		return cpv1.ErrCode_ERR_CODE_IO
	}
}

// ToCoreOutcome maps a wire Outcome to core.Outcome.
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

// CommitSegmentFromCore builds a CommitSegmentRequest from a core op.
func CommitSegmentFromCore(op core.CommitSegment) *cpv1.CommitSegmentRequest {
	req := &cpv1.CommitSegmentRequest{SegmentKey: op.SegmentKey}
	for _, b := range op.Batches {
		req.Batches = append(req.Batches, &cpv1.PendingBatch{
			Tp:      &cpv1.TopicPartition{Topic: b.TP.Topic, Partition: b.TP.Partition},
			Records: b.Records,
			Loc:     &cpv1.SegmentRef{Key: b.Loc.Key, Off: b.Loc.Off, Len: b.Loc.Len},
		})
	}
	return req
}

// CommitSegmentToCore rebuilds a core.CommitSegment from a request.
func CommitSegmentToCore(req *cpv1.CommitSegmentRequest) core.CommitSegment {
	if req == nil {
		return core.CommitSegment{}
	}
	op := core.CommitSegment{SegmentKey: req.SegmentKey}
	for _, b := range req.Batches {
		if b == nil {
			continue
		}
		var tp core.TopicPartition
		if b.Tp != nil {
			tp = core.NewTopicPartition(b.Tp.Topic, b.Tp.Partition)
		}
		var loc core.SegmentRef
		if b.Loc != nil {
			loc = core.SegmentRef{Key: b.Loc.Key, Off: b.Loc.Off, Len: b.Loc.Len}
		}
		op.Batches = append(op.Batches, core.PendingBatch{TP: tp, Records: b.Records, Loc: loc})
	}
	return op
}

// LookupFromCore builds a LookupRequest from a core op.
func LookupFromCore(op core.FindSegments) *cpv1.LookupRequest {
	return &cpv1.LookupRequest{
		Tp:       &cpv1.TopicPartition{Topic: op.TP.Topic, Partition: op.TP.Partition},
		From:     int64(op.From),
		MaxBytes: op.MaxBytes,
	}
}

// LookupToCore rebuilds a core.FindSegments from a request.
func LookupToCore(req *cpv1.LookupRequest) core.FindSegments {
	if req == nil {
		return core.FindSegments{}
	}
	op := core.FindSegments{From: core.Offset(req.From), MaxBytes: req.MaxBytes}
	if req.Tp != nil {
		op.TP = core.NewTopicPartition(req.Tp.Topic, req.Tp.Partition)
	}
	return op
}

// DescribeTopicsFromCore builds a DescribeTopicsRequest from a core op.
func DescribeTopicsFromCore(op core.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: append([]string(nil), op.Topics...)}
}

// DescribeTopicsToCore rebuilds a core.DescribeTopics from a request.
func DescribeTopicsToCore(req *cpv1.DescribeTopicsRequest) core.DescribeTopics {
	if req == nil {
		return core.DescribeTopics{}
	}
	return core.DescribeTopics{Topics: append([]string(nil), req.Topics...)}
}

// CommitSegmentRespFromCore builds a CommitSegmentResponse from a core response.
func CommitSegmentRespFromCore(r core.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	out := &cpv1.CommitSegmentResponse{}
	for _, br := range r.Assigned {
		out.Assigned = append(out.Assigned, batchRangeFromCore(br))
	}
	return out
}

// CommitSegmentRespToCore rebuilds a core.CommitSegmentResp from a response.
func CommitSegmentRespToCore(r *cpv1.CommitSegmentResponse) core.CommitSegmentResp {
	if r == nil {
		return core.CommitSegmentResp{}
	}
	out := core.CommitSegmentResp{}
	for _, br := range r.Assigned {
		out.Assigned = append(out.Assigned, batchRangeToCore(br))
	}
	return out
}

// LookupRespFromCore builds a LookupResponse from a core response.
func LookupRespFromCore(r core.FindSegmentsResp) *cpv1.LookupResponse {
	out := &cpv1.LookupResponse{End: int64(r.End)}
	for _, br := range r.Ranges {
		out.Ranges = append(out.Ranges, batchRangeFromCore(br))
	}
	return out
}

// LookupRespToCore rebuilds a core.FindSegmentsResp from a response.
func LookupRespToCore(r *cpv1.LookupResponse) core.FindSegmentsResp {
	if r == nil {
		return core.FindSegmentsResp{}
	}
	out := core.FindSegmentsResp{End: core.Offset(r.End)}
	for _, br := range r.Ranges {
		out.Ranges = append(out.Ranges, batchRangeToCore(br))
	}
	return out
}

// DescribeTopicsRespFromCore builds a DescribeTopicsResponse from a core response.
func DescribeTopicsRespFromCore(r core.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	out := &cpv1.DescribeTopicsResponse{}
	for _, t := range r.Topics {
		out.Topics = append(out.Topics, &cpv1.TopicInfo{Name: t.Name, Partitions: t.Partitions})
	}
	return out
}

// DescribeTopicsRespToCore rebuilds a core.DescribeTopicsResp from a response.
func DescribeTopicsRespToCore(r *cpv1.DescribeTopicsResponse) core.DescribeTopicsResp {
	if r == nil {
		return core.DescribeTopicsResp{}
	}
	out := core.DescribeTopicsResp{}
	for _, t := range r.Topics {
		if t == nil {
			continue
		}
		out.Topics = append(out.Topics, core.TopicInfo{Name: t.Name, Partitions: t.Partitions})
	}
	return out
}

func batchRangeFromCore(br core.BatchRange) *cpv1.BatchRange {
	return &cpv1.BatchRange{
		Tp:    &cpv1.TopicPartition{Topic: br.TP.Topic, Partition: br.TP.Partition},
		Base:  int64(br.Base),
		Count: br.Count,
		Loc:   &cpv1.SegmentRef{Key: br.Loc.Key, Off: br.Loc.Off, Len: br.Loc.Len},
	}
}

func batchRangeToCore(br *cpv1.BatchRange) core.BatchRange {
	if br == nil {
		return core.BatchRange{}
	}
	out := core.BatchRange{Base: core.Offset(br.Base), Count: br.Count}
	if br.Tp != nil {
		out.TP = core.NewTopicPartition(br.Tp.Topic, br.Tp.Partition)
	}
	if br.Loc != nil {
		out.Loc = core.SegmentRef{Key: br.Loc.Key, Off: br.Loc.Off, Len: br.Loc.Len}
	}
	return out
}
