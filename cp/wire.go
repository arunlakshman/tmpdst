package cp

import (
	"fmt"

	"manifester/core"
)

// JSON wire structs. Err is an ErrCode spelling; empty means OK.

// Batch is one partition batch inside a segment, on the wire.
type Batch struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Records   int64  `json:"records"`
	SegKey    string `json:"seg_key"`
	SegOff    int64  `json:"seg_off"`
	SegLen    int64  `json:"seg_len"`
}

// Range is an assigned offset range for a batch, on the wire.
type Range struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Base      int64  `json:"base"`
	Count     int64  `json:"count"`
	SegKey    string `json:"seg_key"`
	SegOff    int64  `json:"seg_off"`
	SegLen    int64  `json:"seg_len"`
}

// CommitReq is the JSON body of a commit request.
type CommitReq struct {
	SegmentKey string  `json:"segment_key"`
	Batches    []Batch `json:"batches"`
}

// CommitResp is the JSON body of a commit response.
type CommitResp struct {
	Err      string  `json:"err,omitempty"`
	Assigned []Range `json:"assigned,omitempty"`
}

// LookupReq is the JSON body of a lookup request.
type LookupReq struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	From      int64  `json:"from"`
	MaxBytes  int32  `json:"max_bytes"`
}

// LookupResp is the JSON body of a lookup response.
type LookupResp struct {
	Err    string  `json:"err,omitempty"`
	Ranges []Range `json:"ranges,omitempty"`
	End    int64   `json:"end"`
}

// DescribeReq is the JSON body of a describe-topics request.
type DescribeReq struct {
	Topics []string `json:"topics,omitempty"`
}

// Topic is one topic and its partition count, on the wire.
type Topic struct {
	Name       string `json:"name"`
	Partitions int32  `json:"partitions"`
}

// DescribeResp is the JSON body of a describe-topics response.
type DescribeResp struct {
	Err    string  `json:"err,omitempty"`
	Topics []Topic `json:"topics,omitempty"`
}

// CreateTopicReq is the JSON body of a create-topic request.
type CreateTopicReq struct {
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}

// CreateTopicResp is the JSON body of a create-topic response.
type CreateTopicResp struct {
	Err string `json:"err,omitempty"`
}

// ErrString is the wire spelling of c ("" for OK).
func ErrString(c core.ErrCode) string {
	if c == core.OK {
		return ""
	}
	return c.String()
}

// ErrFromWire parses a wire Err. An unknown spelling is an error.
func ErrFromWire(s string) (core.ErrCode, error) {
	if s == "" {
		return core.OK, nil
	}
	if c, ok := ParseErrCode(s); ok {
		return c, nil
	}
	return core.ErrIO, fmt.Errorf("cp: unknown err %q", s)
}

// BatchToWire converts a pending batch to its wire form.
func BatchToWire(b core.PendingBatch) Batch {
	return Batch{b.TP.Topic, b.TP.Partition, b.Records, b.Loc.Key, b.Loc.Off, b.Loc.Len}
}

// BatchFromWire converts a wire batch back to a pending batch.
func BatchFromWire(b Batch) core.PendingBatch {
	return core.PendingBatch{
		TP: core.TopicPartition{Topic: b.Topic, Partition: b.Partition}, Records: b.Records,
		Loc: core.SegmentRef{Key: b.SegKey, Off: b.SegOff, Len: b.SegLen},
	}
}

// RangeToWire converts an assigned range to its wire form.
func RangeToWire(r core.BatchRange) Range {
	return Range{r.TP.Topic, r.TP.Partition, int64(r.Base), r.Count, r.Loc.Key, r.Loc.Off, r.Loc.Len}
}

// RangeFromWire converts a wire range back to a batch range.
func RangeFromWire(r Range) core.BatchRange {
	return core.BatchRange{
		TP: core.TopicPartition{Topic: r.Topic, Partition: r.Partition}, Base: core.Offset(r.Base), Count: r.Count,
		Loc: core.SegmentRef{Key: r.SegKey, Off: r.SegOff, Len: r.SegLen},
	}
}
