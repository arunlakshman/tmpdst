package cp

import (
	"encoding/hex"
	"fmt"

	"manifester/core"
)

// JSON wire structs. Err is an ErrCode spelling; empty means OK.

type NodeRef struct {
	Node string `json:"node"`
	Inc  string `json:"inc"` // hex
}

type Batch struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Records   int64  `json:"records"`
	SegKey    string `json:"seg_key"`
	SegOff    int64  `json:"seg_off"`
	SegLen    int64  `json:"seg_len"`
	Producer  int64  `json:"producer,omitempty"`
	PEpoch    int16  `json:"pepoch,omitempty"`
	BaseSeq   int32  `json:"base_seq,omitempty"`
}

type Range struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Base      int64  `json:"base"`
	Count     int64  `json:"count"`
	SegKey    string `json:"seg_key"`
	SegOff    int64  `json:"seg_off"`
	SegLen    int64  `json:"seg_len"`
	Err       string `json:"err,omitempty"`
	Dup       bool   `json:"dup,omitempty"`
}

type RegisterReq struct {
	Self NodeRef `json:"self"`
}
type RegisterResp struct {
	Err   string `json:"err,omitempty"`
	Epoch uint64 `json:"epoch"`
}

type InitProducerResp struct {
	Err      string `json:"err,omitempty"`
	Producer int64  `json:"producer"`
	PEpoch   int16  `json:"pepoch"`
}

type CommitReq struct {
	SegmentKey string  `json:"segment_key"`
	Owner      NodeRef `json:"owner"`
	Epoch      uint64  `json:"epoch"`
	Batches    []Batch `json:"batches"`
}
type CommitResp struct {
	Err      string  `json:"err,omitempty"`
	Assigned []Range `json:"assigned,omitempty"`
}

type LookupReq struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	From      int64  `json:"from"`
	MaxBytes  int32  `json:"max_bytes"`
}
type LookupResp struct {
	Err           string  `json:"err,omitempty"`
	Ranges        []Range `json:"ranges,omitempty"`
	HighWatermark int64   `json:"high_watermark"`
}

type DescribeReq struct {
	Topics []string `json:"topics,omitempty"`
}
type Topic struct {
	Name       string `json:"name"`
	Partitions int32  `json:"partitions"`
}
type DescribeResp struct {
	Err    string  `json:"err,omitempty"`
	Topics []Topic `json:"topics,omitempty"`
}

type CreateTopicReq struct {
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}
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

func NodeToWire(n core.NodeRef) NodeRef {
	return NodeRef{Node: string(n.Node), Inc: hex.EncodeToString(n.Inc[:])}
}

func NodeFromWire(n NodeRef) (core.NodeRef, error) {
	out := core.NodeRef{Node: core.NodeID(n.Node)}
	b, err := hex.DecodeString(n.Inc)
	if err != nil || len(b) != len(out.Inc) {
		return out, fmt.Errorf("cp: bad incarnation %q", n.Inc)
	}
	copy(out.Inc[:], b)
	return out, nil
}

func BatchToWire(b core.PendingBatch) Batch {
	return Batch{b.TP.Topic, b.TP.Partition, b.Records, b.Loc.Key, b.Loc.Off, b.Loc.Len, int64(b.Producer), b.PEpoch, b.BaseSeq}
}

func BatchFromWire(b Batch) core.PendingBatch {
	return core.PendingBatch{
		TP: core.TopicPartition{Topic: b.Topic, Partition: b.Partition}, Records: b.Records,
		Loc:      core.SegmentRef{Key: b.SegKey, Off: b.SegOff, Len: b.SegLen},
		Producer: core.ProducerID(b.Producer), PEpoch: b.PEpoch, BaseSeq: b.BaseSeq,
	}
}

func RangeToWire(r core.BatchRange) Range {
	return Range{r.TP.Topic, r.TP.Partition, int64(r.Base), r.Count, r.Loc.Key, r.Loc.Off, r.Loc.Len, ErrString(r.Err), r.Dup}
}

func RangeFromWire(r Range) (core.BatchRange, error) {
	e, err := ErrFromWire(r.Err)
	return core.BatchRange{
		TP: core.TopicPartition{Topic: r.Topic, Partition: r.Partition}, Base: core.Offset(r.Base), Count: r.Count,
		Loc: core.SegmentRef{Key: r.SegKey, Off: r.SegOff, Len: r.SegLen}, Err: e, Dup: r.Dup,
	}, err
}
