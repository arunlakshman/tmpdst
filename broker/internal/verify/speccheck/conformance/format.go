package conformance

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/shared/objectstore"
)

// The Format functions turn one operation into a Go expression, for the
// failure report of a lockstep run. A failing run prints its shrunk operations
// with them, so you can paste the list into a regression test.
//
// The text has no request id. The run numbers the calls itself, from 1, in
// order, so a pasted list must go through the same run code.

// FormatObjectOp returns op as a Go expression.
func FormatObjectOp(operation objectstore.Call) string {
	switch operation := operation.(type) {
	case objectstore.Put:
		return fmt.Sprintf("objectstore.Put{Key: %q%s}", operation.Key, formatDataField(operation.Data))
	case objectstore.Get:
		return fmt.Sprintf("objectstore.Get{Ref: %s}", formatRef(operation.Ref))
	case objectstore.Delete:
		return fmt.Sprintf("objectstore.Delete{Key: %q}", operation.Key)
	case objectstore.List:
		return fmt.Sprintf("objectstore.List{Prefix: %q}", operation.Prefix)
	}
	return fmt.Sprintf("%#v", operation)
}

// formatDataField writes the Data field with a leading comma, or nothing when
// Data is nil. It keeps nil and empty data apart.
func formatDataField(data []byte) string {
	switch {
	case data == nil:
		return ""
	case len(data) == 0:
		return ", Data: []byte{}"
	}
	return fmt.Sprintf(", Data: []byte(%q)", string(data))
}

func formatRef(ref objectstore.SegmentRef) string {
	return fmt.Sprintf("objectstore.SegmentRef{Key: %q, Off: %d, Len: %d}", ref.Key, ref.Off, ref.Len)
}

// FormatControlPlaneOp returns op as a Go expression. The expression needs the
// packages core, contract and conformance.
func FormatControlPlaneOp(operation ControlPlaneTestStep) string {
	switch {
	case operation.CreateTopic != nil:
		return fmt.Sprintf("conformance.NewCreateTopic(%q, %d)", operation.CreateTopic.Name, operation.CreateTopic.Partitions)
	case operation.Call == nil:
		return "conformance.ControlPlaneTestStep{}"
	}
	return fmt.Sprintf("conformance.NewCall(core.ControlPlaneCall{Op: %s})", formatCPOp(operation.Call.Op))
}

func formatCPOp(operation contract.Operation) string {
	switch operation := operation.(type) {
	case contract.CreateTopic:
		return fmt.Sprintf("contract.CreateTopic{Name: %q, Partitions: %d, FailIfExists: %t}", operation.Name, operation.Partitions, operation.FailIfExists)
	case contract.CommitSegment:
		return fmt.Sprintf("contract.CommitSegment{SegmentKey: %q%s}", operation.SegmentKey, formatBatchesField(operation.Batches))
	case contract.FindSegments:
		return fmt.Sprintf("contract.FindSegments{TopicPartition: contract.NewTopicPartition(%q, %d), From: %d, MaxBytes: %d}", operation.TopicPartition.Topic, operation.TopicPartition.Partition, operation.From, operation.MaxBytes)
	case contract.DescribeTopics:
		if operation.Topics == nil {
			return "contract.DescribeTopics{}"
		}
		return fmt.Sprintf("contract.DescribeTopics{Topics: []string{%s}}", quoteEach(operation.Topics))
	}
	return fmt.Sprintf("%#v", operation)
}

func formatBatchesField(batches []contract.PendingBatch) string {
	if batches == nil {
		return ""
	}
	parts := make([]string, len(batches))
	for i, b := range batches {
		parts[i] = fmt.Sprintf("{TopicPartition: contract.NewTopicPartition(%q, %d), Records: %d, Loc: %s}", b.TopicPartition.Topic, b.TopicPartition.Partition, b.Records, formatRef(b.Loc))
	}
	return ", Batches: []contract.PendingBatch{" + strings.Join(parts, ", ") + "}"
}

func quoteEach(items []string) string {
	parts := make([]string, len(items))
	for i, s := range items {
		parts[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(parts, ", ")
}
