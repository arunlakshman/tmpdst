package generator

import (
	"fmt"
	"testing"

	"manifester/internal/core"
)

// TestDescribeEveryOperationKind checks the text Describe gives for each kind
// of control-plane operation, and for the empty operation.
func TestDescribeEveryOperationKind(t *testing.T) {
	topic := core.NewTopicPartition("t1", 2)
	cases := []struct {
		name string
		op   ControlPlaneOp
		want string
	}{
		{
			name: "direct topic create",
			op:   NewCreateTopic("orders", 3),
			want: `create topic "orders" with 3 partitions`,
		},
		{
			name: "commit",
			op:   NewCall(core.ControlPlaneCall{Req: 5, Op: core.CommitSegment{SegmentKey: "s", Batches: make([]core.PendingBatch, 2)}}),
			want: `request 5: commit segment "s" with 2 batches`,
		},
		{
			name: "lookup",
			op:   NewCall(core.ControlPlaneCall{Req: 4, Op: core.FindSegments{TP: topic, From: 7, MaxBytes: 60}}),
			want: fmt.Sprintf("request 4: look up %v from offset 7 with max bytes 60", topic),
		},
		{
			name: "describe topics",
			op:   NewCall(core.ControlPlaneCall{Req: 6, Op: core.DescribeTopics{Topics: []string{"a", "b"}}}),
			want: `request 6: describe topics ["a" "b"]`,
		},
		{
			name: "exclusive create",
			op:   NewCall(core.ControlPlaneCall{Req: 7, Op: core.CreateTopic{Name: "n", Partitions: 3, FailIfExists: true}}),
			want: `request 7: create topic "n" with 3 partitions, failing if it exists`,
		},
		{
			name: "group operation falls back to its type",
			op:   NewCall(core.ControlPlaneCall{Req: 8, Op: core.ListGroups{}}),
			want: "request 8: core.ListGroups",
		},
		{
			name: "empty operation",
			op:   ControlPlaneOp{},
			want: "empty operation",
		},
	}
	for _, tc := range cases {
		if got := tc.op.Describe(); got != tc.want {
			t.Errorf("%s: Describe gave %q, want %q.", tc.name, got, tc.want)
		}
	}
}
