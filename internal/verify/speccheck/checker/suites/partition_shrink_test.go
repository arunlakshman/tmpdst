package suites

import (
	"reflect"
	"testing"
)

// The shrinker must give simpler operations, never the same one again. If it
// returned the same operation, the shrink loop would think it made progress
// forever.
func TestShrinkPartitionOp(t *testing.T) {
	cases := []struct {
		name string
		op   PartitionOp
		want []PartitionOp
	}{
		{"append with two batches",
			PartitionOp{Kind: "append", Batch: [][]byte{[]byte("a"), []byte("b")}},
			[]PartitionOp{
				{Kind: "append"},
				{Kind: "append", Batch: [][]byte{[]byte("a")}},
			}},
		{"append with one batch",
			PartitionOp{Kind: "append", Batch: [][]byte{[]byte("a")}},
			[]PartitionOp{{Kind: "append"}}},
		{"append that is already empty", PartitionOp{Kind: "append"}, nil},
		{"read with a limit and a start",
			PartitionOp{Kind: "read", From: 5, Max: 3},
			[]PartitionOp{
				{Kind: "read", From: 5},
				{Kind: "read", From: 0, Max: 3},
			}},
		{"read that is already simple", PartitionOp{Kind: "read"}, nil},
		{"high watermark has nothing to shrink", PartitionOp{Kind: "hwm"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := minimizePartitionOp(c.op)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("the shrink of %#v gave %#v, but the test expected %#v", c.op, got, c.want)
			}
			for _, g := range got {
				if reflect.DeepEqual(g, c.op) {
					t.Fatalf("the shrink of %#v returned the same operation", c.op)
				}
			}
		})
	}
}
