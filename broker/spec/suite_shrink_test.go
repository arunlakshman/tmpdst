package spec

import (
	"reflect"
	"testing"
)

// The shrinker must give simpler operations, never the same one again. If it
// returned the same operation, the shrink loop would think it made progress
// forever.
func TestShrinkBrokerOp(t *testing.T) {
	cases := []struct {
		name      string
		operation SuiteOp
		want      []SuiteOp
	}{
		{"append with two batches",
			SuiteOp{Kind: "append", Batch: [][]byte{[]byte("a"), []byte("b")}},
			[]SuiteOp{
				{Kind: "append"},
				{Kind: "append", Batch: [][]byte{[]byte("a")}},
			}},
		{"append with one batch",
			SuiteOp{Kind: "append", Batch: [][]byte{[]byte("a")}},
			[]SuiteOp{{Kind: "append"}}},
		{"append that is already empty", SuiteOp{Kind: "append"}, nil},
		{"read with a limit and a start",
			SuiteOp{Kind: "read", From: 5, Max: 3},
			[]SuiteOp{
				{Kind: "read", From: 5},
				{Kind: "read", From: 0, Max: 3},
			}},
		{"read that is already simple", SuiteOp{Kind: "read"}, nil},
		{"high watermark has nothing to shrink", SuiteOp{Kind: "hwm"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := minimizeSuiteOp(c.operation)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("the shrink of %#v gave %#v, but the test expected %#v", c.operation, got, c.want)
			}
			for _, g := range got {
				if reflect.DeepEqual(g, c.operation) {
					t.Fatalf("the shrink of %#v returned the same operation", c.operation)
				}
			}
		})
	}
}
