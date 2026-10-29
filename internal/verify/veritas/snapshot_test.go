package veritas

import (
	"slices"
	"testing"
)

func TestEndsAndSegmentKeys(t *testing.T) {
	c := NewControlPlane()
	commit(c, "s2", pb("b", 1, 3), pb("a", 2, 1))
	commit(c, "s1", pb("a", 2, 4), pb("a", 0, 2))
	commit(c, "bad") // invalid: records nothing
	want := []PartitionEnd{{tp("a", 0), 2}, {tp("a", 2), 5}, {tp("b", 1), 3}}
	if got := c.Ends(); !slices.Equal(got, want) {
		t.Fatalf("Ends = %+v, want %+v", got, want)
	}
	if got := c.SegmentKeys(); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Fatalf("SegmentKeys = %v", got)
	}
}
