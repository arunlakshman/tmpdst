package speccheck

import (
	"slices"

	"manifester/internal/core"
)

// PartitionEnd is one partition's next offset.
type PartitionEnd struct {
	TP     core.TopicPartition
	Offset core.Offset
}

// Ends lists every partition that has a run with its end offset, ordered by
// topic then partition. Only a test uses it today, to check the end offsets of one
// control plane. It changes nothing.
func (c *ControlPlane) Ends() []PartitionEnd {
	var out []PartitionEnd
	for _, t := range sortedKeys(c.parts) {
		ps := make([]int32, 0, len(c.parts[t]))
		for p := range c.parts[t] { //determinismcheck:ok partitions are sorted before use
			ps = append(ps, p)
		}
		slices.Sort(ps)
		for _, p := range ps {
			tp := core.NewTopicPartition(t, p)
			out = append(out, PartitionEnd{TP: tp, Offset: c.End(tp)})
		}
	}
	return out
}

// SegmentKeys is every committed segment key, sorted.
func (c *ControlPlane) SegmentKeys() []string { return slices.Clone(c.segKeys) }
