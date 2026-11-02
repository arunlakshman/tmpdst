package speccheck

import (
	"maps"
	"slices"

	"mds/internal/core"
)

// PartitionEnd is one partition's next offset.
type PartitionEnd struct {
	TopicPartition core.TopicPartition
	Offset         core.Offset
}

// Ends lists every partition that has a run with its end offset, ordered by
// topic then partition. Only a test uses it today, to check the end offsets of one
// control plane. It changes nothing.
func (c *ControlPlane) Ends() []PartitionEnd {
	var out []PartitionEnd
	for _, t := range sortedKeys(c.parts) {
		ps := slices.Sorted(maps.Keys(c.parts[t]))
		for _, p := range ps {
			topicPartition := core.NewTopicPartition(t, p)
			out = append(out, PartitionEnd{TopicPartition: topicPartition, Offset: c.End(topicPartition)})
		}
	}
	return out
}

// SegmentKeys is every committed segment key, sorted.
func (c *ControlPlane) SegmentKeys() []string { return slices.Clone(c.segKeys) }
