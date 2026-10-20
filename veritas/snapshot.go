package veritas

import (
	"slices"

	"manifester/core"
)

// End is one partition's next offset.
type End struct {
	TP     core.TopicPartition
	Offset core.Offset
}

// Ends lists every partition that has a run with its end offset, ordered by
// topic then partition. It exists for checkpoints of the command-log prototype
// (cp/logsm); it changes nothing.
func (c *ControlPlane) Ends() []End {
	var out []End
	for _, t := range sortedKeys(c.parts) {
		ps := make([]int32, 0, len(c.parts[t]))
		for p := range c.parts[t] { //detcheck:ok partitions are sorted before use
			ps = append(ps, p)
		}
		slices.Sort(ps)
		for _, p := range ps {
			tp := core.TopicPartition{Topic: t, Partition: p}
			out = append(out, End{TP: tp, Offset: c.End(tp)})
		}
	}
	return out
}

// SegmentKeys is every committed segment key, sorted.
func (c *ControlPlane) SegmentKeys() []string { return slices.Clone(c.segKeys) }
