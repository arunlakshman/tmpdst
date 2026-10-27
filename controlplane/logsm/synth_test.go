package logsm

import (
	"encoding/hex"
	"fmt"

	"manifester/core"
	"manifester/veritas/veritastest"
)

// synth builds commands shaped like a 1 GiB/s workload's segments: a random
// 128-bit key (decision 43) and `parts` distinct partitions out of a universe
// of topics x perTopic.
type synth struct {
	r             *veritastest.Rand
	topics, per   int
	parts         int
	n             int
	segmentLength int64
}

func newSynth(seed uint64, universe, parts int) *synth {
	per := min(universe, 100)
	return &synth{r: veritastest.NewRand(seed), topics: universe / per, per: per, parts: parts, segmentLength: 16 << 20}
}

func (s *synth) key() string {
	var b [16]byte
	for i := range b {
		b[i] = byte(s.r.Uint64())
	}
	return "seg/" + hex.EncodeToString(b[:])
}

// command is one segment commit with s.parts distinct partitions.
func (s *synth) command() Entry {
	key := s.key()
	seen := map[int]bool{}
	c := core.CommitSegment{SegmentKey: key}
	var off int64
	for len(c.Batches) < s.parts {
		p := s.r.Intn(s.topics * s.per)
		if seen[p] {
			continue
		}
		seen[p] = true
		recs := int64(1 + s.r.Intn(40))
		l := recs * int64(500+s.r.Intn(1500))
		c.Batches = append(c.Batches, core.PendingBatch{
			TP:      core.TP(fmt.Sprintf("topic-%03d", p/s.per), int32(p%s.per)),
			Records: recs, Loc: core.SegmentRef{Key: key, Off: off, Len: l},
		})
		off += l
	}
	s.n++
	return Entry{ID: "w/1", Kind: KindCommit, Commit: c}
}
