package logengine

import (
	"slices"
	"strings"

	"manifester/core"
)

// state is one replica's applied state: the result of applying the log in slot
// order. It is an independent implementation of the control plane semantics
// (decision 21): it shares no code with veritas, cp/applier or cp/logcp. All
// collections are sorted slices, never maps.
type state struct {
	parts  []part           // sorted by partition
	topics []core.TopicInfo // sorted by name
	keys   []keyRec         // sorted by key
	digest uint64           // chain over applied slots and forgets
}

type part struct {
	tp   core.TopicPartition
	runs []core.BatchRange // offset order, contiguous
}

// keyRec is a committed segment key inside the idempotency window.
type keyRec struct {
	key     string
	first   int // slot of the first commit
	writer  int // rank that wrote that slot
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

// result is the outcome of one command.
type result struct {
	ranges []core.BatchRange
	err    core.ErrCode
	replay bool
	rec    *keyRec // set on a replay
}

func tpCmp(a, b core.TopicPartition) int {
	if c := strings.Compare(a.Topic, b.Topic); c != 0 {
		return c
	}
	return int(a.Partition) - int(b.Partition)
}

func (s *state) find(tp core.TopicPartition) (int, bool) {
	return slices.BinarySearchFunc(s.parts, tp, func(p part, t core.TopicPartition) int { return tpCmp(p.tp, t) })
}

func (s *state) end(tp core.TopicPartition) core.Offset {
	i, ok := s.find(tp)
	if !ok || len(s.parts[i].runs) == 0 {
		return 0
	}
	r := s.parts[i].runs[len(s.parts[i].runs)-1]
	return r.Base + core.Offset(r.Count)
}

func (s *state) addRun(r core.BatchRange) {
	i, ok := s.find(r.TP)
	if !ok {
		s.parts = slices.Insert(s.parts, i, part{tp: r.TP})
	}
	s.parts[i].runs = append(s.parts[i].runs, r)
	s.grow(r.TP.Topic, r.TP.Partition+1)
}

func (s *state) grow(topic string, n int32) {
	i, ok := slices.BinarySearchFunc(s.topics, topic, func(t core.TopicInfo, k string) int { return strings.Compare(t.Name, k) })
	if !ok {
		s.topics = slices.Insert(s.topics, i, core.TopicInfo{Name: topic})
	}
	s.topics[i].Partitions = max(s.topics[i].Partitions, n)
}

func (s *state) keyAt(key string) (int, bool) {
	return slices.BinarySearchFunc(s.keys, key, func(k keyRec, key string) int { return strings.Compare(k.key, key) })
}

func (s *state) clone() *state {
	c := &state{digest: s.digest, topics: slices.Clone(s.topics), keys: slices.Clone(s.keys)}
	c.parts = make([]part, len(s.parts))
	for i, p := range s.parts {
		c.parts[i] = part{tp: p.tp, runs: slices.Clone(p.runs)}
	}
	return c
}

func validBatch(b core.PendingBatch) bool {
	return b.Records > 0 && b.TP.Topic != "" && b.TP.Partition >= 0
}

// validCommit is the shape check shared by admission and apply.
func validCommit(c core.CommitSegment) bool {
	if c.SegmentKey == "" || len(c.Batches) == 0 {
		return false
	}
	return !slices.ContainsFunc(c.Batches, func(b core.PendingBatch) bool { return !validBatch(b) })
}

// cmd is one commit command inside a log entry. bases is set only by the
// BugWriterOffsets mutant (offsets chosen by the writer, not by apply).
type cmd struct {
	key     string
	batches []core.PendingBatch
	bases   []core.Offset
}

// applyCmd applies one command at slot by the entry of writer.
func (s *state) applyCmd(slot, writer int, c cmd) result {
	if i, ok := s.keyAt(c.key); ok {
		rec := &s.keys[i]
		if !slices.Equal(rec.batches, c.batches) {
			return result{err: core.ErrConflict}
		}
		return result{ranges: slices.Clone(rec.ranges), replay: true, rec: rec}
	}
	if !validCommit(core.CommitSegment{SegmentKey: c.key, Batches: c.batches}) {
		return result{err: core.ErrInvalid}
	}
	rec := keyRec{key: c.key, first: slot, writer: writer, batches: slices.Clone(c.batches)}
	for i, b := range c.batches {
		base := s.end(b.TP)
		if len(c.bases) == len(c.batches) {
			base = c.bases[i]
		}
		r := core.BatchRange{TP: b.TP, Base: base, Count: b.Records, Loc: b.Loc}
		s.addRun(r)
		rec.ranges = append(rec.ranges, r)
	}
	i, _ := s.keyAt(c.key)
	s.keys = slices.Insert(s.keys, i, rec)
	return result{ranges: slices.Clone(rec.ranges)}
}

// forget drops the keys whose first slot is at least window slots before slot.
func (s *state) forget(slot, window int) int {
	n := len(s.keys)
	s.keys = slices.DeleteFunc(s.keys, func(k keyRec) bool { return k.first+window <= slot })
	return n - len(s.keys)
}

// mix folds v into the chain.
func (s *state) mix(v uint64) { s.digest = mixU64(s.digest, v) }

func mixU64(h, v uint64) uint64 {
	if h == 0 {
		h = 14695981039346656037
	}
	for i := 0; i < 8; i++ {
		h = (h ^ (v >> (8 * i) & 0xff)) * 1099511628211
	}
	return h
}

func (s *state) mixStr(x string) {
	s.mix(uint64(len(x)))
	for i := 0; i < len(x); i++ {
		s.mix(uint64(x[i]))
	}
}

// fold records an applied command in the chain.
func (s *state) fold(slot int, key string, r result) {
	s.mix(uint64(slot))
	s.mixStr(key)
	s.mix(uint64(r.err))
	for _, g := range r.ranges {
		s.mixStr(g.TP.Topic)
		s.mix(uint64(g.TP.Partition))
		s.mix(uint64(g.Base))
		s.mix(uint64(g.Count))
	}
}

// foldSlot records an applied slot (any kind) and the forgets it caused.
func (s *state) foldSlot(slot, cmds, forgot int) {
	s.mix(uint64(slot))
	s.mix(uint64(cmds))
	s.mix(uint64(forgot))
}

func (s *state) lookup(op core.Lookup) core.LookupResp {
	resp := core.LookupResp{End: s.end(op.TP)}
	i, ok := s.find(op.TP)
	if !ok {
		return resp
	}
	var bytes int64
	for _, r := range s.parts[i].runs {
		if r.Base+core.Offset(r.Count) <= op.From {
			continue
		}
		resp.Ranges = append(resp.Ranges, r)
		bytes += r.Loc.Len
		if op.MaxBytes > 0 && bytes >= int64(op.MaxBytes) {
			break
		}
	}
	return resp
}

func (s *state) describe(op core.DescribeTopics) core.DescribeTopicsResp {
	var resp core.DescribeTopicsResp
	for _, t := range s.topics {
		if len(op.Topics) == 0 || slices.Contains(op.Topics, t.Name) {
			resp.Topics = append(resp.Topics, t)
		}
	}
	return resp
}
