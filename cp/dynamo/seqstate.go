package dynamo

import (
	"container/list"
	"slices"
	"sync"

	"manifester/core"
	"manifester/cp/cpobs"
)

// seqState is the sequencer's memory: what it believes about the table. It is
// a cache, never an authority: every transaction still carries the CAS on the
// run item and the G# condition, so a wrong belief costs a retry, not an offset.
//
//	ends    next offset of a partition (end of its newest run), loaded lazily
//	topics  partition count of a topic (absent topic: 0)
//	segs    bounded LRU of recently committed segment keys -> request + answer
type seqState struct {
	obs *cpobs.Metrics
	mu  sync.Mutex

	ends   map[core.TopicPartition]int64
	topics map[string]int32

	cap  int
	lru  *list.List // of *segEntry, front = newest
	segs map[string]*list.Element
}

// segEntry is a committed segment: the request (to tell replay from conflict)
// and the answer. Both are immutable once stored.
type segEntry struct {
	key     string
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

func newSeqState(obs *cpobs.Metrics, capacity int) *seqState {
	return &seqState{obs: obs, ends: map[core.TopicPartition]int64{}, topics: map[string]int32{},
		cap: capacity, lru: list.New(), segs: map[string]*list.Element{}}
}

func (s *seqState) end(tp core.TopicPartition) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.ends[tp]
	return v, ok
}

func (s *seqState) setEnd(tp core.TopicPartition, v int64) {
	s.mu.Lock()
	s.ends[tp] = v
	s.mu.Unlock()
}

func (s *seqState) dropEnd(tp core.TopicPartition) {
	s.mu.Lock()
	delete(s.ends, tp)
	s.mu.Unlock()
}

func (s *seqState) topic(t string) (int32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.topics[t]
	return v, ok
}

// growTopic records a count (never lowers it).
func (s *seqState) growTopic(t string, v int32) {
	s.mu.Lock()
	if cur, ok := s.topics[t]; !ok || v > cur {
		s.topics[t] = v
	}
	s.mu.Unlock()
}

func (s *seqState) dropTopic(t string) {
	s.mu.Lock()
	delete(s.topics, t)
	s.mu.Unlock()
}

// seg is the committed segment under key, if still remembered.
func (s *seqState) seg(key string) (*segEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.segs[key]
	if ok {
		s.lru.MoveToFront(el)
		return el.Value.(*segEntry), true
	}
	return nil, false
}

// putSeg remembers a committed segment, evicting the oldest beyond capacity.
func (s *seqState) putSeg(key string, batches []core.PendingBatch, ranges []core.BatchRange) {
	if s.cap <= 0 {
		return
	}
	e := &segEntry{key: key, batches: slices.Clone(batches), ranges: ranges}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.segs[key]; ok {
		el.Value = e
		s.lru.MoveToFront(el)
		return
	}
	s.segs[key] = s.lru.PushFront(e)
	for s.lru.Len() > s.cap {
		old := s.lru.Remove(s.lru.Back()).(*segEntry)
		delete(s.segs, old.key)
	}
}

// size is the number of remembered segments (tests, metrics).
func (s *seqState) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}

// answer is the replay answer for op against a remembered commit: the stored
// ranges if the request is identical, ErrConflict if not.
func (e *segEntry) answer(op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode) {
	if !slices.Equal(e.batches, op.Batches) {
		return core.CommitSegmentResp{}, core.ErrConflict
	}
	return core.CommitSegmentResp{Assigned: slices.Clone(e.ranges)}, core.OK
}
