package spec

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/trace"
)

// StateHash is a hash of the committed segments (sorted by key, with their
// ranges) and the topics (sorted by name). Two models with the same segments
// and topics give the same hash. Groups and offsets are not in it. The
// simulator compares it with the hash of the simulated control plane.
//
// It keeps a prefix cache, so a commit near the end of the key order costs
// little: the hash restarts from the last prefix that did not change.
func (c *ControlPlane) StateHash() uint64 {
	c.segPre = nextPrefix(c.segPre, len(c.segKeys), func(i int, h trace.Hash) trace.Hash {
		h.String(c.segKeys[i])
		hashRanges(&h, c.segments[c.segKeys[i]].ranges)
		return h
	})
	return c.topicsHash(c.segPre[len(c.segKeys)])
}

// fullStateHash is StateHash without the prefix cache (for tests).
func (c *ControlPlane) fullStateHash() uint64 {
	h := trace.New()
	for _, k := range sortedKeys(c.segments) {
		h.String(k)
		hashRanges(&h, c.segments[k].ranges)
	}
	return c.topicsHash(h)
}

// topicsHash folds the topics into h.
func (c *ControlPlane) topicsHash(h trace.Hash) uint64 {
	for _, t := range sortedKeys(c.topics) {
		h.String(t)
		h.Uint64(uint64(c.topics[t]))
	}
	return uint64(h)
}

func hashRanges(h *trace.Hash, ranges []contract.BatchRange) {
	h.Uint64(uint64(len(ranges)))
	for _, r := range ranges {
		h.String(r.TopicPartition.Topic)
		h.Uint64(uint64(r.TopicPartition.Partition))
		h.Uint64(uint64(r.Base))
		h.Uint64(uint64(r.Count))
		h.String(r.Loc.Key)
		h.Uint64(uint64(r.Loc.Off))
		h.Uint64(uint64(r.Loc.Len))
	}
}

// addSegmentKey puts a new key in the sorted list and drops the cached
// prefixes that come after it.
func (c *ControlPlane) addSegmentKey(key string) {
	i, _ := slices.BinarySearch(c.segKeys, key)
	c.segKeys = slices.Insert(c.segKeys, i, key)
	if len(c.segPre) > i+1 {
		c.segPre = c.segPre[:i+1]
	}
}

// nextPrefix folds keys[len(pre)-1, n) onto a prefix cache. pre[0] is the
// empty hash. step receives the key index and the hash of the keys before it.
func nextPrefix(pre []trace.Hash, n int, step func(i int, h trace.Hash) trace.Hash) []trace.Hash {
	if len(pre) == 0 {
		pre = append(pre, trace.New())
	}
	for i := len(pre) - 1; i < n; i++ {
		pre = append(pre, step(i, pre[i]))
	}
	return pre
}
