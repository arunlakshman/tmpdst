package veritas

import (
	"slices"

	"manifester/internal/core"
)

// stateHasher is FNV-1a over a canonical byte stream. Every variable-length field
// is length-prefixed so distinct states cannot collide by concatenation.
type stateHasher = core.Hasher

func newStateHasher() stateHasher { return core.NewHasher() }

func hashTopicPartition(h *stateHasher, tp core.TopicPartition) {
	h.Str(tp.Topic)
	h.U64(uint64(tp.Partition))
}

func hashBatchRanges(h *stateHasher, rs []core.BatchRange) {
	h.U64(uint64(len(rs)))
	for _, r := range rs {
		hashTopicPartition(h, r.TP)
		h.U64(uint64(r.Base))
		h.U64(uint64(r.Count))
		h.Str(r.Loc.Key)
		h.U64(uint64(r.Loc.Off))
		h.U64(uint64(r.Loc.Len))
	}
}

// sortedKeys returns m's keys in order, the only way specs walk a map.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m { //detcheck:ok keys are sorted before use
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// clone copies b; the result is never nil, so empty data reads back the same
// whether it was stored as nil or empty.
func clone(b []byte) []byte { return append([]byte{}, b...) }

// nextPrefix folds keys[len(pre)-1, n) onto a prefix cache. pre[0] is the
// empty stateHasher. step receives the key index and the hash of the keys before it.
func nextPrefix(pre []stateHasher, n int, step func(i int, h stateHasher) stateHasher) []stateHasher {
	if len(pre) == 0 {
		pre = append(pre, newStateHasher())
	}
	for i := len(pre) - 1; i < n; i++ {
		pre = append(pre, step(i, pre[i]))
	}
	return pre
}
