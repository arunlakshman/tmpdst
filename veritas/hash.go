package veritas

import (
	"slices"

	"manifester/core"
)

// hasher is FNV-1a over a canonical byte stream. Every variable-length field
// is length-prefixed so distinct states cannot collide by concatenation.
type hasher uint64

func newHasher() hasher { return 14695981039346656037 }

func (h *hasher) byte(b byte) { *h = (*h ^ hasher(b)) * 1099511628211 }

func (h *hasher) u64(v uint64) {
	for i := range 8 {
		h.byte(byte(v >> (8 * i)))
	}
}

func (h *hasher) bytes(b []byte) {
	h.u64(uint64(len(b)))
	for _, c := range b {
		h.byte(c)
	}
}

func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}

func (h *hasher) tp(tp core.TopicPartition) {
	h.str(tp.Topic)
	h.u64(uint64(tp.Partition))
}

func (h *hasher) ranges(rs []core.BatchRange) {
	h.u64(uint64(len(rs)))
	for _, r := range rs {
		h.tp(r.TP)
		h.u64(uint64(r.Base))
		h.u64(uint64(r.Count))
		h.str(r.Loc.Key)
		h.u64(uint64(r.Loc.Off))
		h.u64(uint64(r.Loc.Len))
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

// extendPrefix folds keys[len(pre)-1, n) onto a prefix cache. pre[0] is the
// empty hasher. step receives the key index and the hash of the keys before it.
func extendPrefix(pre []hasher, n int, step func(i int, h hasher) hasher) []hasher {
	if len(pre) == 0 {
		pre = append(pre, newHasher())
	}
	for i := len(pre) - 1; i < n; i++ {
		pre = append(pre, step(i, pre[i]))
	}
	return pre
}
