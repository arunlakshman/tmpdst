package kafka

import "manifester/core"

// hasher is FNV-1a; hash/fnv is off the deterministic allow-list.
type hasher uint64

func newHasher() hasher { return 14695981039346656037 }

func (h *hasher) byte(b byte)  { *h = (*h ^ hasher(b)) * 1099511628211 }
func (h *hasher) u64(v uint64) { *h = hasher(core.FnvU64(uint64(*h), v)) }
func (h *hasher) i64(v int64)  { h.u64(uint64(v)) }
func (h *hasher) bool(b bool) {
	if b {
		h.u64(1)
	} else {
		h.u64(0)
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
