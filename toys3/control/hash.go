package control

import "manifester/toys3/format"

// StateHash summarizes the server's entire state (FNV-1a over a fixed
// field order; reqs and recIO are slices, so the order is deterministic).
func (s *Controller) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(s.mode))
	h.u64(uint64(s.nextIO))
	h.u64(s.nextSeq)
	h.u64(uint64(s.rec))
	h.u64(uint64(s.recRemoved))
	h.u64(uint64(len(s.recIO)))
	for _, io := range s.recIO {
		h.u64(uint64(io))
	}
	h.u64(uint64(len(s.reqs)))
	for _, r := range s.reqs {
		h.u64(r.seq)
		h.u64(uint64(r.client))
		h.u64(uint64(r.kind))
		h.str(r.key)
		h.str(r.src)
		h.u64(uint64(len(r.data)))
		h.u64(uint64(format.Checksum(r.data)))
		h.u64(uint64(r.step))
		h.u64(uint64(r.io))
		h.str(r.tmp)
	}
	return h.sum
}

// hasher is 64-bit FNV-1a, written out by hand (hash/fnv is not on the
// determinism allow-list, and it is four lines).
type hasher struct{ sum uint64 }

func newHasher() hasher { return hasher{sum: 14695981039346656037} }

func (h *hasher) byte(b byte) {
	h.sum ^= uint64(b)
	h.sum *= 1099511628211
}

func (h *hasher) u64(x uint64) {
	for i := 0; i < 8; i++ {
		h.byte(byte(x >> (8 * i)))
	}
}

func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}
