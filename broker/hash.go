package broker

import (
	"math/bits"

	"manifester/core"
)

// hasher is FNV-1a; hash/fnv is off the deterministic allow-list.
type hasher uint64

func newHasher() hasher { return 14695981039346656037 }

func (h *hasher) byte(b byte)  { *h = (*h ^ hasher(b)) * 1099511628211 }
func (h *hasher) u64(v uint64) { *h = hasher(fnvU64(uint64(*h), v)) }
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

func (h *hasher) ref(r core.SegmentRef) {
	h.str(r.Key)
	h.u64(uint64(r.Off))
	h.u64(uint64(r.Len))
}

// StateHash folds everything that influences behaviour, in a fixed order.
func (b *Broker) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(b.cfg.Bug))
	h.bool(b.started)
	h.u64(uint64(b.epoch))
	h.bool(b.registered)
	h.u64(uint64(b.regReq))
	h.u64(b.regTries)
	h.u64(uint64(b.regGen))
	h.u64(uint64(b.nextReq))
	h.u64(b.nextID)
	h.u64(uint64(b.timerGen))
	h.bool(b.stopping)
	h.bool(b.tripped)
	h.bool(b.exited)
	h.u64(uint64(b.bufBytes))
	h.bool(b.pressure)
	h.u64(b.guess)

	h.u64(uint64(len(b.topics)))
	for _, t := range b.topics {
		h.str(t.Name)
		h.u64(uint64(t.Partitions))
	}
	h.u64(uint64(len(b.conns)))
	for _, c := range b.conns {
		h.u64(uint64(c.id))
		h.u64(uint64(c.rx))
		h.u64(uint64(c.fr.Buffered()))
		h.bool(c.paused)
		h.bool(c.closing)
		h.bool(c.abort)
		h.u64(uint64(len(c.q)))
		for _, s := range c.q {
			h.u64(uint64(uint16(s.h.APIKey)))
			h.u64(uint64(uint32(s.h.CorrelationID)))
			h.u64(uint64(s.pending))
			h.bool(s.ready)
			h.bool(s.silent)
			h.u64(uint64(len(s.facts)))
			h.u64(uint64(uint32(s.fetchMax)))
		}
	}
	h.u64(uint64(len(b.parked)))
	for _, p := range b.parked {
		h.u64(uint64(p.s.conn.id))
		h.u64(uint64(uint32(p.s.h.CorrelationID)))
	}
	h.u64(uint64(b.open.gen))
	h.u64(uint64(b.open.since))
	h.u64(uint64(b.open.bytes))
	h.u64(uint64(len(b.open.parts)))
	h.batches(b.open.batches)
	h.u64(uint64(len(b.segs)))
	for _, s := range b.segs {
		h.u64(s.id)
		h.u64(uint64(s.state))
		h.u64(uint64(s.req))
		h.u64(uint64(s.gen))
		h.u64(uint64(s.attempts))
		h.str(s.key)
		h.u64(uint64(len(s.data)))
		h.batches(s.batches)
	}
	h.u64(uint64(len(b.ops)))
	for _, o := range b.ops {
		h.u64(o.id)
		h.u64(uint64(o.kind))
		h.u64(uint64(o.state))
		h.u64(uint64(o.retry))
		h.u64(uint64(o.req))
		h.u64(uint64(o.gen))
		h.u64(uint64(o.attempts))
		h.u64(uint64(o.s.conn.id))
		h.u64(uint64(uint32(o.s.h.CorrelationID)))
		h.u64(uint64(len(o.names)))
		for _, n := range o.names {
			h.str(n)
		}
		h.str(o.tp.Topic)
		h.u64(uint64(uint32(o.tp.Partition)))
		h.u64(uint64(o.from))
		h.u64(uint64(uint32(o.maxBytes)))
		h.u64(uint64(len(o.ranges)))
		for _, r := range o.ranges {
			h.u64(uint64(r.Base))
			h.u64(uint64(r.Count))
			h.ref(r.Loc)
		}
		h.u64(uint64(o.next))
		h.bytes(o.out)
		h.bool(o.corrupt)
	}
	return uint64(h)
}

func (h *hasher) batches(bs []*pbatch) {
	h.u64(uint64(len(bs)))
	for _, pb := range bs {
		h.u64(uint64(pb.s.conn.id))
		h.u64(uint64(uint32(pb.s.h.CorrelationID)))
		h.str(pb.tp.Topic)
		h.u64(uint64(uint32(pb.tp.Partition)))
		h.bytes(pb.data)
		h.u64(uint64(pb.records))
		h.u64(uint64(pb.pid))
		h.u64(uint64(pb.producer))
		h.u64(uint64(uint16(pb.pepoch)))
		h.u64(uint64(uint32(pb.baseSeq)))
		h.ref(pb.loc)
		h.bool(pb.answered)
	}
}

// fnvP^k mod 2^64, for the zero high bytes of a little-endian u64.
const (
	fnvP1 = 1099511628211
	fnvP2 = (1099511628211 * 1099511628211) % (1 << 64)
	fnvP3 = (fnvP2 * 1099511628211) % (1 << 64)
	fnvP4 = (fnvP3 * 1099511628211) % (1 << 64)
	fnvP5 = (fnvP4 * 1099511628211) % (1 << 64)
	fnvP6 = (fnvP5 * 1099511628211) % (1 << 64)
	fnvP7 = (fnvP6 * 1099511628211) % (1 << 64)
	fnvP8 = (fnvP7 * 1099511628211) % (1 << 64)
)

// fnvU64 folds v as 8 little-endian bytes. A zero byte only multiplies, so the
// leading zero bytes collapse into one multiply by fnvP^k: same value, ~4x
// fewer dependent multiplies for the small integers that dominate.
func fnvU64(x, v uint64) uint64 {
	n := (bits.Len64(v) + 7) / 8
	for i := 0; i < n; i++ {
		x = (x ^ (v & 0xff)) * fnvP1
		v >>= 8
	}
	switch 8 - n {
	case 1:
		return x * fnvP1
	case 2:
		return x * fnvP2
	case 3:
		return x * fnvP3
	case 4:
		return x * fnvP4
	case 5:
		return x * fnvP5
	case 6:
		return x * fnvP6
	case 7:
		return x * fnvP7
	case 8:
		return x * fnvP8
	}
	return x
}
