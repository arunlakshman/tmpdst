package broker

import (
	"manifester/core"
)

// hasher is FNV-1a; hash/fnv is off the deterministic allow-list.
type hasher uint64

func newHasher() hasher { return 14695981039346656037 }

func (h *hasher) byte(b byte)  { *h = (*h ^ hasher(b)) * 1099511628211 }
func (h *hasher) u64(v uint64) { *h = hasher(core.FnvU64(uint64(*h), v)) }
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
	h.u64(uint64(b.nextReq))
	h.u64(b.nextID)
	h.u64(uint64(b.timerGen))
	h.bool(b.stopping)
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
		h.ref(pb.loc)
		h.bool(pb.answered)
	}
}
