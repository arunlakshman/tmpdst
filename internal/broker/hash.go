package broker

import (
	"manifester/internal/core"
)

// stateHasher is FNV-1a; hash/fnv is off the deterministic allow-list.
type stateHasher uint64

func newStateHasher() stateHasher { return 14695981039346656037 }

func (h *stateHasher) byte(b byte)  { *h = (*h ^ stateHasher(b)) * 1099511628211 }
func (h *stateHasher) u64(v uint64) { *h = stateHasher(core.Fnv64Hash(uint64(*h), v)) }
func (h *stateHasher) bool(b bool) {
	if b {
		h.u64(1)
	} else {
		h.u64(0)
	}
}
func (h *stateHasher) bytes(b []byte) {
	h.u64(uint64(len(b)))
	for _, c := range b {
		h.byte(c)
	}
}
func (h *stateHasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}

func (h *stateHasher) ref(r core.SegmentRef) {
	h.str(r.Key)
	h.u64(uint64(r.Off))
	h.u64(uint64(r.Len))
}

// StateHash folds everything that influences behaviour, in a fixed order.
func (b *Broker) StateHash() uint64 {
	h := newStateHasher()
	b.hashScalars(&h)
	h.topics(b.topics)
	h.conns(b.conns)
	h.openSeg(b.open)
	h.segs(b.segs)
	h.ops(b.ops)
	return uint64(h)
}

func (b *Broker) hashScalars(h *stateHasher) {
	h.u64(uint64(b.cfg.InjectedBug))
	h.bool(b.started)
	h.u64(uint64(b.nextReq))
	h.u64(b.nextID)
	h.u64(uint64(b.timerGen))
	h.bool(b.stopping)
	h.bool(b.exited)
	h.u64(uint64(b.bufBytes))
	h.bool(b.pressure)
	h.u64(b.guess)
}

func (h *stateHasher) topics(topics []core.TopicInfo) {
	h.u64(uint64(len(topics)))
	for _, t := range topics {
		h.str(t.Name)
		h.u64(uint64(t.Partitions))
	}
}

func (h *stateHasher) openSeg(o openSegment) {
	h.u64(uint64(o.gen))
	h.u64(uint64(o.since))
	h.u64(uint64(o.bytes))
	h.u64(uint64(len(o.parts)))
	h.batches(o.batches)
}

func (h *stateHasher) conns(conns []*connection) {
	h.u64(uint64(len(conns)))
	for _, c := range conns {
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
}

func (h *stateHasher) segs(segs []*segment) {
	h.u64(uint64(len(segs)))
	for _, s := range segs {
		h.u64(s.id)
		h.u64(uint64(s.state))
		h.u64(uint64(s.req))
		h.u64(uint64(s.gen))
		h.u64(uint64(s.attempts))
		h.str(s.key)
		h.u64(uint64(len(s.data)))
		h.batches(s.batches)
	}
}

func (h *stateHasher) ops(ops []*operation) {
	h.u64(uint64(len(ops)))
	for _, o := range ops {
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
}

func (h *stateHasher) batches(bs []*partitionBatch) {
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
