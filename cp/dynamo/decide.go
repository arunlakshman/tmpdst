package dynamo

import (
	"slices"

	"manifester/core"
)

// The decision for one CommitSegment, as a pure function of a snapshot read
// from the table. It restates the spec's step 4 and idempotence rules
// independently; the lockstep conformance test is what ties it to them.

const (
	firstProducer = 1000
	dupWindow     = 5
)

type skey struct {
	tp core.TopicPartition
	p  core.ProducerID
}

// win is one entry of a producer's last-5 window (stored as JSON).
type win struct {
	Seq     int64  `json:"s"`
	Records int64  `json:"n"`
	Base    int64  `json:"b"`
	Key     string `json:"k"`
	Off     int64  `json:"o"`
	Len     int64  `json:"l"`
}

func (w win) rng(tp core.TopicPartition) core.BatchRange {
	return core.BatchRange{TP: tp, Base: core.Offset(w.Base), Count: w.Records, Loc: core.SegmentRef{Key: w.Key, Off: w.Off, Len: w.Len}}
}

type pstate struct {
	PEpoch int16
	Next   int64
	Window []win // oldest first
}

func (p *pstate) clone() *pstate {
	c := *p
	c.Window = slices.Clone(p.Window)
	return &c
}

// snapshot is what the decision may read. Absent state is a missing key.
type snapshot struct {
	hwm    map[core.TopicPartition]int64
	state  map[skey]*pstate
	topics map[string]int32
	ver    map[skey]int64 // version attribute of each state item present
}

type decision struct {
	ranges []core.BatchRange
	hwm    map[core.TopicPartition]int64 // partitions whose HWM changed
	state  map[skey]*pstate              // producer states that changed
	topics map[string]int32              // topics that must grow
}

func decide(batches []core.PendingBatch, snap *snapshot) *decision {
	d := &decision{
		hwm:    map[core.TopicPartition]int64{},
		state:  map[skey]*pstate{},
		topics: map[string]int32{},
	}
	for _, b := range batches {
		d.ranges = append(d.ranges, d.one(b, snap))
	}
	return d
}

func (d *decision) one(b core.PendingBatch, snap *snapshot) core.BatchRange {
	assign := func() core.BatchRange {
		base, ok := d.hwm[b.TP]
		if !ok {
			base = snap.hwm[b.TP]
		}
		d.hwm[b.TP] = base + b.Records
		cur, ok := d.topics[b.TP.Topic]
		if !ok {
			cur = snap.topics[b.TP.Topic]
		}
		if want := b.TP.Partition + 1; want > cur {
			d.topics[b.TP.Topic] = want
		}
		return core.BatchRange{TP: b.TP, Base: core.Offset(base), Count: b.Records, Loc: b.Loc}
	}
	if b.Producer == core.NoProducer {
		return assign()
	}
	rej := func(e core.ErrCode) core.BatchRange {
		return core.BatchRange{TP: b.TP, Base: -1, Count: b.Records, Loc: b.Loc, Err: e}
	}
	k := skey{b.TP, b.Producer}
	st := d.state[k]
	if st == nil {
		if s := snap.state[k]; s != nil {
			st = s.clone()
		}
	}
	seq := int64(b.BaseSeq)
	entry := func(r core.BatchRange) win {
		return win{Seq: seq, Records: b.Records, Base: int64(r.Base), Key: r.Loc.Key, Off: r.Loc.Off, Len: r.Loc.Len}
	}
	switch {
	case st == nil || b.PEpoch > st.PEpoch:
		if seq != 0 {
			return rej(core.ErrOutOfOrder)
		}
		r := assign()
		d.state[k] = &pstate{PEpoch: b.PEpoch, Next: b.Records, Window: []win{entry(r)}}
		return r
	case b.PEpoch < st.PEpoch:
		return rej(core.ErrFenced)
	}
	for _, w := range st.Window {
		if w.Seq == seq {
			if w.Records != b.Records {
				return rej(core.ErrOutOfOrder)
			}
			r := w.rng(b.TP)
			r.Dup = true
			return r
		}
	}
	if seq != st.Next {
		return rej(core.ErrOutOfOrder)
	}
	r := assign()
	st.Window = append(st.Window, entry(r))
	if len(st.Window) > dupWindow {
		st.Window = st.Window[1:]
	}
	st.Next += b.Records
	d.state[k] = st
	return r
}

// versions lists every producer state the batches read, with its version
// (-1: absent), for pinning in the transaction.
func (s *snapshot) versions(batches []core.PendingBatch) map[skey]int64 {
	out := map[skey]int64{}
	for _, b := range batches {
		if b.Producer == core.NoProducer {
			continue
		}
		k := skey{b.TP, b.Producer}
		if v, ok := s.ver[k]; ok {
			out[k] = v
		} else {
			out[k] = -1
		}
	}
	return out
}
