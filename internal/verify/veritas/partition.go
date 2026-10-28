package veritas

import "manifester/internal/core"

// Partition is the spec of one Kafka partition: an append-only log of records
// with a high watermark. Offsets are dense from 0; a record's offset is its
// index. Everything appended is immediately committed, so the high watermark
// is the next offset to be assigned.
type Partition struct{ recs [][]byte }

// Record is one log entry as read back.
type Record struct {
	Offset core.Offset
	Value  []byte
}

// Append adds a batch and returns the offset of its first record (the high
// watermark before the call). An empty batch changes nothing.
func (p *Partition) Append(batch [][]byte) core.Offset {
	base := p.HighWatermark()
	for _, r := range batch {
		p.recs = append(p.recs, clone(r))
	}
	return base
}

// HighWatermark is the offset one past the last record.
func (p *Partition) HighWatermark() core.Offset { return core.Offset(len(p.recs)) }

// Read returns up to max records (max <= 0: all) starting at from. ok is
// false if from is negative or beyond the high watermark (Kafka's
// OFFSET_OUT_OF_RANGE); from == HighWatermark is ok and empty.
func (p *Partition) Read(from core.Offset, max int) ([]Record, bool) {
	if from < 0 || from > p.HighWatermark() {
		return nil, false
	}
	var recs []Record
	for o := from; o < p.HighWatermark() && (max <= 0 || len(recs) < max); o++ {
		recs = append(recs, Record{Offset: o, Value: clone(p.recs[o])})
	}
	return recs, true
}

// StateHash folds every record in order.
func (p *Partition) StateHash() uint64 {
	h := newHasher()
	h.U64(uint64(len(p.recs)))
	for _, r := range p.recs {
		h.Bytes(r)
	}
	return uint64(h)
}
