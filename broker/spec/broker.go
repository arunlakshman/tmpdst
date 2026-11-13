// Package spec holds the spec of the broker: what a correct broker answers for
// one Kafka partition. It is a small program with no faults that runs one
// operation at a time and keeps plain data. Tests and the simulator compare
// the real broker with it. It shares types with the broker (contract.Offset)
// and never imports the broker. The spec of the control plane is
// controlplane/spec, and the spec of the object store is shared/objectstore.
package spec

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/trace"
)

// Broker is the spec of the broker for one Kafka partition: one part of the
// broker's state, not a service. It is an append-only log of records with a
// high watermark. Offsets are dense from 0; a record's offset is its
// index. Everything appended is immediately committed, so the high watermark
// is the next offset to be assigned.
type Broker struct{ recs [][]byte }

// StoredRecord is one log entry as read back.
type StoredRecord struct {
	Offset contract.Offset
	Value  []byte
}

// Append adds a batch and returns the offset of its first record (the high
// watermark before the call). An empty batch changes nothing.
func (p *Broker) Append(batch [][]byte) contract.Offset {
	base := p.HighWatermark()
	for _, r := range batch {
		p.recs = append(p.recs, clone(r))
	}
	return base
}

// HighWatermark is the offset one past the last record.
func (p *Broker) HighWatermark() contract.Offset { return contract.Offset(len(p.recs)) }

// Read returns up to max records (max <= 0: all) starting at from. ok is
// false if from is negative or beyond the high watermark (Kafka's
// OFFSET_OUT_OF_RANGE); from == HighWatermark is ok and empty.
func (p *Broker) Read(from contract.Offset, max int) ([]StoredRecord, bool) {
	if from < 0 || from > p.HighWatermark() {
		return nil, false
	}
	var recs []StoredRecord
	for o := from; o < p.HighWatermark() && (max <= 0 || len(recs) < max); o++ {
		recs = append(recs, StoredRecord{Offset: o, Value: clone(p.recs[o])})
	}
	return recs, true
}

// StateHash folds every record in order.
func (p *Broker) StateHash() uint64 {
	h := trace.New()
	h.Uint64(uint64(len(p.recs)))
	for _, r := range p.recs {
		h.Bytes(r)
	}
	return uint64(h)
}

// clone copies b; the result is never nil, so empty data reads back the same
// whether it was stored as nil or empty.
func clone(b []byte) []byte { return append([]byte{}, b...) }
