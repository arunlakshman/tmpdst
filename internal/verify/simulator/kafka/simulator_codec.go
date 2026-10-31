package kafka

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/wire/kafkawire"
)

// The broker never decompresses (decision 45), so the simulated clients need no
// real codec: compressValue is a reversible byte scramble that stands in for gzip,
// snappy, lz4 and zstd. A batch carries the real codec id in its attributes
// and an compressValue'd records section the broker must treat as opaque bytes; only the
// simulated consumer and the oracle undo it.
func compressValue(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[len(b)-1-i] = c ^ 0xa5
	}
	return out
}

// decompressValue is compressValue's inverse.
func decompressValue(b []byte) []byte { return compressValue(b) } // reverse and xor are involutions

// buildBatch builds a batch of recs with the given codec id (0 = none).
func buildBatch(pp kafkawire.BatchParams, recs []kmsg.Record, codec int16) ([]byte, error) {
	plain, err := kafkawire.BuildBatch(pp, recs)
	if err != nil || codec == kafkawire.CodecNone {
		return plain, err
	}
	bt, err := kafkawire.ParseBatch(plain)
	if err != nil {
		return nil, err
	}
	pp.Attributes |= codec
	return kafkawire.BuildOpaqueBatch(pp, bt.NumRecords, compressValue(bt.Payload()))
}

// batchRecords decodes a parsed batch's records, undoing compressValue for a compressed
// batch (what a real client does with its own codec).
func batchRecords(bt kafkawire.Batch) ([]kmsg.Record, error) {
	if bt.Codec() == kafkawire.CodecNone {
		return bt.Records()
	}
	return kafkawire.DecodeRecords(decompressValue(bt.Payload()), bt.NumRecords)
}
