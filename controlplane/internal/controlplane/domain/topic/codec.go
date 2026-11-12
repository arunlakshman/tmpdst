package topic

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The state records of the topic context, written by hand. These bytes are
// the stored state: the checkpoint and the state hash read them, so a change
// here moves the golden hashes.

const (
	checksumLength = 16
	runFormat      = 1
)

var errCorrupt = errors.New("topics: corrupt record")

func writeVarint(n int64) []byte { return binary.AppendUvarint(nil, uint64(n)) }

// readVarint decodes a stored non-negative number (0 when absent).
func readVarint(b []byte) int64 {
	v, _ := binary.Uvarint(b)
	return int64(v)
}

// checksum identifies a request's batches (not its key): a replay is the same
// request iff the digests are equal.
func checksum(bs []contract.PendingBatch) [checksumLength]byte {
	sum := sha256.Sum256(checksumBytes(bs))
	return [checksumLength]byte(sum[:checksumLength])
}

func checksumBytes(bs []contract.PendingBatch) []byte {
	n := binary.AppendUvarint(nil, uint64(len(bs)))
	for _, b := range bs {
		n = binary.AppendUvarint(n, uint64(len(b.TopicPartition.Topic)))
		n = append(n, b.TopicPartition.Topic...)
		n = binary.AppendVarint(n, int64(b.TopicPartition.Partition))
		n = binary.AppendVarint(n, b.Records)
		n = binary.AppendUvarint(n, uint64(len(b.Loc.Key)))
		n = append(n, b.Loc.Key...)
		n = binary.AppendVarint(n, b.Loc.Off)
		n = binary.AppendVarint(n, b.Loc.Len)
	}
	return n
}

// duplicateCheckRecord is the stored fact that a key committed: its slot, the request
// digest, and the base offset assigned to each batch, in request order.
func duplicateCheckRecord(slot int64, dig [checksumLength]byte, rs []contract.BatchRange) []byte {
	b := binary.AppendUvarint(nil, uint64(slot))
	b = append(b, dig[:]...)
	for _, r := range rs {
		b = binary.AppendUvarint(b, uint64(r.Base))
	}
	return b
}

// parseDuplicateCheck splits a record into its digest and bases.
func parseDuplicateCheck(b []byte) ([checksumLength]byte, []int64, error) {
	var dig [checksumLength]byte
	var bases []int64
	_, n := binary.Uvarint(b)
	if n <= 0 || len(b) < n+checksumLength {
		return dig, nil, errCorrupt
	}
	copy(dig[:], b[n:])
	b = b[n+checksumLength:]
	for len(b) > 0 {
		v, m := binary.Uvarint(b)
		if m <= 0 {
			return dig, nil, errCorrupt
		}
		bases, b = append(bases, int64(v)), b[m:]
	}
	return dig, bases, nil
}

// encodeBatchRun packs the batches of one run (one partition, contiguous offsets,
// in order): format byte, the distinct segment keys, then per batch the key
// index, record count, offset and length.
func encodeBatchRun(rs []contract.BatchRange) []byte {
	var keys []string
	idx := map[string]int{} // lookups only
	for _, r := range rs {
		if _, ok := idx[r.Loc.Key]; !ok {
			idx[r.Loc.Key] = len(keys)
			keys = append(keys, r.Loc.Key)
		}
	}
	b := []byte{runFormat}
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
	}
	b = binary.AppendUvarint(b, uint64(len(rs)))
	for _, r := range rs {
		b = binary.AppendUvarint(b, uint64(idx[r.Loc.Key]))
		b = binary.AppendUvarint(b, uint64(r.Count))
		b = binary.AppendVarint(b, r.Loc.Off)
		b = binary.AppendVarint(b, r.Loc.Len)
	}
	return b
}

// decodeBatchRun unpacks the run of tp that starts at base.
func decodeBatchRun(topicPartition contract.TopicPartition, base int64, b []byte) ([]contract.BatchRange, error) {
	if len(b) == 0 || b[0] != runFormat {
		return nil, errCorrupt
	}
	r := engine.NewReader(b[1:])
	nk := r.Uvarint()
	if nk > uint64(len(r.Rest())) {
		return nil, errCorrupt
	}
	keys := make([]string, 0, nk)
	for ; nk > 0 && r.OK(); nk-- {
		keys = append(keys, r.String())
	}
	n := r.Uvarint()
	if n > uint64(len(r.Rest())) {
		return nil, errCorrupt
	}
	out := make([]contract.BatchRange, 0, n)
	for ; n > 0 && r.OK(); n-- {
		ki, cnt, off, ln := r.Uvarint(), r.Uvarint(), r.Varint(), r.Varint()
		if ki >= uint64(len(keys)) {
			return nil, errCorrupt
		}
		out = append(out, contract.BatchRange{TopicPartition: topicPartition, Base: contract.Offset(base), Count: int64(cnt),
			Loc: runtime.SegmentRef{Key: keys[ki], Off: off, Len: ln}})
		base += int64(cnt)
	}
	if !r.Done() {
		return nil, errCorrupt
	}
	return out, nil
}
