package dynamo

import (
	"encoding/binary"
	"errors"
	"fmt"

	"manifester/core"
)

// A run is the batches one commit assigned to one partition. Their offsets are
// contiguous (decide hands out base = running HWM), so the run is stored as ONE
// R# item whose sort key is the base offset of its first batch, and the batches
// inside it are a packed list. The commit transaction therefore costs one range
// item per partition however many batches the segment holds.
//
// Item attributes:
//
//	base  first offset of the run (also the sort key)
//	cnt   total records of the run; the run covers [base, base+cnt)
//	runs  packed batches (below)
//
// Packed layout (all integers are varints; the format byte allows a change):
//
//	0x01
//	nkeys, then nkeys x (len, bytes)         distinct Loc.Key values, in first-use order
//	nbatches, then nbatches x (keyIdx, count, off, len)
//
// count is unsigned; off and len are zigzag (signed). Batch i starts at base +
// sum(count[:i]). A batch is about 6 bytes in a segment of one key, so even
// thousands of batches stay far below DynamoDB's 400 KB item limit; maxRun is
// the explicit bound.
const (
	runFormat = 1
	// maxRun bounds one packed run (well under the 400 KB item limit and,
	// with the G# blob, the 4 MB transaction limit).
	maxRun = 200 << 10
)

var errRun = errors.New("dynamo: corrupt range run")

// encodeRun packs the batches of one run (all one partition, contiguous
// offsets, in order). The caller guarantees contiguity.
func encodeRun(rs []core.BatchRange) []byte {
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

// decodeRun unpacks a run that starts at base for tp.
func decodeRun(tp core.TopicPartition, base int64, b []byte) ([]core.BatchRange, error) {
	if len(b) == 0 || b[0] != runFormat {
		return nil, errRun
	}
	b = b[1:]
	uv := func() (uint64, bool) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	sv := func() (int64, bool) {
		v, n := binary.Varint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	nk, ok := uv()
	if !ok || nk > uint64(len(b)) {
		return nil, errRun
	}
	keys := make([]string, nk)
	for i := range keys {
		l, ok := uv()
		if !ok || l > uint64(len(b)) {
			return nil, errRun
		}
		keys[i], b = string(b[:l]), b[l:]
	}
	n, ok := uv()
	if !ok || n > uint64(len(b)) {
		return nil, errRun
	}
	out := make([]core.BatchRange, 0, n)
	for i := uint64(0); i < n; i++ {
		ki, ok1 := uv()
		cnt, ok2 := uv()
		off, ok3 := sv()
		ln, ok4 := sv()
		if !ok1 || !ok2 || !ok3 || !ok4 || ki >= uint64(len(keys)) {
			return nil, errRun
		}
		out = append(out, core.BatchRange{TP: tp, Base: core.Offset(base), Count: int64(cnt),
			Loc: core.SegmentRef{Key: keys[ki], Off: off, Len: ln}})
		base += int64(cnt)
	}
	if len(b) != 0 {
		return nil, errRun
	}
	return out, nil
}

// runItem is the R# item of one run.
func runItem(tp core.TopicPartition, rs []core.BatchRange) item {
	var cnt int64
	for _, r := range rs {
		cnt += r.Count
	}
	return merge(key(partKey("R#", tp), offSK(int64(rs[0].Base))), item{
		"base": num(int64(rs[0].Base)), "cnt": num(cnt), "runs": blob(encodeRun(rs)),
	})
}

// expandRun lists the batches of a run item.
func expandRun(tp core.TopicPartition, it item) ([]core.BatchRange, error) {
	base, _ := getNum(it, "base")
	rs, err := decodeRun(tp, base, getBlob(it, "runs"))
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s", err, getStr(it, "pk"), getStr(it, "sk"))
	}
	return rs, nil
}

// runsOf groups the newly assigned batches (no error, not a dup) by partition
// in first-use order. Each group is one run.
func runsOf(ranges []core.BatchRange) [][]core.BatchRange {
	var out [][]core.BatchRange
	at := map[core.TopicPartition]int{} // lookups only
	for _, r := range ranges {
		if r.Err != core.OK || r.Dup {
			continue
		}
		i, ok := at[r.TP]
		if !ok {
			i = len(out)
			at[r.TP] = i
			out = append(out, nil)
		}
		out[i] = append(out[i], r)
	}
	return out
}
