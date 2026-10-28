package broker

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"testing"

	"manifester/internal/core"
	"manifester/internal/wire/kwire"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func crc(b []byte) uint32 { return crc32.Checksum(b[21:], castagnoli) }

// opaque builds a batch with codec c whose payload is n pseudo-random bytes
// the broker must never look into, claiming recs records.
func opaque(t testing.TB, c int16, recs int32, n int) []byte {
	t.Helper()
	p := params
	p.ProducerID, p.ProducerEpoch, p.BaseSequence, p.Attributes = -1, -1, -1, c
	pay := make([]byte, n)
	x := uint32(c)*7919 + uint32(n) + 1
	for i := range pay {
		x = x*1664525 + 1013904223
		pay[i] = byte(x >> 24)
	}
	b, err := kwire.BuildOpaqueBatch(p, recs, pay)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sameButOffset reports whether got is want with only the base offset and the
// leader epoch rewritten.
func sameButOffset(got, want []byte) bool {
	return len(got) == len(want) && bytes.Equal(got[8:12], want[8:12]) && bytes.Equal(got[16:], want[16:])
}

// Every codec, mixed with plain batches, is committed with offsets taken from
// the header counts and served byte for byte with only the base offset (and
// leader epoch) patched (decision 45).
func TestCompressedBatchesStoredVerbatimMixedSegment(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	batches := [][]byte{
		plain(t, "a", "b"),
		opaque(t, kwire.CodecGzip, 100, 60),
		opaque(t, kwire.CodecSnappy, 3, 200),
		plain(t, "c"),
		opaque(t, kwire.CodecLZ4, 5000, 90),
		opaque(t, kwire.CodecZstd, 7, 33),
	}
	var es []ent
	for _, b := range batches {
		es = append(es, e("t", 0, b))
	}
	h.send(c, verProduce, prodReq(-1, es...))
	h.flush()
	if len(h.puts) != 1 {
		t.Fatalf("%d segments: compressed and plain batches must share one", len(h.puts))
	}
	ps := produced(h.next(c))
	var want int64
	for i, b := range batches {
		bt, _ := kwire.ParseBatch(b)
		if ps[i].ErrorCode != 0 || ps[i].BaseOffset != want {
			t.Fatalf("batch %d: %+v, want base %d", i, ps[i], want)
		}
		want += int64(bt.NumRecords)
	}
	p := fetched(h.call(c, verFetch, fetchReq(core.NewTopicPartition("t", 0), 0, 1<<20)))
	got, err := kwire.SplitBatches(p.RecordBatches)
	if err != nil || len(got) != len(batches) || p.HighWatermark != want {
		t.Fatalf("fetch: %v, %d batches, hwm %d (want %d)", err, len(got), p.HighWatermark, want)
	}
	var base int64
	for i := range batches {
		bt, err := kwire.ParseBatch(got[i])
		if err != nil || bt.BaseOffset != base || !sameButOffset(got[i], batches[i]) {
			t.Fatalf("batch %d: %v base %d want %d", i, err, bt.BaseOffset, base)
		}
		orig, _ := kwire.ParseBatch(batches[i])
		base += int64(orig.NumRecords)
	}
	for _, r := range []string{"gzip", "snappy", "lz4", "zstd"} {
		if !slices.Contains(h.env.reach, "broker: "+r+" batch admitted") {
			t.Errorf("%s admission not declared reachable", r)
		}
	}
	h.noViolations()
}

func TestCompressedBatchRejections(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	badCRC := opaque(t, kwire.CodecLZ4, 4, 50)
	badCRC[len(badCRC)-1] ^= 1
	badCount := opaque(t, kwire.CodecZstd, 4, 50)
	binary.BigEndian.PutUint32(badCount[23:], 9) // lastOffsetDelta != numRecords-1
	binary.BigEndian.PutUint32(badCount[17:], crc(badCount))
	unknown := opaque(t, kwire.CodecGzip, 4, 50)
	unknown[22] = 5
	binary.BigEndian.PutUint32(unknown[17:], crc(unknown))
	h.send(c, verProduce, prodReq(-1,
		e("t", 0, badCRC), e("t", 0, badCount), e("t", 0, unknown), e("t", 0, opaque(t, kwire.CodecSnappy, 2, 10))))
	h.flush()
	ps := produced(h.next(c))
	want := []kafkaErrorCode{errCorruptMessage, errCorruptMessage, errUnsupportedCompress, 0}
	for i, w := range want {
		if kafkaErrorCode(ps[i].ErrorCode) != w {
			t.Errorf("partition %d: error %d, want %d", i, ps[i].ErrorCode, w)
		}
	}
	if end := h.cp.End(core.NewTopicPartition("t", 0)); end != 2 {
		t.Fatalf("end offset %d: a rejected batch was stored", end)
	}
	h.noViolations()
}

// A fetch limit counts the compressed (stored) bytes of a batch: the first
// batch is always returned even when it alone exceeds the limit.
func TestFetchLimitsWithLargeCompressedBatches(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	big := []byte(nil)
	var es []ent
	for i := range 3 {
		big = opaque(t, kwire.CodecZstd, 10, 5000+i)
		es = append(es, e("t", 0, big))
	}
	h.send(c, verProduce, prodReq(-1, es...))
	h.flush()
	h.next(c)
	count := func(max int32) int {
		bs, err := kwire.SplitBatches(fetched(h.call(c, verFetch, fetchReq(core.NewTopicPartition("t", 0), 0, max))).RecordBatches)
		if err != nil {
			t.Fatal(err)
		}
		return len(bs)
	}
	if n := count(1); n != 1 {
		t.Fatalf("limit 1: %d batches, want the first alone", n)
	}
	if n := count(2 * 5200); n != 2 {
		t.Fatalf("limit for two: %d", n)
	}
	if n := count(1 << 20); n != 3 {
		t.Fatalf("no limit: %d", n)
	}
	r := fetchReq(core.NewTopicPartition("t", 0), 0, 1<<20)
	r.MaxBytes = 6000 // request limit: the first partition's data is kept whole
	if p := fetched(h.call(c, verFetch, r)); len(p.RecordBatches) == 0 {
		t.Fatal("first partition dropped by the request limit")
	}
}

// A Produce request near max.request.size=64000000 (the WarpStream benchmark
// client config) is accepted and committed as one oversize segment.
func TestLargeProduceRequest(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	big := opaque(t, kwire.CodecLZ4, 500000, 62_000_000)
	h.send(c, verProduce, prodReq(-1, e("t", 0, big)))
	if h.closed[1] {
		t.Fatal("connection closed on a 62 MB request")
	}
	if len(h.puts) != 1 { // larger than MaxSegmentBytes: sealed alone, no timer
		t.Fatalf("%d segments", len(h.puts))
	}
	ps := produced(h.next(c))
	if ps[0].ErrorCode != 0 || ps[0].BaseOffset != 0 {
		t.Fatalf("%+v", ps[0])
	}
	p := fetched(h.call(c, verFetch, fetchReq(core.NewTopicPartition("t", 0), 0, 1)))
	if len(p.RecordBatches) != len(big) || p.HighWatermark != 500000 || !sameButOffset(p.RecordBatches, big) {
		t.Fatalf("fetched %d bytes, hwm %d", len(p.RecordBatches), p.HighWatermark)
	}
	if DefaultConfig().MaxFrame < 64_000_000+1<<20 {
		t.Fatal("default MaxFrame too small for max.request.size=64000000")
	}
	h.noViolations()
}

func TestSkipProduceCRCMutantAcksCorruptBatch(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InjectedBug = InjectedBugSkipProduceCRC
	h := startedHarness(t, cfg)
	c := h.open(1)
	bad := opaque(t, kwire.CodecLZ4, 4, 50)
	bad[len(bad)-1] ^= 1
	if r := h.produceOne(c, e("t", 0, bad)); r.ErrorCode != 0 {
		t.Fatalf("mutant did not admit the corrupt batch: %d", r.ErrorCode)
	}
}
