package kwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func sampleRecords(rng *xorshift, n int) []kmsg.Record {
	var recs []kmsg.Record
	for i := 0; i < n; i++ {
		r := kmsg.Record{Value: rng.bytes(rng.intn(30)), TimestampDelta64: int64(i * 3)}
		if i%2 == 0 {
			r.Key = rng.bytes(1 + rng.intn(5))
		}
		if i%3 == 0 {
			r.Headers = []kmsg.Header{{Key: "h", Value: []byte("v")}, {Key: "e"}}
		}
		recs = append(recs, r)
	}
	return recs
}

var testParams = BatchParams{LeaderEpoch: -1, ProducerID: 77, ProducerEpoch: 3, BaseSequence: 41, FirstTimestamp: 1000, MaxTimestamp: 1012}

func TestBatchRoundTrip(t *testing.T) {
	rng := xorshift(12345)
	for n := 1; n < 20; n++ {
		recs := sampleRecords(&rng, n)
		b, err := BuildBatch(testParams, recs)
		if err != nil {
			t.Fatal(err)
		}
		pb, err := ParseBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		if pb.Magic != 2 || pb.ProducerID != 77 || pb.ProducerEpoch != 3 || pb.BaseSequence != 41 ||
			pb.NumRecords != int32(n) || pb.LastOffsetDelta != int32(n-1) || pb.FirstTimestamp != 1000 ||
			pb.MaxTimestamp != 1012 || pb.PartitionLeaderEpoch != -1 || pb.BaseOffset != 0 {
			t.Fatalf("header %+v", pb)
		}
		got, err := pb.Records()
		if err != nil || len(got) != n {
			t.Fatal(err, len(got))
		}
		for i, r := range got {
			if r.OffsetDelta != int32(i) || r.TimestampDelta64 != int64(i*3) ||
				!bytes.Equal(r.Value, recs[i].Value) || !bytes.Equal(r.Key, recs[i].Key) ||
				len(r.Headers) != len(recs[i].Headers) {
				t.Fatalf("record %d: %+v vs %+v", i, r, recs[i])
			}
		}
		b2, _ := BuildBatch(testParams, recs)
		if !bytes.Equal(b, b2) {
			t.Fatal("not deterministic")
		}
	}
}

func TestBatchRejects(t *testing.T) {
	if _, err := BuildBatch(testParams, nil); err == nil {
		t.Fatal("empty batch accepted")
	}
	p := testParams
	p.Attributes = 1
	if _, err := BuildBatch(p, []kmsg.Record{{}}); !errors.Is(err, ErrUnsupportedBatch) {
		t.Fatal(err)
	}
	p.Attributes = 5
	if _, err := BuildOpaqueBatch(p, 1, []byte("x")); !errors.Is(err, ErrUnknownCodec) {
		t.Fatal(err)
	}
	if _, err := BuildOpaqueBatch(testParams, 0, []byte("x")); err == nil {
		t.Fatal("zero records accepted")
	}
	if _, err := BuildOpaqueBatch(testParams, 1, nil); err == nil {
		t.Fatal("empty payload accepted")
	}
	b, _ := BuildBatch(testParams, []kmsg.Record{{Value: []byte("hello")}})
	flip := func(i int) []byte { c := append([]byte(nil), b...); c[i] ^= 1; return c }
	if _, err := ParseBatch(flip(len(b) - 1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("body flip: %v", err)
	}
	if _, err := ParseBatch(flip(16)); !errors.Is(err, ErrUnsupportedBatch) {
		t.Fatalf("magic: %v", err)
	}
	if _, err := ParseBatch(flip(22)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("attributes flip must fail the CRC: %v", err)
	}
	if _, err := ParseBatch(b[:len(b)-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("truncated: %v", err)
	}
	if _, err := ParseBatch(append(append([]byte(nil), b...), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing: %v", err)
	}
}

func TestPatchBatch(t *testing.T) {
	b, _ := BuildBatch(testParams, []kmsg.Record{{Value: []byte("x")}, {Value: []byte("y")}})
	orig := append([]byte(nil), b...)
	p := PatchBatch(b, 1<<40+5, 9)
	if !bytes.Equal(b, orig) {
		t.Fatal("input mutated")
	}
	pb, err := ParseBatch(p)
	if err != nil || pb.BaseOffset != 1<<40+5 || pb.PartitionLeaderEpoch != 9 {
		t.Fatalf("%+v %v", pb, err)
	}
	if !bytes.Equal(p[16:], b[16:]) || !bytes.Equal(p[8:12], b[8:12]) {
		t.Fatal("patched outside 0..7, 12..15")
	}
	if got := PatchBatch([]byte{1, 2}, 1, 1); !bytes.Equal(got, []byte{1, 2}) {
		t.Fatal("short input")
	}
}

func TestSplitBatches(t *testing.T) {
	rng := xorshift(99)
	var parts [][]byte
	var cat []byte
	for i := 0; i < 5; i++ {
		b, _ := BuildBatch(testParams, sampleRecords(&rng, 1+i))
		parts = append(parts, b)
		cat = append(cat, b...)
	}
	got, err := SplitBatches(cat)
	if err != nil || len(got) != 5 {
		t.Fatal(err, len(got))
	}
	for i := range got {
		if !bytes.Equal(got[i], parts[i]) {
			t.Fatalf("part %d", i)
		}
	}
	if g, err := SplitBatches(nil); g != nil || err != nil {
		t.Fatal("empty input")
	}
	for cut := 1; cut < len(parts[4]); cut += 7 {
		g, err := SplitBatches(cat[:len(cat)-cut])
		if !errors.Is(err, ErrTruncated) || len(g) != 4 {
			t.Fatalf("cut %d: %d %v", cut, len(g), err)
		}
	}
	if _, err := SplitBatches(make([]byte, 20)); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if one, err := OneBatch(parts[0]); err != nil || !bytes.Equal(one, parts[0]) {
		t.Fatal(err)
	}
	if _, err := OneBatch(cat); !errors.Is(err, ErrNotSingle) {
		t.Fatal(err)
	}
	if _, err := OneBatch(cat[:30]); !errors.Is(err, ErrNotSingle) {
		t.Fatal(err)
	}
}

func FuzzParseBatch(f *testing.F) {
	b, _ := BuildBatch(testParams, []kmsg.Record{{Key: []byte("k"), Value: []byte("v"), Headers: []kmsg.Header{{Key: "a"}}}})
	f.Add(b)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if pb, err := ParseBatch(data); err == nil {
			_, _ = pb.Records()
		}
		_, _ = SplitBatches(data)
		_ = PatchBatch(data, 1, 2)
	})
}

// refix recomputes the CRC after a test edits the header.
func refix(b []byte) []byte {
	binary.BigEndian.PutUint32(b[17:], crc32c(b[crcStart:]))
	return b
}

func TestOpaqueCompressedBatch(t *testing.T) {
	rng := xorshift(7)
	for codec := int16(CodecNone); codec <= CodecZstd; codec++ {
		p := testParams
		p.Attributes = codec
		// 500 records in 40 payload bytes: only possible when compressed.
		payload := rng.bytes(40)
		payload[0] = 1
		n := int32(500)
		if codec == CodecNone {
			n = 20
		}
		b, err := BuildOpaqueBatch(p, n, payload)
		if err != nil {
			t.Fatal(err)
		}
		pb, err := ParseBatch(b)
		if err != nil {
			t.Fatalf("codec %d: %v", codec, err)
		}
		if pb.Codec() != int(codec) || pb.NumRecords != n || pb.LastOffsetDelta != n-1 || pb.IsTransactional() || pb.IsControl() {
			t.Fatalf("codec %d: %+v", codec, pb)
		}
		_, rerr := pb.Records()
		if codec != CodecNone && !errors.Is(rerr, ErrUnsupportedBatch) {
			t.Fatalf("Records on compressed: %v", rerr)
		}
		patched, err := ParseBatch(PatchBatch(b, 1<<33, 4))
		if err != nil || patched.BaseOffset != 1<<33 {
			t.Fatalf("patched compressed batch: %v", err)
		}
		bad := append([]byte(nil), b...)
		bad[len(bad)-1] ^= 0x80
		if _, err := ParseBatch(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("codec %d corrupt payload: %v", codec, err)
		}
	}
}

func TestOpaqueBatchHeaderChecks(t *testing.T) {
	b, _ := BuildOpaqueBatch(BatchParams{Attributes: CodecLZ4, ProducerID: -1}, 3, []byte("zzzz"))
	unk := append([]byte(nil), b...)
	unk[22] = 6 // codec 6, CRC recomputed: valid CRC, unknown codec
	if _, err := ParseBatch(refix(unk)); !errors.Is(err, ErrUnknownCodec) {
		t.Fatalf("unknown codec: %v", err)
	}
	neg := append([]byte(nil), b...)
	binary.BigEndian.PutUint32(neg[57:], uint32(0xffffffff)) // numRecords -1
	if _, err := ParseBatch(refix(neg)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("negative count: %v", err)
	}
	plain, _ := BuildBatch(testParams, []kmsg.Record{{Value: []byte("a")}})
	many := append([]byte(nil), plain...)
	binary.BigEndian.PutUint32(many[57:], 1000)
	if _, err := ParseBatch(refix(many)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("count above payload bytes (plain): %v", err)
	}
	if countFits(true, 5, 0) || !countFits(true, 0, 0) || !countFits(true, 9, 1) {
		t.Fatal("countFits compressed")
	}
}
