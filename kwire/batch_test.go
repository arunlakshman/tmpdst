package kwire

import (
	"bytes"
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
	b, _ := BuildBatch(testParams, []kmsg.Record{{Value: []byte("hello")}})
	flip := func(i int) []byte { c := append([]byte(nil), b...); c[i] ^= 1; return c }
	if _, err := ParseBatch(flip(len(b) - 1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("body flip: %v", err)
	}
	if _, err := ParseBatch(flip(16)); !errors.Is(err, ErrUnsupportedBatch) {
		t.Fatalf("magic: %v", err)
	}
	if _, err := ParseBatch(flip(22)); !errors.Is(err, ErrUnsupportedBatch) {
		t.Fatalf("compressed: %v", err)
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
