package format

import (
	"bytes"
	"errors"
	"testing"

	"manifester/internal/core"
)

func sample() []Batch {
	return []Batch{
		{core.TopicPartitionKey("orders", 0), 3, []byte("hello")},
		{core.TopicPartitionKey("orders", 7), 1, []byte("x")},
		{core.TopicPartitionKey("t/\u00e9", 2), 9, bytes.Repeat([]byte{0xAB}, 40)},
	}
}

func TestRoundTrip(t *testing.T) {
	in := sample()
	obj, ents, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := Decode(obj)
	if err != nil {
		t.Fatal(err)
	}
	if len(seg.Batches) != len(in) {
		t.Fatalf("got %d batches", len(seg.Batches))
	}
	for i, b := range in {
		g := seg.Batches[i]
		if g.TP != b.TP || g.Records != b.Records || !bytes.Equal(g.Data, b.Data) {
			t.Fatalf("batch %d mismatch: %+v", i, g)
		}
		if seg.Entries[i] != ents[i] {
			t.Fatalf("entry %d: decode %+v encode %+v", i, seg.Entries[i], ents[i])
		}
		// independent ranged fetch
		ref := ents[i].Ref("k")
		if ref.Key != "k" || ref.Len != int64(len(b.Data)) {
			t.Fatalf("ref %+v", ref)
		}
		got, err := VerifyBatch(ents[i], obj[ref.Off:ref.Off+ref.Len])
		if err != nil || !bytes.Equal(got, b.Data) {
			t.Fatalf("verify %d: %v", i, err)
		}
	}
	// deterministic and canonical
	obj2, _, _ := Encode(in)
	if !bytes.Equal(obj, obj2) {
		t.Fatal("encode not deterministic")
	}
}

func TestVerifyBatchCorrupt(t *testing.T) {
	obj, ents, _ := Encode(sample())
	e := ents[0]
	b := append([]byte(nil), obj[e.Off:e.Off+e.Len]...)
	b[0] ^= 1
	if _, err := VerifyBatch(e, b); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := VerifyBatch(e, b[:len(b)-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestEncodeInvalid(t *testing.T) {
	bad := [][]Batch{
		nil,
		{{core.TopicPartitionKey("", 0), 1, []byte("x")}},
		{{core.TopicPartitionKey("t", -1), 1, []byte("x")}},
		{{core.TopicPartitionKey("t", 0), 1, nil}},
		{{core.TopicPartitionKey(string(make([]byte, 70000)), 0), 1, []byte("x")}},
	}
	for i, b := range bad {
		if _, _, err := Encode(b); !errors.Is(err, ErrInvalid) || Code(err) != core.ErrInvalid {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}

func TestCode(t *testing.T) {
	if Code(nil) != core.OK || Code(corrupt("x")) != core.ErrCorrupt {
		t.Fatal("Code")
	}
}

func TestFlipEveryByte(t *testing.T) {
	obj, _, _ := Encode(sample())
	for i := range obj {
		for bit := range 8 {
			c := append([]byte(nil), obj...)
			c[i] ^= 1 << bit
			_, err := Decode(c)
			if !errors.Is(err, ErrCorrupt) || Code(err) != core.ErrCorrupt {
				t.Fatalf("byte %d bit %d: err=%v", i, bit, err)
			}
		}
	}
}

func TestTruncation(t *testing.T) {
	obj, _, _ := Encode(sample())
	for n := range obj {
		if _, err := Decode(obj[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("len %d: %v", n, err)
		}
	}
	if _, err := Decode(append(obj, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatal("trailing byte accepted")
	}
	if _, err := Decode(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatal("nil accepted")
	}
}

// Header-class corruptions with a consistent CRC still fail.
func TestSpecificClasses(t *testing.T) {
	obj, _, _ := Encode(sample())
	mut := func(f func([]byte)) error {
		c := append([]byte(nil), obj...)
		f(c)
		_, err := Decode(c)
		return err
	}
	for name, f := range map[string]func([]byte){
		"magic":   func(b []byte) { b[0] = 'X' },
		"version": func(b []byte) { b[5] = 2 },
		"flags":   func(b []byte) { b[7] = 1 },
		"length":  func(b []byte) { b[15]++ },
		"crc":     func(b []byte) { b[19] ^= 0xFF },
		"body":    func(b []byte) { b[len(b)-1] ^= 1 },
	} {
		if err := mut(f); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func FuzzDecode(f *testing.F) {
	obj, _, _ := Encode(sample())
	f.Add(obj)
	f.Add(obj[:HeaderSize])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		seg, err := Decode(b)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("non-corrupt error: %v", err)
			}
			return
		}
		// A valid object re-encodes to identical bytes (canonical).
		out, _, err := Encode(seg.Batches)
		if err != nil || !bytes.Equal(out, b) {
			t.Fatalf("not canonical: %v", err)
		}
	})
}
