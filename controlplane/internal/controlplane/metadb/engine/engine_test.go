package engine_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
)

func TestPrefixEnd(t *testing.T) {
	if string(engine.PrefixEnd([]byte("ab"))) != "ac" || string(engine.PrefixEnd([]byte{'a', 0xFF})) != "b" || engine.PrefixEnd([]byte{0xFF}) != nil {
		t.Fatal("engine.PrefixEnd")
	}
}

func TestReader(t *testing.T) {
	// uvarint 5, varint -3, string "hi", blob "xyz"
	b := []byte{5, 5, 2, 'h', 'i', 3, 'x', 'y', 'z'}
	r := engine.NewReader(b)
	if r.Uvarint() != 5 || r.Varint() != -3 || r.String() != "hi" || string(r.Blob()) != "xyz" || !r.Done() {
		t.Fatal("reader read wrong values")
	}
	short := engine.NewReader([]byte{9, 'a'}) // claims 9 bytes, has 1
	if short.String() != "" || short.OK() || short.Done() {
		t.Fatal("a short string must fail")
	}
	short.Uvarint()
	if short.OK() {
		t.Fatal("a failure must stick")
	}
	if engine.NewReader([]byte{0x80}).Uvarint() != 0 || engine.NewReader([]byte{0x80}).Varint() != 0 {
		t.Fatal("a cut varint must fail")
	}
	if engine.NewReader([]byte{9, 'a'}).Blob() != nil {
		t.Fatal("a short blob must fail")
	}
	left := engine.NewReader([]byte{1, 'a', 7})
	_ = left.String()
	if !left.OK() || left.Done() || len(left.Rest()) != 1 {
		t.Fatal("a leftover byte is not Done")
	}
}

func TestTopicPartitionKeyRoundTrip(t *testing.T) {
	want := contract.NewTopicPartition("orders", 7)
	got, ok := engine.SplitTopicPartition(engine.AppendTopicPartition(nil, want))
	if !ok || got != want {
		t.Fatalf("%+v %v", got, ok)
	}
	if _, ok := engine.SplitTopicPartition(append(engine.AppendTopicPartition(nil, want), 0)); ok {
		t.Fatal("trailing byte accepted")
	}
	if _, ok := engine.SplitTopicPartition([]byte{9, 'a'}); ok {
		t.Fatal("short topic accepted")
	}
}

func TestWriterAndReaderMatch(t *testing.T) {
	w := engine.NewWriter()
	w.Uvarint(300)
	w.Varint(-7)
	w.String("hi")
	w.String("") // an empty byte field
	w.Uvarint(2) // a list of two
	w.String("a")
	w.String("b")
	w.Append([]byte{9})
	r := engine.NewReader(w.Bytes())
	if r.Uvarint() != 300 || r.Varint() != -7 || r.String() != "hi" || r.Bytes() != nil || r.Count() != 2 {
		t.Fatal("values did not round trip")
	}
	if r.String() != "a" || r.String() != "b" || r.Uvarint() != 9 || !r.Done() {
		t.Fatal("list or appended bytes did not round trip")
	}
	b := engine.NewReader([]byte{2, 'o', 'k'})
	if string(b.Bytes()) != "ok" {
		t.Fatal("a byte field was not read")
	}
}

func TestReaderCountAndFail(t *testing.T) {
	// a count of 5 with 2 bytes left must fail, so a damaged record cannot make a caller allocate
	r := engine.NewReader([]byte{5, 1, 1})
	if r.Count() != 0 || r.OK() {
		t.Fatal("a count above the bytes left must fail")
	}
	f := engine.NewReader(nil)
	f.Fail()
	if f.OK() || f.Done() {
		t.Fatal("Fail must mark the record bad")
	}
}
