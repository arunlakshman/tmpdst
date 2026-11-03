package kv_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/core"
)

func TestPrefixEnd(t *testing.T) {
	if string(kv.PrefixEnd([]byte("ab"))) != "ac" || string(kv.PrefixEnd([]byte{'a', 0xFF})) != "b" || kv.PrefixEnd([]byte{0xFF}) != nil {
		t.Fatal("kv.PrefixEnd")
	}
}

func TestMemoryStoreReadsItsOwnWrites(t *testing.T) {
	m := kv.NewMemory()
	err := m.Update(func(tx kv.Transaction) {
		tx.Set([]byte("b"), []byte("2"))
		tx.Set([]byte("a"), []byte("1"))
		tx.Set([]byte("c"), []byte("3"))
		tx.Set([]byte("b"), []byte("22")) // replaces
		if string(tx.Get([]byte("b"))) != "22" {
			t.Error("a transaction does not see its own write")
		}
		tx.Delete([]byte("c"))
		tx.Delete([]byte("zz")) // absent: nothing happens
	})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := m.View(func(tx kv.Transaction) {
		if tx.Get([]byte("c")) != nil || tx.Get([]byte("none")) != nil {
			t.Error("deleted or absent key has a value")
		}
		tx.Ascend([]byte("a"), nil, func(k, _ []byte) bool { keys = append(keys, string(k)); return true })
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"a", "b"}) {
		t.Fatalf("keys %v", keys)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreFloorAndAscendStop(t *testing.T) {
	m := kv.NewMemory()
	_ = m.Update(func(tx kv.Transaction) {
		for _, k := range []string{"a", "c", "e"} {
			tx.Set([]byte(k), []byte(k))
		}
	})
	_ = m.View(func(tx kv.Transaction) {
		if k, _ := tx.Floor([]byte("a"), []byte("d")); string(k) != "c" {
			t.Errorf("floor below d: %q", k)
		}
		if k, _ := tx.Floor([]byte("a"), nil); string(k) != "e" {
			t.Errorf("floor unbounded: %q", k)
		}
		if k, _ := tx.Floor([]byte("d"), []byte("e")); k != nil {
			t.Errorf("floor with nothing in range: %q", k)
		}
		if k, _ := tx.Floor([]byte("a"), []byte("a")); k != nil {
			t.Errorf("floor of an empty range: %q", k)
		}
		n := 0
		tx.Ascend([]byte("a"), []byte("z"), func(_, _ []byte) bool { n++; return n < 2 })
		if n != 2 {
			t.Errorf("ascend did not stop when asked: %d", n)
		}
	})
}

func TestMemoryStoreKeepsOldViewOnUpdate(t *testing.T) {
	m := kv.NewMemory()
	_ = m.Update(func(tx kv.Transaction) { tx.Set([]byte("k"), []byte("v")) })
	_ = m.Update(func(tx kv.Transaction) { tx.Set([]byte("k"), []byte("w")) })
	_ = m.View(func(tx kv.Transaction) {
		if string(tx.Get([]byte("k"))) != "w" {
			t.Error("update lost")
		}
	})
}

func TestReader(t *testing.T) {
	// uvarint 5, varint -3, string "hi", blob "xyz"
	b := []byte{5, 5, 2, 'h', 'i', 3, 'x', 'y', 'z'}
	r := kv.NewReader(b)
	if r.Uvarint() != 5 || r.Varint() != -3 || r.String() != "hi" || string(r.Blob()) != "xyz" || !r.Done() {
		t.Fatal("reader read wrong values")
	}
	short := kv.NewReader([]byte{9, 'a'}) // claims 9 bytes, has 1
	if short.String() != "" || short.OK() || short.Done() {
		t.Fatal("a short string must fail")
	}
	short.Uvarint()
	if short.OK() {
		t.Fatal("a failure must stick")
	}
	if kv.NewReader([]byte{0x80}).Uvarint() != 0 || kv.NewReader([]byte{0x80}).Varint() != 0 {
		t.Fatal("a cut varint must fail")
	}
	if kv.NewReader([]byte{9, 'a'}).Blob() != nil {
		t.Fatal("a short blob must fail")
	}
	left := kv.NewReader([]byte{1, 'a', 7})
	_ = left.String()
	if !left.OK() || left.Done() || len(left.Rest()) != 1 {
		t.Fatal("a leftover byte is not Done")
	}
}

func TestTopicPartitionKeyRoundTrip(t *testing.T) {
	want := core.NewTopicPartition("orders", 7)
	got, ok := kv.SplitTopicPartition(kv.AppendTopicPartition(nil, want))
	if !ok || got != want {
		t.Fatalf("%+v %v", got, ok)
	}
	if _, ok := kv.SplitTopicPartition(append(kv.AppendTopicPartition(nil, want), 0)); ok {
		t.Fatal("trailing byte accepted")
	}
	if _, ok := kv.SplitTopicPartition([]byte{9, 'a'}); ok {
		t.Fatal("short topic accepted")
	}
}
