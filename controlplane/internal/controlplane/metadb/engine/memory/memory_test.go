package memory_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
)

func TestMemoryStoreReadsItsOwnWrites(t *testing.T) {
	m := memory.New()
	err := m.Update(func(tx engine.Txn) {
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
	if err := m.View(func(tx engine.Txn) {
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
	m := memory.New()
	_ = m.Update(func(tx engine.Txn) {
		for _, k := range []string{"a", "c", "e"} {
			tx.Set([]byte(k), []byte(k))
		}
	})
	_ = m.View(func(tx engine.Txn) {
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
	m := memory.New()
	_ = m.Update(func(tx engine.Txn) { tx.Set([]byte("k"), []byte("v")) })
	_ = m.Update(func(tx engine.Txn) { tx.Set([]byte("k"), []byte("w")) })
	_ = m.View(func(tx engine.Txn) {
		if string(tx.Get([]byte("k"))) != "w" {
			t.Error("update lost")
		}
	})
}
