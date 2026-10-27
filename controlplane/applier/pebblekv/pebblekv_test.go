package pebblekv_test

import (
	"path/filepath"
	"testing"

	"manifester/controlplane/applier"
	"manifester/controlplane/applier/pebblekv"
)

func open(t *testing.T) *pebblekv.DB {
	t.Helper()
	db, err := pebblekv.Open(filepath.Join(t.TempDir(), "s"), pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOrderedOps(t *testing.T) {
	db := open(t)
	err := db.Update(func(tx applier.Tx) {
		for _, k := range []string{"b", "d", "f", "h"} {
			tx.Set([]byte(k), []byte("v"+k))
		}
		tx.Delete([]byte("f"))
		if string(tx.Get([]byte("d"))) != "vd" || tx.Get([]byte("f")) != nil {
			t.Error("Get/Delete in the writing transaction")
		}
		if k, _ := tx.Floor([]byte("a"), []byte("e")); string(k) != "d" {
			t.Errorf("Floor sees its own writes: %q", k)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.View(func(tx applier.Tx) {
		cases := []struct{ lo, hi, want string }{
			{"a", "d", "b"}, {"a", "e", "d"}, {"a", "b", ""}, {"c", "d", ""}, {"a", "", "h"}, {"a", "z", "h"}, {"e", "i", "h"}, {"i", "", ""},
		}
		for _, c := range cases {
			var hi []byte
			if c.hi != "" {
				hi = []byte(c.hi)
			}
			if k, _ := tx.Floor([]byte(c.lo), hi); string(k) != c.want {
				t.Errorf("Floor(%q,%q) = %q, want %q", c.lo, c.hi, k, c.want)
			}
		}
		var got []string
		tx.Ascend([]byte("c"), nil, func(k, _ []byte) bool { got = append(got, string(k)); return true })
		if len(got) != 2 || got[0] != "d" || got[1] != "h" {
			t.Errorf("Ascend = %v", got)
		}
		got = nil
		tx.Ascend([]byte("a"), []byte("h"), func(k, _ []byte) bool { got = append(got, string(k)); return false })
		if len(got) != 1 || got[0] != "b" {
			t.Errorf("Ascend stop = %v", got)
		}
	})
}

func TestViewIsASnapshot(t *testing.T) {
	db := open(t)
	_ = db.Update(func(tx applier.Tx) { tx.Set([]byte("k"), []byte("1")) })
	_ = db.View(func(tx applier.Tx) {
		_ = db.Update(func(w applier.Tx) { w.Set([]byte("k"), []byte("2")) })
		if string(tx.Get([]byte("k"))) != "1" {
			t.Error("a View must not see a later commit")
		}
	})
}

func TestOpenFailsOnBadPath(t *testing.T) {
	if _, err := pebblekv.Open("/dev/null/x", pebblekv.Options{}); err == nil {
		t.Fatal("Open must fail")
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	db, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Update(func(tx applier.Tx) { tx.Set([]byte("k"), []byte("v")) })
	_ = db.Close() // closing for the reopen; the test checks what survived
	db, err = pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	_ = db.View(func(tx applier.Tx) {
		if string(tx.Get([]byte("k"))) != "v" {
			t.Error("data lost on reopen")
		}
	})
}
