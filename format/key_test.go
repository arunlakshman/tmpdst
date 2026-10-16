package format

import (
	"strings"
	"testing"

	"manifester/core"
)

func TestKeyRoundTrip(t *testing.T) {
	nodes := []core.NodeID{"", "a", "a/b", "ab", "%", "%25", "A", "a", "n-1_x", "é", "a b", ".", "..", "a\x00b", "seg", "%2f", "%2F"}
	inc := core.Incarnation{1, 2, 3, 0xFF}
	id := core.ID{0xAB, 0xCD, 15: 9}
	seen := map[string]core.NodeID{}
	for _, n := range nodes {
		k := SegmentKey(n, inc, id)
		if !strings.HasPrefix(k, "seg/") || strings.Count(k, "/") != 3 {
			t.Fatalf("shape %q", k)
		}
		if strings.ToLower(k) != k && strings.ContainsAny(k, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && n == "" {
			t.Fatal("unexpected")
		}
		gn, gi, gd, ok := ParseSegmentKey(k)
		if !ok || gn != n || gi != inc || gd != id {
			t.Fatalf("parse(%q) = %q %v %v %v", k, gn, gi, gd, ok)
		}
		if o, dup := seen[k]; dup && o != n {
			t.Fatalf("collision %q vs %q", o, n)
		}
		seen[k] = n
	}
}

func TestKeyInjective(t *testing.T) {
	inc := core.Incarnation{}
	id := core.ID{}
	pairs := [][2]core.NodeID{{"a/b", "ab"}, {"a/b", "a"}, {"%", "%25"}, {"A", "a"}, {"%41", "A"}, {"%41", "a"}, {"a%2Fb", "a/b"}, {"", "%"}}
	for _, p := range pairs {
		if SegmentKey(p[0], inc, id) == SegmentKey(p[1], inc, id) {
			t.Errorf("%q and %q collide", p[0], p[1])
		}
	}
	// no two keys differing only by case
	if strings.EqualFold(SegmentKey("A", inc, id), SegmentKey("a", inc, id)) {
		t.Error("case alias")
	}
	// exhaustive over short byte strings: distinct nodes give distinct keys.
	m := map[string]string{}
	for a := 0; a < 256; a++ {
		for b := -1; b < 256; b += 5 {
			n := string([]byte{byte(a)})
			if b >= 0 {
				n += string([]byte{byte(b)})
			}
			k := SegmentKey(core.NodeID(n), inc, id)
			if o, ok := m[k]; ok {
				t.Fatalf("collision %q %q", o, n)
			}
			m[k] = n
			if gn, _, _, ok := ParseSegmentKey(k); !ok || string(gn) != n {
				t.Fatalf("roundtrip %q", n)
			}
		}
	}
}

func TestParseRejects(t *testing.T) {
	inc, id := core.Incarnation{}, core.ID{}
	good := SegmentKey("n", inc, id)
	zero := strings.Repeat("0", 32)
	bad := []string{
		"", "seg/", "seg/n", "seg/n/" + zero, "x" + good, good + "/",
		"seg/n/" + zero + "/" + strings.ToUpper("abcdefabcdefabcdefabcdefabcdefab"),
		"seg/n/" + strings.Repeat("A", 32) + "/" + zero,
		"seg/N/" + zero + "/" + zero,   // raw upper-case
		"seg/%61/" + zero + "/" + zero, // escaped unreserved
		"seg/%2f/" + zero + "/" + zero, // lower-case hex escape
		"seg/%2/" + zero + "/" + zero,  // truncated escape
		"seg/%/" + zero + "/" + zero,
		"seg/a./" + zero + "/" + zero,
		"seg/n/" + zero[:31] + "/" + zero,
		"seg/n/" + zero + "g/" + zero[:31],
	}
	for _, k := range bad {
		if _, _, _, ok := ParseSegmentKey(k); ok {
			t.Errorf("accepted %q", k)
		}
	}
}

func FuzzParseSegmentKey(f *testing.F) {
	f.Add(SegmentKey("a/b", core.Incarnation{1}, core.ID{2}))
	f.Fuzz(func(t *testing.T, s string) {
		n, i, d, ok := ParseSegmentKey(s)
		if ok && SegmentKey(n, i, d) != s {
			t.Fatalf("non-canonical key accepted: %q", s)
		}
	})
}
