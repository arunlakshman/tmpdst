package format

import (
	"strings"
	"testing"

	"manifester/core"
)

func TestKeyRoundTrip(t *testing.T) {
	ids := []core.ID{{}, {0xAB, 0xCD, 15: 9}, {0xFF, 1: 0xFF, 14: 0xFF, 15: 0xFF}}
	seen := map[string]bool{}
	for _, id := range ids {
		k := SegmentKey(id)
		if !strings.HasPrefix(k, "seg/") || strings.Count(k, "/") != 1 || len(k) != 4+32 || strings.ToLower(k) != k {
			t.Fatalf("shape %q", k)
		}
		if got, ok := ParseSegmentKey(k); !ok || got != id {
			t.Fatalf("parse(%q) = %v %v", k, got, ok)
		}
		if seen[k] {
			t.Fatalf("collision %q", k)
		}
		seen[k] = true
	}
	if SegmentKey(core.ID{0xAB}) != "seg/ab000000000000000000000000000000" {
		t.Fatalf("frozen spelling: %q", SegmentKey(core.ID{0xAB}))
	}
}

func TestParseRejects(t *testing.T) {
	zero := strings.Repeat("0", 32)
	good := SegmentKey(core.ID{})
	bad := []string{
		"", "seg/", "seg", "x" + good, good + "/", good + "0", "seg/" + zero[:31], "seg/" + zero + "0",
		"seg/" + strings.Repeat("A", 32), // upper-case hex is not canonical
		"seg/" + zero[:31] + "g",
		"seg/" + zero[:31] + "\x00",
		"SEG/" + zero,
		// the old nested layout: seg/<node>/<incarnation>/<id>
		"seg/n/" + zero + "/" + zero,
		"seg/" + zero + "/" + zero,
		"seg/%61/" + zero + "/" + zero,
		"seg/" + zero + "/",
		"segs/" + zero,
	}
	for _, k := range bad {
		if _, ok := ParseSegmentKey(k); ok {
			t.Errorf("accepted %q", k)
		}
	}
}

func FuzzParseSegmentKey(f *testing.F) {
	f.Add(SegmentKey(core.ID{2}))
	f.Add("seg/n/" + strings.Repeat("0", 32) + "/" + strings.Repeat("0", 32))
	f.Fuzz(func(t *testing.T, s string) {
		id, ok := ParseSegmentKey(s)
		if ok && SegmentKey(id) != s {
			t.Fatalf("non-canonical key accepted: %q", s)
		}
	})
}
