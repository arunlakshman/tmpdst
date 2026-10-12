package sim

import (
	"testing"

	"manifester/toys3/core"
)

// TestFactHashKeyFrozen pins the hash encoding of facts. If this fails, a
// change altered every trace hash: see factHashKey.
func TestFactHashKeyFrozen(t *testing.T) {
	for _, c := range []struct {
		f    core.Fact
		want string
	}{
		{core.Committed{Client: 7, Key: "k1", Sum: 0xbeef}, `Committed{c7 "k1" crc=0000beef}`},
		{core.Committed{Client: 8, Key: "k2", Deleted: true}, `Committed{c8 delete "k2"}`},
		{core.Recovered{TempsRemoved: 3}, `Recovered{temps=3}`},
		{core.CorruptObject{Key: "k3"}, `CorruptObject{"k3"}`},
	} {
		if got := factHashKey(c.f); got != c.want {
			t.Errorf("factHashKey(%#v) = %s, want %s", c.f, got, c.want)
		}
	}
}
