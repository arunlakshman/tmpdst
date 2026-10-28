package veritas

import (
	"testing"

	"manifester/internal/core"
)

func TestPartition(t *testing.T) {
	var p Partition
	if p.HighWatermark() != 0 {
		t.Fatal("not empty")
	}
	if b := p.Append([][]byte{[]byte("a"), []byte("b")}); b != 0 {
		t.Fatalf("base %d", b)
	}
	if b := p.Append(nil); b != 2 || p.HighWatermark() != 2 {
		t.Fatalf("empty append: base %d hwm %d", b, p.HighWatermark())
	}
	if b := p.Append([][]byte{[]byte("c")}); b != 2 || p.HighWatermark() != 3 {
		t.Fatalf("base %d hwm %d", b, p.HighWatermark())
	}
	recs, ok := p.Read(1, 0)
	if !ok || len(recs) != 2 || recs[0].Offset != 1 || string(recs[1].Value) != "c" {
		t.Fatalf("read: %+v %v", recs, ok)
	}
	if recs, _ := p.Read(0, 2); len(recs) != 2 {
		t.Fatalf("max: %+v", recs)
	}
	if recs, ok := p.Read(3, 0); !ok || len(recs) != 0 {
		t.Fatalf("read at hwm: %+v %v", recs, ok)
	}
	for _, from := range []int64{-1, 4} {
		if _, ok := p.Read(core.Offset(from), 0); ok {
			t.Fatalf("read %d ok", from)
		}
	}
	h := p.StateHash()
	p.Append([][]byte{{}})
	if p.StateHash() == h {
		t.Fatal("hash ignores empty record")
	}
}
