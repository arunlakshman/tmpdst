package dynamo

import (
	"fmt"
	"slices"
	"testing"

	"manifester/core"
)

func TestRunRoundTrip(t *testing.T) {
	tp := core.TopicPartition{Topic: "t", Partition: 3}
	var rs []core.BatchRange
	base := int64(1_000_000_007)
	for i := 0; i < 300; i++ {
		r := core.BatchRange{TP: tp, Base: core.Offset(base), Count: int64(1 + i%7),
			Loc: core.SegmentRef{Key: fmt.Sprintf("seg/%d", i/100), Off: int64(i) * 4096, Len: int64(100 + i)}}
		if i == 5 {
			r.Loc.Off, r.Loc.Len = -1, 0 // zigzag: signed values survive
		}
		rs = append(rs, r)
		base += r.Count
	}
	it := runItem(tp, rs)
	if n, _ := getNum(it, "base"); n != 1_000_000_007 {
		t.Fatalf("base %d", n)
	}
	var cnt int64
	for _, r := range rs {
		cnt += r.Count
	}
	if n, _ := getNum(it, "cnt"); n != cnt {
		t.Fatalf("cnt %d want %d", n, cnt)
	}
	if got := getStr(it, "sk"); got != offSK(1_000_000_007) {
		t.Fatalf("sk %q", got)
	}
	got, err := expandRun(tp, it)
	if err != nil || !slices.Equal(got, rs) {
		t.Fatalf("round trip: err %v\n got %v\nwant %v", err, got[:3], rs[:3])
	}
	// 300 batches over 3 keys: one item, a few KB.
	if n := len(getBlob(it, "runs")); n > 4<<10 {
		t.Fatalf("300 batches packed to %d bytes", n)
	}
}

func TestRunCorrupt(t *testing.T) {
	tp := core.TopicPartition{Topic: "t"}
	good := encodeRun([]core.BatchRange{{TP: tp, Base: 0, Count: 2, Loc: core.SegmentRef{Key: "k", Off: 1, Len: 2}}})
	for name, b := range map[string][]byte{
		"empty": nil, "format": {9}, "truncated": good[:len(good)-1], "trailing": append(slices.Clone(good), 0),
		"key index": {runFormat, 0, 1, 0, 1, 0, 0},
	} {
		if _, err := decodeRun(tp, 0, b); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

// commitItems is the commit's TransactWriteItems count for batches against an
// empty table (topics unknown: each topic grows once).
func commitItems(t *testing.T, bs []core.PendingBatch) (n int, code core.ErrCode) {
	t.Helper()
	s := New(nil, "x")
	ranges, grown := assign(bs, nil, nil)
	items, code := s.txItems(core.CommitSegment{SegmentKey: "s", Batches: bs}, ranges, grown)
	return len(items), code
}

// The item count does not depend on the number of batches: 1 (segment) + one
// run per partition + grown topics.
func TestCommitItemsIndependentOfBatches(t *testing.T) {
	mk := func(n, parts int) []core.PendingBatch {
		var bs []core.PendingBatch
		for i := 0; i < n; i++ {
			bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: "t", Partition: int32(i % parts)}, Records: 3,
				Loc: core.SegmentRef{Key: "k", Off: int64(i) * 10, Len: 10}})
		}
		return bs
	}
	for _, n := range []int{24, 32, 256, 1000} {
		if got, code := commitItems(t, mk(n, 24)); code != core.OK || got != 1+24+1 {
			t.Errorf("%d batches, 24 partitions: %d items, %v", n, got, code)
		}
		if got, code := commitItems(t, mk(n, 6)); code != core.OK || got != 1+6+1 {
			t.Errorf("%d batches, 6 partitions: %d items, %v", n, got, code)
		}
	}
	if _, code := commitItems(t, mk(99, 99)); code != core.ErrTooLarge {
		t.Errorf("99 partitions: %v", code)
	}
}

func TestAssign(t *testing.T) {
	a, b := core.TopicPartition{Topic: "t", Partition: 0}, core.TopicPartition{Topic: "u", Partition: 2}
	bs := []core.PendingBatch{{TP: a, Records: 2}, {TP: b, Records: 1}, {TP: a, Records: 5}}
	ends := map[core.TopicPartition]int64{a: 10}
	topics := map[string]int32{"t": 1}
	rs, grown := assign(bs, ends, topics)
	if rs[0].Base != 10 || rs[1].Base != 0 || rs[2].Base != 12 {
		t.Fatalf("ranges %+v", rs)
	}
	if len(grown) != 1 || grown["u"] != 3 {
		t.Fatalf("grown %v", grown)
	}
	if ends[a] != 10 || len(ends) != 1 {
		t.Fatal("assign changed its input")
	}
}

func TestRunsGroupByPartition(t *testing.T) {
	a, b := core.TopicPartition{Topic: "t"}, core.TopicPartition{Topic: "t", Partition: 1}
	rs := []core.BatchRange{{TP: a, Base: 0, Count: 1}, {TP: b, Base: 7, Count: 1}, {TP: a, Base: 1, Count: 2}}
	runs := runsOf(rs)
	if len(runs) != 2 || len(runs[0]) != 2 || runs[0][1].Base != 1 || runs[1][0].TP != b {
		t.Fatalf("runs %v", runs)
	}
}
