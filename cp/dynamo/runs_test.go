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

// commitItems is the commit's TransactWriteItems count for batches against an empty
// table (topics unknown: each topic grows once).
func commitItems(t *testing.T, bs []core.PendingBatch) (n int, code core.ErrCode) {
	t.Helper()
	s := New(nil, "x")
	snap := &snapshot{hwm: map[core.TopicPartition]int64{}, state: map[skey]*pstate{}, topics: map[string]int32{}, ver: map[skey]int64{}}
	d := decide(bs, snap)
	items, code := s.txItems(core.CommitSegment{SegmentKey: "s", Owner: core.NodeRef{Node: "n"}, Epoch: 1, Batches: bs}, snap, d)
	return len(items), code
}

// The item count no longer depends on the number of batches: 2 (fence, segment)
// + changed partitions (hwm) + grown topics + producer states + one run per
// partition.
func TestCommitItemsIndependentOfBatches(t *testing.T) {
	mk := func(n, parts int, producers bool) []core.PendingBatch {
		var bs []core.PendingBatch
		seq := map[int]int32{}
		for i := 0; i < n; i++ {
			p := i % parts
			b := core.PendingBatch{TP: core.TopicPartition{Topic: "t", Partition: int32(p)}, Records: 3,
				Loc: core.SegmentRef{Key: "k", Off: int64(i) * 10, Len: 10}}
			if producers {
				b.Producer, b.BaseSeq = 1000, seq[p]
				seq[p] += 3
			}
			bs = append(bs, b)
		}
		return bs
	}
	for _, n := range []int{24, 32, 256, 1000} {
		got, code := commitItems(t, mk(n, 24, true))
		// 2 + 24 hwm + 1 topic + 24 states + 24 runs
		if code != core.OK || got != 2+24+1+24+24 {
			t.Errorf("%d batches, 24 partitions, producers: %d items, %v", n, got, code)
		}
		got, code = commitItems(t, mk(n, 6, false))
		if code != core.OK || got != 2+6+1+6 {
			t.Errorf("%d batches, 6 partitions, plain: %d items, %v", n, got, code)
		}
	}
	// 49 partitions: 2 + 49 + 49 > 100 even without producers.
	if _, code := commitItems(t, mk(49, 49, false)); code != core.ErrTooLarge {
		t.Errorf("49 partitions: %v", code)
	}
}

// A dup creates no run data; a segment of only dups needs no run item.
func TestRunsSkipDupsAndRejects(t *testing.T) {
	tp := core.TopicPartition{Topic: "t"}
	rs := []core.BatchRange{
		{TP: tp, Base: 0, Count: 1, Loc: core.SegmentRef{Key: "k"}},
		{TP: tp, Base: 0, Count: 1, Dup: true},
		{TP: tp, Base: -1, Count: 1, Err: core.ErrOutOfOrder},
		{TP: tp, Base: 1, Count: 2, Loc: core.SegmentRef{Key: "k", Off: 5}},
	}
	runs := runsOf(rs)
	if len(runs) != 1 || len(runs[0]) != 2 || runs[0][1].Base != 1 {
		t.Fatalf("runs %v", runs)
	}
	if len(runsOf(rs[1:3])) != 0 {
		t.Fatal("run for dups and rejects")
	}
}
