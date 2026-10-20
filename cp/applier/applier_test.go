package applier_test

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"manifester/core"
	"manifester/cp/applier"
	"manifester/cp/applier/pebblekv"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

func newPebble(t testing.TB) applier.KV {
	t.Helper()
	kv, err := pebblekv.Open(filepath.Join(t.TempDir(), "state"), pebblekv.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { kv.Close() })
	return kv
}

var backends = map[string]func(testing.TB) applier.KV{
	"mem":    func(testing.TB) applier.KV { return applier.NewMemKV() },
	"pebble": newPebble,
}

// genOp draws one operation: commits from a small key pool (so duplicates and
// conflicts are common), invalid commands, topics, lookups and describes.
func genOp(r *veritastest.Rand, slot int64) core.CPCall {
	switch k := r.Intn(20); {
	case k == 0:
		return core.CPCall{Req: core.ReqID(slot), Op: core.CommitSegment{SegmentKey: "bad" + strconv.Itoa(r.Intn(2))}}
	case k < 4:
		return core.CPCall{Req: core.ReqID(slot), Op: core.DescribeTopics{Topics: [][]string{nil, {"a"}, {"b", "zz"}}[r.Intn(3)]}}
	case k < 9:
		return core.CPCall{Req: core.ReqID(slot), Op: core.Lookup{
			TP:   core.TopicPartition{Topic: []string{"a", "b", "none"}[r.Intn(3)], Partition: int32(r.Intn(5))},
			From: core.Offset(r.Intn(40) - 1), MaxBytes: int32(r.Intn(3) * 70)}}
	}
	key := "seg/" + strconv.Itoa(r.Intn(60))
	variant := 0
	if r.Intn(8) == 0 {
		variant = 1
	}
	pr := veritastest.NewRand(uint64(len(key))*977 + uint64(key[len(key)-1]) + uint64(variant)*13)
	var bs []core.PendingBatch
	for i := 1 + pr.Intn(6); i > 0; i-- {
		bs = append(bs, core.PendingBatch{
			TP:      core.TopicPartition{Topic: []string{"a", "b"}[pr.Intn(2)], Partition: int32(pr.Intn(5))},
			Records: int64(1 + pr.Intn(4)),
			Loc:     core.SegmentRef{Key: key, Off: int64(pr.Intn(5)) * 100, Len: int64(1 + pr.Intn(3)*50)},
		})
	}
	return core.CPCall{Req: core.ReqID(slot), Op: core.CommitSegment{SegmentKey: key, Batches: bs}}
}

func lockstep(t *testing.T, mk func(testing.TB) applier.KV, seeds, ops int) {
	t.Helper()
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		r := veritastest.NewRand(seed)
		a, o := applier.New(mk(t)), veritas.NewControlPlane()
		for i := 0; i < ops; i++ {
			if r.Intn(25) == 0 {
				name, n := "t"+strconv.Itoa(r.Intn(3)), int32(r.Intn(4))
				if got, want := a.CreateTopic(name, n), o.CreateTopic(name, n); got != want {
					t.Fatalf("seed %d op %d CreateTopic: %v vs oracle %v", seed, i, got, want)
				}
				continue
			}
			call := genOp(r, int64(i))
			got, want := a.Exec(call), o.Exec(call)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d op %d %+v\n got %+v\nwant %+v", seed, i, call.Op, got, want)
			}
			if lk, ok := call.Op.(core.Lookup); ok {
				edgeLookups(t, a, o, lk, want.(core.CPDone))
			}
		}
		for _, tp := range []core.TopicPartition{{Topic: "a", Partition: 1}, {Topic: "b", Partition: 4}, {Topic: "q"}} {
			if a.End(tp) != o.End(tp) {
				t.Fatalf("seed %d End(%v) %d vs oracle %d", seed, tp, a.End(tp), o.End(tp))
			}
		}
		a.Close()
	}
}

func TestLockstepMem(t *testing.T)    { lockstep(t, backends["mem"], 300, 300) }
func TestLockstepPebble(t *testing.T) { lockstep(t, backends["pebble"], 15, 300) }

func commit(key string, bs ...core.PendingBatch) core.CommitSegment {
	return core.CommitSegment{SegmentKey: key, Batches: bs}
}

func pb(topic string, p int32, n int64) core.PendingBatch {
	return core.PendingBatch{TP: core.TopicPartition{Topic: topic, Partition: p}, Records: n,
		Loc: core.SegmentRef{Key: topic, Off: n * 10, Len: n}}
}

func mustCommit(t *testing.T, a *applier.Applier, slot int64, op core.CommitSegment) core.CPDone {
	t.Helper()
	d, err := a.Commit(slot, op)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSnapshotRestoreAndReplay(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := applier.New(mk(t))
			first := mustCommit(t, a, 0, commit("k0", pb("a", 0, 3), pb("a", 0, 2), pb("b", 7, 1)))
			mustCommit(t, a, 1, commit("k1", pb("a", 0, 4)))
			var buf bytes.Buffer
			if err := a.Snapshot(&buf, 2); err != nil {
				t.Fatal(err)
			}
			b := applier.New(mk(t))
			slot, err := b.Restore(bytes.NewReader(buf.Bytes()))
			if err != nil || slot != 2 {
				t.Fatalf("restore slot %d err %v", slot, err)
			}
			if a.StateHash() != b.StateHash() || b.Window() != 2 {
				t.Fatalf("restored state differs: hash %x vs %x, window %d", a.StateHash(), b.StateHash(), b.Window())
			}
			// A replay of a window key answers with the ORIGINAL ranges after restore.
			if got := mustCommit(t, b, 9, commit("k0", pb("a", 0, 3), pb("a", 0, 2), pb("b", 7, 1))); !reflect.DeepEqual(got, first) {
				t.Fatalf("replay after restore %+v, want %+v", got, first)
			}
			if got := mustCommit(t, b, 9, commit("k0", pb("a", 0, 9))); got.Err != core.ErrConflict {
				t.Fatalf("conflict after restore: %+v", got)
			}
			// Continue both identically.
			x, y := mustCommit(t, a, 2, commit("k2", pb("a", 0, 1))), mustCommit(t, b, 2, commit("k2", pb("a", 0, 1)))
			if !reflect.DeepEqual(x, y) || a.StateHash() != b.StateHash() {
				t.Fatalf("diverged after restore: %+v vs %+v", x, y)
			}
		})
	}
}

func TestTrimForgetsKeysNotOffsets(t *testing.T) {
	a := applier.New(applier.NewMemKV())
	mustCommit(t, a, 0, commit("old", pb("a", 0, 5)))
	mustCommit(t, a, 5, commit("new", pb("a", 0, 5)))
	h := a.StateHash()
	if n, err := a.Trim(5); err != nil || n != 1 || a.Window() != 1 {
		t.Fatalf("Trim dropped %d err %v window %d", n, err, a.Window())
	}
	if a.StateHash() != h || a.End(core.TopicPartition{Topic: "a"}) != 10 {
		t.Fatal("trim changed offsets or runs")
	}
	// "new" is still in the window: replay answers the original ranges.
	if d := mustCommit(t, a, 6, commit("new", pb("a", 0, 5))); d.Resp.(core.CommitSegmentResp).Assigned[0].Base != 5 {
		t.Fatalf("window replay: %+v", d)
	}
	// "old" left the window: the same key commits again as a NEW segment (decision 48).
	d := mustCommit(t, a, 7, commit("old", pb("a", 0, 5)))
	if d.Err != core.OK || d.Resp.(core.CommitSegmentResp).Assigned[0].Base != 10 {
		t.Fatalf("after trim the key must commit again at 10: %+v", d)
	}
}

func TestRestoreRejectsDamage(t *testing.T) {
	a := applier.New(applier.NewMemKV())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 2)))
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 1); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	cases := map[string][]byte{"empty": nil, "magic": []byte("XXXXXX"), "no end mark": good[:len(good)-1], "cut": good[:len(good)/2], "header only": good[:6]}
	for name, b := range cases {
		if _, err := applier.New(applier.NewMemKV()).Restore(bytes.NewReader(b)); err == nil {
			t.Errorf("%s: restore accepted a damaged checkpoint", name)
		}
	}
}

func TestInvalidAndUnknown(t *testing.T) {
	a := applier.New(applier.NewMemKV())
	if a.CreateTopic("", 1) != core.ErrInvalid || a.CreateTopic("t", 0) != core.ErrInvalid {
		t.Fatal("CreateTopic must reject empty name and non-positive partitions")
	}
	if d := a.Exec(core.CPCall{Op: core.Lookup{From: -1}}).(core.CPDone); d.Err != core.ErrInvalid {
		t.Fatalf("Lookup From<0: %+v", d)
	}
	if d := a.Exec(core.CPCall{Op: nil}).(core.CPDone); d.Err != core.ErrInvalid {
		t.Fatalf("unknown op: %+v", d)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Exec of a non-CPCall must panic")
		}
	}()
	a.Exec(core.Dial{})
}

func TestStoreAdapter(t *testing.T) {
	ctx := context.Background()
	s := &applier.Store{A: applier.New(applier.NewMemKV())}
	if c, err := s.CreateTopic(ctx, "t", 3); c != core.OK || err != nil {
		t.Fatal(c, err)
	}
	resp, c, err := s.CommitSegment(ctx, commit("k", pb("t", 2, 4)))
	if err != nil || c != core.OK || resp.Assigned[0].Base != 0 {
		t.Fatalf("%+v %v %v", resp, c, err)
	}
	lk, c, _ := s.Lookup(ctx, core.Lookup{TP: core.TopicPartition{Topic: "t", Partition: 2}})
	if c != core.OK || lk.End != 4 || len(lk.Ranges) != 1 {
		t.Fatalf("%+v %v", lk, c)
	}
	if _, c, _ = s.Lookup(ctx, core.Lookup{From: -1}); c != core.ErrInvalid {
		t.Fatal(c)
	}
	d, err := s.DescribeTopics(ctx, core.DescribeTopics{})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 3 {
		t.Fatalf("%+v %v", d, err)
	}
}

func TestCommitBatchEqualsSequential(t *testing.T) {
	cmds := []applier.Command{
		{Slot: 0, Op: commit("k0", pb("a", 0, 3), pb("a", 1, 2))},
		{Slot: 1, Op: commit("k1", pb("a", 0, 4))},                // continues partition 0 inside the same transaction
		{Slot: 2, Op: commit("k0", pb("a", 0, 3), pb("a", 1, 2))}, // a replay of a key committed in this very batch
		{Slot: 3, Op: commit("k1", pb("a", 0, 1))},                // conflict
		{Slot: 4, Op: commit("bad")},
	}
	one, many := applier.New(applier.NewMemKV()), applier.New(applier.NewMemKV())
	got, err := many.CommitBatch(cmds)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cmds {
		want := mustCommit(t, one, c.Slot, c.Op)
		if !reflect.DeepEqual(got[i], want) {
			t.Fatalf("command %d: batch %+v, sequential %+v", i, got[i], want)
		}
	}
	if one.StateHash() != many.StateHash() || one.Window() != many.Window() {
		t.Fatal("state differs")
	}
}

// edgeLookups re-asks lk with MaxBytes at, just below and just above every
// running total of Len in the answer: the stop rule is a >= on exactly these.
func edgeLookups(t *testing.T, a *applier.Applier, o *veritas.ControlPlane, lk core.Lookup, d core.CPDone) {
	t.Helper()
	resp, _ := d.Resp.(core.LookupResp)
	var sum int64
	for _, r := range resp.Ranges {
		sum += r.Loc.Len
		for e := int64(-1); e <= 1; e++ {
			if sum+e <= 0 {
				continue
			}
			q := core.CPCall{Op: core.Lookup{TP: lk.TP, From: lk.From, MaxBytes: int32(sum + e)}}
			if got, want := a.Exec(q), o.Exec(q); !reflect.DeepEqual(got, want) {
				t.Fatalf("%+v\n got %+v\nwant %+v", q.Op, got, want)
			}
		}
	}
}
