package metadata_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/kvtest"
	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/metadatastore"
	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/storecall"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
)

func TestSnapshotRestoreAndReplay(t *testing.T) {
	for name, mk := range kvtest.Backends {
		t.Run(name, func(t *testing.T) {
			a := metadata.New(mk(t))
			first := mustCommit(t, a, 0, commit("k0", pb("a", 0, 3), pb("a", 0, 2), pb("b", 7, 1)))
			mustCommit(t, a, 1, commit("k1", pb("a", 0, 4)))
			var buf bytes.Buffer
			if err := a.Snapshot(&buf, 2); err != nil {
				t.Fatal(err)
			}
			b := metadata.New(mk(t))
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

func TestRestoreRejectsDamage(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 2)))
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 1); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	cases := map[string][]byte{"empty": nil, "magic": []byte("XXXXXX"), "no end mark": good[:len(good)-1], "cut": good[:len(good)/2], "header only": good[:6]}
	for name, b := range cases {
		if _, err := metadata.New(kv.NewMemory()).Restore(bytes.NewReader(b)); err == nil {
			t.Errorf("%s: restore accepted a damaged checkpoint", name)
		}
	}
}

func TestStoreAdapter(t *testing.T) {
	ctx := t.Context()
	s := &metadatastore.Store{A: metadata.New(kv.NewMemory())}
	if c, err := storecall.Code(ctx, s, core.CreateTopic{Name: "t", Partitions: 3}); c != core.OK || err != nil {
		t.Fatal(c, err)
	}
	response, c, err := storecall.Call[core.CommitSegmentResp](ctx, s, commit("k", pb("t", 2, 4)))
	if err != nil || c != core.OK || response.Assigned[0].Base != 0 {
		t.Fatalf("%+v %v %v", response, c, err)
	}
	lk, c, _ := storecall.Call[core.FindSegmentsResp](ctx, s, core.FindSegments{TopicPartition: core.NewTopicPartition("t", 2)})
	if c != core.OK || lk.End != 4 || len(lk.Ranges) != 1 {
		t.Fatalf("%+v %v", lk, c)
	}
	if _, c, _ = storecall.Call[core.FindSegmentsResp](ctx, s, core.FindSegments{From: -1}); c != core.ErrInvalid {
		t.Fatal(c)
	}
	d, err := storecall.Read[core.DescribeTopicsResp](ctx, s, core.DescribeTopics{})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 3 {
		t.Fatalf("%+v %v", d, err)
	}
}

func TestCommitBatchEqualsSequential(t *testing.T) {
	cmds := []metadata.Command{
		{Slot: 0, Op: commit("k0", pb("a", 0, 3), pb("a", 1, 2))},
		{Slot: 1, Op: commit("k1", pb("a", 0, 4))},                // continues partition 0 inside the same transaction
		{Slot: 2, Op: commit("k0", pb("a", 0, 3), pb("a", 1, 2))}, // a replay of a key committed in this very batch
		{Slot: 3, Op: commit("k1", pb("a", 0, 1))},                // conflict
		{Slot: 4, Op: commit("bad")},
	}
	one, many := metadata.New(kv.NewMemory()), metadata.New(kv.NewMemory())
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

func TestSnapshotCRCFindsOneFlippedBit(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 2)))
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 1); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	if err := metadata.CheckSnapshot(good); err != nil {
		t.Fatalf("good checkpoint refused: %v", err)
	}
	for i := range good {
		bad := bytes.Clone(good)
		bad[i] ^= 1
		if metadata.CheckSnapshot(bad) == nil {
			t.Fatalf("CheckSnapshot accepted a flipped bit at byte %d", i)
		}
		if _, err := metadata.New(kv.NewMemory()).Restore(bytes.NewReader(bad)); err == nil {
			t.Fatalf("Restore accepted a flipped bit at byte %d", i)
		}
	}
}

// TestCreateEntryReplay checks that a log entry for an exclusive create gives
// the same answer on every replay: the answer depends only on the state
// before the entry.
func TestCreateEntryReplay(t *testing.T) {
	entries := []metadata.Entry{
		metadata.CreateEntry(core.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}),
		metadata.CreateEntry(core.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}),
		metadata.CreateEntry(core.CreateTopic{Name: "t", Partitions: 2}),
	}
	want := []core.ErrCode{core.OK, core.ErrTopicExists, core.OK}
	for run := range 2 {
		a := metadata.NewMem()
		t.Cleanup(func() { _ = a.Close() })
		for i, e := range entries {
			d, err := a.ApplyBytes(int64(i), metadata.Encode(e))
			if err != nil || d.Err != want[i] {
				t.Fatalf("run %d entry %d: %v %v, want %v", run, i, d.Err, err, want[i])
			}
		}
	}
}

func TestSnapshotAtTagsTheViewsSlot(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 1)))
	var buf bytes.Buffer
	called := 0
	if err := a.SnapshotAt(&buf, func() int64 { called++; return 77 }); err != nil || called != 1 {
		t.Fatalf("%v %d", err, called)
	}
	b := metadata.New(kv.NewMemory())
	if slot, err := b.Restore(&buf); err != nil || slot != 77 {
		t.Fatalf("slot %d err %v", slot, err)
	}
	if a.StateHash() != b.StateHash() {
		t.Fatal("state differs")
	}
}

func commit(key string, bs ...core.PendingBatch) core.CommitSegment {
	return core.CommitSegment{SegmentKey: key, Batches: bs}
}

func pb(topic string, p int32, n int64) core.PendingBatch {
	return core.PendingBatch{TopicPartition: core.NewTopicPartition(topic, p), Records: n,
		Loc: core.SegmentRef{Key: topic, Off: n * 10, Len: n}}
}

func mustCommit(t *testing.T, a *metadata.Store, slot int64, operation core.CommitSegment) core.ControlPlaneDone {
	t.Helper()
	d, err := a.Commit(slot, operation)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
