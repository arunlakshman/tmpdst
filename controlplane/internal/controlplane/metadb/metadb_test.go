package metadb_test

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/pebble"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// backends are the stores that a semantic test runs on, by name: the in-memory
// one and Pebble in a temporary folder.
var backends = map[string]func(testing.TB) engine.Engine{
	"mem": func(testing.TB) engine.Engine { return memory.New() },
	"pebble": func(t testing.TB) engine.Engine {
		t.Helper()
		store, err := pebble.Open(filepath.Join(t.TempDir(), "state"), pebble.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	},
}

func TestSnapshotRestoreAndReplay(t *testing.T) {
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := metadb.New(mk(t))
			first := mustCommit(t, a, 0, commit("k0", pb("a", 0, 3), pb("a", 0, 2), pb("b", 7, 1)))
			mustCommit(t, a, 1, commit("k1", pb("a", 0, 4)))
			var buf bytes.Buffer
			if err := a.Snapshot(&buf, 2); err != nil {
				t.Fatal(err)
			}
			b := metadb.New(mk(t))
			slot, err := b.Restore(bytes.NewReader(buf.Bytes()))
			if err != nil || slot != 2 {
				t.Fatalf("restore slot %d err %v", slot, err)
			}
			if a.StateHash() != b.StateHash() {
				t.Fatalf("restored state differs: hash %x vs %x", a.StateHash(), b.StateHash())
			}
			// A replay of a window key answers with the ORIGINAL ranges after restore.
			if got := mustCommit(t, b, 9, commit("k0", pb("a", 0, 3), pb("a", 0, 2), pb("b", 7, 1))); !reflect.DeepEqual(got, first) {
				t.Fatalf("replay after restore %+v, want %+v", got, first)
			}
			if got := mustCommit(t, b, 9, commit("k0", pb("a", 0, 9))); got.Err != runtime.ErrConflict {
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
	a := metadb.New(memory.New())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 2)))
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 1); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	cases := map[string][]byte{"empty": nil, "magic": []byte("XXXXXX"), "no end mark": good[:len(good)-1], "cut": good[:len(good)/2], "header only": good[:6]}
	for name, b := range cases {
		if _, err := metadb.New(memory.New()).Restore(bytes.NewReader(b)); err == nil {
			t.Errorf("%s: restore accepted a damaged checkpoint", name)
		}
	}
}

// localStore drives a metadb.DB as a single node with no log: a command
// gets the next local slot. The log-backed service applies commands at their
// real slots, not through this adapter.
type localStore struct {
	a *metadb.DB

	mu   sync.Mutex
	next int64
}

func (s *localStore) Exec(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	switch s.a.Mode(operation) {
	case metadb.Logged:
		s.mu.Lock()
		defer s.mu.Unlock()
		slot := s.next
		s.next++
		return s.a.Write(slot, operation)
	case metadb.ReadOnly:
		return s.a.Read(operation)
	}
	return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}, nil
}

// callOp runs an operation that has a response and a business code.
func callOp[R any](ctx context.Context, s *localStore, operation runtime.ControlPlaneOperation) (R, runtime.ErrCode, error) {
	d, err := s.Exec(ctx, operation)
	return runtime.ResponseAs[R](d), d.Err, err
}

// callCode runs an operation that has only a business code.
func callCode(ctx context.Context, s *localStore, operation runtime.ControlPlaneOperation) (runtime.ErrCode, error) {
	d, err := s.Exec(ctx, operation)
	return d.Err, err
}

// readOp runs an operation that has a response and no business code.
func readOp[R any](ctx context.Context, s *localStore, operation runtime.ControlPlaneOperation) (R, error) {
	d, err := s.Exec(ctx, operation)
	return runtime.ResponseAs[R](d), err
}

func TestStoreAdapter(t *testing.T) {
	ctx := t.Context()
	s := &localStore{a: metadb.New(memory.New())}
	if c, err := callCode(ctx, s, contract.CreateTopic{Name: "t", Partitions: 3}); c != runtime.OK || err != nil {
		t.Fatal(c, err)
	}
	response, c, err := callOp[contract.CommitSegmentResp](ctx, s, commit("k", pb("t", 2, 4)))
	if err != nil || c != runtime.OK || response.Assigned[0].Base != 0 {
		t.Fatalf("%+v %v %v", response, c, err)
	}
	lk, c, _ := callOp[contract.FindSegmentsResp](ctx, s, contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 2)})
	if c != runtime.OK || lk.End != 4 || len(lk.Ranges) != 1 {
		t.Fatalf("%+v %v", lk, c)
	}
	if _, c, _ = callOp[contract.FindSegmentsResp](ctx, s, contract.FindSegments{From: -1}); c != runtime.ErrInvalid {
		t.Fatal(c)
	}
	d, err := readOp[contract.DescribeTopicsResp](ctx, s, contract.DescribeTopics{})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 3 {
		t.Fatalf("%+v %v", d, err)
	}
}

func TestSnapshotCRCFindsOneFlippedBit(t *testing.T) {
	a := metadb.New(memory.New())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 2)))
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 1); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	if err := metadb.CheckSnapshot(good); err != nil {
		t.Fatalf("good checkpoint refused: %v", err)
	}
	for i := range good {
		bad := bytes.Clone(good)
		bad[i] ^= 1
		if metadb.CheckSnapshot(bad) == nil {
			t.Fatalf("CheckSnapshot accepted a flipped bit at byte %d", i)
		}
		if _, err := metadb.New(memory.New()).Restore(bytes.NewReader(bad)); err == nil {
			t.Fatalf("Restore accepted a flipped bit at byte %d", i)
		}
	}
}

// TestCreateEntryReplay checks that a log entry for an exclusive create gives
// the same answer on every replay: the answer depends only on the state
// before the entry.
func TestCreateEntryReplay(t *testing.T) {
	entries := []metadb.Entry{
		entryOf(contract.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}),
		entryOf(contract.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}),
		entryOf(contract.CreateTopic{Name: "t", Partitions: 2}),
	}
	want := []runtime.ErrCode{runtime.OK, runtime.ErrTopicExists, runtime.OK}
	for run := range 2 {
		a := metadb.NewMem()
		t.Cleanup(func() { _ = a.Close() })
		for i, e := range entries {
			d, err := a.ApplyBytes(int64(i), metadb.Encode(e))
			if err != nil || d.Err != want[i] {
				t.Fatalf("run %d entry %d: %v %v, want %v", run, i, d.Err, err, want[i])
			}
		}
	}
}

func commit(key string, bs ...contract.PendingBatch) contract.CommitSegment {
	return contract.CommitSegment{SegmentKey: key, Batches: bs}
}

func pb(topic string, p int32, n int64) contract.PendingBatch {
	return contract.PendingBatch{TopicPartition: contract.NewTopicPartition(topic, p), Records: n,
		Loc: runtime.SegmentRef{Key: topic, Off: n * 10, Len: n}}
}

func mustCommit(t *testing.T, a *metadb.DB, slot int64, operation contract.CommitSegment) runtime.ControlPlaneDone {
	t.Helper()
	d, err := a.Write(slot, operation)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// endOf is the next offset of a partition, read through FindSegments.
func endOf(t testing.TB, a *metadb.DB, tp contract.TopicPartition) contract.Offset {
	t.Helper()
	d, err := a.Read(contract.FindSegments{TopicPartition: tp})
	if err != nil || d.Err != runtime.OK {
		t.Fatalf("FindSegments: %v %v", d.Err, err)
	}
	return runtime.ResponseAs[contract.FindSegmentsResp](d).End
}
