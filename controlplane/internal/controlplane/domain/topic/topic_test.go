package topic_test

import (
	"path/filepath"
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

// The topics rules are compared with the model by the conformance tests in
// test/conformance (metadata over memory and Pebble).

func TestTrimForgetsKeysNotOffsets(t *testing.T) {
	a := metadb.New(memory.New())
	mustCommit(t, a, 0, commit("old", pb("a", 0, 5)))
	mustCommit(t, a, 5, commit("new", pb("a", 0, 5)))
	h := a.StateHash()
	if n, err := a.Trim(5); err != nil || n != 1 {
		t.Fatalf("Trim dropped %d err %v", n, err)
	}
	if a.StateHash() != h || endOf(t, a, contract.NewTopicPartition("a", 0)) != 10 {
		t.Fatal("trim changed offsets or runs")
	}
	// "new" is still in the window: replay answers the original ranges.
	if d := mustCommit(t, a, 6, commit("new", pb("a", 0, 5))); d.Resp.(contract.CommitSegmentResp).Assigned[0].Base != 5 {
		t.Fatalf("window replay: %+v", d)
	}
	// "old" left the window: the same key commits again as a NEW segment (decision 48).
	d := mustCommit(t, a, 7, commit("old", pb("a", 0, 5)))
	if d.Err != runtime.OK || d.Resp.(contract.CommitSegmentResp).Assigned[0].Base != 10 {
		t.Fatalf("after trim the key must commit again at 10: %+v", d)
	}
}

func TestInvalidAndUnknown(t *testing.T) {
	a := metadb.New(memory.New())
	if a.CreateTopic("", 1) != runtime.ErrInvalid || a.CreateTopic("t", 0) != runtime.ErrInvalid {
		t.Fatal("CreateTopic must reject empty name and non-positive partitions")
	}
	if d := exec(t, a, contract.FindSegments{From: -1}); d.Err != runtime.ErrInvalid {
		t.Fatalf("Lookup From<0: %+v", d)
	}
	if d := exec(t, a, nil); d.Err != runtime.ErrInvalid {
		t.Fatalf("unknown op: %+v", d)
	}
	// A read is not a command and a command is not a read.
	if d, err := a.Write(0, contract.DescribeTopics{}); err != nil || d.Err != runtime.ErrInvalid {
		t.Fatalf("Write of a read: %+v %v", d, err)
	}
	if d, err := a.Read(contract.CreateTopic{Name: "t", Partitions: 1}); err != nil || d.Err != runtime.ErrInvalid {
		t.Fatalf("Read of a command: %+v %v", d, err)
	}
}

// TestCreateTopicExclusive runs the create steps in log order on both stores.
// The exclusive create checks and writes in one step, so a second exclusive
// create of a name that exists is ErrTopicExists and changes nothing.
func TestCreateTopicExclusive(t *testing.T) {
	steps := []struct {
		name      string
		operation contract.CreateTopic
		code      runtime.ErrCode
		parts     int32 // partitions of the topic after the step
	}{
		{"exclusive create of a new topic", contract.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}, runtime.OK, 2},
		{"exclusive create again", contract.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}, runtime.ErrTopicExists, 2},
		{"plain create again is still at least", contract.CreateTopic{Name: "t", Partitions: 4}, runtime.OK, 4},
		{"plain create never shrinks", contract.CreateTopic{Name: "t", Partitions: 1}, runtime.OK, 4},
		{"exclusive create after a plain one", contract.CreateTopic{Name: "t", Partitions: 1, FailIfExists: true}, runtime.ErrTopicExists, 4},
		{"exclusive create of another new topic", contract.CreateTopic{Name: "u", Partitions: 1, FailIfExists: true}, runtime.OK, 1},
		{"exclusive create, empty name", contract.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}, runtime.ErrInvalid, 0},
		{"exclusive create, zero partitions", contract.CreateTopic{Name: "z", Partitions: 0, FailIfExists: true}, runtime.ErrInvalid, 0},
	}
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			a := metadb.New(mk(t))
			t.Cleanup(func() { _ = a.Close() })
			for i, s := range steps {
				before := a.StateHash()
				d, err := a.Write(int64(i), s.operation)
				if err != nil {
					t.Fatal(err)
				}
				if d.Err != s.code {
					t.Fatalf("%s: code %v, want %v", s.name, d.Err, s.code)
				}
				if s.code != runtime.OK && a.StateHash() != before {
					t.Fatalf("%s: the state changed on a failed create", s.name)
				}
				if s.parts == 0 {
					continue
				}
				got, err := a.Read(contract.DescribeTopics{Topics: []string{s.operation.Name}})
				if err != nil {
					t.Fatal(err)
				}
				if topics := got.Resp.(contract.DescribeTopicsResp).Topics; len(topics) != 1 || topics[0].Partitions != s.parts {
					t.Fatalf("%s: topics %+v, want %d partitions", s.name, topics, s.parts)
				}
			}
		})
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

// exec runs an operation as a command or a read, whichever the store says it is.
func exec(t testing.TB, a *metadb.DB, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	t.Helper()
	var d runtime.ControlPlaneDone
	var err error
	switch a.Mode(operation) {
	case metadb.Logged:
		d, err = a.Write(0, operation)
	case metadb.ReadOnly:
		d, err = a.Read(operation)
	default:
		d.Err = runtime.ErrInvalid
	}
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// endOf is the next offset of a partition, read through FindSegments.
func endOf(t testing.TB, a *metadb.DB, tp contract.TopicPartition) contract.Offset {
	t.Helper()
	d := exec(t, a, contract.FindSegments{TopicPartition: tp})
	if d.Err != runtime.OK {
		t.Fatalf("FindSegments: %v", d.Err)
	}
	return runtime.ResponseAs[contract.FindSegmentsResp](d).End
}
