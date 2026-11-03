package topics_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/kvtest"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/generator"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/injectedbug"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/observation"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/runsettings"
	"github.com/manifesterio/mds/internal/verify/speccheck/checker/suites"
)

// metadataSubject runs the Store directly as a suite subject. The applier
// hash is not the hash of the spec, so StateHash says no.
type metadataSubject struct{ a *metadata.Store }

func (s metadataSubject) Do(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	switch {
	case operation.CreateTopic != nil:
		return observation.ControlPlaneResult{Code: s.a.CreateTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)}, nil
	case operation.Call != nil:
		done, ok := s.a.Exec(*operation.Call).(core.ControlPlaneDone)
		if !ok {
			return observation.ControlPlaneResult{}, errors.New("the applier did not return a ControlPlaneDone")
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (metadataSubject) StateHash() (uint64, bool) { return 0, false }

func newMetadataSubject(t testing.TB, mk func(testing.TB) kv.Store) suites.ControlPlaneSubject {
	a := metadata.New(mk(t))
	t.Cleanup(func() { _ = a.Close() })
	return metadataSubject{a}
}

// runSuite runs the shared control plane suite on the applier for every
// profile. Pebble is slower, so it passes a smaller default number of seeds.
func runSuite(t *testing.T, mk func(testing.TB) kv.Store, defaultSeeds int) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, func() suites.ControlPlaneSubject { return newMetadataSubject(t, mk) },
				runsettings.Options(t, defaultSeeds))
		})
	}
}

func TestLockstepMem(t *testing.T) { runSuite(t, kvtest.Backends["mem"], 20) }

func TestLockstepPebble(t *testing.T) { runSuite(t, kvtest.Backends["pebble"], 3) }

// TestLookupMaxBytesEdges checks what the shared suite does not: a Lookup with
// MaxBytes at, just below and just above every running total of Len in an
// answer. The stop rule is a >= on exactly these values, and the suite only
// uses fixed MaxBytes values.
func TestLookupMaxBytesEdges(t *testing.T) {
	for name, mk := range kvtest.Backends {
		seeds := 40
		if name == "pebble" {
			seeds = 3
		}
		t.Run(name, func(t *testing.T) {
			profile := generator.LookupCuts()
			ops := generator.ControlPlaneOps(profile)
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				r := checker.NewRand(seed)
				a, spec := metadata.New(mk(t)), suites.NewSpecControlPlaneSubject()
				subject := metadataSubject{a}
				for i := range 120 {
					operation := ops(r, i)
					expected, _ := spec.Do(operation)
					actual, _ := subject.Do(operation)
					if diff := observation.DiffControlPlane(expected, actual); diff != "" {
						t.Fatalf("Seed %d, operation %d (%s): %s", seed, i, operation.Describe(), diff)
					}
					if operation.Call != nil {
						if lk, ok := operation.Call.Op.(core.FindSegments); ok {
							edgeLookups(t, subject, spec, lk, expected)
						}
					}
				}
				_ = a.Close()
			}
		})
	}
}

func TestTrimForgetsKeysNotOffsets(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	mustCommit(t, a, 0, commit("old", pb("a", 0, 5)))
	mustCommit(t, a, 5, commit("new", pb("a", 0, 5)))
	h := a.StateHash()
	if n, err := a.Trim(5); err != nil || n != 1 || a.Window() != 1 {
		t.Fatalf("Trim dropped %d err %v window %d", n, err, a.Window())
	}
	if a.StateHash() != h || a.End(core.NewTopicPartition("a", 0)) != 10 {
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

func TestInvalidAndUnknown(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	if a.CreateTopic("", 1) != core.ErrInvalid || a.CreateTopic("t", 0) != core.ErrInvalid {
		t.Fatal("CreateTopic must reject empty name and non-positive partitions")
	}
	if d := a.Exec(core.ControlPlaneCall{Op: core.FindSegments{From: -1}}).(core.ControlPlaneDone); d.Err != core.ErrInvalid {
		t.Fatalf("Lookup From<0: %+v", d)
	}
	if d := a.Exec(core.ControlPlaneCall{Op: nil}).(core.ControlPlaneDone); d.Err != core.ErrInvalid {
		t.Fatalf("unknown op: %+v", d)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Exec of a non-ControlPlaneCall must panic")
		}
	}()
	a.Exec(core.Connect{})
}

// edgeLookups asks lk again with MaxBytes at, just below and just above every
// running total of Len in the expected answer.
func edgeLookups(t *testing.T, subject, spec suites.ControlPlaneSubject, lk core.FindSegments, expected observation.ControlPlaneResult) {
	t.Helper()
	var sum int64
	for _, r := range expected.Ranges {
		sum += r.Loc.Len
		for e := int64(-1); e <= 1; e++ {
			if sum+e <= 0 {
				continue
			}
			q := generator.NewCall(core.ControlPlaneCall{Op: core.FindSegments{TopicPartition: lk.TopicPartition, From: lk.From, MaxBytes: int32(sum + e)}})
			want, _ := spec.Do(q)
			got, _ := subject.Do(q)
			if diff := observation.DiffControlPlane(want, got); diff != "" {
				t.Fatalf("For %s: %s", q.Describe(), diff)
			}
		}
	}
}

// TestInjectedBugsCaughtMetadataMem proves that the suite fails when the applier
// has a known bug. The applier hash is not the hash of the spec, so the helper
// skips the one bug that only a state hash can show.
func TestInjectedBugsCaughtMetadataMem(t *testing.T) {
	newCorrect := func() suites.ControlPlaneSubject { return newMetadataSubject(t, kvtest.Backends["mem"]) }
	injectedbug.RequireAllControlPlaneBugsDetected(t, newCorrect, 100)
}

// TestCreateTopicExclusive runs the create steps in log order on both stores.
// The exclusive create checks and writes in one step, so a second exclusive
// create of a name that exists is ErrTopicExists and changes nothing.
func TestCreateTopicExclusive(t *testing.T) {
	steps := []struct {
		name      string
		operation core.CreateTopic
		code      core.ErrCode
		parts     int32 // partitions of the topic after the step
	}{
		{"exclusive create of a new topic", core.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}, core.OK, 2},
		{"exclusive create again", core.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}, core.ErrTopicExists, 2},
		{"plain create again is still at least", core.CreateTopic{Name: "t", Partitions: 4}, core.OK, 4},
		{"plain create never shrinks", core.CreateTopic{Name: "t", Partitions: 1}, core.OK, 4},
		{"exclusive create after a plain one", core.CreateTopic{Name: "t", Partitions: 1, FailIfExists: true}, core.ErrTopicExists, 4},
		{"exclusive create of another new topic", core.CreateTopic{Name: "u", Partitions: 1, FailIfExists: true}, core.OK, 1},
		{"exclusive create, empty name", core.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}, core.ErrInvalid, 0},
		{"exclusive create, zero partitions", core.CreateTopic{Name: "z", Partitions: 0, FailIfExists: true}, core.ErrInvalid, 0},
	}
	for name, mk := range kvtest.Backends {
		t.Run(name, func(t *testing.T) {
			a := metadata.New(mk(t))
			t.Cleanup(func() { _ = a.Close() })
			for i, s := range steps {
				before := a.StateHash()
				d := a.Exec(core.ControlPlaneCall{Req: core.RequestID(i), Op: s.operation}).(core.ControlPlaneDone)
				if d.Err != s.code {
					t.Fatalf("%s: code %v, want %v", s.name, d.Err, s.code)
				}
				if s.code != core.OK && a.StateHash() != before {
					t.Fatalf("%s: the state changed on a failed create", s.name)
				}
				if s.code == core.OK && d.Outcome != core.Applied {
					t.Fatalf("%s: outcome %v, want Applied", s.name, d.Outcome)
				}
				if s.parts == 0 {
					continue
				}
				got, err := a.Describe(core.DescribeTopics{Topics: []string{s.operation.Name}})
				if err != nil {
					t.Fatal(err)
				}
				if topics := got.Resp.(core.DescribeTopicsResp).Topics; len(topics) != 1 || topics[0].Partitions != s.parts {
					t.Fatalf("%s: topics %+v, want %d partitions", s.name, topics, s.parts)
				}
			}
		})
	}
}

func TestAssignedStates(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	operation := commit("k1", pb("a", 0, 3), pb("a", 1, 2))
	first := mustCommit(t, a, 5, operation)
	want := first.Resp.(core.CommitSegmentResp).Assigned

	rs, st, err := a.Assigned(5, operation)
	if err != nil || st != metadata.OffsetOwnedHere || !reflect.DeepEqual(rs, want) {
		t.Fatalf("here: %v %v %+v", err, st, rs)
	}
	if _, st, _ = a.Assigned(9, operation); st != metadata.OffsetOwnedElsewhere { // a replay at a later slot
		t.Fatalf("other slot: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("k1", pb("a", 0, 4))); st != metadata.OffsetOwnedElsewhere { // a conflict
		t.Fatalf("conflict: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("nope", pb("a", 0, 1))); st != metadata.OffsetOwnerUnknown {
		t.Fatalf("unknown: %v", st)
	}
	if _, err = a.Trim(6); err != nil {
		t.Fatal(err)
	}
	if _, st, _ = a.Assigned(5, operation); st != metadata.OffsetOwnerUnknown { // forgotten by the window
		t.Fatalf("trimmed: %v", st)
	}
}

func TestCommitWithWriterBases(t *testing.T) {
	a := metadata.NewMem()
	operation := core.CommitSegment{
		SegmentKey: "seg/1",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("t", 0), Records: 3, Loc: core.SegmentRef{Key: "o", Off: 0, Len: 1}},
		},
	}
	d := a.Apply(1, metadata.Entry{
		ID: "w/1", Kind: metadata.KindCommit, Op: operation,
		WriterBases: []core.Offset{5},
	})
	if d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatal(d)
	}
	response := core.ResponseAs[core.CommitSegmentResp](d)
	if len(response.Assigned) != 1 || response.Assigned[0].Base != 5 || response.Assigned[0].Count != 3 {
		t.Fatalf("%+v", response)
	}
	if a.End(core.NewTopicPartition("t", 0)) != 8 {
		t.Fatal(a.End(core.NewTopicPartition("t", 0)))
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
