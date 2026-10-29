package stateupdater_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/controlplane/stateupdater/pebblestore"
	"manifester/internal/core"
	"manifester/internal/verify/speccheck/checker"
	"manifester/internal/verify/speccheck/checker/generator"
	"manifester/internal/verify/speccheck/checker/injectedbug"
	"manifester/internal/verify/speccheck/checker/observation"
	"manifester/internal/verify/speccheck/checker/runcontrol"
	"manifester/internal/verify/speccheck/checker/suites"
)

func newPebble(t testing.TB) stateupdater.KeyValueStore {
	t.Helper()
	kv, err := pebblestore.Open(filepath.Join(t.TempDir(), "state"), pebblestore.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

var backends = map[string]func(testing.TB) stateupdater.KeyValueStore{
	"mem":    func(testing.TB) stateupdater.KeyValueStore { return stateupdater.NewMemoryKeyValueStore() },
	"pebble": newPebble,
}

// applierSubject runs the StateUpdater directly as a suite subject. The applier
// hash is not the hash of the spec, so StateHash says no.
type applierSubject struct{ a *stateupdater.StateUpdater }

func (s applierSubject) Do(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
	switch {
	case op.CreateTopic != nil:
		return observation.ControlPlaneResult{Code: s.a.CreateTopic(op.CreateTopic.Name, op.CreateTopic.Partitions)}, nil
	case op.Call != nil:
		done, ok := s.a.Exec(*op.Call).(core.ControlPlaneDone)
		if !ok {
			return observation.ControlPlaneResult{}, errors.New("the applier did not return a CPDone")
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (applierSubject) StateHash() (uint64, bool) { return 0, false }

func newApplierSubject(t testing.TB, mk func(testing.TB) stateupdater.KeyValueStore) suites.ControlPlaneSubject {
	a := stateupdater.New(mk(t))
	t.Cleanup(func() { _ = a.Close() })
	return applierSubject{a}
}

// runSuite runs the shared control plane suite on the applier for every
// profile. Pebble is slower, so it passes a smaller default number of seeds.
func runSuite(t *testing.T, mk func(testing.TB) stateupdater.KeyValueStore, defaultSeeds int) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, func() suites.ControlPlaneSubject { return newApplierSubject(t, mk) },
				runcontrol.Options(t, defaultSeeds))
		})
	}
}

func TestLockstepMem(t *testing.T)    { runSuite(t, backends["mem"], 20) }
func TestLockstepPebble(t *testing.T) { runSuite(t, backends["pebble"], 3) }

// TestLookupMaxBytesEdges checks what the shared suite does not: a Lookup with
// MaxBytes at, just below and just above every running total of Len in an
// answer. The stop rule is a >= on exactly these values, and the suite only
// uses fixed MaxBytes values.
func TestLookupMaxBytesEdges(t *testing.T) {
	for name, mk := range backends {
		seeds := 40
		if name == "pebble" {
			seeds = 3
		}
		t.Run(name, func(t *testing.T) {
			profile := generator.LookupCuts()
			ops := generator.ControlPlaneOps(profile)
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				r := checker.NewRand(seed)
				a, spec := stateupdater.New(mk(t)), suites.NewSpecControlPlaneSubject()
				subject := applierSubject{a}
				for i := range 120 {
					op := ops(r, i)
					expected, _ := spec.Do(op)
					actual, _ := subject.Do(op)
					if diff := observation.DiffControlPlane(expected, actual); diff != "" {
						t.Fatalf("Seed %d, operation %d (%s): %s", seed, i, op.Describe(), diff)
					}
					if op.Call != nil {
						if lk, ok := op.Call.Op.(core.FindSegments); ok {
							edgeLookups(t, subject, spec, lk, expected)
						}
					}
				}
				_ = a.Close()
			}
		})
	}
}

func commit(key string, bs ...core.PendingBatch) core.CommitSegment {
	return core.CommitSegment{SegmentKey: key, Batches: bs}
}

func pb(topic string, p int32, n int64) core.PendingBatch {
	return core.PendingBatch{TP: core.NewTopicPartition(topic, p), Records: n,
		Loc: core.SegmentRef{Key: topic, Off: n * 10, Len: n}}
}

func mustCommit(t *testing.T, a *stateupdater.StateUpdater, slot int64, op core.CommitSegment) core.ControlPlaneDone {
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
			a := stateupdater.New(mk(t))
			first := mustCommit(t, a, 0, commit("k0", pb("a", 0, 3), pb("a", 0, 2), pb("b", 7, 1)))
			mustCommit(t, a, 1, commit("k1", pb("a", 0, 4)))
			var buf bytes.Buffer
			if err := a.Snapshot(&buf, 2); err != nil {
				t.Fatal(err)
			}
			b := stateupdater.New(mk(t))
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
	a := stateupdater.New(stateupdater.NewMemoryKeyValueStore())
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

func TestRestoreRejectsDamage(t *testing.T) {
	a := stateupdater.New(stateupdater.NewMemoryKeyValueStore())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 2)))
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 1); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	cases := map[string][]byte{"empty": nil, "magic": []byte("XXXXXX"), "no end mark": good[:len(good)-1], "cut": good[:len(good)/2], "header only": good[:6]}
	for name, b := range cases {
		if _, err := stateupdater.New(stateupdater.NewMemoryKeyValueStore()).Restore(bytes.NewReader(b)); err == nil {
			t.Errorf("%s: restore accepted a damaged checkpoint", name)
		}
	}
}

func TestInvalidAndUnknown(t *testing.T) {
	a := stateupdater.New(stateupdater.NewMemoryKeyValueStore())
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
			t.Fatal("Exec of a non-CPCall must panic")
		}
	}()
	a.Exec(core.Connect{})
}

func TestStoreAdapter(t *testing.T) {
	ctx := t.Context()
	s := &stateupdater.Store{A: stateupdater.New(stateupdater.NewMemoryKeyValueStore())}
	if c, err := s.CreateTopic(ctx, "t", 3); c != core.OK || err != nil {
		t.Fatal(c, err)
	}
	resp, c, err := s.CommitSegment(ctx, commit("k", pb("t", 2, 4)))
	if err != nil || c != core.OK || resp.Assigned[0].Base != 0 {
		t.Fatalf("%+v %v %v", resp, c, err)
	}
	lk, c, _ := s.Lookup(ctx, core.FindSegments{TP: core.NewTopicPartition("t", 2)})
	if c != core.OK || lk.End != 4 || len(lk.Ranges) != 1 {
		t.Fatalf("%+v %v", lk, c)
	}
	if _, c, _ = s.Lookup(ctx, core.FindSegments{From: -1}); c != core.ErrInvalid {
		t.Fatal(c)
	}
	d, err := s.DescribeTopics(ctx, core.DescribeTopics{})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 3 {
		t.Fatalf("%+v %v", d, err)
	}
}

func TestCommitBatchEqualsSequential(t *testing.T) {
	cmds := []stateupdater.Command{
		{Slot: 0, Op: commit("k0", pb("a", 0, 3), pb("a", 1, 2))},
		{Slot: 1, Op: commit("k1", pb("a", 0, 4))},                // continues partition 0 inside the same transaction
		{Slot: 2, Op: commit("k0", pb("a", 0, 3), pb("a", 1, 2))}, // a replay of a key committed in this very batch
		{Slot: 3, Op: commit("k1", pb("a", 0, 1))},                // conflict
		{Slot: 4, Op: commit("bad")},
	}
	one, many := stateupdater.New(stateupdater.NewMemoryKeyValueStore()), stateupdater.New(stateupdater.NewMemoryKeyValueStore())
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
			q := generator.NewCall(core.ControlPlaneCall{Op: core.FindSegments{TP: lk.TP, From: lk.From, MaxBytes: int32(sum + e)}})
			want, _ := spec.Do(q)
			got, _ := subject.Do(q)
			if diff := observation.DiffControlPlane(want, got); diff != "" {
				t.Fatalf("For %s: %s", q.Describe(), diff)
			}
		}
	}
}

// TestPlantedBugsCaughtApplierMem proves that the suite fails when the applier
// has a known bug. The applier hash is not the hash of the spec, so the helper
// skips the one bug that only a state hash can show.
func TestPlantedBugsCaughtApplierMem(t *testing.T) {
	newCorrect := func() suites.ControlPlaneSubject { return newApplierSubject(t, backends["mem"]) }
	injectedbug.RequireAllControlPlaneBugsDetected(t, newCorrect, 100)
}
