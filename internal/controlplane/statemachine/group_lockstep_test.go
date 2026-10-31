package statemachine_test

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
	"mds/internal/verify/speccheck"
)

// Random lockstep for the consumer group commands. Each step is a log write or
// a read. The writes go to speccheck and to a stateupdater fold, and the
// reads are compared on both. Some runs take a checkpoint in the middle,
// restore it into a new fold, and keep comparing. At the end the same log is
// fed through the machine, and its reads are compared too.

var (
	runGroups  = []string{"g1", "g2", "g3", ""}
	runMembers = []string{"m1", "m2", "m3", "m4", "m5", ""}
	runTopics  = []string{"orders", "pay", ""}
)

// groupRun builds random steps. It asks speccheck for the current group
// state, so most commands are valid and some are not.
type groupRun struct {
	rng  *rand.Rand
	orc  *speccheck.ControlPlane
	now  int64
	seen map[core.ErrCode]int
}

func (r *groupRun) pick(xs []string) string { return xs[r.rng.IntN(len(xs))] }

func (r *groupRun) chance(pct int) bool { return r.rng.IntN(100) < pct }

func (r *groupRun) current(group string) core.DescribeGroupResp {
	return r.orc.Exec(core.ControlPlaneCall{Op: core.DescribeGroup{Group: group}}).(core.ControlPlaneDone).Resp.(core.DescribeGroupResp)
}

// subset picks n distinct members from runMembers.
func (r *groupRun) subset(n int) []string {
	var out []string
	for _, i := range r.rng.Perm(len(runMembers))[:n] {
		out = append(out, runMembers[i])
	}
	return out
}

func (r *groupRun) install() core.ControlPlaneOperation {
	g := r.pick(runGroups)
	cur := r.current(g).Generation
	gen := cur + 1
	switch q := r.rng.IntN(100); {
	case q < 15:
		gen = cur
	case q < 20:
		gen = cur + 2
	case q < 25:
		gen = r.rng.Int32N(5)
	}
	operation := core.GroupInstall{Group: g, ProtocolType: "consumer", Protocol: "range", Generation: gen, TimeMs: r.now}
	ids := r.subset(r.rng.IntN(4))
	for _, id := range ids {
		operation.Members = append(operation.Members, core.GroupMember{MemberID: id, ClientID: "c-" + id, SessionTimeoutMs: 10000,
			RebalanceTimeoutMs: 30000, Metadata: []byte(strings.Repeat("s", r.rng.IntN(3)))})
	}
	if len(ids) > 0 && r.chance(5) {
		operation.Members = append(operation.Members, operation.Members[0])
	}
	operation.Leader = r.pick(runMembers)
	if len(ids) > 0 && r.chance(80) {
		operation.Leader = ids[r.rng.IntN(len(ids))]
	}
	return operation
}

func (r *groupRun) assign() core.ControlPlaneOperation {
	g := r.pick(runGroups)
	cur := r.current(g)
	gen := cur.Generation
	if !r.chance(80) {
		gen += int32(r.rng.IntN(3)) - 1
	}
	operation := core.GroupAssign{Group: g, Generation: gen, Leader: cur.Leader, TimeMs: r.now}
	if !r.chance(80) {
		operation.Leader = r.pick(runMembers)
	}
	for _, m := range cur.Members {
		if r.chance(70) {
			operation.Assignments = append(operation.Assignments, core.GroupAssignment{Member: m.MemberID, Bytes: []byte("a-" + m.MemberID)})
		}
	}
	if r.chance(10) {
		operation.Assignments = append(operation.Assignments, core.GroupAssignment{Member: "zz", Bytes: []byte("z")})
	}
	if r.chance(1) {
		operation.Assignments = append(operation.Assignments, core.GroupAssignment{Member: "zz", Bytes: bytes.Repeat([]byte("x"), 300<<10)})
	}
	return operation
}

func (r *groupRun) remove() core.ControlPlaneOperation {
	reason := "leave"
	if r.chance(30) {
		reason = "expire"
	}
	return core.GroupRemove{Group: r.pick(runGroups), Members: r.subset(r.rng.IntN(3)), TimeMs: r.now, Reason: reason}
}

func (r *groupRun) commit() core.ControlPlaneOperation {
	g := r.pick(runGroups)
	cur := r.current(g)
	gen := cur.Generation
	switch q := r.rng.IntN(100); {
	case q < 15:
		gen = -1
	case q < 50:
		gen = int32(r.rng.IntN(6)) - 2
	}
	operation := core.OffsetCommit{Group: g, Member: r.pick(runMembers), Generation: gen, TimeMs: r.now}
	if r.chance(15) {
		operation.Member = ""
	}
	if r.chance(40) && len(cur.Members) > 0 {
		// a valid managed commit: a real member at the current generation
		operation.Member = cur.Members[r.rng.IntN(len(cur.Members))].MemberID
		operation.Generation = cur.Generation
	}
	for range r.rng.IntN(4) {
		o := core.PartitionOffset{TopicPartition: core.NewTopicPartition(r.pick(append(runTopics, "ghost")), int32(r.rng.IntN(4)-1))}
		o.Offset = int64(r.rng.IntN(10))
		if r.chance(10) {
			o.Offset = -1
		}
		o.LeaderEpoch = int32(r.rng.IntN(3))
		o.Metadata = strings.Repeat("m", r.rng.IntN(3))
		operation.Offsets = append(operation.Offsets, o)
	}
	if r.chance(1) {
		operation.Offsets = append(operation.Offsets, core.PartitionOffset{TopicPartition: core.NewTopicPartition("orders", 0), Offset: 1,
			Metadata: strings.Repeat("m", 256<<10)})
	}
	return operation
}

func (r *groupRun) write() core.ControlPlaneOperation {
	switch p := r.rng.IntN(100); {
	case p < 5:
		return core.CreateTopic{Name: r.pick(runTopics), Partitions: int32(r.rng.IntN(4)), FailIfExists: r.chance(50)}
	case p < 20:
		return r.install()
	case p < 40:
		return r.assign()
	case p < 55:
		return r.remove()
	}
	return r.commit()
}

func (r *groupRun) read() core.ControlPlaneOperation {
	switch r.rng.IntN(3) {
	case 0:
		return core.DescribeGroup{Group: r.pick(runGroups)}
	case 1:
		operation := core.OffsetFetch{Group: r.pick(runGroups)}
		for n := r.rng.IntN(4); n > 0 && r.chance(50); n-- {
			operation.Partitions = append(operation.Partitions, core.NewTopicPartition(r.pick(runTopics[:2]), int32(r.rng.IntN(4))))
		}
		return operation
	}
	return core.ListGroups{}
}

// entryFor is the log entry of a write.
func entryFor(t *testing.T, operation core.ControlPlaneOperation, slot int) stateupdater.Entry {
	t.Helper()
	if c, ok := operation.(core.CreateTopic); ok {
		return stateupdater.CreateEntry(c)
	}
	e, ok := stateupdater.GroupEntry(operation)
	if !ok {
		t.Fatalf("slot %d: %T is not a write", slot, operation)
	}
	return e
}

// sameAnswer compares two answers. Req is not compared. Slice 1's CreateTopic
// apply path sets no Outcome, so only its Err is compared.
func sameAnswer(t *testing.T, where string, operation core.ControlPlaneOperation, got, want core.ControlPlaneDone) {
	t.Helper()
	got.Req, want.Req = 0, 0
	if _, ok := operation.(core.CreateTopic); ok {
		got.Outcome = want.Outcome
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: %T\n got  %+v\n want %+v", where, operation, got, want)
	}
}

// groupLockstep runs one seed. crashAt, when >= 0, is the step where the fold
// is checkpointed, dropped and restored.
func groupLockstep(t *testing.T, seed int64, crashAt int, seen map[core.ErrCode]int) {
	t.Helper()
	rng := rand.New(rand.NewPCG(uint64(seed), 0x6a)) //nolint:gosec // G404: deterministic test PRNG
	r := &groupRun{rng: rng, orc: speccheck.NewControlPlane(), seen: seen}
	fold := stateupdater.NewMem()
	defer func() { _ = fold.Close() }()
	var writes []stateupdater.Entry
	const steps = 400
	for i := range steps {
		r.now = int64(i) * 10
		if i == crashAt {
			var buf bytes.Buffer
			if err := fold.Snapshot(&buf, int64(len(writes))); err != nil {
				t.Fatal(err)
			}
			_ = fold.Close()
			fold = stateupdater.NewMem()
			if _, err := fold.Restore(&buf); err != nil {
				t.Fatal(err)
			}
		}
		if r.chance(25) {
			operation := r.read()
			got := fold.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
			want := r.orc.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
			sameAnswer(t, "read", operation, got, want)
			continue
		}
		operation := r.write()
		slot := len(writes)
		e := entryFor(t, operation, slot)
		writes = append(writes, e)
		got := fold.Apply(int64(slot), e)
		want := r.orc.Exec(core.ControlPlaneCall{Req: core.RequestID(slot), Op: operation}).(core.ControlPlaneDone)
		sameAnswer(t, "write", operation, got, want)
		seen[got.Err]++
		if response, ok := got.Resp.(core.OffsetCommitResp); ok {
			for _, c := range response.Errs {
				seen[c]++
			}
		}
	}
	checkMachineGroups(t, r, writes)
}

// checkMachineGroups feeds the writes through the machine and compares its
// reads with speccheck's state at the end.
func checkMachineGroups(t *testing.T, r *groupRun, writes []stateupdater.Entry) {
	t.Helper()
	m := newMachine(t, statemachine.Config{CatchUpEvery: core.Millisecond, GapAfter: 0})
	feedContiguous(t, m, writes)
	var ops []core.ControlPlaneOperation
	ops = append(ops, core.ListGroups{})
	for _, g := range runGroups {
		ops = append(ops, core.DescribeGroup{Group: g}, core.OffsetFetch{Group: g})
	}
	for _, operation := range ops {
		got := m.Fold().Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
		want := r.orc.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
		sameAnswer(t, "machine read", operation, got, want)
	}
}

func TestGroupLockstepRandom(t *testing.T) {
	seen := map[core.ErrCode]int{}
	for seed := int64(1); seed <= 8; seed++ {
		t.Run("plain", func(t *testing.T) { groupLockstep(t, seed, -1, seen) })
		t.Run("crash", func(t *testing.T) { groupLockstep(t, seed+100, 150+int(seed)*17, seen) })
	}
	// The run must reach every answer, or the random steps are too weak.
	for _, c := range []core.ErrCode{core.OK, core.ErrUnknownMember, core.ErrIllegalGeneration,
		core.ErrUnknownPartition, core.ErrInvalid, core.ErrTooLarge} {
		if seen[c] == 0 {
			t.Errorf("no write answered %v in any run (answers %v)", c, seen)
		}
	}
	t.Logf("write answers over all seeds: %v", seen)
}
