package statemachine_test

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"manifester/internal/controlplane/statemachine"
	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
	"manifester/internal/verify/speccheck"
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
	op := core.GroupInstall{Group: g, ProtocolType: "consumer", Protocol: "range", Generation: gen, TimeMs: r.now}
	ids := r.subset(r.rng.IntN(4))
	for _, id := range ids {
		op.Members = append(op.Members, core.GroupMember{MemberID: id, ClientID: "c-" + id, SessionTimeoutMs: 10000,
			RebalanceTimeoutMs: 30000, Metadata: []byte(strings.Repeat("s", r.rng.IntN(3)))})
	}
	if len(ids) > 0 && r.chance(5) {
		op.Members = append(op.Members, op.Members[0])
	}
	op.Leader = r.pick(runMembers)
	if len(ids) > 0 && r.chance(80) {
		op.Leader = ids[r.rng.IntN(len(ids))]
	}
	return op
}

func (r *groupRun) assign() core.ControlPlaneOperation {
	g := r.pick(runGroups)
	cur := r.current(g)
	gen := cur.Generation
	if !r.chance(80) {
		gen += int32(r.rng.IntN(3)) - 1
	}
	op := core.GroupAssign{Group: g, Generation: gen, Leader: cur.Leader, TimeMs: r.now}
	if !r.chance(80) {
		op.Leader = r.pick(runMembers)
	}
	for _, m := range cur.Members {
		if r.chance(70) {
			op.Assignments = append(op.Assignments, core.GroupAssignment{Member: m.MemberID, Bytes: []byte("a-" + m.MemberID)})
		}
	}
	if r.chance(10) {
		op.Assignments = append(op.Assignments, core.GroupAssignment{Member: "zz", Bytes: []byte("z")})
	}
	if r.chance(1) {
		op.Assignments = append(op.Assignments, core.GroupAssignment{Member: "zz", Bytes: bytes.Repeat([]byte("x"), 300<<10)})
	}
	return op
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
	op := core.OffsetCommit{Group: g, Member: r.pick(runMembers), Generation: gen, TimeMs: r.now}
	if r.chance(15) {
		op.Member = ""
	}
	if r.chance(40) && len(cur.Members) > 0 {
		// a valid managed commit: a real member at the current generation
		op.Member = cur.Members[r.rng.IntN(len(cur.Members))].MemberID
		op.Generation = cur.Generation
	}
	for range r.rng.IntN(4) {
		o := core.PartitionOffset{TP: core.NewTopicPartition(r.pick(append(runTopics, "ghost")), int32(r.rng.IntN(4)-1))}
		o.Offset = int64(r.rng.IntN(10))
		if r.chance(10) {
			o.Offset = -1
		}
		o.LeaderEpoch = int32(r.rng.IntN(3))
		o.Metadata = strings.Repeat("m", r.rng.IntN(3))
		op.Offsets = append(op.Offsets, o)
	}
	if r.chance(1) {
		op.Offsets = append(op.Offsets, core.PartitionOffset{TP: core.NewTopicPartition("orders", 0), Offset: 1,
			Metadata: strings.Repeat("m", 256<<10)})
	}
	return op
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
		op := core.OffsetFetch{Group: r.pick(runGroups)}
		for n := r.rng.IntN(4); n > 0 && r.chance(50); n-- {
			op.Partitions = append(op.Partitions, core.NewTopicPartition(r.pick(runTopics[:2]), int32(r.rng.IntN(4))))
		}
		return op
	}
	return core.ListGroups{}
}

// entryFor is the log entry of a write.
func entryFor(t *testing.T, op core.ControlPlaneOperation, slot int) stateupdater.Entry {
	t.Helper()
	if c, ok := op.(core.CreateTopic); ok {
		return stateupdater.CreateEntry(c)
	}
	e, ok := stateupdater.GroupEntry(op)
	if !ok {
		t.Fatalf("slot %d: %T is not a write", slot, op)
	}
	return e
}

// sameAnswer compares two answers. Req is not compared. Slice 1's CreateTopic
// apply path sets no Outcome, so only its Err is compared.
func sameAnswer(t *testing.T, where string, op core.ControlPlaneOperation, got, want core.ControlPlaneDone) {
	t.Helper()
	got.Req, want.Req = 0, 0
	if _, ok := op.(core.CreateTopic); ok {
		got.Outcome = want.Outcome
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: %T\n got  %+v\n want %+v", where, op, got, want)
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
			op := r.read()
			got := fold.Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone)
			want := r.orc.Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone)
			sameAnswer(t, "read", op, got, want)
			continue
		}
		op := r.write()
		slot := len(writes)
		e := entryFor(t, op, slot)
		writes = append(writes, e)
		got := fold.Apply(int64(slot), e)
		want := r.orc.Exec(core.ControlPlaneCall{Req: core.RequestID(slot), Op: op}).(core.ControlPlaneDone)
		sameAnswer(t, "write", op, got, want)
		seen[got.Err]++
		if resp, ok := got.Resp.(core.OffsetCommitResp); ok {
			for _, c := range resp.Errs {
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
	for _, op := range ops {
		got := m.Fold().Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone)
		want := r.orc.Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone)
		sameAnswer(t, "machine read", op, got, want)
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
