package veritas

import (
	"fmt"
	"slices"
	"testing"

	"manifester/core"
	"manifester/veritas/veritastest"
)

func node(n string) core.NodeRef { return core.NodeRef{Node: core.NodeID(n)} }

func tp(t string, p int32) core.TopicPartition { return core.TopicPartition{Topic: t, Partition: p} }

func call(c *ControlPlane, op core.CPOp) core.CPDone {
	return c.Exec(core.CPCall{Req: 9, Op: op}).(core.CPDone)
}

func register(t *testing.T, c *ControlPlane, n string) core.Epoch {
	t.Helper()
	d := call(c, core.Register{Self: node(n)})
	if d.Err != core.OK || d.Outcome != core.Applied || d.Req != 9 {
		t.Fatalf("register: %+v", d)
	}
	return d.Resp.(core.RegisterResp).Epoch
}

func commit(c *ControlPlane, n string, e core.Epoch, key string, bs ...core.PendingBatch) core.CPDone {
	return call(c, core.CommitSegment{SegmentKey: key, Owner: node(n), Epoch: e, Batches: bs})
}

func pb(t string, p int32, recs int64) core.PendingBatch {
	return core.PendingBatch{TP: tp(t, p), Records: recs, Loc: core.SegmentRef{Key: "o", Off: 0, Len: recs * 10}}
}

func assigned(t *testing.T, d core.CPDone) []core.BatchRange {
	t.Helper()
	if d.Err != core.OK {
		t.Fatalf("commit: %+v", d)
	}
	return d.Resp.(core.CommitSegmentResp).Assigned
}

func TestRegisterEpochsMonotonicPerNode(t *testing.T) {
	c := NewControlPlane()
	a1, b1, a2 := register(t, c, "a"), register(t, c, "b"), register(t, c, "a")
	if a1 != 1 || b1 != 1 || a2 != 2 {
		t.Fatalf("epochs %d %d %d", a1, b1, a2)
	}
}

func TestCommitContiguousAcrossPartitions(t *testing.T) {
	c := NewControlPlane()
	e := register(t, c, "n")
	r1 := assigned(t, commit(c, "n", e, "s1", pb("t", 0, 3), pb("t", 1, 2), pb("t", 0, 4)))
	if r1[0].Base != 0 || r1[1].Base != 0 || r1[2].Base != 3 {
		t.Fatalf("s1: %+v", r1)
	}
	r2 := assigned(t, commit(c, "n", e, "s2", pb("t", 0, 1), pb("u", 0, 5)))
	if r2[0].Base != 7 || r2[1].Base != 0 {
		t.Fatalf("s2: %+v", r2)
	}
	if c.HighWatermark(tp("t", 0)) != 8 || c.HighWatermark(tp("t", 1)) != 2 || c.HighWatermark(tp("x", 0)) != 0 {
		t.Fatal("watermarks")
	}
}

func TestCommitIdempotent(t *testing.T) {
	c := NewControlPlane()
	e := register(t, c, "n")
	first := assigned(t, commit(c, "n", e, "s1", pb("t", 0, 3)))
	assigned(t, commit(c, "n", e, "s2", pb("t", 0, 2))) // moves the watermark on
	h := c.StateHash()
	d := commit(c, "n", e, "s1", pb("t", 0, 3))
	if d.Outcome != core.Applied || !slices.Equal(assigned(t, d), first) {
		t.Fatalf("retry returned %+v, want %+v", d, first)
	}
	if c.StateHash() != h || c.HighWatermark(tp("t", 0)) != 5 {
		t.Fatal("retry changed state")
	}
	// Even after the owner is fenced, a retry gets the original answer.
	register(t, c, "n")
	if got := assigned(t, commit(c, "n", e, "s1", pb("t", 0, 3))); !slices.Equal(got, first) {
		t.Fatalf("stale retry: %+v", got)
	}
	if d := commit(c, "n", e, "s1", pb("t", 0, 4)); d.Err != core.ErrConflict {
		t.Fatalf("different batches under same key: %+v", d)
	}
}

func TestCommitFencing(t *testing.T) {
	c := NewControlPlane()
	if d := commit(c, "n", 0, "s0", pb("t", 0, 1)); d.Err != core.ErrFenced {
		t.Fatalf("unregistered: %+v", d)
	}
	old := register(t, c, "n")
	cur := register(t, c, "n")
	h := c.StateHash()
	for _, e := range []core.Epoch{old, cur + 1} {
		d := commit(c, "n", e, "s1", pb("t", 0, 1))
		if d.Err != core.ErrFenced || d.Outcome != core.NotApplied || d.Resp != nil {
			t.Fatalf("epoch %d: %+v", e, d)
		}
	}
	if c.StateHash() != h {
		t.Fatal("fenced commit changed state")
	}
	assigned(t, commit(c, "n", cur, "s1", pb("t", 0, 1)))
	// Epochs are per node: another node's epoch does not help.
	if d := commit(c, "m", cur, "s2", pb("t", 0, 1)); d.Err != core.ErrFenced {
		t.Fatalf("other node: %+v", d)
	}
}

func TestCommitInvalid(t *testing.T) {
	c := NewControlPlane()
	e := register(t, c, "n")
	h := c.StateHash()
	for _, d := range []core.CPDone{
		commit(c, "n", e, ""),
		commit(c, "n", e, "k"),
		commit(c, "n", e, "k", pb("t", 0, 0)),
		commit(c, "n", e, "k", pb("", 0, 1)),
		commit(c, "n", e, "k", pb("t", -1, 1)),
		// valid first batch, invalid second: atomic, nothing applied
		commit(c, "n", e, "k", pb("t", 0, 1), pb("t", 1, 0)),
	} {
		if d.Err != core.ErrInvalid || d.Outcome != core.NotApplied {
			t.Fatalf("%+v", d)
		}
	}
	if c.StateHash() != h {
		t.Fatal("invalid commit changed state")
	}
}

func TestLookup(t *testing.T) {
	c := NewControlPlane()
	e := register(t, c, "n")
	assigned(t, commit(c, "n", e, "s1", pb("t", 0, 3)))
	assigned(t, commit(c, "n", e, "s2", pb("t", 0, 2), pb("t", 1, 1)))
	assigned(t, commit(c, "n", e, "s3", pb("t", 0, 4)))
	look := func(p int32, from core.Offset, max int32) core.LookupResp {
		d := call(c, core.Lookup{TP: tp("t", p), From: from, MaxBytes: max})
		if d.Err != core.OK {
			t.Fatalf("lookup: %+v", d)
		}
		return d.Resp.(core.LookupResp)
	}
	bases := func(r core.LookupResp) []core.Offset {
		var out []core.Offset
		for _, x := range r.Ranges {
			out = append(out, x.Base)
		}
		return out
	}
	if r := look(0, 0, 0); !slices.Equal(bases(r), []core.Offset{0, 3, 5}) || r.HighWatermark != 9 {
		t.Fatalf("from 0: %+v", r)
	}
	if r := look(0, 4, 0); !slices.Equal(bases(r), []core.Offset{3, 5}) { // batch 3..4 holds offset 4
		t.Fatalf("from 4: %+v", r)
	}
	if r := look(0, 5, 0); !slices.Equal(bases(r), []core.Offset{5}) {
		t.Fatalf("from 5: %+v", r)
	}
	if r := look(0, 9, 0); len(r.Ranges) != 0 || r.HighWatermark != 9 {
		t.Fatalf("from hwm: %+v", r)
	}
	if r := look(0, 0, 1); !slices.Equal(bases(r), []core.Offset{0}) { // at least one
		t.Fatalf("max 1: %+v", r)
	}
	if r := look(0, 0, 50); !slices.Equal(bases(r), []core.Offset{0, 3}) { // 30+20 >= 50
		t.Fatalf("max 50: %+v", r)
	}
	if r := look(7, 0, 0); len(r.Ranges) != 0 || r.HighWatermark != 0 {
		t.Fatalf("unknown partition: %+v", r)
	}
	if d := call(c, core.Lookup{TP: tp("t", 0), From: -1}); d.Err != core.ErrInvalid {
		t.Fatalf("negative from: %+v", d)
	}
}

func TestDescribeTopics(t *testing.T) {
	c := NewControlPlane()
	e := register(t, c, "n")
	if c.CreateTopic("b", 3) != core.OK || c.CreateTopic("", 1) != core.ErrInvalid || c.CreateTopic("x", 0) != core.ErrInvalid {
		t.Fatal("CreateTopic")
	}
	assigned(t, commit(c, "n", e, "s1", pb("a", 4, 1), pb("b", 1, 1))) // a is created implicitly
	desc := func(names ...string) []core.TopicInfo {
		return call(c, core.DescribeTopics{Topics: names}).Resp.(core.DescribeTopicsResp).Topics
	}
	want := []core.TopicInfo{{Name: "a", Partitions: 5}, {Name: "b", Partitions: 3}}
	if got := desc(); !slices.Equal(got, want) {
		t.Fatalf("all: %+v", got)
	}
	if got := desc("b", "nope"); !slices.Equal(got, want[1:]) {
		t.Fatalf("some: %+v", got)
	}
	c.CreateTopic("b", 2) // never shrinks
	if got := desc("b"); got[0].Partitions != 3 {
		t.Fatalf("shrunk: %+v", got)
	}
}

// ---- lockstep invariants ----

type cpExec interface {
	Exec(core.IOAction) core.Completion
	StateHash() uint64
}

// cpModel is what the check believes: epochs, committed segments and per
// partition counts, kept independently of the spec.
type cpModel struct {
	epoch map[string]core.Epoch
	segs  map[string][]core.BatchRange
	count map[core.TopicPartition]int64
}

type cpState struct {
	cp cpExec
	m  cpModel
}

// cpOp is a compact operation; Kind 0 Register, 1 Commit, 2 Lookup, 3 Describe.
type cpOp struct {
	Kind  int
	Node  int
	Epoch uint64 // commit: epoch presented
	Seg   int    // commit: segment key index
	TP    int    // lookup: partition index
	From  int64
}

func (o cpOp) nodeRef() core.NodeRef { return node(fmt.Sprintf("n%d", o.Node)) }

var cpTPs = []core.TopicPartition{tp("t", 0), tp("t", 1), tp("u", 0)}

// segBatches is a fixed function of the segment index, so a retry of a key
// always carries the same batches.
func segBatches(seg int) []core.PendingBatch {
	var bs []core.PendingBatch
	for i := 0; i <= seg%3; i++ {
		b := pb(cpTPs[(seg+i)%3].Topic, cpTPs[(seg+i)%3].Partition, int64(1+(seg+i)%4))
		b.Loc.Key = fmt.Sprintf("obj%d", seg)
		bs = append(bs, b)
	}
	return bs
}

func cpCheck(impl func() cpExec) veritastest.Check[*cpState, cpOp] {
	return veritastest.Check[*cpState, cpOp]{
		Name: "cpOp",
		New: func() *cpState {
			return &cpState{impl(), cpModel{map[string]core.Epoch{}, map[string][]core.BatchRange{}, map[core.TopicPartition]int64{}}}
		},
		Gen: func(r *veritastest.Rand, i int) cpOp {
			o := cpOp{Kind: r.Intn(4), Node: r.Intn(2), Epoch: uint64(r.Intn(4)), Seg: r.Intn(5), TP: r.Intn(3), From: int64(r.Intn(8))}
			if o.Kind == 3 {
				o.Kind = 1 // weight commits
			}
			return o
		},
		Step: func(s *cpState, o cpOp) error {
			do := func(op core.CPOp) core.CPDone { return s.cp.Exec(core.CPCall{Req: 1, Op: op}).(core.CPDone) }
			switch o.Kind {
			case 0:
				d := do(core.Register{Self: o.nodeRef()})
				n := string(o.nodeRef().Node)
				s.m.epoch[n]++
				if d.Err != core.OK || d.Resp != (core.RegisterResp{Epoch: s.m.epoch[n]}) {
					return fmt.Errorf("register %s: %+v, want epoch %d", n, d, s.m.epoch[n])
				}
			case 1:
				key := fmt.Sprintf("seg%d", o.Seg)
				op := core.CommitSegment{SegmentKey: key, Owner: o.nodeRef(), Epoch: core.Epoch(o.Epoch), Batches: segBatches(o.Seg)}
				before := s.cp.StateHash()
				d := do(op)
				if orig, done := s.m.segs[key]; done {
					if d.Err != core.OK || !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, orig) {
						return fmt.Errorf("retry of %s: %+v, want %+v", key, d, orig)
					}
					if s.cp.StateHash() != before {
						return fmt.Errorf("retry of %s changed state", key)
					}
					return nil
				}
				if cur := s.m.epoch[string(o.nodeRef().Node)]; cur == 0 || core.Epoch(o.Epoch) != cur {
					if d.Err != core.ErrFenced || s.cp.StateHash() != before {
						return fmt.Errorf("stale commit not fenced cleanly: %+v", d)
					}
					return nil
				}
				var want []core.BatchRange
				for _, b := range op.Batches {
					want = append(want, core.BatchRange{TP: b.TP, Base: core.Offset(s.m.count[b.TP]), Count: b.Records, Loc: b.Loc})
					s.m.count[b.TP] += b.Records
				}
				s.m.segs[key] = want
				if d.Err != core.OK || !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, want) {
					return fmt.Errorf("commit %s: %+v, want %+v", key, d, want)
				}
				// Idempotence: asking again changes nothing and repeats the answer.
				after := s.cp.StateHash()
				if d2 := do(op); d2.Err != core.OK || !slices.Equal(d2.Resp.(core.CommitSegmentResp).Assigned, want) || s.cp.StateHash() != after {
					return fmt.Errorf("commit %s not idempotent: %+v", key, d2)
				}
			case 2:
				d := do(core.Lookup{TP: cpTPs[o.TP], From: core.Offset(o.From)})
				var want []core.BatchRange
				for _, k := range []string{"seg0", "seg1", "seg2", "seg3", "seg4"} {
					for _, r := range s.m.segs[k] {
						if r.TP == cpTPs[o.TP] && int64(r.Base)+r.Count > o.From {
							want = append(want, r)
						}
					}
				}
				slices.SortFunc(want, func(a, b core.BatchRange) int { return int(a.Base - b.Base) })
				got := d.Resp.(core.LookupResp)
				if d.Err != core.OK || !slices.Equal(got.Ranges, want) || int64(got.HighWatermark) != s.m.count[cpTPs[o.TP]] {
					return fmt.Errorf("lookup: %+v, want %+v", got, want)
				}
			}
			return nil
		},
		Invariant: func(s *cpState) error {
			for _, p := range cpTPs {
				d := s.cp.Exec(core.CPCall{Op: core.Lookup{TP: p}}).(core.CPDone)
				l := d.Resp.(core.LookupResp)
				next, sum := core.Offset(0), int64(0)
				for _, r := range l.Ranges {
					if r.Base != next {
						return fmt.Errorf("%v: range at %d, want %d (gap or overlap)", p, r.Base, next)
					}
					next, sum = r.Base+core.Offset(r.Count), sum+r.Count
				}
				if l.HighWatermark != next || int64(l.HighWatermark) != s.m.count[p] || sum != s.m.count[p] {
					return fmt.Errorf("%v: hwm %d, ranges end %d, committed %d", p, l.HighWatermark, next, s.m.count[p])
				}
			}
			return nil
		},
		Shrink: func(o cpOp) []cpOp {
			var out []cpOp
			if o.Node != 0 {
				x := o
				x.Node = 0
				out = append(out, x)
			}
			if o.Seg != 0 {
				x := o
				x.Seg = 0
				out = append(out, x)
			}
			if o.Epoch > 1 {
				x := o
				x.Epoch = o.Epoch - 1
				out = append(out, x)
			}
			return out
		},
		ShrinkAll: func(ops []cpOp) [][]cpOp {
			// Renaming a node in every op at once keeps register and commit paired.
			one := slices.Clone(ops)
			for i := range one {
				one[i].Node = 0
			}
			if slices.Equal(one, ops) {
				return nil // an unchanged candidate would "make progress" forever
			}
			return [][]cpOp{one}
		},
		Format: func(o cpOp) string { return fmt.Sprintf("%#v", o) },
		Ops:    80,
	}
}

func TestControlPlaneInvariants(t *testing.T) {
	veritastest.Run(t, cpCheck(func() cpExec { return NewControlPlane() }), veritastest.Options{Seeds: 300, Seed: -1})
}

// nonIdempotent gives every commit a fresh key, so a retry gets new offsets.
type nonIdempotent struct {
	*ControlPlane
	n int
}

func (b *nonIdempotent) Exec(a core.IOAction) core.Completion {
	if c, ok := a.(core.CPCall); ok {
		if cs, ok := c.Op.(core.CommitSegment); ok {
			b.n++
			cs.SegmentKey = fmt.Sprintf("%s#%d", cs.SegmentKey, b.n)
			c.Op = cs
			a = c
		}
	}
	return b.ControlPlane.Exec(a)
}

func TestControlPlaneCheckShrinksPlantedBug(t *testing.T) {
	c := cpCheck(func() cpExec { return &nonIdempotent{ControlPlane: NewControlPlane()} })
	var f *veritastest.Failure[cpOp]
	for seed := uint64(1); f == nil && seed <= 50; seed++ {
		f = c.RunSeed(seed)
	}
	if f == nil {
		t.Fatal("planted bug not found")
	}
	if len(f.Ops) != 2 || f.Ops[0].Kind != 0 || f.Ops[1].Kind != 1 || f.Ops[1].Epoch != 1 || f.Ops[1].Seg != 0 || f.Ops[1].Node != 0 {
		t.Fatalf("not minimal (want register, commit):\n%s", c.Report(f, ""))
	}
	t.Logf("example report:\n%s", c.Report(f, "go test ./veritas -run TestControlPlaneCheckShrinksPlantedBug"))
}
