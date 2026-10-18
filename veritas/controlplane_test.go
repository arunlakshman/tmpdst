package veritas

import (
	"fmt"
	"slices"
	"testing"

	"manifester/core"
	"manifester/veritas/veritastest"
)

func tp(t string, p int32) core.TopicPartition { return core.TopicPartition{Topic: t, Partition: p} }

func call(c *ControlPlane, op core.CPOp) core.CPDone {
	return c.Exec(core.CPCall{Req: 9, Op: op}).(core.CPDone)
}

func commit(c *ControlPlane, key string, bs ...core.PendingBatch) core.CPDone {
	return call(c, core.CommitSegment{SegmentKey: key, Batches: bs})
}

// pb is a batch of recs records expected at offset base.
func pb(t string, p int32, base core.Offset, recs int64) core.PendingBatch {
	return core.PendingBatch{TP: tp(t, p), Base: base, Records: recs, Loc: core.SegmentRef{Key: "o", Off: 0, Len: recs * 10}}
}

func assigned(t *testing.T, d core.CPDone) []core.BatchRange {
	t.Helper()
	if d.Err != core.OK {
		t.Fatalf("commit: %+v", d)
	}
	return d.Resp.(core.CommitSegmentResp).Assigned
}

func ends(t *testing.T, c *ControlPlane, tps ...core.TopicPartition) []core.Offset {
	t.Helper()
	d := call(c, core.EndOffsets{TPs: tps})
	if d.Err != core.OK || d.Outcome != core.NotApplied {
		t.Fatalf("ends: %+v", d)
	}
	return d.Resp.(core.EndOffsetsResp).Ends
}

func TestCommitContiguousAcrossPartitions(t *testing.T) {
	c := NewControlPlane()
	r1 := assigned(t, commit(c, "s1", pb("t", 0, 0, 3), pb("t", 1, 0, 2), pb("t", 0, 3, 4)))
	if r1[0].Base != 0 || r1[1].Base != 0 || r1[2].Base != 3 || r1[2].Count != 4 || r1[0].TP != tp("t", 0) {
		t.Fatalf("s1: %+v", r1)
	}
	r2 := assigned(t, commit(c, "s2", pb("t", 0, 7, 1), pb("u", 0, 0, 5)))
	if r2[0].Base != 7 || r2[1].Base != 0 {
		t.Fatalf("s2: %+v", r2)
	}
	if c.End(tp("t", 0)) != 8 || c.End(tp("t", 1)) != 2 || c.End(tp("u", 0)) != 5 || c.End(tp("x", 0)) != 0 {
		t.Fatal("ends")
	}
}

// TestCASByBase: a run may be appended only at the partition's next offset.
func TestCASByBase(t *testing.T) {
	c := NewControlPlane()
	assigned(t, commit(c, "s1", pb("t", 0, 0, 3)))
	h := c.StateHash()
	for name, bs := range map[string][]core.PendingBatch{
		"taken: same base":        {pb("t", 0, 0, 3)},
		"taken: inside a run":     {pb("t", 0, 2, 1)},
		"taken after other ok":    {pb("t", 1, 0, 1), pb("t", 0, 1, 1)},
		"conflict beats hole":     {pb("t", 0, 0, 1), pb("t", 1, 5, 1)},
		"taken within request":    {pb("t", 0, 3, 2), pb("t", 0, 3, 2)},
		"chain broken in request": {pb("t", 0, 3, 2), pb("t", 0, 4, 1)},
	} {
		d := commit(c, "k", bs...)
		if d.Err != core.ErrConflict || d.Outcome != core.NotApplied || d.Resp != nil {
			t.Fatalf("%s: %+v", name, d)
		}
		if c.StateHash() != h {
			t.Fatalf("%s: a conflict changed state", name)
		}
	}
	// A base above the next offset would leave a hole: malformed, not a lost race.
	for name, bs := range map[string][]core.PendingBatch{
		"hole":                     {pb("t", 0, 4, 1)},
		"hole on empty":            {pb("t", 1, 1, 1)},
		"hole within request":      {pb("t", 0, 3, 2), pb("t", 0, 6, 1)},
		"hole after ok batch":      {pb("t", 1, 0, 1), pb("t", 0, 9, 1)},
		"first failure is invalid": {pb("t", 0, 4, 1), pb("t", 0, 0, 1)},
	} {
		d := commit(c, "k", bs...)
		if d.Err != core.ErrInvalid || c.StateHash() != h {
			t.Fatalf("%s: %+v", name, d)
		}
	}
	// Conflicts are all-or-nothing: the key and the free partition stay unused.
	if c.End(tp("t", 1)) != 0 {
		t.Fatal("partial apply")
	}
	// The key of a failed commit is reusable with fresh bases.
	r := assigned(t, commit(c, "k", pb("t", 1, 0, 1), pb("t", 0, 3, 1)))
	if r[1].Base != 3 || c.End(tp("t", 0)) != 4 || c.End(tp("t", 1)) != 1 {
		t.Fatalf("retry: %+v", r)
	}
}

// TestCASRace: two writers read the same end offset; the second loses, re-reads
// and wins on the next try. No offset is ever assigned twice.
func TestCASRace(t *testing.T) {
	c := NewControlPlane()
	assigned(t, commit(c, "s0", pb("t", 0, 0, 2)))
	a, b := ends(t, c, tp("t", 0))[0], ends(t, c, tp("t", 0))[0]
	assigned(t, commit(c, "A", pb("t", 0, a, 3)))
	if d := commit(c, "B", pb("t", 0, b, 4)); d.Err != core.ErrConflict {
		t.Fatalf("loser: %+v", d)
	}
	b = ends(t, c, tp("t", 0))[0]
	if b != 5 {
		t.Fatalf("end %d", b)
	}
	assigned(t, commit(c, "B", pb("t", 0, b, 4)))
	l := call(c, core.Lookup{TP: tp("t", 0)}).Resp.(core.LookupResp)
	if l.End != 9 || len(l.Ranges) != 3 || l.Ranges[1].Base != 2 || l.Ranges[2].Base != 5 {
		t.Fatalf("%+v", l)
	}
}

func TestCommitIdempotent(t *testing.T) {
	c := NewControlPlane()
	first := assigned(t, commit(c, "s1", pb("t", 0, 0, 3)))
	assigned(t, commit(c, "s2", pb("t", 0, 3, 2))) // moves the end on
	h := c.StateHash()
	d := commit(c, "s1", pb("t", 0, 0, 3))
	if d.Outcome != core.Applied || !slices.Equal(assigned(t, d), first) {
		t.Fatalf("replay returned %+v, want %+v", d, first)
	}
	if c.StateHash() != h || c.End(tp("t", 0)) != 5 {
		t.Fatal("replay changed state")
	}
	// A different request under the same key is a conflict, whatever the bases.
	for name, bs := range map[string][]core.PendingBatch{
		"records":    {pb("t", 0, 0, 4)},
		"base":       {pb("t", 0, 5, 3)},
		"partition":  {pb("t", 1, 0, 3)},
		"extra":      {pb("t", 0, 0, 3), pb("t", 1, 0, 1)},
		"loc":        {{TP: tp("t", 0), Base: 0, Records: 3, Loc: core.SegmentRef{Key: "other", Len: 30}}},
		"no batches": nil,
	} {
		if d := commit(c, "s1", bs...); d.Err != core.ErrConflict || d.Outcome != core.NotApplied {
			t.Fatalf("%s: %+v", name, d)
		}
	}
	if c.StateHash() != h {
		t.Fatal("conflicting replay changed state")
	}
}

// TestManyBatchSegment: one segment with many batches over few partitions
// chains bases per partition and replays as a whole.
func TestManyBatchSegment(t *testing.T) {
	c := NewControlPlane()
	assigned(t, commit(c, "pre", pb("t", 1, 0, 7)))
	var bs []core.PendingBatch
	next := map[int32]core.Offset{0: 0, 1: 7, 2: 0}
	for i := 0; i < 300; i++ {
		p := int32(i % 3)
		n := int64(1 + i%4)
		bs = append(bs, pb("t", p, next[p], n))
		next[p] += core.Offset(n)
	}
	rs := assigned(t, commit(c, "big", bs...))
	if len(rs) != len(bs) {
		t.Fatalf("%d ranges", len(rs))
	}
	for i, r := range rs {
		if r.Base != bs[i].Base || r.Count != bs[i].Records || r.TP != bs[i].TP || r.Loc != bs[i].Loc {
			t.Fatalf("range %d: %+v", i, r)
		}
	}
	for p, e := range next {
		if c.End(tp("t", p)) != e {
			t.Fatalf("end of %d: %d, want %d", p, c.End(tp("t", p)), e)
		}
	}
	// One bad batch at the end undoes the lot.
	h := c.StateHash()
	bad := slices.Clone(bs)
	for i := range bad {
		bad[i].Base += next[bad[i].TP.Partition]
	}
	bad = append(bad[:len(bs):len(bs)], pb("t", 0, 0, 1))
	if d := commit(c, "big2", bad...); d.Err == core.OK || c.StateHash() != h {
		t.Fatalf("%+v", d)
	}
	if d := commit(c, "big", bs...); !slices.Equal(assigned(t, d), rs) {
		t.Fatal("replay differs")
	}
}

func TestCommitInvalid(t *testing.T) {
	c := NewControlPlane()
	h := c.StateHash()
	neg := pb("t", 0, 0, 1)
	neg.Base = -1
	for _, d := range []core.CPDone{
		commit(c, ""),
		commit(c, "", pb("t", 0, 0, 1)),
		commit(c, "k"),
		commit(c, "k", pb("t", 0, 0, 0)),
		commit(c, "k", pb("", 0, 0, 1)),
		commit(c, "k", pb("t", -1, 0, 1)),
		commit(c, "k", neg),
		// valid first batch, invalid second: atomic, nothing applied
		commit(c, "k", pb("t", 0, 0, 1), pb("t", 1, 0, 0)),
	} {
		if d.Err != core.ErrInvalid || d.Outcome != core.NotApplied {
			t.Fatalf("%+v", d)
		}
	}
	if c.StateHash() != h {
		t.Fatal("invalid commit changed state")
	}
}

func TestEndOffsets(t *testing.T) {
	c := NewControlPlane()
	if got := ends(t, c); len(got) != 0 {
		t.Fatalf("empty request: %v", got)
	}
	assigned(t, commit(c, "s1", pb("t", 0, 0, 3), pb("t", 1, 0, 2), pb("t", 0, 3, 4)))
	got := ends(t, c, tp("t", 1), tp("nope", 0), tp("t", 0), tp("t", 1))
	if !slices.Equal(got, []core.Offset{2, 0, 7, 2}) {
		t.Fatalf("ends %v", got)
	}
}

func TestLookup(t *testing.T) {
	c := NewControlPlane()
	assigned(t, commit(c, "s1", pb("t", 0, 0, 3)))
	assigned(t, commit(c, "s2", pb("t", 0, 3, 2), pb("t", 1, 0, 1)))
	assigned(t, commit(c, "s3", pb("t", 0, 5, 4)))
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
	if r := look(0, 0, 0); !slices.Equal(bases(r), []core.Offset{0, 3, 5}) || r.End != 9 {
		t.Fatalf("from 0: %+v", r)
	}
	if r := look(0, 4, 0); !slices.Equal(bases(r), []core.Offset{3, 5}) { // batch 3..4 holds offset 4
		t.Fatalf("from 4: %+v", r)
	}
	if r := look(0, 5, 0); !slices.Equal(bases(r), []core.Offset{5}) {
		t.Fatalf("from 5: %+v", r)
	}
	if r := look(0, 9, 0); len(r.Ranges) != 0 || r.End != 9 {
		t.Fatalf("from end: %+v", r)
	}
	if r := look(0, 0, 1); !slices.Equal(bases(r), []core.Offset{0}) { // at least one
		t.Fatalf("max 1: %+v", r)
	}
	if r := look(0, 0, 50); !slices.Equal(bases(r), []core.Offset{0, 3}) { // 30+20 >= 50
		t.Fatalf("max 50: %+v", r)
	}
	if r := look(7, 0, 0); len(r.Ranges) != 0 || r.End != 0 {
		t.Fatalf("unknown partition: %+v", r)
	}
	if d := call(c, core.Lookup{TP: tp("t", 0), From: -1}); d.Err != core.ErrInvalid {
		t.Fatalf("negative from: %+v", d)
	}
}

func TestDescribeTopics(t *testing.T) {
	c := NewControlPlane()
	if c.CreateTopic("b", 3) != core.OK || c.CreateTopic("", 1) != core.ErrInvalid || c.CreateTopic("x", 0) != core.ErrInvalid {
		t.Fatal("CreateTopic")
	}
	assigned(t, commit(c, "s1", pb("a", 4, 0, 1), pb("b", 1, 0, 1))) // a is created implicitly
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

// cpModel is what the check believes: the request and answer of every
// committed segment and the end offset per partition, kept independently of
// the spec.
type cpModel struct {
	segs map[string]modelSeg
	end  map[core.TopicPartition]int64
}

type modelSeg struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

type cpState struct {
	cp cpExec
	m  cpModel
}

// cpOp is a compact operation. Kind 0 commit segment Seg built from the model's
// ends (Skew 0 fresh, 1 first base stale, 2 first base a hole), 1 replay of a
// committed segment, 2 lookup, 3 end offsets.
type cpOp struct {
	Kind int
	Seg  int // segment key index
	Skew int
	TP   int // lookup: partition index
	From int64
}

var cpTPs = []core.TopicPartition{tp("t", 0), tp("t", 1), tp("u", 0)}

// segBatches is a fixed function of the segment index and the model's ends, so
// a replay of a key can carry the very same batches. Bases chain per partition.
func (m *cpModel) segBatches(seg, skew int) []core.PendingBatch {
	next := map[core.TopicPartition]int64{}
	var bs []core.PendingBatch
	for i := 0; i <= seg%3; i++ {
		p := cpTPs[(seg+i)%3]
		e, ok := next[p]
		if !ok {
			e = m.end[p]
		}
		n := int64(1 + (seg+i)%4)
		b := pb(p.Topic, p.Partition, core.Offset(e), n)
		b.Loc.Key = fmt.Sprintf("obj%d", seg)
		if i == 0 && skew == 1 && e > 0 {
			b.Base--
		}
		if i == 0 && skew == 2 {
			b.Base++
		}
		bs = append(bs, b)
		next[p] = e + n
	}
	return bs
}

func cpCheck(impl func() cpExec) veritastest.Check[*cpState, cpOp] {
	return veritastest.Check[*cpState, cpOp]{
		Name: "cpOp",
		New: func() *cpState {
			return &cpState{impl(), cpModel{map[string]modelSeg{}, map[core.TopicPartition]int64{}}}
		},
		Gen: func(r *veritastest.Rand, i int) cpOp {
			o := cpOp{Kind: r.Intn(4), Seg: r.Intn(5), Skew: r.Intn(4) % 3, TP: r.Intn(3), From: int64(r.Intn(8))}
			if o.Kind == 3 && r.Intn(2) == 0 {
				o.Kind = 0 // weight commits
			}
			return o
		},
		Step: func(s *cpState, o cpOp) error {
			do := func(op core.CPOp) core.CPDone { return s.cp.Exec(core.CPCall{Req: 1, Op: op}).(core.CPDone) }
			key := fmt.Sprintf("seg%d", o.Seg)
			switch o.Kind {
			case 0, 1:
				return s.stepCommit(do, key, o)
			case 2:
				d := do(core.Lookup{TP: cpTPs[o.TP], From: core.Offset(o.From)})
				var want []core.BatchRange
				for _, k := range []string{"seg0", "seg1", "seg2", "seg3", "seg4"} {
					for _, r := range s.m.segs[k].ranges {
						if r.TP == cpTPs[o.TP] && int64(r.Base)+r.Count > o.From {
							want = append(want, r)
						}
					}
				}
				slices.SortFunc(want, func(a, b core.BatchRange) int { return int(a.Base - b.Base) })
				got := d.Resp.(core.LookupResp)
				if d.Err != core.OK || !slices.Equal(got.Ranges, want) || int64(got.End) != s.m.end[cpTPs[o.TP]] {
					return fmt.Errorf("lookup: %+v, want %+v", got, want)
				}
			case 3:
				d := do(core.EndOffsets{TPs: cpTPs})
				got := d.Resp.(core.EndOffsetsResp).Ends
				for i, p := range cpTPs {
					if d.Err != core.OK || int64(got[i]) != s.m.end[p] {
						return fmt.Errorf("end offsets: %+v, want %v", got, s.m.end)
					}
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
				if l.End != next || int64(l.End) != s.m.end[p] || sum != s.m.end[p] {
					return fmt.Errorf("%v: end %d, ranges end %d, committed %d", p, l.End, next, s.m.end[p])
				}
			}
			return nil
		},
		Shrink: func(o cpOp) []cpOp {
			var out []cpOp
			if o.Seg != 0 {
				x := o
				x.Seg = 0
				out = append(out, x)
			}
			if o.Skew != 0 {
				x := o
				x.Skew = 0
				out = append(out, x)
			}
			return out
		},
		ShrinkAll: func(ops []cpOp) [][]cpOp {
			// Renaming the segment in every op at once keeps commit and replay paired.
			one := slices.Clone(ops)
			for i := range one {
				one[i].Seg = 0
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

// stepCommit applies a commit or replay op to the spec and the model and
// compares them.
func (s *cpState) stepCommit(do func(core.CPOp) core.CPDone, key string, o cpOp) error {
	orig, committed := s.m.segs[key]
	var batches []core.PendingBatch
	switch {
	case o.Kind == 1 && !committed:
		return nil
	case o.Kind == 1:
		batches = orig.batches
	default:
		batches = s.m.segBatches(o.Seg, o.Skew)
	}
	op := core.CommitSegment{SegmentKey: key, Batches: batches}
	before := s.cp.StateHash()
	d := do(op)
	unchanged := func() error {
		if s.cp.StateHash() != before {
			return fmt.Errorf("%s: failed or replayed commit changed state", key)
		}
		return nil
	}
	if committed {
		if slices.Equal(orig.batches, batches) {
			if d.Err != core.OK || !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, orig.ranges) {
				return fmt.Errorf("replay of %s: %+v, want %+v", key, d, orig.ranges)
			}
		} else if d.Err != core.ErrConflict {
			return fmt.Errorf("different request under %s: %+v, want ErrConflict", key, d)
		}
		return unchanged()
	}
	if o.Skew == 2 { // a hole
		if d.Err != core.ErrInvalid {
			return fmt.Errorf("hole in %s: %+v, want ErrInvalid", key, d)
		}
		return unchanged()
	}
	if o.Skew == 1 && s.m.end[batches[0].TP] > 0 { // stale base
		if d.Err != core.ErrConflict {
			return fmt.Errorf("stale base in %s: %+v, want ErrConflict", key, d)
		}
		return unchanged()
	}
	var want []core.BatchRange
	for _, b := range batches {
		want = append(want, core.BatchRange{TP: b.TP, Base: b.Base, Count: b.Records, Loc: b.Loc})
		s.m.end[b.TP] += b.Records
	}
	s.m.segs[key] = modelSeg{batches, want}
	if d.Err != core.OK || !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, want) {
		return fmt.Errorf("commit %s: %+v, want %+v", key, d, want)
	}
	return nil
}

func TestControlPlaneInvariants(t *testing.T) {
	veritastest.Run(t, cpCheck(func() cpExec { return NewControlPlane() }), veritastest.Options{Seeds: 300, Seed: -1})
}

// nonIdempotent gives every commit a fresh key, so a replay is a new commit and loses the compare-and-set.
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
	if len(f.Ops) != 2 || f.Ops[0].Kind != 0 || f.Ops[1].Kind != 1 || f.Ops[0].Seg != 0 || f.Ops[1].Seg != 0 || f.Ops[0].Skew != 0 {
		t.Fatalf("not minimal (want commit, replay):\n%s", c.Report(f, ""))
	}
	t.Logf("example report:\n%s", c.Report(f, "go test ./veritas -run TestControlPlaneCheckShrinksPlantedBug"))
}
