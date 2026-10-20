package engines

import (
	"fmt"
	"testing"

	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/cpengine"
	"manifester/sim/engines/internal/enginetest"
	"manifester/sim/engines/logengine"
	"manifester/sim/engines/objstore"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

// Conformance C2: with every fault probability zero, an engine completes each
// operation exactly as its spec does and its cached spec hash tracks the
// spec's StateHash after every operation.

type engine interface {
	sim.Engine
	SpecStateHash() uint64
}

// rig is an engine under a test context plus a fresh spec to compare with.
type rig struct {
	eng  sim.Engine
	sh   func() uint64 // the engine's cached spec hash; nil when it has none (the log engine)
	run  func()        // drains the engine's work for one operation (default: the queue)
	ctx  *enginetest.Ctx
	from core.NodeRef
	spec func(op any) string // runs op on the spec, returns the rendered result
	hash func() uint64       // the spec's StateHash
	call func(op any) string // issues op to the engine; a non-empty result is immediate
}

// finish runs the engine and renders the one completion for req.
func (g *rig) finish(req core.ReqID) (string, error) {
	if g.run != nil {
		g.run()
	} else {
		g.ctx.Run()
	}
	c := g.ctx.Find(req)
	if c == nil {
		return "", fmt.Errorf("req %d: no completion", req)
	}
	return render(c), nil
}

func render(c core.Completion) string { return fmt.Sprintf("%+v", c) }

func reqOf(op any) core.ReqID {
	switch o := op.(type) {
	case core.IOAction:
		return o.ReqID()
	}
	panic("op")
}

type createTopic struct {
	Name  string
	Parts int32
}

func step(g *rig, op any) error {
	want := g.spec(op)
	got := g.call(op)
	if got == "" {
		var err error
		if got, err = g.finish(reqOf(op)); err != nil {
			return err
		}
	}
	if got != want {
		return fmt.Errorf("engine: %s\n  spec: %s", got, want)
	}
	if g.sh == nil {
		return nil
	}
	if h, w := g.sh(), g.hash(); h != w {
		return fmt.Errorf("state hash: engine %#x, spec %#x (after %s)", h, w, want)
	}
	return nil
}

func format(op any) string { return fmt.Sprintf("%#v", op) }

// ---- object store ----

func newObjRig() *rig {
	e, spec := objstore.New(sim.ObjectStoreFaults{}), veritas.NewObjectStore()
	g := &rig{eng: e, sh: e.SpecStateHash, ctx: enginetest.New(e, 1), from: enginetest.Ref("a"), hash: spec.StateHash}
	g.spec = func(op any) string { return render(spec.Exec(op.(core.IOAction))) }
	g.call = func(op any) string { e.Execute(g.ctx, g.from, op.(core.IOAction)); return "" }
	return g
}

func genObj(r *veritastest.Rand, i int) any {
	keys := []string{"", "a", "a/b", "a/c", "b"}
	prefixes := []string{"", "a", "a/", "b", "z"}
	req, key := core.ReqID(i+1), keys[r.Intn(len(keys))]
	switch r.Intn(7) {
	case 0, 1, 2:
		data := make([]byte, r.Intn(5))
		for j := range data {
			data[j] = byte('a' + r.Intn(26))
		}
		return core.ObjPut{Req: req, Key: key, Data: data}
	case 3, 4:
		return core.ObjGet{Req: req, Ref: core.SegmentRef{Key: key, Off: int64(r.Intn(7) - 1), Len: int64(r.Intn(7) - 1)}}
	case 5:
		return core.ObjDelete{Req: req, Key: key}
	}
	return core.ObjList{Req: req, Prefix: prefixes[r.Intn(len(prefixes))]}
}

// ---- control plane ----

func newCPRig() *rig {
	e, spec := cpengine.New(sim.ControlPlaneFaults{}), veritas.NewControlPlane()
	g := &rig{eng: e, sh: e.SpecStateHash, ctx: enginetest.New(e, 1), from: enginetest.Ref("a"), hash: spec.StateHash}
	g.spec = func(op any) string {
		if c, ok := op.(createTopic); ok {
			return spec.CreateTopic(c.Name, c.Parts).String()
		}
		return render(spec.Exec(op.(core.IOAction)))
	}
	g.call = func(op any) string {
		if c, ok := op.(createTopic); ok {
			return e.CreateTopic(c.Name, c.Parts).String()
		}
		e.Execute(g.ctx, g.from, op.(core.IOAction))
		return ""
	}
	return g
}

var (
	cpTopics = []string{"t1", "t2", "t3"}
)

func genCP(r *veritastest.Rand, i int) any {
	pick := func(xs []string) string { return xs[r.Intn(len(xs))] }
	req := core.ReqID(i + 1)
	tp := func() core.TopicPartition {
		return core.TopicPartition{Topic: pick(cpTopics), Partition: int32(r.Intn(3))}
	}
	call := func(op core.CPOp) any { return core.CPCall{Req: req, Op: op} }
	switch k := r.Intn(32); {
	case k < 4:
		return createTopic{Name: pick(append([]string{""}, cpTopics...)), Parts: int32(r.Intn(5)) - 1}
	case k < 20:
		key := fmt.Sprintf("seg%d", r.Intn(8))
		shape := key
		variant := 0
		if r.Intn(6) == 0 {
			variant = 1 + r.Intn(3)
		}
		if r.Intn(4) == 0 { // a retry of the same batches under a fresh key
			key = fmt.Sprintf("re%d", r.Intn(1000))
		}
		return call(core.CommitSegment{SegmentKey: key,
			Batches: genBatches(veritastest.NewRand(uint64(len(shape)*7+int(shape[len(shape)-1])*131+variant)), key)})
	case k < 28:
		return call(core.Lookup{TP: tp(), From: core.Offset(r.Intn(8) - 1), MaxBytes: int32(r.Intn(3)) * 60})
	default:
		d := core.DescribeTopics{}
		for j := r.Intn(3); j > 0; j-- {
			d.Topics = append(d.Topics, pick(append([]string{"nope"}, cpTopics...)))
		}
		return call(d)
	}
}

func genBatches(r *veritastest.Rand, key string) []core.PendingBatch {
	var out []core.PendingBatch
	n := r.Intn(4)
	for j := 0; j < n; j++ {
		b := core.PendingBatch{
			TP:      core.TopicPartition{Topic: cpTopics[r.Intn(2)], Partition: int32(r.Intn(2))},
			Records: 1 + int64(r.Intn(3)),
			Loc:     core.SegmentRef{Key: key, Off: int64(r.Intn(4)) * 100, Len: int64(1 + r.Intn(3)*50)},
		}
		out = append(out, b)
	}
	return out
}

// newLogRig is the log engine under the same operations as the spec. The
// engine's tick chain never drains, so each operation runs for 100 ms of
// virtual time (a few ticks of both replicas); there is no spec hash to compare.
func newLogRig() *rig {
	e, spec := logengine.New(sim.ControlPlaneFaults{Log: sim.LogParams{Enabled: true}}), veritas.NewControlPlane()
	g := &rig{eng: e, ctx: enginetest.New(e, 1), from: enginetest.Ref("a"), hash: spec.StateHash}
	g.run = func() { g.ctx.RunUntil(g.ctx.T + sim.Time(100*core.Millisecond)) }
	g.spec = func(op any) string {
		if c, ok := op.(createTopic); ok {
			return spec.CreateTopic(c.Name, c.Parts).String()
		}
		return render(spec.Exec(op.(core.IOAction)))
	}
	g.call = func(op any) string {
		if c, ok := op.(createTopic); ok {
			return e.CreateTopic(c.Name, c.Parts).String()
		}
		e.Execute(g.ctx, g.from, op.(core.IOAction))
		return ""
	}
	return g
}

func check(newRig func() *rig, gen func(*veritastest.Rand, int) any) veritastest.Check[*rig, any] {
	return veritastest.Check[*rig, any]{
		Name: "any", New: newRig, Gen: gen, Ops: 120, MaxShrinkRuns: 300,
		Step: step, Format: format,
	}
}

func run(t *testing.T, c veritastest.Check[*rig, any]) {
	t.Helper()
	veritastest.Run(t, c, veritastest.Options{Seeds: 200, Seed: -1})
}

func TestConformanceObjStore(t *testing.T) { run(t, check(newObjRig, genObj)) }
func TestConformanceCPEngine(t *testing.T) { run(t, check(newCPRig, genCP)) }

// C2 for the log engine: fault-free, it answers every operation as the spec does.
func TestConformanceLogEngine(t *testing.T) { run(t, check(newLogRig, genCP)) }
