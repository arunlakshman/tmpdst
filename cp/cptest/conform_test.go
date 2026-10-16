package cptest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

// Conformance C3 for the control-plane stores: the same random operations
// against the store under test and a fresh veritas.ControlPlane, every
// response compared.

type op struct {
	Kind     string // register, init, create, commit, lookup, describe
	Node     string
	Name     string // segment key or topic
	Epoch    uint64
	Batches  []core.PendingBatch
	TP       core.TopicPartition
	From     int64
	MaxBytes int32
	Parts    int32
	Topics   []string
}

type pair struct {
	impl impl
	spec *veritas.ControlPlane
}

var (
	nodes  = []string{"a", "b"}
	topics = []string{"t1", "t2", "t3"}
)

func gen(r *veritastest.Rand, i int) op {
	pick := func(xs []string) string { return xs[r.Intn(len(xs))] }
	tp := func() core.TopicPartition {
		return core.TopicPartition{Topic: pick(topics), Partition: int32(r.Intn(3))}
	}
	switch k := r.Intn(32); {
	case i < 4 && i%2 == 0:
		return op{Kind: "register", Node: nodes[i/2]}
	case i < 4:
		return op{Kind: "init"}
	case k < 1:
		return op{Kind: "register", Node: pick(nodes)}
	case k < 3:
		return op{Kind: "init"}
	case k < 4:
		return op{Kind: "create", Name: pick(append([]string{""}, topics...)), Parts: int32(r.Intn(5)) - 1}
	case k < 20:
		o := op{Kind: "commit", Node: pick(nodes), Name: fmt.Sprintf("seg%d", r.Intn(8)), Epoch: uint64(1 + r.Intn(3))}
		// A key's batches repeat (a replay) unless the variant changes.
		variant := 0
		if r.Intn(6) == 0 {
			variant = 1 + r.Intn(3)
		}
		shape := o.Name
		if r.Intn(4) == 0 { // a retry of the same batches under a fresh key
			o.Name = fmt.Sprintf("re%d", r.Intn(1000))
		}
		o.Batches = batches(veritastest.NewRand(uint64(len(shape)*7+int(shape[len(shape)-1])*131+variant)), o.Name)
		return o
	case k < 28:
		return op{Kind: "lookup", TP: tp(), From: int64(r.Intn(8)) - 1, MaxBytes: int32(r.Intn(3)) * 60}
	default:
		o := op{Kind: "describe"}
		for i := r.Intn(3); i > 0; i-- {
			o.Topics = append(o.Topics, pick(append([]string{"nope"}, topics...)))
		}
		return o
	}
}

func batches(r *veritastest.Rand, key string) []core.PendingBatch {
	var out []core.PendingBatch
	n := r.Intn(4)
	if r.Intn(40) == 0 {
		n = 0
	}
	for i := 0; i < n; i++ {
		b := core.PendingBatch{
			TP:      core.TopicPartition{Topic: topics[r.Intn(2)], Partition: int32(r.Intn(2))},
			Records: 1,
			Loc:     core.SegmentRef{Key: key, Off: int64(r.Intn(4)) * 100, Len: int64(1 + r.Intn(3)*50)},
		}
		if r.Intn(4) == 0 {
			b.Records = int64(r.Intn(3))
			if r.Intn(10) != 0 {
				b.Records++
			}
		}
		if r.Intn(3) != 0 {
			b.Producer = core.ProducerID(1000 + r.Intn(2))
			if r.Intn(60) == 0 {
				b.Producer = core.ProducerID(998 + r.Intn(8))
			}
			b.PEpoch = int16(r.Intn(3))
			b.BaseSeq = int32(r.Intn(5))
			if r.Intn(80) == 0 {
				b.BaseSeq = -1
			}
		}
		if r.Intn(100) == 0 {
			b.TP.Partition = -1
		}
		out = append(out, b)
	}
	return out
}

func cpOp(o op) core.CPOp {
	switch o.Kind {
	case "register":
		return core.Register{Self: core.NodeRef{Node: core.NodeID(o.Node)}}
	case "init":
		return core.InitProducer{}
	case "commit":
		return core.CommitSegment{SegmentKey: o.Name, Owner: core.NodeRef{Node: core.NodeID(o.Node)}, Epoch: core.Epoch(o.Epoch), Batches: o.Batches}
	case "lookup":
		return core.Lookup{TP: o.TP, From: core.Offset(o.From), MaxBytes: o.MaxBytes}
	case "describe":
		return core.DescribeTopics{Topics: o.Topics}
	}
	return nil
}

// show renders a result; %+v prints nil and empty slices alike.
func show(resp core.CPResp, code core.ErrCode) string {
	if code != core.OK {
		return code.String()
	}
	return fmt.Sprintf("%+v", resp)
}

func step(p pair, o op) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got, want string
	if o.Kind == "create" {
		c, err := p.impl.CreateTopic(ctx, o.Name, o.Parts)
		if err != nil {
			return fmt.Errorf("infra error: %w", err)
		}
		got, want = c.String(), p.spec.CreateTopic(o.Name, o.Parts).String()
	} else {
		resp, code, err := p.impl.Exec(ctx, cpOp(o))
		if err != nil {
			return fmt.Errorf("infra error: %w", err)
		}
		d := p.spec.Exec(core.CPCall{Op: cpOp(o)}).(core.CPDone)
		got, want = show(resp, code), show(d.Resp, d.Err)
	}
	if got != want {
		return fmt.Errorf("store: %s\n spec: %s", got, want)
	}
	return nil
}

func shrinkOp(o op) []op {
	var out []op
	if len(o.Batches) > 1 {
		c := o
		c.Batches = o.Batches[:len(o.Batches)-1]
		out = append(out, c)
	}
	return out
}

func run(t *testing.T, newImpl func() impl, ops, seeds int) {
	t.Helper()
	c := veritastest.Check[pair, op]{
		Name: "cptest.op", Gen: gen, Step: step, Shrink: shrinkOp, Ops: ops, MaxShrinkRuns: 300,
		New:    func() pair { return pair{newImpl(), veritas.NewControlPlane()} },
		Format: func(o op) string { return fmt.Sprintf("%#v", o) },
	}
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		if f := c.RunSeed(seed); f != nil {
			t.Fatal(c.Report(f, fmt.Sprintf("go test ./cp/cptest -run %s", t.Name())))
		}
	}
}

func TestConformanceMemstore(t *testing.T) {
	run(t, func() impl { return storeImpl{newMem()} }, 200, 200)
}

func TestConformanceMemstoreHTTP(t *testing.T) {
	run(t, func() impl { return httpOver(t, newMem()) }, 100, 30)
}

func TestConformanceDynamo(t *testing.T) {
	needDynamo(t)
	run(t, func() impl { return storeImpl{dynamoStore(t)} }, 80, 12)
}

func TestConformanceDynamoHTTP(t *testing.T) {
	needDynamo(t)
	run(t, func() impl { return httpOver(t, dynamoStore(t)) }, 80, 4)
}

var _ cp.Store = newMem()
