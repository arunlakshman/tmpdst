package kafka_test

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/kafka"
	"manifester/internal/verify/veritas"
	"manifester/internal/wire/kwire"
)

// C4 world: a random correct at-least-once Kafka history generated against the
// veritas specs (ControlPlane assigns offsets and replays a committed key,
// Partition is the expected log; a retried batch is committed again, so it is
// in the log twice), exposed to the oracle through a fake sim.View. Faults
// mutate the finished world (c4_faults_test.go).

// failures counts violated Always props (the fake core.Assertions).
type failures map[core.PropertyID]int

func (f failures) Always(c bool, p core.PropertyID, _ ...core.ContextPair) {
	if !c {
		f[p]++
	}
}
func (f failures) AlwaysOrUnreachable(c bool, p core.PropertyID, d ...core.ContextPair) {
	f.Always(c, p, d...)
}
func (f failures) Sometimes(bool, core.PropertyID)                      {}
func (f failures) Reachable(core.PropertyID)                            {}
func (f failures) Unreachable(p core.PropertyID, _ ...core.ContextPair) { f[p]++ }

type stats struct{ retries, replays, indetLand, indetNo, rejected, multi int }

type producer struct{ nb int }

type broker struct{ ref core.NodeInstance }

// world is the fake sim.View plus the spec-side truth it was built from.
type world struct {
	opts   kafka.Opts
	tps    []core.TopicPartition
	cp     *veritas.ControlPlane
	parts  map[core.TopicPartition]*veritas.Partition
	prods  []*producer
	brks   []*broker
	pos    map[[2]int]int64 // (consumer, tp index) -> next offset
	st     stats
	nextID uint64
	nseg   int
	now    sim.Time

	// What the oracle sees (mutable by faults).
	ranges map[core.TopicPartition][]core.BatchRange
	hwmAdj map[core.TopicPartition]int64
	objs   map[string][]byte
	ops    []sim.HistOp
	raw    []sim.TimedFact
	tl     *sim.MemTimeline
	gen    uint64
	as     failures

	invs []sim.Invariant // live observers, nil in one-shot mode
}

func (w *world) Step() uint64                 { return uint64(len(w.raw)) }
func (w *world) Now() sim.Time                { return w.now }
func (w *world) Phase() sim.Phase             { return sim.PhaseSettling }
func (w *world) Alive(core.NodeInstance) bool { return true }
func (w *world) Timeline() sim.Timeline       { return w.tl }
func (w *world) History() sim.History         { return (*hist)(w) }
func (w *world) Objects() sim.ObjectTruth     { return (*objView)(w) }
func (w *world) CP() sim.CPTruth              { return (*cpView)(w) }
func (w *world) Assert() core.Assertions      { return w.as }
func (w *world) touch()                       { w.gen++ }

type hist world

func (h *hist) Invoke(core.NodeInstance, core.ID, core.ID, any) {
	panic("C4: history is built directly")
}
func (h *hist) Return(core.ID, any, sim.OpOutcome) bool { panic("C4: history is built directly") }
func (h *hist) Ops() []sim.HistOp                       { return h.ops }

type objView world

func (o *objView) Object(k string) ([]byte, bool)  { b, ok := o.objs[k]; return b, ok }
func (o *objView) EachObject(func(string, []byte)) {}
func (o *objView) Gen() uint64                     { return o.gen }

type cpView world

func (c *cpView) Committed(tp core.TopicPartition) []core.BatchRange { return c.ranges[tp] }
func (c *cpView) Gen() uint64                                        { return c.gen }
func (c *cpView) HighWatermark(tp core.TopicPartition) core.Offset {
	rs := c.ranges[tp]
	h := int64(0)
	if len(rs) > 0 {
		h = int64(rs[len(rs)-1].Base) + rs[len(rs)-1].Count
	}
	return core.Offset(h + c.hwmAdj[tp])
}

func (w *world) rebuildTL() {
	w.tl = sim.NewTimeline()
	for _, tf := range w.raw {
		w.tl.Append(tf.Source, tf.At, tf.Fact)
	}
}

func (w *world) emit(src core.NodeInstance, f core.Fact) {
	tf := sim.TimedFact{Seq: uint64(len(w.raw) + 1), At: w.now, Source: src, Fact: f}
	w.raw = append(w.raw, tf)
	w.tl.Append(src, w.now, f)
}

// step ends one simulator step: truth changed, live invariants observe.
func (w *world) step() {
	w.now += 1000
	w.touch()
	for _, i := range w.invs {
		i.Observe(w)
	}
}

func nodeRef(s string) core.NodeInstance { return core.NodeInstance{Node: core.NodeID(s)} }

func newWorld(seed uint64, live bool) *world {
	r := rand.New(rand.NewPCG(seed, 0xc4)) //nolint:gosec // G404: deterministic test/sim PRNG
	opts := kafka.DefaultOpts()
	w := &world{opts: opts, cp: veritas.NewControlPlane(), parts: map[core.TopicPartition]*veritas.Partition{},
		pos: map[[2]int]int64{}, ranges: map[core.TopicPartition][]core.BatchRange{},
		hwmAdj: map[core.TopicPartition]int64{}, objs: map[string][]byte{}, tl: sim.NewTimeline(), as: failures{}}
	opts.Partitions = int32(1 + r.IntN(3))
	w.opts = opts
	for _, t := range opts.Topics {
		w.cp.CreateTopic(t, opts.Partitions)
		for p := int32(0); p < opts.Partitions; p++ {
			tp := core.TopicPartitionKey(t, p)
			w.tps = append(w.tps, tp)
			w.parts[tp] = &veritas.Partition{}
		}
	}
	for i := range 3 {
		w.brks = append(w.brks, &broker{ref: nodeRef(fmt.Sprintf("b%d", i))})
	}
	for i := 0; i < opts.Producers; i++ {
		w.prods = append(w.prods, &producer{})
	}
	if live {
		w.invs = []sim.Invariant{kafka.NewOracle(opts)}
	}
	steps := 20 + r.IntN(30)
	for range steps {
		if r.IntN(3) == 0 {
			w.readStep(r)
		} else {
			w.produceStep(r)
		}
	}
	for c := opts.Producers; c < opts.Producers+opts.Consumers; c++ { // catch up
		for ti := range w.tps {
			for w.pos[[2]int{c, ti}] < int64(w.parts[w.tps[ti]].HighWatermark()) {
				w.read(r, c, []int{ti})
			}
		}
	}
	return w
}

func (w *world) newID() core.ID {
	w.nextID++
	var id core.ID
	binary.BigEndian.PutUint64(id[:], w.nextID)
	return id
}

type item struct {
	op    int // index in w.ops
	po    kafka.ProduceOp
	p     *producer
	fate  int
	bytes []byte
	pb    core.PendingBatch
}

const (
	fNormal    = iota
	fIndetLand // committed, the client never learns it
	fIndetNo   // never reaches the broker
	fRetry     // committed, response lost, resent: committed a second time
	fReplay    // the broker replays a MaybeApplied commit under the same key
	fIdem      // claims a producer id: the broker rejects it, never committed
)

func (w *world) buildItem(r *rand.Rand, c int, tp core.TopicPartition) *item {
	p := w.prods[c]
	it := &item{p: p, fate: fNormal}
	switch x := r.IntN(18); {
	case x == 0:
		it.fate = fIndetNo
	case x == 1:
		it.fate = fIndetLand
	case x <= 3:
		it.fate = fRetry
	case x == 4:
		it.fate = fReplay
	case x == 5:
		it.fate = fIdem
	}
	p.nb++
	n := 1 + r.IntN(3)
	vals := make([]string, n)
	recs := make([]kmsg.Record, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("c%d.b%d.r%d|%s", c, p.nb, i, "pad"[:r.IntN(4)])
		recs[i] = kmsg.Record{Value: []byte(vals[i])}
	}
	bp := kwire.BatchParams{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1}
	if it.fate == fIdem {
		bp.ProducerID, bp.ProducerEpoch, bp.BaseSequence = 7, 0, 0
	}
	it.pb = core.PendingBatch{TP: tp, Records: int64(n)}
	var err error
	if it.bytes, err = kwire.BuildBatch(bp, recs); err != nil {
		panic(err)
	}
	it.po = kafka.ProduceOp{Client: c, Batch: p.nb, TP: tp, Vals: vals, Idem: it.fate == fIdem}
	return it
}

func (w *world) pickBroker(r *rand.Rand) *broker { return w.brks[r.IntN(len(w.brks))] }

// commit sends items (one segment, a fresh key) to the spec CP and mirrors
// the result. Replaying the key must return the same answer and change nothing.
func (w *world) commit(items []*item, replay bool) []core.BatchRange {
	w.nseg++
	key := fmt.Sprintf("seg/%032x", w.nseg)
	var obj []byte
	var pbs []core.PendingBatch
	for _, it := range items {
		it.pb.Loc = core.SegmentRef{Key: key, Off: int64(len(obj)), Len: int64(len(it.bytes))}
		obj = append(obj, it.bytes...)
		pbs = append(pbs, it.pb)
	}
	w.objs[key] = obj
	op := core.CommitSegment{SegmentKey: key, Batches: pbs}
	d := w.cp.Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone)
	if d.Err != core.OK {
		panic(fmt.Sprintf("spec CP refused a commit: %v", d.Err))
	}
	as := d.Resp.(core.CommitSegmentResp).Assigned
	if replay {
		w.st.replays++
		d2 := w.cp.Exec(core.ControlPlaneCall{Op: op}).(core.ControlPlaneDone)
		if d2.Err != core.OK || !slices.Equal(d2.Resp.(core.CommitSegmentResp).Assigned, as) {
			panic("spec CP: a replayed commit did not return the original answer")
		}
	}
	touched := map[core.TopicPartition]bool{}
	for i, rg := range as {
		if i < len(items) {
			base := w.parts[rg.TP].Append(valBytes(items[i].po.Vals))
			if base != rg.Base {
				panic(fmt.Sprintf("Partition base %d != CP base %d", base, rg.Base))
			}
			touched[rg.TP] = true
			continue
		}
		panic(fmt.Sprintf("spec CP assigned %d ranges for %d batches", len(as), len(items)))
	}
	for tp := range touched { // map order is irrelevant: independent entries
		lk := w.cp.Exec(core.ControlPlaneCall{Op: core.FindSegments{TP: tp}}).(core.ControlPlaneDone).Resp.(core.FindSegmentsResp)
		w.ranges[tp] = lk.Ranges
		if lk.End != w.parts[tp].HighWatermark() {
			panic("Partition end != CP end")
		}
	}
	return as
}

func valBytes(vs []string) [][]byte {
	out := make([][]byte, len(vs))
	for i, v := range vs {
		out[i] = []byte(v)
	}
	return out
}

func (w *world) ack(b *broker, rg core.BatchRange, it *item) {
	w.emit(b.ref, core.Fact{Name: "produce_acked", Fields: []core.Field{
		{Key: "topic", Value: core.Str(rg.TP.Topic)}, {Key: "partition", Value: core.I64(rg.TP.Partition)},
		{Key: "base_offset", Value: core.I64(rg.Base)}, {Key: "count", Value: core.I64(rg.Count)},
		{Key: "segment", Value: core.Str(rg.Loc.Key)}}})
}

func (w *world) invoke(it *item, c int) {
	w.ops = append(w.ops, sim.HistOp{ID: w.newID(), Value: w.newID(), Client: nodeRef(fmt.Sprintf("c%d", c)), Op: it.po,
		InvSeq: uint64(len(w.ops)*2 + 1), Invoke: w.now, Outcome: sim.OpPending})
	it.op = len(w.ops) - 1
}

func (w *world) ret(it *item, res kafka.ProduceRes, o sim.OpOutcome) {
	op := &w.ops[it.op]
	op.Result, op.Outcome, op.Return, op.RetSeq = res, o, w.now, op.InvSeq+1
}

func (w *world) produceStep(r *rand.Rand) {
	b := w.pickBroker(r)
	n := min(1+r.IntN(3), len(w.tps))
	if n > 1 {
		w.st.multi++
	}
	var items, send []*item
	for _, ti := range r.Perm(len(w.tps))[:n] {
		it := w.buildItem(r, r.IntN(w.opts.Producers), w.tps[ti])
		w.invoke(it, it.po.Client)
		items = append(items, it)
		if it.fate != fIndetNo && it.fate != fIdem {
			send = append(send, it)
		}
	}
	w.step() // ops pending
	if len(send) > 0 {
		replay := false
		for _, it := range send {
			replay = replay || it.fate == fReplay
		}
		as := w.commit(send, replay)
		var again []*item
		for i, it := range send {
			rg := as[i]
			switch it.fate {
			case fIndetLand:
				w.st.indetLand++
				if r.IntN(2) == 0 {
					w.ack(b, rg, it)
				}
				w.ret(it, kafka.ProduceRes{Base: -1, Code: 7, Attempts: 3}, sim.OpIndeterminate)
			case fRetry: // the answer was lost: the client resends below
				if r.IntN(2) == 0 {
					w.ack(b, rg, it)
				}
				again = append(again, it)
			default:
				w.ack(b, rg, it)
				w.ret(it, kafka.ProduceRes{Base: int64(rg.Base), Attempts: 1}, sim.OpDefinite)
			}
		}
		w.step()
		for _, it := range again { // at-least-once: the same batch is committed again
			w.st.retries++
			rg := w.commit([]*item{it}, false)[0]
			w.ack(b, rg, it)
			w.ret(it, kafka.ProduceRes{Base: int64(rg.Base), Attempts: 2}, sim.OpDefinite)
			w.step()
		}
	}
	for _, it := range items {
		switch it.fate {
		case fIndetNo:
			w.st.indetNo++
			w.ret(it, kafka.ProduceRes{Base: -1, Code: 7, Attempts: 3}, sim.OpIndeterminate)
		case fIdem:
			w.st.rejected++
			w.ret(it, kafka.ProduceRes{Base: -1, Code: 42, Why: "rejected", Attempts: 1}, sim.OpAbandoned)
		}
	}
	w.step()
}

func (w *world) readStep(r *rand.Rand) {
	c := w.opts.Producers + r.IntN(w.opts.Consumers)
	n := min(1+r.IntN(2), len(w.tps))
	w.read(r, c, r.Perm(len(w.tps))[:n])
}

// read is one Fetch of consumer c over the partition indexes tis: a prefix of
// what the Partition spec holds from the consumer's position.
func (w *world) read(r *rand.Rand, c int, tis []int) {
	ro := kafka.ReadOp{Client: c}
	var res kafka.ReadRes
	for _, ti := range tis {
		tp := w.tps[ti]
		from := w.pos[[2]int{c, ti}]
		ro.Parts = append(ro.Parts, kafka.ReadFrom{TP: tp, Off: from})
		recs, ok := w.parts[tp].Read(core.Offset(from), 1+r.IntN(4))
		if !ok {
			panic("spec Partition refused a read")
		}
		rp := kafka.ReadPart{HWM: int64(w.parts[tp].HighWatermark())}
		for _, x := range recs {
			rp.Recs = append(rp.Recs, kafka.Rec{Off: int64(x.Offset), Val: string(x.Value)})
		}
		w.pos[[2]int{c, ti}] = from + int64(len(recs))
		res.Parts = append(res.Parts, rp)
	}
	w.ops = append(w.ops, sim.HistOp{ID: w.newID(), Value: w.newID(), Client: nodeRef(fmt.Sprintf("c%d", c)), Op: ro,
		InvSeq: uint64(len(w.ops)*2 + 1), Invoke: w.now, Outcome: sim.OpPending})
	w.step()
	op := &w.ops[len(w.ops)-1]
	op.Result, op.Outcome, op.Return, op.RetSeq = res, sim.OpDefinite, w.now, op.InvSeq+1
	w.step()
}
