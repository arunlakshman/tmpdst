package kafka_test

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
	"manifester/sim"
	"manifester/sim/kafka"
	"manifester/veritas"
)

// C4 world: a random correct Kafka history generated against the veritas
// specs (ControlPlane assigns offsets and deduplicates, Partition is the
// expected log), exposed to the oracle through a fake sim.View. Faults mutate
// the finished world (c4_faults_test.go).

// failures counts violated Always props (the fake core.Assertions).
type failures map[core.Prop]int

func (f failures) Always(c bool, p core.Prop, _ ...core.Detail) {
	if !c {
		f[p]++
	}
}
func (f failures) AlwaysOrUnreachable(c bool, p core.Prop, d ...core.Detail) { f.Always(c, p, d...) }
func (f failures) Sometimes(bool, core.Prop)                                 {}
func (f failures) Reachable(core.Prop)                                       {}
func (f failures) Unreachable(p core.Prop, _ ...core.Detail)                 { f[p]++ }

type bmeta struct {
	pid   int64
	pep   int16
	seq   int32
	acked bool
}

type tpBase struct {
	tp   core.TopicPartition
	base int64
}

type stats struct{ dups, indetLand, indetNo, rejected, reinit, bump, multi, nonIdem, fenced int }

type producer struct {
	pid core.ProducerID
	pep int16
	seq map[core.TopicPartition]int32
	nb  int
}

type broker struct {
	ref   core.NodeRef
	epoch core.Epoch
	dead  bool
}

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
	meta   map[tpBase]*bmeta
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

func (w *world) Step() uint64             { return uint64(len(w.raw)) }
func (w *world) Now() sim.Time            { return w.now }
func (w *world) Phase() sim.Phase         { return sim.PhaseSettling }
func (w *world) Alive(core.NodeRef) bool  { return true }
func (w *world) Timeline() sim.Timeline   { return w.tl }
func (w *world) History() sim.History     { return (*hist)(w) }
func (w *world) Objects() sim.ObjectTruth { return (*objView)(w) }
func (w *world) CP() sim.CPTruth          { return (*cpView)(w) }
func (w *world) Assert() core.Assertions  { return w.as }
func (w *world) touch()                   { w.gen++ }

type hist world

func (h *hist) Invoke(core.NodeRef, core.ID, core.ID, any) { panic("C4: history is built directly") }
func (h *hist) Return(core.ID, any, sim.OpOutcome) bool    { panic("C4: history is built directly") }
func (h *hist) Ops() []sim.HistOp                          { return h.ops }

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

func (w *world) emit(src core.NodeRef, f core.Fact) {
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

func nodeRef(s string) core.NodeRef { return core.NodeRef{Node: core.NodeID(s)} }

func newWorld(seed uint64, live bool) *world {
	r := rand.New(rand.NewPCG(seed, 0xc4))
	opts := kafka.DefaultOpts()
	w := &world{opts: opts, cp: veritas.NewControlPlane(), parts: map[core.TopicPartition]*veritas.Partition{},
		pos: map[[2]int]int64{}, meta: map[tpBase]*bmeta{}, ranges: map[core.TopicPartition][]core.BatchRange{},
		hwmAdj: map[core.TopicPartition]int64{}, objs: map[string][]byte{}, tl: sim.NewTimeline(), as: failures{}}
	opts.Partitions = int32(1 + r.IntN(3))
	w.opts = opts
	for _, t := range opts.Topics {
		w.cp.CreateTopic(t, opts.Partitions)
		for p := int32(0); p < opts.Partitions; p++ {
			tp := core.TopicPartition{Topic: t, Partition: p}
			w.tps = append(w.tps, tp)
			w.parts[tp] = &veritas.Partition{}
		}
	}
	for i := 0; i < 3; i++ {
		b := &broker{ref: nodeRef(fmt.Sprintf("b%d", i))}
		d := w.cp.Exec(core.CPCall{Op: core.Register{Self: b.ref}}).(core.CPDone)
		b.epoch = d.Resp.(core.RegisterResp).Epoch
		w.brks = append(w.brks, b)
	}
	for i := 0; i < opts.Producers; i++ {
		p := &producer{seq: map[core.TopicPartition]int32{}}
		w.initProducer(p)
		w.prods = append(w.prods, p)
	}
	if live {
		w.invs = []sim.Invariant{kafka.NewOracle(opts), kafka.NewTripped()}
	}
	steps := 20 + r.IntN(30)
	for i := 0; i < steps; i++ {
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

func (w *world) initProducer(p *producer) {
	d := w.cp.Exec(core.CPCall{Op: core.InitProducer{}}).(core.CPDone)
	p.pid, p.pep, p.seq = d.Resp.(core.InitProducerResp).Producer, 0, map[core.TopicPartition]int32{}
}

func (w *world) newID() core.ID {
	w.nextID++
	var id core.ID
	binary.BigEndian.PutUint64(id[:], w.nextID)
	return id
}

type item struct {
	op     int // index in w.ops
	po     kafka.ProduceOp
	p      *producer
	idem   bool
	fate   int
	bytes  []byte
	pb     core.PendingBatch
	landed bool
}

const (
	fNormal = iota
	fIndetLand
	fIndetNo
	fDup
	fStale
)

func (w *world) buildItem(r *rand.Rand, c int, tp core.TopicPartition) *item {
	p := w.prods[c]
	switch r.IntN(14) {
	case 0:
		w.initProducer(p)
		w.st.reinit++
	case 1:
		p.pep++
		p.seq = map[core.TopicPartition]int32{}
		w.st.bump++
	}
	it := &item{p: p, idem: r.IntN(10) != 0}
	it.fate = fNormal
	switch x := r.IntN(14); {
	case x == 0:
		it.fate = fIndetNo
	case x == 1:
		it.fate = fIndetLand
	case x <= 3 && it.idem:
		it.fate = fDup
	case x == 4 && it.idem && p.pep > 0:
		it.fate = fStale
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
	it.pb = core.PendingBatch{TP: tp, Records: int64(n)}
	pid, pep, seq := int64(-1), int16(-1), int32(-1)
	if it.idem {
		pid, pep, seq = int64(p.pid), p.pep, p.seq[tp]
		if it.fate == fStale {
			pep, seq = p.pep-1, 1000
		}
		bp.ProducerID, bp.ProducerEpoch, bp.BaseSequence = pid, pep, seq
		it.pb.Producer, it.pb.PEpoch, it.pb.BaseSeq = p.pid, pep, seq
	} else {
		w.st.nonIdem++
	}
	var err error
	if it.bytes, err = kwire.BuildBatch(bp, recs); err != nil {
		panic(err)
	}
	it.po = kafka.ProduceOp{Client: c, Batch: p.nb, TP: tp, Vals: vals, Producer: pid, PEpoch: pep, BaseSeq: seq}
	return it
}

func (w *world) pickBroker(r *rand.Rand) *broker {
	var alive []*broker
	for _, b := range w.brks {
		if !b.dead {
			alive = append(alive, b)
		}
	}
	return alive[r.IntN(len(alive))]
}

// commit sends items (one segment) to the spec CP and mirrors the result.
func (w *world) commit(b *broker, items []*item) []core.BatchRange {
	w.nseg++
	key := fmt.Sprintf("seg/%d", w.nseg)
	var obj []byte
	var pbs []core.PendingBatch
	for _, it := range items {
		it.pb.Loc = core.SegmentRef{Key: key, Off: int64(len(obj)), Len: int64(len(it.bytes))}
		obj = append(obj, it.bytes...)
		pbs = append(pbs, it.pb)
	}
	w.objs[key] = obj
	d := w.cp.Exec(core.CPCall{Op: core.CommitSegment{SegmentKey: key, Owner: b.ref, Epoch: b.epoch, Batches: pbs}}).(core.CPDone)
	if d.Err != core.OK {
		panic(fmt.Sprintf("spec CP refused a commit: %v", d.Err))
	}
	as := d.Resp.(core.CommitSegmentResp).Assigned
	touched := map[core.TopicPartition]bool{}
	for i, rg := range as {
		it := items[i]
		if rg.Err != core.OK || rg.Dup {
			continue
		}
		base := w.parts[rg.TP].Append(valBytes(it.po.Vals))
		if base != rg.Base {
			panic(fmt.Sprintf("Partition base %d != CP base %d", base, rg.Base))
		}
		touched[rg.TP] = true
		w.meta[tpBase{rg.TP, int64(rg.Base)}] = &bmeta{pid: it.po.Producer, pep: it.po.PEpoch, seq: it.po.BaseSeq}
		if it.idem {
			it.p.seq[rg.TP] += int32(rg.Count)
		}
	}
	for tp := range touched { // map order is irrelevant: independent entries
		lk := w.cp.Exec(core.CPCall{Op: core.Lookup{TP: tp}}).(core.CPDone).Resp.(core.LookupResp)
		w.ranges[tp] = lk.Ranges
		if lk.HighWatermark != w.parts[tp].HighWatermark() {
			panic("Partition HWM != CP HWM")
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
		{Key: "producer", Value: core.I64(it.po.Producer)}, {Key: "pepoch", Value: core.I64(it.po.PEpoch)},
		{Key: "base_seq", Value: core.I64(it.po.BaseSeq)}, {Key: "dup", Value: core.Bool(rg.Dup)},
		{Key: "segment", Value: core.Str(rg.Loc.Key)}}})
	if m := w.meta[tpBase{rg.TP, int64(rg.Base)}]; m != nil {
		m.acked = true
	}
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
		if it.fate != fIndetNo {
			send = append(send, it)
		}
	}
	w.step() // ops pending
	if len(send) > 0 {
		as := w.commit(b, send)
		for i, it := range send {
			rg := as[i]
			switch {
			case rg.Err != core.OK:
				w.st.rejected++
				w.ret(it, kafka.ProduceRes{Base: -1, Code: 1, Attempts: 1}, sim.OpAbandoned)
			case it.fate == fIndetLand:
				w.st.indetLand++
				if r.IntN(2) == 0 {
					w.ack(b, rg, it)
				}
				w.ret(it, kafka.ProduceRes{Base: -1, Code: 7, Attempts: 3}, sim.OpIndeterminate)
			default:
				w.ack(b, rg, it)
				w.ret(it, kafka.ProduceRes{Base: int64(rg.Base), Attempts: 1}, sim.OpDefinite)
			}
		}
		w.step()
		for _, it := range send { // retry of an acked idempotent batch: a Dup range
			if it.fate != fDup || w.ops[it.op].Outcome != sim.OpDefinite {
				continue
			}
			w.st.dups++
			rg := w.commit(b, []*item{it})[0]
			if !rg.Dup {
				panic("retry was not deduplicated by the spec CP")
			}
			w.ack(b, rg, it)
			w.ops[it.op].Result = kafka.ProduceRes{Base: int64(rg.Base), Attempts: 2}
			w.step()
		}
	}
	for _, it := range items {
		if it.fate == fIndetNo {
			w.st.indetNo++
			w.ret(it, kafka.ProduceRes{Base: -1, Code: 7, Attempts: 3}, sim.OpIndeterminate)
			if it.idem { // the client re-inits its epoch, as a real one would
				it.p.pep++
				it.p.seq = map[core.TopicPartition]int32{}
			}
		}
	}
	live := 0
	for _, x := range w.brks {
		if !x.dead {
			live++
		}
	}
	if live > 1 && r.IntN(20) == 0 {
		w.st.fenced++
		b.dead = true
		w.emit(b.ref, core.Fact{Name: "broker_fenced", Fields: []core.Field{{Key: "epoch", Value: core.U64(b.epoch)}}})
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
