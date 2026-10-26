package kafka_test

import (
	"math/rand/v2"
	"slices"
	"strconv"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
	"manifester/sim"
	"manifester/sim/kafka"
)

// Oracle property names (the stable strings sim/kafka reports).
const (
	pLocTwice   core.Prop = "kafka: a stored batch is committed at most once"
	pPerSend    core.Prop = "kafka: a batch is committed at most once per send"
	pOneBatch   core.Prop = "kafka: a committed range is one whole produced batch"
	pAckOrder   core.Prop = "kafka: a client's acked batches for a partition are at increasing offsets"
	pOrder      core.Prop = "kafka: records of a batch keep their order"
	pProduced   core.Prop = "kafka: every committed record was produced"
	pAsProduced core.Prop = "kafka: a record is committed where and as it was produced"
	pRejected   core.Prop = "kafka: a definitely rejected batch is never committed"
	pAckInCP    core.Prop = "kafka: acked range is committed in the control plane"
	pContig     core.Prop = "kafka: committed offsets are contiguous"
	pSegment    core.Prop = "kafka: committed segment exists in the object store"
	pInside     core.Prop = "kafka: committed range lies inside its segment"
	pIntact     core.Prop = "kafka: committed batch is intact"
	pCount      core.Prop = "kafka: committed range holds its record count"
	pHWM        core.Prop = "kafka: the high watermark is the end of the committed log"
	pAckedAt    core.Prop = "kafka: an acked record is in the log at its acked offset"
	pCaughtUp   core.Prop = "kafka: every consumer caught up with the final log"
	pCorrupt    core.Prop = "kafka: a consumer never receives a corrupt batch"
	pRange      core.Prop = "kafka: a consumer's fetch is never out of range"
	pHWMAbove   core.Prop = "kafka: a fetch never reports a high watermark above the committed log"
	pResume     core.Prop = "kafka: a consumer resumes exactly where its last read ended"
	pSkipped    core.Prop = "kafka: a consumer sees every offset in order, none skipped"
	pPastHWM    core.Prop = "kafka: a consumer never reads past the high watermark"
	pReadValue  core.Prop = "kafka: a consumer reads the committed value at each offset"
)

// fault is one corruption of a finished world. apply reports false when the
// world has nothing to corrupt that way. The oracle must report all of want
// (or at least one when anyOf).
type fault struct {
	name  string
	apply func(w *world, r *rand.Rand) bool
	want  []core.Prop
	anyOf bool
}

func pick[T any](r *rand.Rand, xs []T) (T, bool) {
	var z T
	if len(xs) == 0 {
		return z, false
	}
	return xs[r.IntN(len(xs))], true
}

// definiteAt: an acked (OpDefinite) produce landed at base of tp.
func (w *world) definiteAt(tp core.TopicPartition, base int64) bool {
	for _, op := range w.ops {
		if po, ok := op.Op.(kafka.ProduceOp); ok && op.Outcome == sim.OpDefinite && po.TP == tp {
			if res := op.Result.(kafka.ProduceRes); res.Base == base {
				return true
			}
		}
	}
	return false
}

func (w *world) tpsWith(min int) []core.TopicPartition {
	var out []core.TopicPartition
	for _, tp := range w.tps {
		if len(w.ranges[tp]) >= min {
			out = append(out, tp)
		}
	}
	return out
}

type partRef struct{ op, part int }

func (w *world) readParts(ok func(tp core.TopicPartition, p kafka.ReadPart, from int64) bool) []partRef {
	var out []partRef
	for i, op := range w.ops {
		if ro, isRead := op.Op.(kafka.ReadOp); isRead {
			for j, p := range op.Result.(kafka.ReadRes).Parts {
				if ok(ro.Parts[j].TP, p, ro.Parts[j].Off) {
					out = append(out, partRef{i, j})
				}
			}
		}
	}
	return out
}

func (w *world) part(p partRef) *kafka.ReadPart {
	return &w.ops[p.op].Result.(kafka.ReadRes).Parts[p.part]
}

// finalParts: reads that ended at the end of the final log.
func (w *world) finalParts() []partRef {
	return w.readParts(func(tp core.TopicPartition, p kafka.ReadPart, from int64) bool {
		return p.Code == 0 && p.HWM == int64(w.parts[tp].HighWatermark())
	})
}

func (w *world) anyParts() []partRef {
	return w.readParts(func(core.TopicPartition, kafka.ReadPart, int64) bool { return true })
}

func (w *world) ackFacts() []int {
	var out []int
	for i, tf := range w.raw {
		if tf.Fact.Name == "produce_acked" {
			out = append(out, i)
		}
	}
	return out
}

func (w *world) setAck(i int, key string, add int64) {
	for k, f := range w.raw[i].Fact.Fields {
		if f.Key == key {
			w.raw[i].Fact.Fields[k].Value = f.Value.(core.I64) + core.I64(add)
		}
	}
	w.rebuildTL()
}

func (w *world) definiteOps() []int {
	var out []int
	for i, op := range w.ops {
		if _, ok := op.Op.(kafka.ProduceOp); ok && op.Outcome == sim.OpDefinite {
			out = append(out, i)
		}
	}
	return out
}

// swapAdj swaps log entries i and i+1 of tp, keeping offsets contiguous.
func (w *world) swapAdj(tp core.TopicPartition, i int) {
	rs := w.ranges[tp]
	a, b := rs[i], rs[i+1]
	na, nb := b, a
	na.Base, nb.Base = a.Base, a.Base+core.Offset(b.Count)
	rs[i], rs[i+1] = na, nb
}

// adjPairs lists (tp, i) with entries i, i+1 passing ok.
func (w *world) adjPairs(ok func(tp core.TopicPartition, a, b core.BatchRange) bool) ([]partRef, []core.TopicPartition) {
	var out []partRef
	var tps []core.TopicPartition
	for _, tp := range w.tps {
		rs := w.ranges[tp]
		for i := 0; i+1 < len(rs); i++ {
			if ok(tp, rs[i], rs[i+1]) {
				out, tps = append(out, partRef{0, i}), append(tps, tp)
			}
		}
	}
	return out, tps
}

func (w *world) put(obj []byte) core.SegmentRef {
	w.nseg++
	key := "c4/" + strconv.Itoa(w.nseg)
	w.objs[key] = obj
	return core.SegmentRef{Key: key, Len: int64(len(obj))}
}

func recsOf(vals ...string) []kmsg.Record {
	out := make([]kmsg.Record, len(vals))
	for i, v := range vals {
		out[i] = kmsg.Record{Value: []byte(v)}
	}
	return out
}

func lastRange(w *world, tp core.TopicPartition) core.BatchRange {
	rs := w.ranges[tp]
	return rs[len(rs)-1]
}

func faults() []fault {
	return []fault{
		{"duplicated record in the log", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if !ok {
				return false
			}
			d := lastRange(w, tp)
			d.Base = core.Offset(w.cp0(tp))
			w.ranges[tp] = append(w.ranges[tp], d)
			return true
		}, []core.Prop{pLocTwice}, false},

		{"duplicated record in another partition", func(w *world, r *rand.Rand) bool {
			a, ok := pick(r, w.tpsWith(1))
			if !ok || len(w.tps) < 2 {
				return false
			}
			b := w.tps[(indexOf(w.tps, a)+1)%len(w.tps)]
			d := lastRange(w, a)
			d.TP, d.Base = b, core.Offset(w.cp0(b))
			w.ranges[b] = append(w.ranges[b], d)
			return true
		}, []core.Prop{pLocTwice, pAsProduced}, false},

		{"acked record missing (last range dropped)", func(w *world, r *rand.Rand) bool {
			var c []core.TopicPartition
			for _, tp := range w.tpsWith(1) {
				if w.definiteAt(tp, int64(lastRange(w, tp).Base)) {
					c = append(c, tp)
				}
			}
			tp, ok := pick(r, c)
			if ok {
				w.ranges[tp] = w.ranges[tp][:len(w.ranges[tp])-1]
			}
			return ok
		}, []core.Prop{pAckInCP, pAckedAt}, false},

		{"acked record missing (middle range dropped, offsets closed up)", func(w *world, r *rand.Rand) bool {
			var c []partRef
			var tps []core.TopicPartition
			for _, tp := range w.tpsWith(3) {
				for k, rg := range w.ranges[tp][:len(w.ranges[tp])-1] {
					if w.definiteAt(tp, int64(rg.Base)) {
						c, tps = append(c, partRef{0, k}), append(tps, tp)
					}
				}
			}
			if len(c) == 0 {
				return false
			}
			i := r.IntN(len(c))
			tp, k := tps[i], c[i].part
			rs := w.ranges[tp]
			gone := rs[k]
			rs = append(rs[:k:k], rs[k+1:]...)
			for j := k; j < len(rs); j++ {
				rs[j].Base -= core.Offset(gone.Count)
			}
			w.ranges[tp] = rs
			return true
		}, []core.Prop{pAckedAt}, false},

		{"record at the wrong offset (acked offset off by one)", func(w *world, r *rand.Rand) bool {
			i, ok := pick(r, w.definiteOps())
			if ok {
				res := w.ops[i].Result.(kafka.ProduceRes)
				res.Base++
				w.ops[i].Result = res
			}
			return ok
		}, []core.Prop{pAckedAt}, false},

		{"records swapped in the log", func(w *world, r *rand.Rand) bool {
			ps, tps := w.adjPairs(func(tp core.TopicPartition, a, b core.BatchRange) bool { return w.definiteAt(tp, int64(a.Base)) })
			i, ok := pick(r, seq(len(ps)))
			if ok {
				w.swapAdj(tps[i], ps[i].part)
			}
			return ok
		}, []core.Prop{pAckedAt}, false},

		{"offset gap", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if !ok {
				return false
			}
			rs := w.ranges[tp]
			for j := r.IntN(len(rs)); j < len(rs); j++ {
				rs[j].Base++
			}
			return true
		}, []core.Prop{pContig}, false},

		{"acked range not in the control plane", func(w *world, r *rand.Rand) bool {
			i, ok := pick(r, w.ackFacts())
			if ok {
				w.setAck(i, "base_offset", 1000)
			}
			return ok
		}, []core.Prop{pAckInCP}, false},

		{"consumer read value differs from the log", func(w *world, r *rand.Rand) bool {
			ps := w.readParts(func(_ core.TopicPartition, p kafka.ReadPart, _ int64) bool { return len(p.Recs) > 0 })
			p, ok := pick(r, ps)
			if ok {
				rp := w.part(p)
				rp.Recs[r.IntN(len(rp.Recs))].Val += "x"
			}
			return ok
		}, []core.Prop{pReadValue}, false},

		{"consumer skips an offset inside a read", func(w *world, r *rand.Rand) bool {
			ps := w.readParts(func(_ core.TopicPartition, p kafka.ReadPart, _ int64) bool { return len(p.Recs) >= 2 })
			p, ok := pick(r, ps)
			if ok {
				w.part(p).Recs = w.part(p).Recs[1:]
			}
			return ok
		}, []core.Prop{pSkipped}, false},

		{"consumer skips an offset between reads", func(w *world, r *rand.Rand) bool {
			var c []partRef
			for _, p := range w.readParts(func(_ core.TopicPartition, p kafka.ReadPart, _ int64) bool { return len(p.Recs) > 0 }) {
				ro, rp := w.ops[p.op].Op.(kafka.ReadOp), w.part(p)
				end := ro.Parts[p.part].Off + int64(len(rp.Recs))
				for _, q := range w.anyParts() {
					qo := w.ops[q.op].Op.(kafka.ReadOp)
					if q.op > p.op && qo.Client == ro.Client && qo.Parts[q.part].TP == ro.Parts[p.part].TP && qo.Parts[q.part].Off == end {
						c = append(c, p)
						break
					}
				}
			}
			p, ok := pick(r, c)
			if ok {
				w.part(p).Recs = w.part(p).Recs[:len(w.part(p).Recs)-1]
			}
			return ok
		}, []core.Prop{pResume}, false},

		{"consumer reads past the reported high watermark", func(w *world, r *rand.Rand) bool {
			ps := w.readParts(func(_ core.TopicPartition, p kafka.ReadPart, _ int64) bool { return p.Code == 0 && len(p.Recs) > 0 })
			p, ok := pick(r, ps)
			if ok {
				rp := w.part(p)
				rp.HWM = rp.Recs[len(rp.Recs)-1].Off
			}
			return ok
		}, []core.Prop{pPastHWM}, false},

		{"consumer reads beyond the end of the log", func(w *world, r *rand.Rand) bool {
			p, ok := pick(r, w.finalParts())
			if ok {
				rp := w.part(p)
				rp.Recs = append(rp.Recs, kafka.Rec{Off: rp.HWM, Val: "c0.b1.r0|x"})
			}
			return ok
		}, []core.Prop{pPastHWM}, false},

		{"fetch reports a high watermark above the log", func(w *world, r *rand.Rand) bool {
			p, ok := pick(r, w.finalParts())
			if ok {
				w.part(p).HWM++
			}
			return ok
		}, []core.Prop{pHWMAbove}, false},

		{"fetch answers out of range", func(w *world, r *rand.Rand) bool {
			p, ok := pick(r, w.anyParts())
			if ok {
				w.part(p).Code = 1
			}
			return ok
		}, []core.Prop{pRange}, false},

		{"consumer receives a corrupt batch", func(w *world, r *rand.Rand) bool {
			p, ok := pick(r, w.anyParts())
			if ok {
				w.part(p).Corrupt = true
			}
			return ok
		}, []core.Prop{pCorrupt}, false},

		{"consumer never catches up (last read emptied)", func(w *world, r *rand.Rand) bool {
			var c []partRef
			seen := map[[2]any]bool{}
			ps := w.anyParts() // time order: walk back to find the last read per (client, tp)
			for _, p := range slices.Backward(ps) {
				ro := w.ops[p.op].Op.(kafka.ReadOp)
				k := [2]any{ro.Client, ro.Parts[p.part].TP}
				if !seen[k] && len(w.part(p).Recs) > 0 {
					c = append(c, p)
				}
				seen[k] = true
			}
			p, ok := pick(r, c)
			if ok {
				w.part(p).Recs = nil
			}
			return ok
		}, []core.Prop{pCaughtUp}, false},

		{"batch committed more often than it was sent", func(w *world, r *rand.Rand) bool {
			var c []int
			for _, i := range w.definiteOps() {
				if w.ops[i].Result.(kafka.ProduceRes).Attempts == 1 {
					c = append(c, i)
				}
			}
			i, ok := pick(r, c)
			if !ok {
				return false
			}
			po, res := w.ops[i].Op.(kafka.ProduceOp), w.ops[i].Result.(kafka.ProduceRes)
			rs := w.ranges[po.TP]
			for k := range rs {
				if int64(rs[k].Base) == res.Base { // the same bytes stored and committed once more
					rg := rs[k]
					raw := w.objs[rg.Loc.Key][rg.Loc.Off : rg.Loc.Off+rg.Loc.Len]
					rg.Base, rg.Loc = core.Offset(w.cp0(po.TP)), w.put(slices.Clone(raw))
					w.ranges[po.TP] = append(rs, rg)
					return true
				}
			}
			return false
		}, []core.Prop{pPerSend}, false},

		{"offsets overlap", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(2))
			if !ok {
				return false
			}
			rs := w.ranges[tp]
			for j := 1 + r.IntN(len(rs)-1); j < len(rs); j++ {
				rs[j].Base--
			}
			return true
		}, []core.Prop{pContig}, false},

		{"a client's acked offsets go backwards", func(w *world, r *rand.Rand) bool {
			var c [][2]int // pairs of definite ops of one client and partition, in history order
			ds := w.definiteOps()
			for x, i := range ds {
				for _, j := range ds[x+1:] {
					a, b := w.ops[i].Op.(kafka.ProduceOp), w.ops[j].Op.(kafka.ProduceOp)
					if a.Client == b.Client && a.TP == b.TP {
						c = append(c, [2]int{i, j})
					}
				}
			}
			p, ok := pick(r, c)
			if !ok {
				return false
			}
			a, b := w.ops[p[0]].Result.(kafka.ProduceRes), w.ops[p[1]].Result.(kafka.ProduceRes)
			a.Base, b.Base = b.Base, a.Base
			w.ops[p[0]].Result, w.ops[p[1]].Result = a, b
			return true
		}, []core.Prop{pAckOrder, pAckedAt}, false},

		{"phantom record never produced", func(w *world, r *rand.Rand) bool {
			tp, _ := pick(r, w.tps)
			b, err := kwire.BuildBatch(kwire.BatchParams{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1}, recsOf("c0.b99999.r0|ph"))
			if err != nil {
				panic(err)
			}
			rg := core.BatchRange{TP: tp, Base: core.Offset(w.cp0(tp)), Count: 1, Loc: w.put(b)}
			w.ranges[tp] = append(w.ranges[tp], rg)
			return true
		}, []core.Prop{pProduced}, false},

		{"record of a definitely rejected batch is present", func(w *world, r *rand.Rand) bool {
			i, ok := pick(r, w.definiteOps())
			if ok {
				w.ops[i].Outcome = sim.OpAbandoned
			}
			return ok
		}, []core.Prop{pRejected}, false},

		{"records of a batch out of order", func(w *world, r *rand.Rand) bool {
			var c []partRef
			var tps []core.TopicPartition
			for _, tp := range w.tps {
				for k, rg := range w.ranges[tp] {
					if rg.Count >= 2 {
						c, tps = append(c, partRef{0, k}), append(tps, tp)
					}
				}
			}
			i, ok := pick(r, seq(len(c)))
			if !ok {
				return false
			}
			rg := &w.ranges[tps[i]][c[i].part]
			raw := w.objs[rg.Loc.Key][rg.Loc.Off : rg.Loc.Off+rg.Loc.Len]
			bt, _ := kwire.ParseBatch(raw)
			recs, _ := bt.Records()
			vals := make([]string, len(recs))
			for j, x := range recs {
				vals[j] = string(x.Value)
			}
			vals[0], vals[1] = vals[1], vals[0]
			nb, err := kwire.BuildBatch(kwire.BatchParams{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1}, recsOf(vals...))
			if err != nil {
				panic(err)
			}
			rg.Loc = w.put(nb)
			return true
		}, []core.Prop{pOrder}, false},

		{"a retry commits only part of a batch", func(w *world, r *rand.Rand) bool {
			var c []partRef
			var tps []core.TopicPartition
			for _, tp := range w.tps {
				for k, rg := range w.ranges[tp] {
					if rg.Count >= 2 {
						c, tps = append(c, partRef{0, k}), append(tps, tp)
					}
				}
			}
			i, ok := pick(r, seq(len(c)))
			if !ok {
				return false
			}
			rs := w.ranges[tps[i]]
			k := c[i].part
			rg := &rs[k]
			raw := w.objs[rg.Loc.Key][rg.Loc.Off : rg.Loc.Off+rg.Loc.Len]
			bt, _ := kwire.ParseBatch(raw)
			recs, _ := bt.Records()
			nb, err := kwire.BuildBatch(kwire.BatchParams{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1}, recsOf(string(recs[0].Value)))
			if err != nil {
				panic(err)
			}
			gone := rg.Count - 1
			rg.Loc, rg.Count = w.put(nb), 1
			for j := k + 1; j < len(rs); j++ {
				rs[j].Base -= core.Offset(gone)
			}
			return true
		}, []core.Prop{pOneBatch}, false},

		{"corrupt batch bytes in a segment", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if ok {
				rg := lastRange(w, tp)
				w.objs[rg.Loc.Key][rg.Loc.Off+rg.Loc.Len-1] ^= 0xff
			}
			return ok
		}, []core.Prop{pIntact}, false},

		{"segment object missing", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if ok {
				delete(w.objs, lastRange(w, tp).Loc.Key)
			}
			return ok
		}, []core.Prop{pSegment}, false},

		{"committed range runs past its segment", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if !ok {
				return false
			}
			rg := lastRange(w, tp)
			if o := w.objs[rg.Loc.Key]; rg.Loc.Off+rg.Loc.Len == int64(len(o)) {
				w.objs[rg.Loc.Key] = o[:len(o)-1]
				return true
			}
			return false
		}, []core.Prop{pInside}, false},

		{"committed count differs from the batch", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if ok {
				w.ranges[tp][len(w.ranges[tp])-1].Count++
			}
			return ok
		}, []core.Prop{pCount}, false},

		{"high watermark above the log", func(w *world, r *rand.Rand) bool {
			tp, _ := pick(r, w.tps)
			w.hwmAdj[tp] = 1
			return true
		}, []core.Prop{pHWM}, false},

		{"high watermark below the log", func(w *world, r *rand.Rand) bool {
			tp, ok := pick(r, w.tpsWith(1))
			if ok {
				w.hwmAdj[tp] = -1
			}
			return ok
		}, []core.Prop{pHWM}, false},
	}
}

func (w *world) cp0(tp core.TopicPartition) int64 {
	return int64((*cpView)(w).HighWatermark(tp)) - w.hwmAdj[tp]
}

func indexOf(tps []core.TopicPartition, tp core.TopicPartition) int {
	for i, x := range tps {
		if x == tp {
			return i
		}
	}
	return -1
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
