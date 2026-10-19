package dynamo

import (
	"maps"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

// groupPlan is a built group: the accepted requests with their ranges, and
// the one transaction that commits them. Item layout (cancellation reasons
// are mapped back through it): [0,n) the G# item of accepted[i], then one run
// item per partition in tps, then one Update per grown topic in topics.
type groupPlan struct {
	accepted []*seqReq
	ranges   [][]core.BatchRange
	items    []types.TransactWriteItem
	tps      []core.TopicPartition
	topics   []string
	newEnds  map[core.TopicPartition]int64
	grown    map[string]int32
}

// builder accumulates requests into a group, in order. A request that does
// not fit (items, bytes) is deferred to the next group, and so is every later
// request that shares a partition with it, so per-partition order holds.
type builder struct {
	s        *Sequencer
	next     map[core.TopicPartition]int64 // working ends
	topics   map[string]int32              // known counts
	grown    map[string]int32              // topics to grow, and to what
	parts    map[core.TopicPartition]bool
	deferP   map[core.TopicPartition]bool
	segItems []types.TransactWriteItem
	blob     int
	acc      []*seqReq
	ranges   [][]core.BatchRange
	deferred []*seqReq
	tooLarge []*seqReq
}

type verdict int

const (
	accepted verdict = iota
	deferredV
	tooLargeV
)

func (s *Sequencer) newBuilder() *builder {
	return &builder{s: s, next: map[core.TopicPartition]int64{}, topics: map[string]int32{}, grown: map[string]int32{},
		parts: map[core.TopicPartition]bool{}, deferP: map[core.TopicPartition]bool{}}
}

// seed copies what the state knows about the partitions and topics of reqs.
func (b *builder) seed(reqs []*seqReq) {
	for _, r := range reqs {
		for _, tp := range distinctPartitions(r.op.Batches) {
			if _, ok := b.next[tp]; !ok {
				v, _ := b.s.state.end(tp)
				b.next[tp] = v
			}
			if _, ok := b.topics[tp.Topic]; !ok {
				v, _ := b.s.state.topic(tp.Topic)
				b.topics[tp.Topic] = v
			}
		}
	}
}

// limit is the item budget for the next request: the group budget, or the
// transaction limit for a request that is alone (its own ErrTooLarge rule).
func (b *builder) limit() int {
	if len(b.acc) == 0 {
		return MaxTxItems
	}
	return b.s.o.MaxItems
}

func (b *builder) try(r *seqReq) verdict {
	tps := distinctPartitions(r.op.Batches)
	if slices.ContainsFunc(tps, func(tp core.TopicPartition) bool { return b.deferP[tp] }) {
		b.deferR(r, tps)
		return deferredV
	}
	sub := map[core.TopicPartition]int64{}
	for _, tp := range tps {
		sub[tp] = b.next[tp]
	}
	view := maps.Clone(b.topics)
	for t, n := range b.grown {
		view[t] = max(view[t], n)
	}
	ranges, grownR := assign(r.op.Batches, sub, view)
	seg, ok := b.s.st.segmentItem(r.op, ranges)
	if !ok {
		return b.refuse(r)
	}
	blobLen := len(getBlob(seg.Put.Item, "blob"))
	if b.itemsWith(tps, grownR) > b.limit() || (len(b.acc) > 0 && b.blob+blobLen > b.s.maxGroupBytes) {
		if len(b.acc) == 0 {
			return b.refuse(r)
		}
		b.deferR(r, tps)
		return deferredV
	}
	b.accept(r, tps, ranges, grownR, seg, blobLen)
	return accepted
}

func (b *builder) itemsWith(tps []core.TopicPartition, grownR map[string]int32) int {
	n := len(b.acc) + 1 + len(b.parts)
	for _, tp := range tps {
		if !b.parts[tp] {
			n++
		}
	}
	topics := len(b.grown)
	for t := range grownR {
		if _, in := b.grown[t]; !in {
			topics++
		}
	}
	return n + topics
}

func (b *builder) refuse(r *seqReq) verdict {
	b.tooLarge = append(b.tooLarge, r)
	return tooLargeV
}

func (b *builder) deferR(r *seqReq, tps []core.TopicPartition) {
	b.deferred = append(b.deferred, r)
	for _, tp := range tps {
		b.deferP[tp] = true
	}
}

func (b *builder) accept(r *seqReq, tps []core.TopicPartition, ranges []core.BatchRange, grownR map[string]int32, seg types.TransactWriteItem, blobLen int) {
	for _, br := range ranges {
		b.next[br.TP] = int64(br.Base) + br.Count
	}
	for _, tp := range tps {
		b.parts[tp] = true
	}
	for t, n := range grownR {
		b.grown[t] = max(b.grown[t], n)
	}
	b.acc = append(b.acc, r)
	b.ranges = append(b.ranges, ranges)
	b.segItems = append(b.segItems, seg)
	b.blob += blobLen
}

// finish builds the transaction of the accepted requests. ok is false if a
// merged run is too large for one item.
func (b *builder) finish() (p *groupPlan, ok bool) {
	var flat []core.BatchRange
	for _, rs := range b.ranges {
		flat = append(flat, rs...)
	}
	items := slices.Clone(b.segItems)
	p = &groupPlan{accepted: b.acc, ranges: b.ranges, newEnds: map[core.TopicPartition]int64{}, grown: b.grown}
	for _, run := range runsOf(flat) {
		it := runItem(run[0].TP, run)
		if len(getBlob(it, "runs")) > b.s.maxRunBytes {
			return nil, false
		}
		items = append(items, putIfAbsent(b.s.table(), it, "attribute_not_exists(sk)"))
		p.tps = append(p.tps, run[0].TP)
		last := run[len(run)-1]
		p.newEnds[run[0].TP] = int64(last.Base) + last.Count
	}
	p.topics = slices.Sorted(maps.Keys(b.grown))
	items = append(items, b.s.st.topicItems(b.grown)...)
	p.items = items
	return p, true
}

func (s *Sequencer) table() string { return s.st.table }

// build forms the transaction for reqs (in queue order). Requests that must
// wait for a later group come back in deferred; refused ones (too large even
// alone) in tooLarge. If a merged run would not fit one item the group is
// halved until it does; one request alone is then ErrTooLarge, as for a Store.
func (s *Sequencer) build(reqs []*seqReq) (p *groupPlan, deferred, tooLarge []*seqReq) {
	for {
		b := s.newBuilder()
		b.seed(reqs)
		for _, r := range reqs {
			b.try(r)
		}
		deferred, tooLarge = append(deferred, b.deferred...), append(tooLarge, b.tooLarge...)
		if len(b.acc) == 0 {
			return nil, deferred, tooLarge
		}
		if p, ok := b.finish(); ok {
			return p, deferred, tooLarge
		}
		if len(b.acc) == 1 {
			return nil, deferred, append(tooLarge, b.acc...)
		}
		half := len(b.acc) / 2
		deferred = append(deferred, b.acc[half:]...)
		reqs = b.acc[:half]
	}
}
