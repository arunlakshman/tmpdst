package dynamo

import (
	"time"

	"manifester/core"
)

// resource is what a group reserves while it is in flight. Two groups never
// hold the same resource, which is the whole pipelining rule:
//
//	'p' a partition (its run item is compare-and-set, and its next offset is
//	    chained: a later group must not start from an end that is still moving)
//	't' a topic that may need to grow (one Update per topic item at a time:
//	    concurrent transactions on one item abort each other)
//	'g' a segment key (a duplicate in flight waits, then replays)
type resource struct {
	kind byte
	name string
	p    int32
}

// resourcesOf lists the partitions and the segment key of a commit.
func resourcesOf(op core.CommitSegment) []resource {
	out := []resource{{kind: 'g', name: op.SegmentKey}}
	for _, tp := range distinctPartitions(op.Batches) {
		out = append(out, resource{kind: 'p', name: tp.Topic, p: tp.Partition})
	}
	return out
}

// growRes adds a 't' resource for every topic of the op whose cached count
// does not cover its partitions (unknown counts included): the commit may grow it.
func (s *Sequencer) growRes(op core.CommitSegment) []resource {
	need := map[string]int32{}
	var order []string
	for _, b := range op.Batches {
		if _, ok := need[b.TP.Topic]; !ok {
			order = append(order, b.TP.Topic)
		}
		need[b.TP.Topic] = max(need[b.TP.Topic], b.TP.Partition+1)
	}
	var out []resource
	for _, t := range order {
		if have, ok := s.state.topic(t); !ok || have < need[t] {
			out = append(out, resource{kind: 't', name: t})
		}
	}
	return out
}

// seqGroup is a set of requests committed by one transaction, and the
// resources they hold.
type seqGroup struct {
	reqs     []*seqReq
	use      map[resource]struct{}
	parts    int // 'p' resources in use
	topics   int // 't' resources in use
	deferred []*seqReq
}

// itemsWith is the item count of the group if r (with resources res) joined:
// a G# item per segment, a run per partition, an Update per growing topic.
func (g *seqGroup) itemsWith(res []resource) int {
	n := len(g.reqs) + 1 + g.parts + g.topics
	for _, r := range res {
		if _, in := g.use[r]; !in {
			n += kindCost(r.kind)
		}
	}
	return n
}

func kindCost(k byte) int {
	if k == 'g' {
		return 0
	}
	return 1
}

func (g *seqGroup) add(r *seqReq, res []resource) {
	g.reqs = append(g.reqs, r)
	for _, x := range res {
		if _, in := g.use[x]; in {
			continue
		}
		g.use[x] = struct{}{}
		switch x.kind {
		case 'p':
			g.parts++
		case 't':
			g.topics++
		}
	}
}

// plan forms the next group from the queue, or returns nil (nothing queued, the
// pipeline is full, or every queued request conflicts). It scans in queue order
// and keeps order per resource: a request that cannot join (it conflicts with a
// group in flight, or does not fit) blocks everything behind it that shares one
// of its resources. No waiting: a lone request goes at once. mu is held.
func (s *Sequencer) plan() *seqGroup {
	if s.inflight >= s.o.Pipeline || len(s.queue) == 0 {
		return nil
	}
	g := &seqGroup{use: map[resource]struct{}{}}
	skipped := map[resource]struct{}{}
	var rest []*seqReq
	for _, r := range s.queue {
		if err := r.ctx.Err(); err != nil {
			r.answer(core.CommitSegmentResp{}, 0, err) // withdrawn while queued
			continue
		}
		res := append(slicesClone(r.res), s.growRes(r.op)...)
		if s.blocked(res, skipped) || !s.fits(g, res) {
			for _, x := range res {
				skipped[x] = struct{}{}
			}
			rest = append(rest, r)
			continue
		}
		g.add(r, res)
	}
	s.queue = rest
	s.obs.SeqQueueDepth(len(rest))
	if len(g.reqs) == 0 {
		return nil
	}
	s.reserve(g)
	return g
}

func slicesClone(r []resource) []resource { return append([]resource(nil), r...) }

// blocked: a resource is held by a group in flight, or by an earlier request
// that stays queued.
func (s *Sequencer) blocked(res []resource, skipped map[resource]struct{}) bool {
	for _, x := range res {
		if _, b := s.busy[x]; b {
			return true
		}
		if _, k := skipped[x]; k {
			return true
		}
	}
	return false
}

// fits: the first request of a group always joins (an oversized one is
// answered ErrTooLarge when the group is built); later ones must fit the item budget.
func (s *Sequencer) fits(g *seqGroup, res []resource) bool {
	return len(g.reqs) == 0 || g.itemsWith(res) <= s.o.MaxItems
}

func (s *Sequencer) reserve(g *seqGroup) {
	now := time.Now()
	for res := range g.use {
		s.busy[res] = struct{}{}
	}
	for _, r := range g.reqs {
		s.obs.SeqQueueWait(now.Sub(r.enq))
	}
	s.stats.groups.Add(1)
}
