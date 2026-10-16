package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
)

type opKind uint8

const (
	opRefresh opKind = iota // DescribeTopics to resolve a topic-cache miss, then resume the request
	opMeta                  // DescribeTopics answering Metadata
	opInit                  // InitProducer
	opList                  // Lookup for one ListOffsets partition
	opFetch                 // Lookup, then ranged GETs, for one Fetch partition
)

type opState uint8

const (
	opCP opState = iota // a CPCall is in flight
	opGet
	opBackoff
)

// op is one control-plane / object-store interaction serving a request part.
// It retries with backoff; a part never fails silently.
type op struct {
	id       uint64
	kind     opKind
	state    opState
	retry    opState // state a backoff returns to
	req      core.ReqID
	gen      core.TimerGen
	attempts int
	s        *slot

	names []string // refresh, meta: topics asked
	creq  core.Request

	tp       core.TopicPartition
	from     core.Offset
	maxBytes int32
	lpart    *kmsg.ListOffsetsResponseTopicPartition
	fpart    *kmsg.FetchResponseTopicPartition

	ranges  []core.BatchRange
	next    int
	out     []byte
	corrupt bool // the last failure was a corrupt GET
}

func (b *Broker) findOp(r core.ReqID) *op {
	for _, o := range b.ops {
		if o.req == r && o.req != 0 {
			return o
		}
	}
	return nil
}

func (b *Broker) dropOp(o *op) {
	for i, x := range b.ops {
		if x == o {
			b.ops = append(b.ops[:i:i], b.ops[i+1:]...)
			return
		}
	}
}

// startOp registers o as a pending part of its slot and issues its first call.
func (b *Broker) startOp(o *op) []core.Action {
	o.id = b.id()
	o.state = opCP
	o.s.pending++
	b.ops = append(b.ops, o)
	return b.issue(o)
}

func (b *Broker) issue(o *op) []core.Action {
	o.req = b.req()
	if o.state == opGet {
		return []core.Action{core.ObjGet{Req: o.req, Ref: o.ranges[o.next].Loc}}
	}
	var cop core.CPOp
	switch o.kind {
	case opRefresh, opMeta:
		cop = core.DescribeTopics{Topics: o.names}
	case opInit:
		cop = core.InitProducer{}
	case opList: // only the high watermark is wanted: look past the end
		cop = core.Lookup{TP: o.tp, From: core.Offset(1<<63 - 1), MaxBytes: 1}
	case opFetch:
		cop = core.Lookup{TP: o.tp, From: o.from, MaxBytes: o.maxBytes}
	}
	return []core.Action{core.CPCall{Req: o.req, Op: cop}}
}

// retryOp backs off and repeats the call, or gives up after MaxAttempts or
// when the client is gone.
func (b *Broker) retryOp(o *op) []core.Action {
	if o.s.conn.closed {
		return b.endOp(o)
	}
	if o.attempts++; o.attempts >= b.cfg.MaxAttempts {
		b.env.Reachable("broker: request retries exhausted")
		return b.failOp(o, b.exhaustCode(o))
	}
	o.retry, o.state, o.req = o.state, opBackoff, 0
	t, g := b.timer(core.TimerID(o.id), o.attempts)
	o.gen = g
	return []core.Action{t}
}

func (b *Broker) exhaustCode(o *op) int16 {
	switch o.kind {
	case opMeta:
		return errLeaderNotAvailable
	case opInit:
		return errCoordNotAvailable
	case opFetch:
		if o.corrupt {
			return errKafkaStorage
		}
	}
	return errRequestTimedOut
}

func (b *Broker) opTimer(o *op, gen core.TimerGen) []core.Action {
	if o.state != opBackoff || o.gen != gen {
		return nil
	}
	o.state = o.retry
	return b.issue(o)
}

// endOp retires o without touching its response.
func (b *Broker) endOp(o *op) []core.Action {
	b.dropOp(o)
	b.finishPart(o.s)
	return nil
}

// failOp answers o's part with code and retires it.
func (b *Broker) failOp(o *op, code int16) []core.Action {
	switch o.kind {
	case opRefresh:
		return b.resume(o)
	case opMeta:
		b.metaResp(o, nil, code)
	case opInit:
		o.s.resp.(*kmsg.InitProducerIDResponse).ErrorCode = code
	case opList:
		o.lpart.ErrorCode = code
	case opFetch:
		o.fpart.ErrorCode = code
	}
	return b.endOp(o)
}

func (b *Broker) onOpCP(o *op, e core.CPDone) []core.Action {
	if e.Err == core.ErrInvalid {
		return b.failOp(o, errUnknownServer)
	}
	if e.Err != core.OK {
		return b.retryOp(o)
	}
	switch r := e.Resp.(type) {
	case core.DescribeTopicsResp:
		if o.kind == opRefresh || o.kind == opMeta {
			b.merge(r.Topics)
			if o.kind == opRefresh {
				return b.resume(o)
			}
			b.metaResp(o, r.Topics, errNone)
			return b.endOp(o)
		}
	case core.InitProducerResp:
		if o.kind == opInit {
			resp := o.s.resp.(*kmsg.InitProducerIDResponse)
			resp.ProducerID, resp.ProducerEpoch = int64(r.Producer), r.PEpoch
			return b.endOp(o)
		}
	case core.LookupResp:
		switch o.kind {
		case opList:
			o.lpart.Offset = int64(r.HighWatermark)
			return b.endOp(o)
		case opFetch:
			return b.onLookup(o, r)
		}
	}
	return b.retryOp(o)
}

// ---- dispatch ----

// parked requests and fresh ones both enter here, once registered.
func (b *Broker) handle(s *slot, req core.Request) []core.Action {
	acts := b.dispatch(s, req, false)
	b.finishPart(s) // release the construction guard
	return acts
}

// dispatch runs a request. resolved is true once a topic-cache refresh has
// been tried, so it is not tried again.
func (b *Broker) dispatch(s *slot, req core.Request, resolved bool) []core.Action {
	if !resolved {
		if names := b.missing(req); len(names) > 0 {
			return b.startOp(&op{kind: opRefresh, s: s, names: names, creq: req})
		}
	}
	switch r := req.(type) {
	case *kmsg.MetadataRequest:
		return b.metadata(s, r)
	case *kmsg.InitProducerIDRequest:
		return b.initProducer(s, r)
	case *kmsg.ProduceRequest:
		if r.Acks == 0 { // unsupported: no response, close
			s.silent, s.conn.closing = true, true
			return nil
		}
		return b.produce(s, r)
	case *kmsg.FetchRequest:
		return b.fetch(s, r)
	case *kmsg.ListOffsetsRequest:
		return b.listOffsets(s, r)
	}
	s.silent, s.conn.closing = true, true
	return nil
}

// resume runs the request a refresh was holding, with whatever the cache now has.
func (b *Broker) resume(o *op) []core.Action {
	b.dropOp(o)
	var acts []core.Action
	if !o.s.conn.closed {
		acts = b.dispatch(o.s, o.creq, true)
	}
	b.finishPart(o.s)
	return acts
}

// ---- topic cache ----

func (b *Broker) knows(tp core.TopicPartition) bool {
	for _, t := range b.topics {
		if t.Name == tp.Topic {
			return tp.Partition >= 0 && tp.Partition < t.Partitions
		}
	}
	return false
}

func (b *Broker) merge(infos []core.TopicInfo) {
	for _, in := range infos {
		found := false
		for i := range b.topics {
			if b.topics[i].Name == in.Name {
				b.topics[i].Partitions = max(b.topics[i].Partitions, in.Partitions)
				found = true
				break
			}
		}
		if !found {
			b.topics = append(b.topics, in)
		}
	}
}

// missing lists the topics of req (once each) that the cache cannot resolve.
func (b *Broker) missing(req core.Request) []string {
	var out []string
	add := func(topic string, part int32) {
		if b.knows(core.TopicPartition{Topic: topic, Partition: part}) {
			return
		}
		for _, n := range out {
			if n == topic {
				return
			}
		}
		out = append(out, topic)
	}
	switch r := req.(type) {
	case *kmsg.ProduceRequest:
		if r.Acks == 0 {
			return nil
		}
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(t.Topic, p.Partition)
			}
		}
	case *kmsg.FetchRequest:
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(t.Topic, p.Partition)
			}
		}
	case *kmsg.ListOffsetsRequest:
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(t.Topic, p.Partition)
			}
		}
	}
	return out
}
