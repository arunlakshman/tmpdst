package broker

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

type operationKind uint8

const (
	opRefresh  operationKind = iota // DescribeTopics to resolve a topic-cache miss, then resume the request
	opMetadata                      // DescribeTopics answering Metadata
	opList                          // FindSegments for one ListOffsets partition
	opFetch                         // FindSegments, then ranged GETs, for one Fetch partition
)

type operationState uint8

const (
	opControlPlane operationState = iota // a ControlPlaneCall is in flight
	opGet
	opBackoff
)

// operation is one control-plane / object-store interaction serving a request part.
// It retries with backoff; a part never fails silently.
type operation struct {
	id       uint64
	kind     operationKind
	state    operationState
	retry    operationState // state a backoff returns to
	req      core.RequestID
	gen      core.TimerVersion
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

func (b *Broker) findOp(r core.RequestID) *operation {
	for _, o := range b.ops {
		if o.req == r && o.req != 0 {
			return o
		}
	}
	return nil
}

func (b *Broker) dropOp(o *operation) {
	for i, x := range b.ops {
		if x == o {
			b.ops = append(b.ops[:i:i], b.ops[i+1:]...)
			return
		}
	}
}

func (b *Broker) addOp(o *operation) { b.ops = append(b.ops, o) }

func (b *Broker) opByTimer(id core.TimerID) *operation {
	for _, o := range b.ops {
		if core.TimerID(o.id) == id {
			return o
		}
	}
	return nil
}

// startOp registers o as a pending part of its slot and issues its first call.
func (b *Broker) startOp(o *operation) []core.Action {
	o.id = b.id()
	o.state = opControlPlane
	o.s.pending++
	b.addOp(o)
	return b.issue(o)
}

func (b *Broker) issue(o *operation) []core.Action {
	o.req = b.req()
	if o.state == opGet {
		return []core.Action{core.ObjectGet{Req: o.req, Ref: o.ranges[o.next].Loc}}
	}
	var cop core.ControlPlaneOperation
	switch o.kind {
	case opRefresh, opMetadata:
		cop = core.DescribeTopics{Topics: o.names}
	case opList: // only the end offset is wanted: look past the end
		cop = core.FindSegments{TP: o.tp, From: core.Offset(1<<63 - 1), MaxBytes: 1}
	case opFetch:
		cop = core.FindSegments{TP: o.tp, From: o.from, MaxBytes: o.maxBytes}
	}
	return []core.Action{core.ControlPlaneCall{Req: o.req, Op: cop}}
}

// retryOp backs off and repeats the call, or gives up after MaxAttempts or
// when the client is gone.
func (b *Broker) retryOp(o *operation) []core.Action {
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

func (b *Broker) exhaustCode(o *operation) kafkaErrorCode {
	switch o.kind {
	case opMetadata:
		return errLeaderNotAvailable
	case opFetch:
		if o.corrupt {
			return errKafkaStorage
		}
	}
	return errRequestTimedOut
}

func (b *Broker) opTimer(o *operation, gen core.TimerVersion) []core.Action {
	if o.state != opBackoff || o.gen != gen {
		return nil
	}
	o.state = o.retry
	return b.issue(o)
}

// endOp retires o without touching its response.
func (b *Broker) endOp(o *operation) []core.Action {
	b.dropOp(o)
	b.finishPart(o.s)
	return nil
}

// failOp answers o's part with code and retires it.
func (b *Broker) failOp(o *operation, code kafkaErrorCode) []core.Action {
	switch o.kind {
	case opRefresh:
		return b.resume(o)
	case opMetadata:
		b.metaResp(o, nil, code)
	case opList:
		o.lpart.ErrorCode = int16(code)
	case opFetch:
		o.fpart.ErrorCode = int16(code)
	}
	return b.endOp(o)
}

func (b *Broker) onOpCP(o *operation, e core.ControlPlaneDone) []core.Action {
	if e.Err == core.ErrInvalid {
		return b.failOp(o, errUnknownServer)
	}
	if e.Err != core.OK {
		return b.retryOp(o)
	}
	switch r := e.Resp.(type) {
	case core.DescribeTopicsResp:
		if o.kind == opRefresh || o.kind == opMetadata {
			b.merge(r.Topics)
			if o.kind == opRefresh {
				return b.resume(o)
			}
			b.metaResp(o, r.Topics, errNone)
			return b.endOp(o)
		}
	case core.FindSegmentsResp:
		switch o.kind {
		case opList:
			o.lpart.Offset = int64(r.End)
			return b.endOp(o)
		case opFetch:
			return b.onLookup(o, r)
		}
	}
	return b.retryOp(o)
}

// ---- dispatch ----

// handle runs a fresh request.
func (b *Broker) handle(s *slot, req core.Request) []core.Action {
	acts := b.dispatch(s, req)
	b.finishPart(s) // release the construction guard
	return acts
}

// dispatch runs a request, refreshing the topic cache first when needed.
func (b *Broker) dispatch(s *slot, req core.Request) []core.Action {
	if names := b.missing(req); len(names) > 0 {
		return b.startOp(&operation{kind: opRefresh, s: s, names: names, creq: req})
	}
	return b.dispatchAfterRefresh(s, req)
}

// dispatchAfterRefresh runs a request without trying another topic-cache refresh.
func (b *Broker) dispatchAfterRefresh(s *slot, req core.Request) []core.Action {
	switch r := req.(type) {
	case *kmsg.MetadataRequest:
		return b.metadata(s, r)
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
func (b *Broker) resume(o *operation) []core.Action {
	b.dropOp(o)
	var acts []core.Action
	if !o.s.conn.closed {
		acts = b.dispatchAfterRefresh(o.s, o.creq)
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
	add := func(tp core.TopicPartition) {
		if b.knows(tp) {
			return
		}
		if slices.Contains(out, tp.Topic) {
			return
		}
		out = append(out, tp.Topic)
	}
	switch r := req.(type) {
	case *kmsg.ProduceRequest:
		if r.Acks == 0 {
			return nil
		}
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(core.NewTopicPartition(t.Topic, p.Partition))
			}
		}
	case *kmsg.FetchRequest:
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(core.NewTopicPartition(t.Topic, p.Partition))
			}
		}
	case *kmsg.ListOffsetsRequest:
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(core.NewTopicPartition(t.Topic, p.Partition))
			}
		}
	}
	return out
}
