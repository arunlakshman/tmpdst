// Package kafka is the Kafka scenario: sans-I/O Kafka clients (at-least-once
// producers, no idempotence, and consumers over kwire), an oracle that rebuilds each
// partition's committed log from the control plane and object store truth, the
// invariants over it, and the campaign preset. See docs/decisions.md rows
// 12-17, 26-31.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
	"manifester/sim"
)

// Opts configures the workload.
type Opts struct {
	Producers, Consumers int
	Topics               []string
	Partitions           int32
}

// DefaultOpts is 3 at-least-once producers and 2 consumers over 2 topics x 3
// partitions.
func DefaultOpts() Opts {
	return Opts{Producers: 3, Consumers: 2, Topics: []string{"t0", "t1"}, Partitions: 3}
}

func (o Opts) tps() []core.TopicPartition {
	var out []core.TopicPartition
	for _, t := range o.Topics {
		for p := int32(0); p < o.Partitions; p++ {
			out = append(out, core.TopicPartition{Topic: t, Partition: p})
		}
	}
	return out
}

const (
	reqTimeout  = 1500 * core.Millisecond // a request with no answer: drop the connection (Kafka does)
	fetchWait   = 10 * core.Second        // a fetch reads a batch at a time from S3: it is slow on a long log
	maxAttempts = 3                       // sends of one produce batch before it is abandoned
	maxFrame    = 8 << 20
)

type cstate uint8

const (
	csBackoff cstate = iota // wake timer pending, then dial
	csDial
	csUp // connected; wake timer pending or a request outstanding
)

type stage uint8

const (
	stAPI stage = iota
	stMeta
	stList // consumers: ListOffsets earliest
	stRun
)

type reqKind uint8

const (
	rqAPI reqKind = iota
	rqMeta
	rqList
	rqProduce
	rqFetch
	nKinds
)

type inflight struct {
	kind reqKind
	h    core.RequestHeader
}

type client struct {
	id       int
	producer bool
	state    cstate
	stage    stage
	conn     core.ConnID
	fr       *kwire.Framer
	dialReq  core.ReqID
	sendReq  core.ReqID
	dials    int
	fails    int // consecutive dial/connection failures
	wakeGen  core.TimerGen
	reqGen   core.TimerGen
	corr     int32
	ver      [nKinds]int16
	out      *inflight
	p        producer
	c        consumer
}

// Workload is the Kafka clients. Each client has one request outstanding at a
// time, which also gives max.in.flight=1 per (producer, partition): a batch is
// resolved (acked or abandoned) before the next one for that partition is
// built, so the oracle can check that acked offsets follow send order.
type Workload struct {
	env     core.Env
	self    core.NodeRef
	brokers []core.NodeID
	opts    Opts
	tps     []core.TopicPartition
	hist    sim.History
	codec   kwire.Codec

	nextReq core.ReqID
	clients []*client
}

// NewWorkload builds the workload machine.
func NewWorkload(env core.Env, self core.NodeRef, brokers []core.NodeRef, o Opts) *Workload {
	w := &Workload{env: env, self: self, opts: o, tps: o.tps()}
	for _, b := range brokers {
		w.brokers = append(w.brokers, b.Node)
	}
	for i := 0; i < o.Producers+o.Consumers; i++ {
		c := &client{id: i, producer: i < o.Producers}
		if !c.producer {
			c.c.pos = make([]int64, len(w.tps))
		}
		w.clients = append(w.clients, c)
	}
	return w
}

// Name implements sim.Workload.
func (w *Workload) Name() string { return "kafka" }

// SetHistory is called by the kernel.
func (w *Workload) SetHistory(h sim.History) { w.hist = h }

// Quiescent reports that no produce batch is unresolved. Consumers poll
// forever and do not count.
func (w *Workload) Quiescent() bool {
	for _, c := range w.clients {
		if len(c.p.cur) > 0 {
			return false
		}
	}
	return true
}

func wakeID(c *client) core.TimerID { return core.TimerID(1 + c.id*2) }
func reqID(c *client) core.TimerID  { return core.TimerID(2 + c.id*2) }

func (w *Workload) req() core.ReqID { w.nextReq++; return w.nextReq }

// Step implements core.StateMachine.
func (w *Workload) Step(ev core.Event) []core.Action {
	var acts []core.Action
	switch e := ev.Body.(type) {
	case core.Started:
		for _, c := range w.clients {
			acts = append(acts, w.dial(c)...)
		}
	case core.TimerFired:
		acts = w.onTimer(ev.At, e)
	case core.DialDone:
		for _, c := range w.clients {
			if c.state == csDial && c.dialReq == e.Req {
				acts = w.onDial(c, e)
			}
		}
	case core.BytesReceived:
		if c := w.byConn(e.Conn); c != nil {
			acts = w.onBytes(ev.At, c, e.Data)
		}
	case core.ConnClosed:
		if c := w.byConn(e.Conn); c != nil {
			acts = w.lostPeerClosed(c, "conn_closed")
		}
	case core.SendDone:
		if c := w.byConn(e.Conn); c != nil && e.Err != core.OK && c.sendReq == e.Req {
			acts = w.lostAbort(c, "send_failed")
		}
	}
	return acts
}

// OnPhase implements sim.PhaseAware. Entering Settling the producers start no
// new batch (in-flight ones finish); the consumers keep reading. Entering
// Settle (the drain) each consumer stops after a fetch that finds the end of
// every partition, so no read is outstanding at the end.
func (w *Workload) OnPhase(p sim.Phase) []core.Action {
	for _, c := range w.clients {
		switch p {
		case sim.PhaseSettling:
			c.p.noNew = true
		case sim.PhaseSettle:
			c.c.drain = true
		}
	}
	return nil
}

func (w *Workload) byConn(id core.ConnID) *client {
	for _, c := range w.clients {
		if c.conn == id && id != 0 && c.state == csUp {
			return c
		}
	}
	return nil
}

func (w *Workload) later(c *client, d core.Duration) core.Action {
	c.wakeGen++
	return core.SetTimer{ID: wakeID(c), Gen: c.wakeGen, After: d}
}

func retryDelay(n int) core.Duration {
	if n > 4 {
		n = 4
	}
	return 50 * core.Millisecond << n
}

func (w *Workload) dial(c *client) []core.Action {
	c.state, c.dialReq = csDial, w.req()
	addr := string(w.brokers[(c.id+c.dials)%len(w.brokers)])
	c.dials++
	return []core.Action{core.Dial{Req: c.dialReq, Addr: addr}}
}

func (w *Workload) onDial(c *client, e core.DialDone) []core.Action {
	c.dialReq = 0
	if e.Err != core.OK {
		c.fails++
		c.state = csBackoff
		return []core.Action{w.later(c, retryDelay(c.fails))}
	}
	c.conn, c.fr, c.state, c.stage, c.out = e.Conn, kwire.NewFramer(maxFrame), csUp, stAPI, nil
	r, _ := kwire.Range(kwire.KeyApiVersions)
	c.ver[rqAPI] = r.Min + int16(w.env.Intn(int(r.Max-r.Min)+1))
	return w.advance(c)
}

func (w *Workload) onTimer(now core.Time, e core.TimerFired) []core.Action {
	for _, c := range w.clients {
		switch {
		case e.ID == wakeID(c):
			if e.Gen != c.wakeGen {
				return nil
			}
			switch {
			case c.state == csBackoff:
				return w.dial(c)
			case c.state == csUp && c.out == nil:
				return w.advance(c)
			}
			return nil
		case e.ID == reqID(c):
			if e.Gen != c.reqGen || c.out == nil {
				return nil
			}
			return w.lostAbort(c, "timeout")
		}
	}
	return nil
}

// advance sends the request of the client's current stage.
func (w *Workload) advance(c *client) []core.Action {
	switch c.stage {
	case stAPI:
		r := kmsg.NewPtrApiVersionsRequest()
		r.ClientSoftwareName, r.ClientSoftwareVersion = "manisim", "1"
		return w.send(c, rqAPI, r)
	case stMeta:
		r := kmsg.NewPtrMetadataRequest()
		for _, t := range w.opts.Topics {
			r.Topics = append(r.Topics, kmsg.MetadataRequestTopic{Topic: &t})
		}
		return w.send(c, rqMeta, r)
	case stList:
		return w.send(c, rqList, w.listReq())
	}
	if c.producer {
		return w.produceNext(c)
	}
	return w.fetchNext(c)
}

// send encodes req at the client's version for kind and starts the timeout.
func (w *Workload) send(c *client, kind reqKind, req kmsg.Request) []core.Action {
	c.corr++
	h := core.RequestHeader{APIKey: req.Key(), APIVersion: c.ver[kind], CorrelationID: c.corr, ClientID: fmt.Sprintf("c%d", c.id)}
	data, err := w.codec.EncodeRequest(h, req)
	if err != nil {
		w.env.Unreachable("kafka: client request encodes", core.Detail{Key: "err", Value: err.Error()})
		return nil
	}
	timeout := reqTimeout
	if kind == rqFetch {
		timeout = fetchWait
	}
	c.out = &inflight{kind: kind, h: h}
	c.sendReq = w.req()
	c.reqGen++
	return []core.Action{
		core.Send{Req: c.sendReq, Conn: c.conn, Data: data},
		core.SetTimer{ID: reqID(c), Gen: c.reqGen, After: timeout},
	}
}

func (w *Workload) onBytes(now core.Time, c *client, data []byte) []core.Action {
	frames, err := c.fr.Push(data)
	var acts []core.Action
	for _, f := range frames {
		if c.state != csUp { // lost while handling an earlier frame
			break
		}
		if c.out == nil { // unsolicited reply
			return append(acts, w.lostAbort(c, "bad_stream")...)
		}
		in := c.out
		resp, derr := w.codec.DecodeResponse(in.h, f)
		if derr != nil {
			return append(acts, w.lostAbort(c, "bad_response")...)
		}
		c.out = nil
		c.reqGen++
		acts = append(acts, core.CancelTimer{ID: reqID(c)})
		acts = append(acts, w.handle(c, in.kind, resp)...)
	}
	if err != nil && c.state == csUp {
		return append(acts, w.lostAbort(c, "bad_stream")...)
	}
	return acts
}

func (w *Workload) handle(c *client, k reqKind, resp core.Response) []core.Action {
	switch r := resp.(type) {
	case *kmsg.ApiVersionsResponse:
		return w.onAPI(c, r)
	case *kmsg.MetadataResponse:
		return w.onMeta(c, r)
	case *kmsg.ListOffsetsResponse:
		return w.onList(c, r)
	case *kmsg.ProduceResponse:
		return w.onProduce(c, r)
	case *kmsg.FetchResponse:
		return w.onFetch(c, r)
	}
	return w.lostAbort(c, "bad_response")
}

// onAPI picks, per API, a version inside both our supported range and the
// broker's.
func (w *Workload) onAPI(c *client, r *kmsg.ApiVersionsResponse) []core.Action {
	if r.ErrorCode != 0 {
		return w.retryStage(c)
	}
	pick := func(k reqKind, key int16) bool {
		lo, hi := int16(0), int16(-1)
		if s, ok := kwire.Range(key); ok {
			lo, hi = s.Min, s.Max
		}
		found := false
		for _, a := range r.ApiKeys {
			if a.ApiKey == key {
				lo, hi, found = max(lo, a.MinVersion), min(hi, a.MaxVersion), true
			}
		}
		if !found || lo > hi {
			return false
		}
		c.ver[k] = lo + int16(w.env.Intn(int(hi-lo)+1))
		return true
	}
	if !pick(rqMeta, kwire.KeyMetadata) || !pick(rqList, kwire.KeyListOffsets) ||
		!pick(rqProduce, kwire.KeyProduce) || !pick(rqFetch, kwire.KeyFetch) {
		return w.retryStage(c)
	}
	c.stage = stMeta
	return w.advance(c)
}

// onMeta checks that every topic exists with the expected partition count.
func (w *Workload) onMeta(c *client, r *kmsg.MetadataResponse) []core.Action {
	ok := len(r.Topics) == len(w.opts.Topics)
	for _, t := range r.Topics {
		ok = ok && t.ErrorCode == 0 && int32(len(t.Partitions)) == w.opts.Partitions
	}
	if !ok {
		return w.retryStage(c)
	}
	switch {
	case !c.producer && !c.c.listed:
		c.stage = stList
	default:
		c.stage = stRun
	}
	return w.advance(c)
}

// retryStage repeats the current stage's request after a backoff (a refused
// but harmless answer: the connection stays).
func (w *Workload) retryStage(c *client) []core.Action {
	c.fails++
	return []core.Action{w.later(c, retryDelay(c.fails))}
}

// lost handles a dead or unusable connection: whatever was outstanding has an
// unknown fate, and the client reconnects (to the next broker) with backoff.
func (w *Workload) lostPeerClosed(c *client, why string) []core.Action {
	return w.lost(c, why, nil)
}

func (w *Workload) lostAbort(c *client, why string) []core.Action {
	return w.lost(c, why, []core.Action{core.CloseConn{Conn: c.conn, Abort: true}})
}

func (w *Workload) lost(c *client, why string, acts []core.Action) []core.Action {
	if c.out != nil {
		switch c.out.kind {
		case rqProduce:
			for _, b := range c.p.cur {
				b.ambiguous = true
			}
		case rqFetch:
			w.abandonFetch(c, why)
		}
	}
	c.out, c.conn, c.fr = nil, 0, nil
	c.reqGen++
	c.fails++
	c.stage, c.state = stAPI, csBackoff
	return append(acts, core.CancelTimer{ID: reqID(c)}, w.later(c, retryDelay(c.fails)))
}

// StateHash folds all state.
func (w *Workload) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(w.nextReq))
	for _, c := range w.clients {
		h.u64(uint64(c.state))
		h.u64(uint64(c.stage))
		h.u64(uint64(c.conn))
		h.u64(uint64(c.dialReq))
		h.u64(uint64(c.sendReq))
		h.u64(uint64(c.dials))
		h.u64(uint64(c.fails))
		h.u64(uint64(c.wakeGen))
		h.u64(uint64(c.reqGen))
		h.u64(uint64(c.corr))
		for _, v := range c.ver {
			h.u64(uint64(v))
		}
		if c.fr != nil {
			h.u64(uint64(c.fr.Buffered()))
		}
		h.bool(c.out != nil)
		if c.out != nil {
			h.u64(uint64(c.out.kind))
			h.u64(uint64(c.out.h.CorrelationID))
		}
		c.p.hash(&h)
		c.c.hash(&h)
	}
	return uint64(h)
}

var (
	_ sim.Workload   = (*Workload)(nil)
	_ sim.PhaseAware = (*Workload)(nil)
)
