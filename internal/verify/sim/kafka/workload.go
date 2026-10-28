// Package kafka is the Kafka scenario: sans-I/O Kafka clients (at-least-once
// producers, no idempotence, and consumers over kwire), an oracle that rebuilds each
// partition's committed log from the control plane and object store truth, the
// invariants over it, and the campaign preset. See docs/decisions.md rows
// 12-17, 26-31.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/internal/session"
	"manifester/internal/wire/kwire"
)

// Opts configures the workload.
type Opts struct {
	Producers, Consumers int
	Topics               []string
	Partitions           int32
	// MaxAttempts caps client produce sends before abandon. Zero means 3.
	MaxAttempts int
	// ReqTimeout is how long a non-fetch request waits before the client drops
	// the connection. Zero means 1500 ms.
	ReqTimeout core.Duration
}

// DefaultOpts is 3 at-least-once producers and 2 consumers over 2 topics x 3
// partitions.
func DefaultOpts() Opts {
	return Opts{Producers: 3, Consumers: 2, Topics: []string{"t0", "t1"}, Partitions: 3}
}

func (o Opts) maxAttempts() int {
	if o.MaxAttempts > 0 {
		return o.MaxAttempts
	}
	return maxAttempts
}

func (o Opts) reqTimeout() core.Duration {
	if o.ReqTimeout > 0 {
		return o.ReqTimeout
	}
	return reqTimeout
}

func (o Opts) tps() []core.TopicPartition {
	var out []core.TopicPartition
	for _, t := range o.Topics {
		for p := int32(0); p < o.Partitions; p++ {
			out = append(out, core.TP(t, p))
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
	session.Client
	producer bool
	stage    stage
	fr       *kwire.Framer
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
	env   core.Env
	self  core.NodeRef
	opts  Opts
	tps   []core.TopicPartition
	hist  sim.History
	codec kwire.Codec
	loop  session.Loop[*client]
}

// NewWorkload builds the workload machine.
func NewWorkload(env core.Env, self core.NodeRef, brokers []core.NodeRef, o Opts) *Workload {
	w := &Workload{env: env, self: self, opts: o, tps: o.tps()}
	w.loop = session.Loop[*client]{
		Connected: func(c *client) bool { return c.State == session.Up },
		Busy:      func(c *client) bool { return c.out != nil },
		Ready:     w.onReady,
		Idle:      w.advance,
		Bytes:     w.onBytes,
		Lost:      w.lost,
	}
	for _, b := range brokers {
		w.loop.Brokers = append(w.loop.Brokers, b.Node)
	}
	for i := range o.Producers + o.Consumers {
		c := &client{Client: session.Client{ID: i}, producer: i < o.Producers}
		if !c.producer {
			c.c.pos = make([]int64, len(w.tps))
		}
		w.loop.Clients = append(w.loop.Clients, c)
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
	for _, c := range w.loop.Clients {
		if len(c.p.cur) > 0 {
			return false
		}
	}
	return true
}

// Step implements core.StateMachine.
func (w *Workload) Step(ev core.Event) []core.Action { return w.loop.Step(ev) }

// OnPhase implements sim.PhaseAware. Entering Settling the producers start no
// new batch (in-flight ones finish); the consumers keep reading. Entering
// Settle (the drain) each consumer stops after a fetch that finds the end of
// every partition, so no read is outstanding at the end.
func (w *Workload) OnPhase(p sim.Phase) []core.Action {
	for _, c := range w.loop.Clients {
		switch p {
		case sim.PhaseSettling:
			c.p.noNew = true
		case sim.PhaseSettle:
			c.c.drain = true
		}
	}
	return nil
}

func (w *Workload) onReady(c *client) []core.Action {
	c.fr, c.stage, c.out = kwire.NewFramer(maxFrame), stAPI, nil
	r, _ := kwire.Range(kwire.KeyApiVersions)
	c.ver[rqAPI] = r.Min + int16(w.env.Intn(int(r.Max-r.Min)+1))
	return w.advance(c)
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
	h := core.RequestHeader{APIKey: req.Key(), APIVersion: c.ver[kind], CorrelationID: c.corr, ClientID: fmt.Sprintf("c%d", c.ID)}
	data, err := w.codec.EncodeRequest(h, req)
	if err != nil {
		w.env.Unreachable("kafka: client request encodes", core.Detail{Key: "err", Value: err.Error()})
		return nil
	}
	timeout := w.opts.reqTimeout()
	if kind == rqFetch {
		timeout = fetchWait
	}
	c.out = &inflight{kind: kind, h: h}
	return w.loop.Request(c, data, timeout)
}

func (w *Workload) onBytes(c *client, data []byte) []core.Action {
	frames, err := c.fr.Push(data)
	var acts []core.Action
	for _, f := range frames {
		if c.State != session.Up { // lost while handling an earlier frame
			break
		}
		if c.out == nil { // unsolicited reply
			return append(acts, w.loop.Abort(c, "bad_stream")...)
		}
		in := c.out
		resp, derr := w.codec.DecodeResponse(in.h, f)
		if derr != nil {
			return append(acts, w.loop.Abort(c, "bad_response")...)
		}
		c.out = nil
		acts = append(acts, c.Answered())
		acts = append(acts, w.handle(c, in.kind, resp)...)
	}
	if err != nil && c.State == session.Up {
		return append(acts, w.loop.Abort(c, "bad_stream")...)
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
	return w.loop.Abort(c, "bad_response")
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
	c.Fails++
	return []core.Action{c.Wake(session.RetryDelay(c.Fails))}
}

// lost settles whatever was outstanding, whose fate is unknown.
func (w *Workload) lost(c *client, why string) {
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
	c.out, c.fr, c.stage = nil, nil, stAPI
}

// StateHash folds all state.
func (w *Workload) StateHash() uint64 {
	h := core.NewHasher()
	h.U64(uint64(w.loop.NextReq))
	for _, c := range w.loop.Clients {
		h.U64(uint64(c.State))
		h.U64(uint64(c.stage))
		h.U64(uint64(c.Conn))
		h.U64(uint64(c.DialReq))
		h.U64(uint64(c.SendReq))
		h.U64(uint64(c.Dials))
		h.U64(uint64(c.Fails))
		h.U64(uint64(c.WakeGen))
		h.U64(uint64(c.ReqGen))
		h.U64(uint64(c.corr))
		for _, v := range c.ver {
			h.U64(uint64(v))
		}
		if c.fr != nil {
			h.U64(uint64(c.fr.Buffered()))
		}
		h.Bool(c.out != nil)
		if c.out != nil {
			h.U64(uint64(c.out.kind))
			h.U64(uint64(c.out.h.CorrelationID))
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
