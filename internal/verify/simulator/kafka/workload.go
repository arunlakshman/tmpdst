// Package kafka is the Kafka scenario: sans-I/O Kafka clients (at-least-once
// producers, no idempotence, and consumers over kafkawire), an oracle that rebuilds each
// partition's committed log from the control plane and object store truth, the
// invariants over it, and the campaign preset. See docs/decisions.md rows
// 12-17, 26-31.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/internal/session"
	"mds/internal/wire/kafkawire"
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

func (o Opts) requestTimeout() core.Duration {
	if o.ReqTimeout > 0 {
		return o.ReqTimeout
	}
	return requestTimeout
}

func (o Opts) topicPartitions() []core.TopicPartition {
	var out []core.TopicPartition
	for _, t := range o.Topics {
		for p := int32(0); p < o.Partitions; p++ {
			out = append(out, core.NewTopicPartition(t, p))
		}
	}
	return out
}

const (
	requestTimeout = 1500 * core.Millisecond // a request with no answer: drop the connection (Kafka does)
	fetchWait      = 10 * core.Second        // a fetch reads a batch at a time from S3: it is slow on a long log
	maxAttempts    = 3                       // sends of one produce batch before it is abandoned
	maxFrame       = 8 << 20
)

type stage uint8

const (
	stateAPIVersions stage = iota
	stateMetadata
	stateListOffsets // consumers: ListOffsets earliest
	stateRunning
)

type requestKind uint8

const (
	requestAPIVersions requestKind = iota
	requestMetadata
	requestListOffsets
	requestProduce
	requestFetch
	kindCount
)

type waitingRequests struct {
	kind requestKind
	h    core.RequestHeader
}

type client struct {
	session.Client
	producer bool
	stage    stage
	fr       *kafkawire.Framer
	corr     int32
	ver      [kindCount]int16
	out      *waitingRequests
	p        producer
	c        consumer
}

// Workload is the Kafka clients. Each client has one request outstanding at a
// time, which also gives max.in.flight=1 per (producer, partition): a batch is
// resolved (acked or abandoned) before the next one for that partition is
// built, so the oracle can check that acked offsets follow send order.
type Workload struct {
	env     core.Environment
	self    core.NodeInstance
	opts    Opts
	tps     []core.TopicPartition
	history simulator.History
	codec   kafkawire.Codec
	loop    session.Loop[*client]
}

// NewWorkload builds the workload machine.
func NewWorkload(env core.Environment, self core.NodeInstance, brokers []core.NodeInstance, o Opts) *Workload {
	w := &Workload{env: env, self: self, opts: o, tps: o.topicPartitions()}
	w.loop = session.Loop[*client]{
		Connected: func(c *client) bool { return c.State == session.Connected },
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

// Name implements simulator.Workload.
func (w *Workload) Name() string { return "kafka" }

// SetHistory is called by the kernel.
func (w *Workload) SetHistory(h simulator.History) { w.history = h }

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
func (w *Workload) Step(event core.Event) []core.Action { return w.loop.Step(event) }

// OnPhase implements simulator.PhaseAware. Entering Settling the producers start no
// new batch (in-flight ones finish); the consumers keep reading. Entering
// Settle (the drain) each consumer stops after a fetch that finds the end of
// every partition, so no read is outstanding at the end.
func (w *Workload) OnPhase(p simulator.Phase) []core.Action {
	for _, c := range w.loop.Clients {
		switch p {
		case simulator.PhaseRecovering:
			c.p.noNew = true
		case simulator.PhaseRecover:
			c.c.drain = true
		}
	}
	return nil
}

func (w *Workload) onReady(c *client) []core.Action {
	c.fr, c.stage, c.out = kafkawire.NewFramer(maxFrame), stateAPIVersions, nil
	r, _ := kafkawire.Range(kafkawire.KeyApiVersions)
	c.ver[requestAPIVersions] = r.Min + int16(w.env.Intn(int(r.Max-r.Min)+1))
	return w.advance(c)
}

// advance sends the request of the client's current stage.
func (w *Workload) advance(c *client) []core.Action {
	switch c.stage {
	case stateAPIVersions:
		r := kmsg.NewPtrApiVersionsRequest()
		r.ClientSoftwareName, r.ClientSoftwareVersion = "mdssimulator", "1"
		return w.send(c, requestAPIVersions, r)
	case stateMetadata:
		r := kmsg.NewPtrMetadataRequest()
		for _, t := range w.opts.Topics {
			r.Topics = append(r.Topics, kmsg.MetadataRequestTopic{Topic: &t})
		}
		return w.send(c, requestMetadata, r)
	case stateListOffsets:
		return w.send(c, requestListOffsets, w.listReq())
	}
	if c.producer {
		return w.produceNext(c)
	}
	return w.fetchNext(c)
}

// send encodes req at the client's version for kind and starts the timeout.
func (w *Workload) send(c *client, kind requestKind, request kmsg.Request) []core.Action {
	c.corr++
	h := core.RequestHeader{APIKey: request.Key(), APIVersion: c.ver[kind], CorrelationID: c.corr, ClientID: fmt.Sprintf("c%d", c.ID)}
	data, err := w.codec.EncodeRequest(h, request)
	if err != nil {
		w.env.Unreachable("kafka: client request encodes", core.ContextPair{Key: "err", Value: err.Error()})
		return nil
	}
	timeout := w.opts.requestTimeout()
	if kind == requestFetch {
		timeout = fetchWait
	}
	c.out = &waitingRequests{kind: kind, h: h}
	return w.loop.Request(c, data, timeout)
}

func (w *Workload) onBytes(c *client, data []byte) []core.Action {
	frames, err := c.fr.Push(data)
	var acts []core.Action
	for _, f := range frames {
		if c.State != session.Connected { // lost while handling an earlier frame
			break
		}
		if c.out == nil { // unsolicited reply
			return append(acts, w.loop.Abort(c, "bad_stream")...)
		}
		in := c.out
		response, derr := w.codec.DecodeResponse(in.h, f)
		if derr != nil {
			return append(acts, w.loop.Abort(c, "bad_response")...)
		}
		c.out = nil
		acts = append(acts, c.Answered())
		acts = append(acts, w.handle(c, in.kind, response)...)
	}
	if err != nil && c.State == session.Connected {
		return append(acts, w.loop.Abort(c, "bad_stream")...)
	}
	return acts
}

func (w *Workload) handle(c *client, k requestKind, response core.Response) []core.Action {
	switch r := response.(type) {
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
	pick := func(k requestKind, key int16) bool {
		lo, hi := int16(0), int16(-1)
		if s, ok := kafkawire.Range(key); ok {
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
	if !pick(requestMetadata, kafkawire.KeyMetadata) || !pick(requestListOffsets, kafkawire.KeyListOffsets) ||
		!pick(requestProduce, kafkawire.KeyProduce) || !pick(requestFetch, kafkawire.KeyFetch) {
		return w.retryStage(c)
	}
	c.stage = stateMetadata
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
		c.stage = stateListOffsets
	default:
		c.stage = stateRunning
	}
	return w.advance(c)
}

// retryStage repeats the current stage's request after a backoff (a refused
// but harmless answer: the connection stays).
func (w *Workload) retryStage(c *client) []core.Action {
	c.Fails++
	return []core.Action{c.Wake(session.RetryWaitTime(c.Fails))}
}

// lost settles whatever was outstanding, whose fate is unknown.
func (w *Workload) lost(c *client, why string) {
	if c.out != nil {
		switch c.out.kind {
		case requestProduce:
			for _, b := range c.p.cur {
				b.ambiguous = true
			}
		case requestFetch:
			w.abandonFetch(c, why)
		}
	}
	c.out, c.fr, c.stage = nil, nil, stateAPIVersions
}

// StateHash folds all state.
func (w *Workload) StateHash() uint64 {
	h := core.NewHasher()
	h.Uint64Value(uint64(w.loop.NextReq))
	for _, c := range w.loop.Clients {
		h.Uint64Value(uint64(c.State))
		h.Uint64Value(uint64(c.stage))
		h.Uint64Value(uint64(c.Conn))
		h.Uint64Value(uint64(c.DialReq))
		h.Uint64Value(uint64(c.SendReq))
		h.Uint64Value(uint64(c.Dials))
		h.Uint64Value(uint64(c.Fails))
		h.Uint64Value(uint64(c.WakeGen))
		h.Uint64Value(uint64(c.ReqGen))
		h.Uint64Value(uint64(c.corr))
		for _, v := range c.ver {
			h.Uint64Value(uint64(v))
		}
		if c.fr != nil {
			h.Uint64Value(uint64(c.fr.Buffered()))
		}
		h.Bool(c.out != nil)
		if c.out != nil {
			h.Uint64Value(uint64(c.out.kind))
			h.Uint64Value(uint64(c.out.h.CorrelationID))
		}
		c.p.hash(&h)
		c.c.hash(&h)
	}
	return uint64(h)
}

var (
	_ simulator.Workload            = (*Workload)(nil)
	_ simulator.ReceivesPhaseChange = (*Workload)(nil)
)
