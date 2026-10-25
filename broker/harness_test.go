package broker

import (
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
	"manifester/veritas"
)

// testEnv is a seeded core.Env that records assertion violations.
type testEnv struct {
	state uint64
	viol  []string
	reach []string
}

func (e *testEnv) Uint64() uint64 {
	e.state += 0x9e3779b97f4a7c15
	z := e.state
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}
func (e *testEnv) Intn(n int) int { return int(e.Uint64() % uint64(n)) }
func (e *testEnv) NewID() core.ID {
	var id core.ID
	for i := 0; i < 16; i += 8 {
		v := e.Uint64()
		for j := range 8 {
			id[i+j] = byte(v >> (8 * j))
		}
	}
	return id
}
func (e *testEnv) Maybe(core.Site) bool           { return false }
func (e *testEnv) MaybeP(core.Site, float64) bool { return false }
func (e *testEnv) Fault(core.Site, float64) bool  { return false }
func (e *testEnv) Always(c bool, p core.Prop, _ ...core.Detail) {
	if !c {
		e.viol = append(e.viol, string(p))
	}
}
func (e *testEnv) AlwaysOrUnreachable(c bool, p core.Prop, d ...core.Detail) { e.Always(c, p, d...) }
func (e *testEnv) Sometimes(bool, core.Prop)                                 {}
func (e *testEnv) Reachable(p core.Prop)                                     { e.reach = append(e.reach, string(p)) }
func (e *testEnv) Unreachable(p core.Prop, _ ...core.Detail)                 { e.viol = append(e.viol, string(p)) }

// client is the test's view of one connection.
type client struct {
	id   core.ConnID
	want []kwire.RequestHeader
	out  [][]byte // response frames (length prefix stripped) not yet decoded
	fr   *kwire.Framer
	corr int32
}

// harness drives a Broker synchronously against the veritas specs. Completions
// are computed when the I/O action is issued and delivered through a queue, so
// tests can hold them back (holdCP, holdPut) or fault them (cpHook, objHook).
type harness struct {
	t     *testing.T
	b     *Broker
	env   *testEnv
	store *veritas.ObjectStore
	cp    *veritas.ControlPlane
	cfg   Config
	self  core.NodeRef
	now   core.Time

	// cpHook/objHook may replace an I/O action's outcome; exec runs the spec.
	cpHook  func(a core.CPCall, exec func() core.Completion) core.Completion
	objHook func(a core.IOAction, exec func() core.Completion) core.Completion
	holdCP  bool
	holdPut bool
	held    []core.Completion

	queue   []core.Completion
	timers  map[core.TimerID]core.TimerGen
	clients map[core.ConnID]*client
	all     []core.Action // every action emitted, in order
	paused  map[core.ConnID]bool
	closed  map[core.ConnID]bool
	exited  bool
	cpCalls []core.CPOp
	puts    []string
}

func newHarness(t *testing.T, cfg Config) *harness {
	h := &harness{t: t, env: &testEnv{state: 7}, store: veritas.NewObjectStore(), cp: veritas.NewControlPlane(),
		cfg: cfg, timers: map[core.TimerID]core.TimerGen{}, clients: map[core.ConnID]*client{},
		paused: map[core.ConnID]bool{}, closed: map[core.ConnID]bool{}}
	h.cp.CreateTopic("t", 2)
	h.self = core.NodeRef{Node: "b1", Inc: core.Incarnation{1}}
	h.b = New(cfg)(h.env, h.self).(*Broker)
	return h
}

// startedHarness boots the broker.
func startedHarness(t *testing.T, cfg Config) *harness {
	h := newHarness(t, cfg)
	h.do(core.Started{Self: h.self})
	return h
}

func (h *harness) step(body core.EventBody) []core.Action {
	h.now += core.Time(core.Millisecond)
	acts := h.b.Step(core.Event{At: h.now, Wall: core.WallTime(h.now), Body: body})
	h.all = append(h.all, acts...)
	for _, a := range acts {
		h.exec(a)
	}
	return acts
}

// do steps body and then delivers queued completions until none is left.
func (h *harness) do(body core.EventBody) []core.Action {
	acts := h.step(body)
	h.run()
	return acts
}

func (h *harness) run() {
	for len(h.queue) > 0 {
		c := h.queue[0]
		h.queue = h.queue[1:]
		h.step(c)
	}
}

func (h *harness) deliver(c core.Completion) {
	_, isCP := c.(core.CPDone)
	_, isPut := c.(core.ObjPutDone)
	if (isCP && h.holdCP) || (isPut && h.holdPut) {
		h.held = append(h.held, c)
		return
	}
	h.queue = append(h.queue, c)
}

// releaseHeld delivers held completions and runs to quiescence.
func (h *harness) releaseHeld() {
	held := h.held
	h.held = nil
	h.queue = append(h.queue, held...)
	h.run()
}

func (h *harness) exec(a core.Action) {
	switch a := a.(type) {
	case core.ObjPut:
		h.puts = append(h.puts, a.Key)
		run := func() core.Completion { return h.store.Exec(a) }
		h.deliver(h.objDone(a, run))
	case core.ObjGet:
		run := func() core.Completion { return h.store.Exec(a) }
		h.deliver(h.objDone(a, run))
	case core.CPCall:
		h.cpCalls = append(h.cpCalls, a.Op)
		run := func() core.Completion { return h.cp.Exec(a) }
		if h.cpHook != nil {
			h.deliver(h.cpHook(a, run))
		} else {
			h.deliver(run())
		}
	case core.SetTimer:
		h.timers[a.ID] = a.Gen
	case core.CancelTimer:
		delete(h.timers, a.ID)
	case core.Send:
		c := h.clients[a.Conn]
		if c == nil {
			h.t.Fatalf("send to unknown conn %d", a.Conn)
		}
		frames, err := c.fr.Push(a.Data)
		if err != nil {
			h.t.Fatal(err)
		}
		c.out = append(c.out, frames...)
	case core.CloseConn:
		h.closed[a.Conn] = true
	case core.PauseReads:
		h.paused[a.Conn] = true
	case core.ResumeReads:
		h.paused[a.Conn] = false
	case core.Exit:
		h.exited = true
	}
}

func (h *harness) objDone(a core.IOAction, run func() core.Completion) core.Completion {
	if h.objHook != nil {
		return h.objHook(a, run)
	}
	return run()
}

// fire delivers the current generation of timer id.
func (h *harness) fire(id core.TimerID) {
	g, ok := h.timers[id]
	if !ok {
		h.t.Fatalf("timer %d not armed", id)
	}
	delete(h.timers, id)
	h.do(core.TimerFired{ID: id, Gen: g})
}

// fireAll fires every armed timer, lowest id first, until none is armed.
func (h *harness) fireAll() {
	for n := 0; len(h.timers) > 0; n++ {
		if n > 1000 {
			h.t.Fatal("timers never settle")
		}
		var ids []core.TimerID
		for id := range h.timers {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		h.fire(ids[0])
	}
}

func (h *harness) flush() { h.fire(timerFlush) }

// ---- client side ----

func (h *harness) open(id core.ConnID) *client {
	c := &client{id: id, fr: kwire.NewFramer(1 << 26)}
	h.clients[id] = c
	h.do(core.ConnOpened{Conn: id, Peer: "test"})
	return c
}

// frame encodes a request without sending it.
func (h *harness) frame(c *client, ver int16, req kmsg.Request) []byte {
	c.corr++
	hd := kwire.RequestHeader{APIKey: req.Key(), APIVersion: ver, CorrelationID: c.corr, ClientID: "test"}
	b, err := kwire.Codec{}.EncodeRequest(hd, req)
	if err != nil {
		h.t.Fatal(err)
	}
	c.want = append(c.want, hd)
	return b
}

// send writes requests to the broker in one chunk and runs to quiescence.
func (h *harness) send(c *client, ver int16, req kmsg.Request) {
	h.do(core.BytesReceived{Conn: c.id, Data: h.frame(c, ver, req)})
}

// next decodes the oldest undelivered response on c.
func (h *harness) next(c *client) kmsg.Response {
	h.t.Helper()
	if len(c.out) == 0 {
		h.t.Fatalf("conn %d: no response", c.id)
	}
	hd := c.want[0]
	resp, err := kwire.Codec{}.DecodeResponse(hd, c.out[0])
	if err != nil {
		h.t.Fatalf("conn %d: decode %d: %v", c.id, hd.APIKey, err)
	}
	c.want, c.out = c.want[1:], c.out[1:]
	return resp.(kmsg.Response)
}

func (h *harness) call(c *client, ver int16, req kmsg.Request) kmsg.Response {
	h.t.Helper()
	h.send(c, ver, req)
	return h.next(c)
}

// ---- request builders ----

const (
	verProduce  = 7
	verFetch    = 11
	verList     = 5
	verMetadata = 9
)

var params = kwire.BatchParams{FirstTimestamp: 1000, MaxTimestamp: 1000}

// mkBatch builds a batch of len(vals) records; pid -1 means non-idempotent.
func mkBatch(t testing.TB, pid int64, epoch int16, seq int32, vals ...string) []byte {
	t.Helper()
	var recs []kmsg.Record
	for _, v := range vals {
		recs = append(recs, kmsg.Record{Value: []byte(v)})
	}
	p := params
	p.ProducerID, p.ProducerEpoch, p.BaseSequence = pid, epoch, seq
	b, err := kwire.BuildBatch(p, recs)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func plain(t testing.TB, vals ...string) []byte { return mkBatch(t, -1, -1, -1, vals...) }

type ent struct {
	tp  core.TopicPartition
	rec []byte
}

func e(topic string, part int32, rec []byte) ent {
	return ent{tp: core.TP(topic, part), rec: rec}
}

func prodReq(acks int16, es ...ent) *kmsg.ProduceRequest {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = acks, 1000
	for _, e := range es {
		r.Topics = append(r.Topics, kmsg.ProduceRequestTopic{Topic: e.tp.Topic,
			Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: e.tp.Partition, Records: e.rec}}})
	}
	return r
}

// produced flattens a produce response's partitions in request order.
func produced(r kmsg.Response) []kmsg.ProduceResponseTopicPartition {
	var out []kmsg.ProduceResponseTopicPartition
	for _, t := range r.(*kmsg.ProduceResponse).Topics {
		out = append(out, t.Partitions...)
	}
	return out
}

func fetchReq(tp core.TopicPartition, off int64, max int32) *kmsg.FetchRequest {
	r := kmsg.NewPtrFetchRequest()
	r.MaxBytes, r.MinBytes, r.MaxWaitMillis = 1<<20, 1, 100
	r.Topics = []kmsg.FetchRequestTopic{{Topic: tp.Topic, Partitions: []kmsg.FetchRequestTopicPartition{
		{Partition: tp.Partition, FetchOffset: off, PartitionMaxBytes: max, CurrentLeaderEpoch: -1}}}}
	return r
}

func fetched(r kmsg.Response) kmsg.FetchResponseTopicPartition {
	return r.(*kmsg.FetchResponse).Topics[0].Partitions[0]
}

// produceOne produces one entry, flushes and returns its partition result.
func (h *harness) produceOne(c *client, e ent) kmsg.ProduceResponseTopicPartition {
	h.t.Helper()
	h.send(c, verProduce, prodReq(-1, e))
	if _, ok := h.timers[timerFlush]; ok {
		h.flush()
	}
	return produced(h.next(c))[0]
}

// sends lists the actions of the given kind in emission order.
func sends(all []core.Action) []core.Send {
	var out []core.Send
	for _, a := range all {
		if s, ok := a.(core.Send); ok {
			out = append(out, s)
		}
	}
	return out
}

func factsNamed(all []core.Action, name string) []core.Fact {
	var out []core.Fact
	for _, a := range all {
		if e, ok := a.(core.Emit); ok && e.Fact.Name == name {
			out = append(out, e.Fact)
		}
	}
	return out
}

func (h *harness) noViolations() {
	h.t.Helper()
	if len(h.env.viol) > 0 {
		h.t.Fatalf("assertion violations: %v", h.env.viol)
	}
}
