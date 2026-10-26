package broker

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

// Conformance C1: the real broker state machine, driven by the fault-free
// harness over veritas.ObjectStore and veritas.ControlPlane, against one
// veritas.Partition per (topic, partition). Client requests are built with
// kwire; every ack, fetch and ListOffsets is compared with the model.

var c1TPs = []core.TopicPartition{core.TP("t", 0), core.TP("t", 1),
	core.TP("u", 0), core.TP("u", 1), core.TP("u", 2)}

// Idem entries carry a producer id: the broker refuses them (decision 41).
type c1ent struct {
	TP, N int
	Idem  bool
}

type c1op struct {
	Kind  string // produce, fetch, list
	Tag   int    // makes values unique
	Conn  int
	Ents  []c1ent
	TP    int
	From  int
	Small bool // fetch: partition limit of 1 byte (one batch)
	Last  bool // list: latest, else earliest
}

type c1 struct {
	t       *testing.T
	h       *harness
	conns   []*client
	model   map[core.TopicPartition]*veritas.Partition
	corrupt bool
}

func newC1(t *testing.T, bug Bug, corrupt bool, tune func(*Config)) *c1 {
	cfg := DefaultConfig()
	cfg.Bug = bug
	if tune != nil {
		tune(&cfg)
	}
	h := newHarness(t, cfg)
	h.cp.CreateTopic("u", 3)
	h.do(core.Started{Self: h.self})
	s := &c1{t: t, h: h, model: map[core.TopicPartition]*veritas.Partition{}, corrupt: corrupt}
	for _, tp := range c1TPs {
		s.model[tp] = &veritas.Partition{}
	}
	s.conns = []*client{h.open(1), h.open(2)}
	if corrupt { // every third GET comes back with a flipped byte
		gets := 0
		h.objHook = func(a core.IOAction, run func() core.Completion) core.Completion {
			d := run()
			if g, ok := d.(core.ObjGetDone); ok && g.Err == core.OK && len(g.Data) > 0 {
				if gets++; gets%3 == 0 {
					g.Data = append([]byte(nil), g.Data...)
					g.Data[len(g.Data)-1] ^= 0x55
					return g
				}
			}
			return d
		}
	}
	return s
}

// await returns c's next response, firing retry timers if it is not there yet.
func (s *c1) await(c *client) (kmsg.Response, error) {
	if _, ok := s.h.timers[timerFlush]; ok {
		s.h.flush()
	}
	if len(c.out) == 0 {
		s.h.fireAll()
	}
	if len(c.out) == 0 {
		return nil, errors.New("no response")
	}
	return s.h.next(c), nil
}

func (s *c1) step(op c1op) error {
	c := s.conns[op.Conn%len(s.conns)]
	switch op.Kind {
	case "produce":
		return s.produce(c, op)
	case "fetch":
		return s.fetch(c, op)
	}
	return s.list(c, op)
}

// await2 sends req and awaits the response.
func (s *c1) await2(c *client, ver int16, req kmsg.Request) (kmsg.Response, error) {
	s.h.send(c, ver, req)
	return s.await(c)
}

type expect struct {
	err  kafkaCode
	base core.Offset
	add  [][]byte // appended to the model on success
	ti   int
}

func (s *c1) produce(c *client, op c1op) error {
	var ents []ent
	var exps []expect
	seen := map[int]bool{}
	for j, ce := range op.Ents {
		ti := ce.TP % len(c1TPs)
		if seen[ti] {
			continue
		}
		seen[ti] = true
		tp := c1TPs[ti]
		m := s.model[tp]
		var vals []string
		for k := 0; k <= ce.N%3; k++ {
			vals = append(vals, fmt.Sprintf("%d.%d.%d", op.Tag, j, k))
		}
		x := expect{ti: ti}
		raw := plain(s.t, vals...)
		if ce.Idem {
			raw = mkBatch(s.t, int64(1000+j), 0, 0, vals...)
			x.err, x.base = errInvalidRequest, -1
		} else {
			x.base = m.HighWatermark()
			for _, v := range vals {
				x.add = append(x.add, []byte(v))
			}
		}
		ents = append(ents, e(tp.Topic, tp.Partition, raw))
		exps = append(exps, x)
	}
	if len(ents) == 0 {
		return nil
	}
	r, err := s.await2(c, verProduce, prodReq(-1, ents...))
	if err != nil {
		return err
	}
	got := produced(r)
	if len(got) != len(exps) {
		return fmt.Errorf("produce: %d results for %d entries", len(got), len(exps))
	}
	for i, x := range exps {
		tp := c1TPs[x.ti]
		if kafkaCode(got[i].ErrorCode) != x.err || got[i].BaseOffset != int64(x.base) {
			return fmt.Errorf("produce %v: error %d base %d, want error %d base %d (model hwm %d)",
				tp, got[i].ErrorCode, got[i].BaseOffset, x.err, x.base, s.model[tp].HighWatermark())
		}
		if len(x.add) > 0 {
			s.model[tp].Append(x.add)
		}
	}
	return nil
}

func (s *c1) fetch(c *client, op c1op) error {
	tp := c1TPs[op.TP%len(c1TPs)]
	m := s.model[tp]
	hwm := m.HighWatermark()
	from := core.Offset(op.From % int(hwm+2)) // hwm+1 is out of range
	max := int32(1 << 20)
	if op.Small {
		max = 1
	}
	r, err := s.await2(c, verFetch, fetchReq(tp, int64(from), max))
	if err != nil {
		return err
	}
	p := fetched(r)
	if p.HighWatermark != int64(hwm) {
		return fmt.Errorf("fetch %v@%d: hwm %d, want %d", tp, from, p.HighWatermark, hwm)
	}
	if from > hwm {
		if kafkaCode(p.ErrorCode) != errOffsetOutOfRange {
			return fmt.Errorf("fetch %v@%d beyond hwm %d: error %d", tp, from, hwm, p.ErrorCode)
		}
		return nil
	}
	if p.ErrorCode != 0 || p.LastStableOffset != int64(hwm) || p.LogStartOffset != 0 {
		return fmt.Errorf("fetch %v@%d: error %d lso %d log start %d", tp, from, p.ErrorCode, p.LastStableOffset, p.LogStartOffset)
	}
	bs, err := kwire.SplitBatches(p.RecordBatches)
	if err != nil {
		return fmt.Errorf("fetch %v@%d: %w", tp, from, err)
	}
	if from == hwm {
		if len(bs) != 0 {
			return fmt.Errorf("fetch %v@hwm %d returned %d batches", tp, hwm, len(bs))
		}
		return nil
	}
	if len(bs) == 0 || op.Small && len(bs) != 1 {
		return fmt.Errorf("fetch %v@%d small=%v: %d batches", tp, from, op.Small, len(bs))
	}
	next := core.Offset(-1)
	for _, b := range bs {
		bt, err := kwire.ParseBatch(b) // verifies the CRC
		if err != nil {
			return fmt.Errorf("fetch %v@%d: %w", tp, from, err)
		}
		recs, err := bt.Records()
		if err != nil {
			return fmt.Errorf("fetch %v@%d: %w", tp, from, err)
		}
		base := core.Offset(bt.BaseOffset)
		if next < 0 { // the first batch is the one containing from
			next = base
			if base > from || base+core.Offset(len(recs)) <= from {
				return fmt.Errorf("fetch %v@%d: first batch is [%d,%d)", tp, from, base, base+core.Offset(len(recs)))
			}
		}
		if base != next {
			return fmt.Errorf("fetch %v@%d: batch at %d, want %d (offsets not contiguous)", tp, from, base, next)
		}
		want, _ := m.Read(base, len(recs))
		if len(want) != len(recs) {
			return fmt.Errorf("fetch %v@%d: batch [%d,+%d) beyond model hwm %d", tp, from, base, len(recs), hwm)
		}
		for i, rec := range recs {
			if string(rec.Value) != string(want[i].Value) {
				return fmt.Errorf("fetch %v@%d: offset %d value %q, want %q", tp, from, base+core.Offset(i), rec.Value, want[i].Value)
			}
		}
		next = base + core.Offset(len(recs))
	}
	if !op.Small && next != hwm {
		return fmt.Errorf("fetch %v@%d: stopped at %d, hwm %d", tp, from, next, hwm)
	}
	return nil
}

func (s *c1) list(c *client, op c1op) error {
	tp := c1TPs[op.TP%len(c1TPs)]
	ts, want := int64(-2), core.Offset(0)
	if op.Last {
		ts, want = -1, s.model[tp].HighWatermark()
	}
	q := kmsg.NewPtrListOffsetsRequest()
	q.ReplicaID = -1
	q.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: tp.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{
		{Partition: tp.Partition, Timestamp: ts, CurrentLeaderEpoch: -1}}}}
	r, err := s.await2(c, verList, q)
	if err != nil {
		return err
	}
	p := r.(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0]
	if p.ErrorCode != 0 || p.Offset != int64(want) {
		return fmt.Errorf("list %v ts %d: error %d offset %d, want %d", tp, ts, p.ErrorCode, p.Offset, want)
	}
	return nil
}

func (s *c1) invariant() error {
	if len(s.h.env.viol) > 0 {
		return fmt.Errorf("assertion violations: %v", s.h.env.viol)
	}
	for _, c := range s.conns {
		if len(c.out) != 0 {
			return fmt.Errorf("conn %d: stray response", c.id)
		}
	}
	return nil
}

func genC1(r *veritastest.Rand, i int) c1op {
	op := c1op{Tag: i, Conn: r.Intn(2), TP: r.Intn(len(c1TPs)), From: r.Intn(1 << 10), Small: r.Intn(4) == 0, Last: r.Intn(2) == 0}
	switch k := r.Intn(20); {
	case k < 12:
		op.Kind = "produce"
		for n := 1 + r.Intn(2); n > 0; n-- {
			e := c1ent{TP: r.Intn(len(c1TPs)), N: r.Intn(3), Idem: r.Intn(5) == 0}
			op.Ents = append(op.Ents, e)
		}
	case k < 17:
		op.Kind = "fetch"
	default:
		op.Kind = "list"
	}
	return op
}

func shrinkC1(op c1op) []c1op {
	var out []c1op
	if len(op.Ents) > 1 {
		c := op
		c.Ents = op.Ents[:1]
		out = append(out, c)
	}
	return out
}

func c1Check(t *testing.T, bug Bug, corrupt bool, tune func(*Config)) veritastest.Check[*c1, c1op] {
	return veritastest.Check[*c1, c1op]{
		Name: "c1op", Ops: 80, MaxShrinkRuns: 300,
		New:       func() *c1 { return newC1(t, bug, corrupt, tune) },
		Gen:       genC1,
		Step:      func(s *c1, op c1op) error { return s.step(op) },
		Invariant: func(s *c1) error { return s.invariant() },
		Shrink:    shrinkC1,
		Format:    func(op c1op) string { return fmt.Sprintf("%#v", op) },
	}
}

// c1Fail runs seeds 1..n and returns the first failure.
func c1Fail(t *testing.T, bug Bug, corrupt bool, n int) *veritastest.Failure[c1op] {
	c := c1Check(t, bug, corrupt, nil)
	for seed := uint64(1); seed <= uint64(n); seed++ {
		if f := c.RunSeed(seed); f != nil {
			t.Log(c.Report(f, ""))
			return f
		}
	}
	return nil
}

func TestConformanceBroker(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corruptGets=%v", corrupt), func(t *testing.T) {
			veritastest.Run(t, c1Check(t, BugNone, corrupt, nil), veritastest.Options{Seeds: 100, Seed: -1})
		})
	}
}

// Small segment caps make every seal rule fire inside the generated runs, and
// the log must still match the Partition spec.
func TestConformanceBrokerSmallSegments(t *testing.T) {
	caps := map[string]func(*Config){
		"bytes":      func(c *Config) { c.MaxSegmentBytes = 120 },
		"batches":    func(c *Config) { c.MaxSegmentBatches = 2 },
		"partitions": func(c *Config) { c.MaxSegmentPartitions = 2 },
		"all": func(c *Config) {
			c.MaxSegmentBytes, c.MaxSegmentBatches, c.MaxSegmentPartitions = 200, 3, 2
		},
	}
	for _, name := range []string{"bytes", "batches", "partitions", "all"} {
		for _, corrupt := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/corruptGets=%v", name, corrupt), func(t *testing.T) {
				veritastest.Run(t, c1Check(t, BugNone, corrupt, caps[name]), veritastest.Options{Seeds: 50, Seed: -1})
			})
		}
	}
}

// The check is not vacuous: each broken broker is caught.
func TestConformanceBrokerCatchesMutants(t *testing.T) {
	for _, m := range []struct {
		bug     Bug
		corrupt bool
	}{{BugWrongOffset, false}, {BugServeCorrupt, true}} {
		t.Run(fmt.Sprintf("bug%d", m.bug), func(t *testing.T) {
			if c1Fail(t, m.bug, m.corrupt, 20) == nil {
				t.Fatal("mutant not caught")
			}
		})
	}
}
