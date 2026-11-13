package broker

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
	"pgregory.net/rapid"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	brokerspec "github.com/manifesterio/mds/broker/spec"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/spectest"
	"github.com/manifesterio/mds/dst/trace"
)

// Conformance C1 tests the REAL broker logic against SIMULATED fakes of the
// world. The fakes are the speccheck spec object store and the speccheck spec
// control plane, and the harness runs without faults. The test sends client
// requests built with kafkawire. It compares every ack, fetch and ListOffsets
// answer with one brokerspec.Broker per topic partition.
//
// The check is a spectest.Lockstep. The spec side (c1Spec) keeps one
// brokerspec.Broker for each topic partition and gives the answer that a correct
// broker gives. The subject side (c1) sends the request to the real broker and
// reads the answer. Both give a c1result, and equalC1 compares them.

var c1TPs = []core.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("t", 1),
	contract.NewTopicPartition("u", 0), contract.NewTopicPartition("u", 1), contract.NewTopicPartition("u", 2)}

// An Idem entry carries a producer id. The broker refuses these entries
// (decision 41), so the test expects an error and no new record.
type c1ent struct {
	TopicPartitionIndex, N int
	Idem                   bool
}

// c1op is one operation. It has no tag: both sides count the operations, and
// the n-th operation makes its record values from the number n (so every
// value is unique).
type c1op struct {
	Kind                string // produce, fetch, list
	Conn                int
	Ents                []c1ent
	TopicPartitionIndex int
	From                int
	Small               bool // fetch: partition limit of 1 byte (one batch)
	Last                bool // list: latest, else earliest
}

// c1produced is the answer to one entry of a produce request.
type c1produced struct {
	TopicPartition core.TopicPartition
	Err            kafkaErrorCode
	Base           core.Offset
}

// c1record is one record of a fetch answer.
type c1record struct {
	Offset core.Offset
	Value  string
}

// c1result is the answer to one operation. Only the fields of its Kind are
// used.
type c1result struct {
	Kind string

	// produce: one answer for each entry, in request order.
	Produced []c1produced

	// fetch. The spec side fills Log with the value of every record of the
	// partition (by offset). The subject side fills Records with the records
	// of the batches it got, and Batches, and Problem when the batches are not
	// well formed (a bad CRC, a gap, or a first batch that does not hold From).
	TopicPartition core.TopicPartition
	From           core.Offset
	Small          bool
	HWM            int64
	LSO            int64
	LogStart       int64
	Err            kafkaErrorCode
	Log            []string
	Batches        int
	Records        []c1record
	Problem        string

	// list: the offset that ListOffsets gave.
	ListOffset int64
}

// c1entry is one entry of a produce request: a topic partition and values.
type c1entry struct {
	index int // into c1TPs
	vals  []string
	idem  bool
}

// c1entries makes the entries of a produce operation. A topic partition that
// comes twice is used once. n is the number of the operation.
func c1entries(operation c1op, n int) []c1entry {
	var out []c1entry
	seen := map[int]bool{}
	for j, ce := range operation.Ents {
		ti := ce.TopicPartitionIndex % len(c1TPs)
		if seen[ti] {
			continue
		}
		seen[ti] = true
		var vals []string
		for k := range ce.N%3 + 1 {
			vals = append(vals, fmt.Sprintf("%d.%d.%d", n, j, k))
		}
		out = append(out, c1entry{index: ti, vals: vals, idem: ce.Idem})
	}
	return out
}

// c1Spec is the spec side of C1.
type c1Spec struct {
	model map[core.TopicPartition]*brokerspec.Broker
	calls int
}

func newC1Spec() *c1Spec {
	s := &c1Spec{model: map[core.TopicPartition]*brokerspec.Broker{}}
	for _, topicPartition := range c1TPs {
		s.model[topicPartition] = &brokerspec.Broker{}
	}
	return s
}

// StateHash folds the state hash of every partition.
func (s *c1Spec) StateHash() uint64 {
	h := trace.New()
	for _, topicPartition := range c1TPs {
		h.Uint64(s.model[topicPartition].StateHash())
	}
	return uint64(h)
}

// Apply gives the answer of a correct broker, and updates the model.
func (s *c1Spec) Apply(operation c1op) c1result {
	n := s.calls
	s.calls++
	switch operation.Kind {
	case "produce":
		return s.produce(operation, n)
	case "fetch":
		return s.fetch(operation)
	}
	return s.list(operation)
}

func (s *c1Spec) produce(operation c1op, n int) c1result {
	result := c1result{Kind: "produce"}
	for _, e := range c1entries(operation, n) {
		topicPartition := c1TPs[e.index]
		m := s.model[topicPartition]
		if e.idem {
			result.Produced = append(result.Produced, c1produced{TopicPartition: topicPartition, Err: errInvalidRequest, Base: -1})
			continue
		}
		result.Produced = append(result.Produced, c1produced{TopicPartition: topicPartition, Base: m.HighWatermark()})
		for _, v := range e.vals {
			m.Append([][]byte{[]byte(v)})
		}
	}
	return result
}

func (s *c1Spec) fetch(operation c1op) c1result {
	topicPartition := c1TPs[operation.TopicPartitionIndex%len(c1TPs)]
	m := s.model[topicPartition]
	hwm := m.HighWatermark()
	from := core.Offset(operation.From % int(hwm+2)) // hwm+1 is out of range
	result := c1result{Kind: "fetch", TopicPartition: topicPartition, From: from, Small: operation.Small, HWM: int64(hwm)}
	if from > hwm {
		result.Err = errOffsetOutOfRange
		return result
	}
	result.LSO = int64(hwm)
	records, _ := m.Read(0, 0)
	for _, r := range records {
		result.Log = append(result.Log, string(r.Value))
	}
	return result
}

func (s *c1Spec) list(operation c1op) c1result {
	topicPartition := c1TPs[operation.TopicPartitionIndex%len(c1TPs)]
	result := c1result{Kind: "list", TopicPartition: topicPartition}
	if operation.Last {
		result.ListOffset = int64(s.model[topicPartition].HighWatermark())
	}
	return result
}

// equalC1 compares the answer of the spec with the answer of the broker.
func equalC1(want, got c1result) error {
	if want.Kind != got.Kind {
		return fmt.Errorf("the answer is a %s answer, but a %s answer was expected", got.Kind, want.Kind)
	}
	switch want.Kind {
	case "produce":
		if !slices.Equal(want.Produced, got.Produced) {
			return fmt.Errorf("produce: the acks are %+v, want %+v", got.Produced, want.Produced)
		}
		return nil
	case "fetch":
		return equalFetch(want, got)
	}
	if want.ListOffset != got.ListOffset || want.Err != got.Err {
		return fmt.Errorf("list %v: error %d offset %d, want error %d offset %d",
			want.TopicPartition, got.Err, got.ListOffset, want.Err, want.ListOffset)
	}
	return nil
}

// equalFetch compares two fetch answers. The broker may return fewer records
// than the spec has (a fetch with a small partition limit returns one batch),
// but the records it returns must be the right ones.
func equalFetch(want, got c1result) error {
	where := fmt.Sprintf("fetch %v@%d", want.TopicPartition, want.From)
	if got.From != want.From {
		return fmt.Errorf("%s: the test asked for the offset %d", where, got.From)
	}
	if got.HWM != want.HWM {
		return fmt.Errorf("%s: hwm %d, want %d", where, got.HWM, want.HWM)
	}
	if got.Err != want.Err {
		return fmt.Errorf("%s: error %d, want %d (hwm %d)", where, got.Err, want.Err, want.HWM)
	}
	if want.Err != errNone {
		return nil
	}
	if got.LSO != want.LSO || got.LogStart != want.LogStart {
		return fmt.Errorf("%s: lso %d log start %d, want lso %d log start 0", where, got.LSO, got.LogStart, want.LSO)
	}
	if got.Problem != "" {
		return fmt.Errorf("%s: %s", where, got.Problem)
	}
	if int64(want.From) == want.HWM {
		if got.Batches != 0 {
			return fmt.Errorf("%s: returned %d batches at the hwm", where, got.Batches)
		}
		return nil
	}
	if got.Batches == 0 || want.Small && got.Batches != 1 || len(got.Records) == 0 {
		return fmt.Errorf("%s small=%v: %d batches with %d records", where, want.Small, got.Batches, len(got.Records))
	}
	for _, r := range got.Records {
		if int(r.Offset) >= len(want.Log) {
			return fmt.Errorf("%s: offset %d is beyond the spec hwm %d", where, r.Offset, want.HWM)
		}
		if w := want.Log[r.Offset]; r.Value != w {
			return fmt.Errorf("%s: offset %d value %q, want %q", where, r.Offset, r.Value, w)
		}
	}
	if last := got.Records[len(got.Records)-1].Offset + 1; !want.Small && int64(last) != want.HWM {
		return fmt.Errorf("%s: stopped at %d, hwm %d", where, last, want.HWM)
	}
	return nil
}

// c1 is the subject side of C1: the real broker in the harness.
type c1 struct {
	t       testT
	h       *harness
	conns   []*client
	corrupt bool
	calls   int
	// seen is the end offset of each partition, from the acks of the broker.
	// The test needs it to choose the offset of a fetch.
	seen map[core.TopicPartition]core.Offset
}

func newC1(t testT, bug InjectedBug, corrupt bool, tune func(*Config)) *c1 {
	config := DefaultConfig()
	config.InjectedBug = bug
	if tune != nil {
		tune(&config)
	}
	h := newHarness(t, config)
	h.cp.CreateTopic("u", 3)
	h.do(core.NodeStarted{Self: h.self})
	s := &c1{t: t, h: h, corrupt: corrupt, seen: map[core.TopicPartition]core.Offset{}}
	s.conns = []*client{h.open(1), h.open(2)}
	if corrupt { // every third GET comes back with a flipped byte
		gets := 0
		h.objHook = func(a core.IOCall, run func() core.IOResult) core.IOResult {
			d := run()
			if g, ok := getDone(d); ok && g.Err == core.OK && len(g.Data) > 0 {
				if gets++; gets%3 == 0 {
					g.Data = bytes.Clone(g.Data)
					g.Data[len(g.Data)-1] ^= 0x55
					return core.ObjectDone{Result: g}
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

// await2 sends req and awaits the response.
func (s *c1) await2(c *client, ver int16, request kmsg.Request) (kmsg.Response, error) {
	s.h.send(c, ver, request)
	return s.await(c)
}

// Apply sends the operation to the broker and returns the answer. The error
// means that no answer came back.
func (s *c1) Apply(operation c1op) (c1result, error) {
	n := s.calls
	s.calls++
	c := s.conns[operation.Conn%len(s.conns)]
	switch operation.Kind {
	case "produce":
		return s.produce(c, operation, n)
	case "fetch":
		return s.fetch(c, operation)
	}
	return s.list(c, operation)
}

func (s *c1) produce(c *client, operation c1op, n int) (c1result, error) {
	var ents []ent
	entries := c1entries(operation, n)
	for _, e := range entries {
		topicPartition := c1TPs[e.index]
		raw := plain(s.t, e.vals...)
		if e.idem {
			raw = mkBatch(s.t, int64(1000+e.index), 0, 0, e.vals...)
		}
		ents = append(ents, ent{topicPartition: topicPartition, rec: raw})
	}
	result := c1result{Kind: "produce"}
	if len(ents) == 0 {
		return result, nil
	}
	r, err := s.await2(c, verProduce, prodReq(-1, ents...))
	if err != nil {
		return result, err
	}
	for i, got := range produced(r) {
		answer := c1produced{Err: kafkaErrorCode(got.ErrorCode), Base: core.Offset(got.BaseOffset)}
		if i < len(entries) {
			answer.TopicPartition = c1TPs[entries[i].index]
			if got.ErrorCode == 0 {
				s.seen[answer.TopicPartition] = answer.Base + core.Offset(len(entries[i].vals))
			}
		}
		result.Produced = append(result.Produced, answer)
	}
	return result, nil
}

func (s *c1) fetch(c *client, operation c1op) (c1result, error) {
	topicPartition := c1TPs[operation.TopicPartitionIndex%len(c1TPs)]
	from := core.Offset(operation.From % int(s.seen[topicPartition]+2))
	max := int32(1 << 20)
	if operation.Small {
		max = 1
	}
	r, err := s.await2(c, verFetch, fetchReq(topicPartition, int64(from), max))
	if err != nil {
		return c1result{}, err
	}
	p := fetched(r)
	result := c1result{Kind: "fetch", TopicPartition: topicPartition, From: from, Small: operation.Small,
		HWM: p.HighWatermark, LSO: p.LastStableOffset, LogStart: p.LogStartOffset, Err: kafkaErrorCode(p.ErrorCode)}
	if result.Err == errNone {
		s.readBatches(&result, p.RecordBatches)
	}
	return result, nil
}

// readBatches reads the batches of a fetch answer into result.
func (s *c1) readBatches(result *c1result, data []byte) {
	bs, err := kafkawire.SplitBatches(data)
	if err != nil {
		result.Problem = err.Error()
		return
	}
	result.Batches = len(bs)
	next := core.Offset(-1)
	for _, b := range bs {
		bt, err := kafkawire.ParseBatch(b) // verifies the CRC
		if err != nil {
			result.Problem = err.Error()
			return
		}
		recs, err := bt.Records()
		if err != nil {
			result.Problem = err.Error()
			return
		}
		base := core.Offset(bt.BaseOffset)
		if next < 0 { // the first batch is the one containing from
			next = base
			if base > result.From || base+core.Offset(len(recs)) <= result.From {
				result.Problem = fmt.Sprintf("first batch is [%d,%d)", base, base+core.Offset(len(recs)))
				return
			}
		}
		if base != next {
			result.Problem = fmt.Sprintf("batch at %d, want %d (offsets not contiguous)", base, next)
			return
		}
		for i, rec := range recs {
			result.Records = append(result.Records, c1record{Offset: base + core.Offset(i), Value: string(rec.Value)})
		}
		next = base + core.Offset(len(recs))
	}
}

func (s *c1) list(c *client, operation c1op) (c1result, error) {
	topicPartition := c1TPs[operation.TopicPartitionIndex%len(c1TPs)]
	ts := int64(-2)
	if operation.Last {
		ts = -1
	}
	q := kmsg.NewPtrListOffsetsRequest()
	q.ReplicaID = -1
	q.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topicPartition.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{
		{Partition: topicPartition.Partition, Timestamp: ts, CurrentLeaderEpoch: -1}}}}
	r, err := s.await2(c, verList, q)
	if err != nil {
		return c1result{}, err
	}
	p := r.(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0]
	return c1result{Kind: "list", TopicPartition: topicPartition, Err: kafkaErrorCode(p.ErrorCode), ListOffset: p.Offset}, nil
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

// genC1 draws one operation. The kinds have the weights 12 : 5 : 3 (produce,
// fetch, list). It uses the fair draws of the conformance package, so the
// weights are the real chances.
func genC1() *rapid.Generator[c1op] {
	return conformance.Custom(func(r conformance.Rand) c1op {
		operation := c1op{Conn: r.Intn(2), TopicPartitionIndex: r.Intn(len(c1TPs)), From: r.Intn(1 << 10), Small: r.Intn(4) == 0, Last: r.Intn(2) == 0}
		switch k := r.Intn(20); {
		case k < 12:
			operation.Kind = "produce"
			for n := 1 + r.Intn(2); n > 0; n-- {
				e := c1ent{TopicPartitionIndex: r.Intn(len(c1TPs)), N: r.Intn(3), Idem: r.Intn(5) == 0}
				operation.Ents = append(operation.Ents, e)
			}
		case k < 17:
			operation.Kind = "fetch"
		default:
			operation.Kind = "list"
		}
		return operation
	})
}

// c1Lockstep builds the C1 check. bug is the planted bug of the broker (none
// for the real one). corrupt makes every third GET of the fake object store
// return a flipped byte. checks is the number of checks.
func c1Lockstep(bug InjectedBug, corrupt bool, tune func(*Config), checks int) spectest.Lockstep[c1op, c1result, *c1] {
	return spectest.Lockstep[c1op, c1result, *c1]{
		Name:          "c1op",
		NewSpec:       func() spectest.Spec[c1op, c1result] { return newC1Spec() },
		NewSubject:    func(t *rapid.T) *c1 { return newC1(t, bug, corrupt, tune) },
		Ops:           genC1(),
		Equal:         equalC1,
		Invariant:     (*c1).invariant,
		Format:        func(operation c1op) string { return fmt.Sprintf("%#v", operation) },
		Rerun:         spectest.Rerun{EnvPrefix: conformance.EnvPrefix},
		Checks:        checks,
		StepsPerCheck: 80,
	}
}

func TestConformanceBroker(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corruptGets=%v", corrupt), func(t *testing.T) {
			c1Lockstep(InjectedBugNone, corrupt, nil, 100).Check(t)
		})
	}
}

// Small segment caps make every seal rule fire during the generated runs. The
// log must still match the Broker spec after each seal.
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
				c1Lockstep(InjectedBugNone, corrupt, caps[name], 50).Check(t)
			})
		}
	}
}

// This test shows that the C1 comparison can fail. Each planted bug below
// changes something the harness can observe, so C1 must catch it. A run with no
// bug must not fail on the same seeds.
//
// C1 cannot catch the other injected bugs, because the client cannot see them
// in a fault-free run. The simulator tests in internal/verify/simulator/kafka catch
// them: TestMutantsAreCaught in kafka_test.go runs every InjectedBug value and names
// the invariant that must fail.
//   - InjectedBugNoBatchCap, InjectedBugNoPartitionCap, InjectedBugSealOffByOne change only where the
//     broker cuts a segment. The records and offsets stay the same. The sim
//     invariants on segment size are expected to catch them.
//   - InjectedBugTimerFromLastBatch changes only when the flush timer fires. C1 does
//     not check time, so the sim invariant on the flush window must catch it.
//   - InjectedBugFreshKeyOnRetry acts only when a commit is retried, and the C1 harness
//     has no faults, so no retry happens. The sim invariant "a stored batch is
//     committed at most once" must catch it.
//   - InjectedBugSkipProduceCRC matters only for a corrupt produce batch, and the C1
//     generator sends only valid batches. TestSkipProduceCRCMutantAcksCorruptBatch
//     and the sim invariant "committed batch is intact" must catch it.
func TestConformanceBrokerCatchesPlantedBugs(t *testing.T) {
	const budget = 20
	planted := map[spectest.Bug]struct {
		bug     InjectedBug
		corrupt bool
	}{
		"BugWrongOffset":  {InjectedBugWrongOffset, false},
		"BugServeCorrupt": {InjectedBugServeCorrupt, true},
		"BugEarlyAck":     {InjectedBugEarlyAck, false},
	}
	bugs := []spectest.Bug{"BugWrongOffset", "BugServeCorrupt", "BugEarlyAck"}
	spectest.RequireCaught(t, bugs, budget, func(name spectest.Bug, seed uint64) bool {
		m := planted[name]
		f := c1Lockstep(m.bug, m.corrupt, nil, 0).RunSeed(seed)
		if f != nil {
			t.Logf("%s, seed %d, shrunk to %d steps: %s", name, seed, len(f.Steps), f.Message)
		}
		return f != nil
	})
	for _, corrupt := range []bool{false, true} {
		spectest.RequireClean(t, budget, func(seed uint64) bool {
			return c1Lockstep(InjectedBugNone, corrupt, nil, 0).RunSeed(seed) != nil
		})
	}
}
