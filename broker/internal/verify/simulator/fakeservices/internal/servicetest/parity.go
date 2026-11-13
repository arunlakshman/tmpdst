package servicetest

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// This file holds the parity cases: scripted runs of a fake service that are
// run once against the old fake (in a throwaway generator, deleted in T1.12d)
// and once against the new fake. Both runs use this harness and this
// kernel, so the same seed and the same requests must give the same transcript,
// including the number of random draws. The old runs are saved in
// testdata/parity_*.txt. The case lists and the scripts are shared, so the two
// sides cannot drift apart. The fault knobs are plain data here (Mix, NetMix);
// each side turns them into the config type of its own fake.

const ms = machine.Millisecond

// Timing is a delay: mostly fast, with a rare slow tail.
type Timing struct {
	FastMin, FastMax, SlowMin, SlowMax machine.Duration
	SlowProb                           float64
}

// Mix are the fault knobs of the object store, the control plane and the log
// bridge.
type Mix struct {
	Apply, Respond Timing
	P503, Timeout  float64
	Lost, Corrupt  float64
	OutageProb     float64
	OutageDur      [2]machine.Duration
	CallerTimeout  machine.Duration
	MaxCommitItems int
}

// NetMix are the fault knobs of the network.
type NetMix struct {
	Latency                        Timing
	PartialIOMax                   int
	PartitionProb                  float64
	PartitionDur                   [2]machine.Duration
	ClogProb                       float64
	ClogDur                        [2]machine.Duration
	RandomCloseProb, BlackHoleProb float64
	PairLatencyMax                 machine.Duration
}

// Subject is what a script may ask of the service it drives, besides the requests.
type Subject struct {
	StateHash   func() uint64                               // the state hash of the service
	SpecHash    func() uint64                               // the cached spec hash; nil if there is none
	CreateTopic func(name string, parts int32) core.ErrCode // nil if the service has no topics
	AddNode     func(a Addr)                                // tells the network about a node; nil if not a network
}

// Case is one scripted run.
type Case struct {
	Name  string
	Seed  uint64
	Nodes []string // the caller nodes of the world
	Mix   Mix
	Net   NetMix
	Run   func(h *Harness, sv Subject)
}

// The fault mixes of the cases. They are functions, not variables: the
// determinism gate allows no package-level state.

func fastSlow() Timing {
	return Timing{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms, SlowProb: 0.1}
}

func noFaults() Mix {
	return Mix{Apply: fastSlow(), Respond: fastSlow(), CallerTimeout: 1000 * ms}
}

func faulty() Mix {
	m := noFaults()
	m.P503, m.Timeout, m.Lost, m.Corrupt = 0.2, 0.2, 0.2, 0.3
	m.OutageProb, m.OutageDur = 0.05, [2]machine.Duration{10 * ms, 20 * ms}
	return m
}

func outage() Mix {
	m := noFaults()
	m.OutageProb, m.OutageDur = 1, [2]machine.Duration{15 * ms, 45 * ms}
	return m
}

func slow() Mix {
	return Mix{
		Apply:         Timing{FastMin: ms, FastMax: 2 * ms, SlowMin: 20 * ms, SlowMax: 40 * ms, SlowProb: 0.5},
		Respond:       Timing{FastMin: ms, FastMax: 3 * ms, SlowMin: 20 * ms, SlowMax: 40 * ms, SlowProb: 0.5},
		CallerTimeout: 30 * ms, Timeout: 0.1, Lost: 0.1,
	}
}

// withSeeds repeats a case for seeds 1, 2 and 3.
func withSeeds(c Case) []Case {
	var out []Case
	for seed := uint64(1); seed <= 3; seed++ {
		d := c
		d.Seed = seed
		d.Name = c.Name + "/seed" + strconv.FormatUint(seed, 10)
		out = append(out, d)
	}
	return out
}

// ---- object store ----

func objectCall(r *rng.Rand, i int) core.ObjectCall {
	k := string(rune('a' + r.Intn(5)))
	req := machine.RequestID(i)
	switch r.Intn(6) {
	case 0, 1:
		return core.ObjectCall{Call: objectstore.Put{Req: req, Key: k, Data: []byte("data-" + k + strconv.Itoa(i))}}
	case 2, 3:
		return core.ObjectCall{Call: objectstore.Get{Req: req, Ref: core.SegmentRef{Key: k}}}
	case 4:
		return core.ObjectCall{Call: objectstore.Delete{Req: req, Key: k}}
	}
	return core.ObjectCall{Call: objectstore.List{Req: req}}
}

// objectRun issues 60 calls from two callers, crashes one of them while calls are
// in flight, enters recovery, and issues 20 more.
func objectRun(h *Harness, sv Subject) {
	r := rng.New(h.Seed + 1000)
	for i := range 60 {
		h.DoAfter(machine.Duration(i)*ms, []string{"a", "b"}[i%2], objectCall(r, i))
	}
	h.RunTo(30 * kernel.Time(ms))
	h.Sample("mid", sv.StateHash())
	h.Crash("b")
	h.RunTo(80 * kernel.Time(ms))
	h.Boot("b")
	h.Recover()
	h.Sample("recovering", sv.StateHash())
	for i := 60; i < 80; i++ {
		h.DoAfter(machine.Duration(i-60)*ms, []string{"a", "b"}[i%2], objectCall(r, i))
	}
	h.Run()
	h.Sample("end", sv.StateHash())
	h.Sample("spec", sv.SpecHash())
}

// ObjectCases are the parity cases of the object store.
func ObjectCases() []Case {
	var out []Case
	for _, c := range []struct {
		name string
		mix  Mix
	}{{"clean", noFaults()}, {"faulty", faulty()}, {"outage", outage()}, {"slow", slow()}} {
		out = append(out, withSeeds(Case{Name: "object/" + c.name, Nodes: []string{"a", "b"}, Mix: c.mix, Run: objectRun})...)
	}
	return out
}

// ---- control plane ----

func topicPartitions() []core.TopicPartition {
	return []core.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("t", 1), contract.NewTopicPartition("u", 0)}
}

func controlPlaneCall(r *rng.Rand, i int) core.ControlPlaneCall {
	tps := topicPartitions()
	req := machine.RequestID(i)
	switch r.Intn(5) {
	case 0, 1:
		// Keys repeat: replays, and conflicts when the request differs.
		batches := []contract.PendingBatch{{TopicPartition: tps[r.Intn(3)], Records: int64(1 + r.Intn(3))}}
		if r.Chance(0.3) {
			batches = append(batches, contract.PendingBatch{TopicPartition: tps[r.Intn(3)], Records: 2})
		}
		return core.ControlPlaneCall{Req: req, Op: contract.CommitSegment{SegmentKey: string(rune('a' + r.Intn(8))), Batches: batches}}
	case 2, 3:
		return core.ControlPlaneCall{Req: req, Op: contract.FindSegments{TopicPartition: tps[r.Intn(3)]}}
	}
	return core.ControlPlaneCall{Req: req, Op: contract.DescribeTopics{}}
}

// controlPlaneRun is objectRun for the control plane. The topics "t" (2
// partitions) and "u" (1 partition) are created before the run.
func controlPlaneRun(h *Harness, sv Subject) {
	sv.CreateTopic("t", 2)
	sv.CreateTopic("u", 1)
	h.Sample("topics", sv.StateHash())
	r := rng.New(h.Seed + 2000)
	for i := range 60 {
		h.DoAfter(machine.Duration(i)*ms, []string{"a", "b"}[i%2], controlPlaneCall(r, i))
	}
	h.RunTo(30 * kernel.Time(ms))
	h.Sample("mid", sv.StateHash())
	h.Crash("b")
	h.RunTo(80 * kernel.Time(ms))
	h.Boot("b")
	h.Recover()
	h.Sample("recovering", sv.StateHash())
	for i := 60; i < 80; i++ {
		h.DoAfter(machine.Duration(i-60)*ms, []string{"a", "b"}[i%2], controlPlaneCall(r, i))
	}
	h.Run()
	h.Sample("end", sv.StateHash())
	h.Sample("spec", sv.SpecHash())
}

// ControlPlaneCases are the parity cases of the control plane.
func ControlPlaneCases() []Case {
	limited := noFaults()
	limited.MaxCommitItems = 3
	limitedFaulty := faulty()
	limitedFaulty.MaxCommitItems = 3
	var out []Case
	for _, c := range []struct {
		name string
		mix  Mix
	}{{"clean", noFaults()}, {"faulty", faulty()}, {"outage", outage()}, {"slow", slow()}, {"limited", limited}, {"limited-faulty", limitedFaulty}} {
		out = append(out, withSeeds(Case{Name: "controlplane/" + c.name, Nodes: []string{"a", "b"}, Mix: c.mix, Run: controlPlaneRun})...)
	}
	return out
}

// ---- network ----

func netNodes() []string { return []string{"a", "b", "c"} }

// netView finds connection ids in the deliveries.
type netView struct {
	h    *Harness
	used map[string]bool // listener conns already given to a pair
}

func (v *netView) dialed(req machine.RequestID) (core.ConnID, bool) {
	for _, d := range v.h.Delivered {
		if x, ok := d.Body.(core.ConnectDone); ok && x.Req == req && x.Err == core.OK {
			return x.Conn, true
		}
	}
	return 0, false
}

func (v *netView) opened(to, peer string) (core.ConnID, bool) {
	for _, d := range v.h.Delivered {
		x, ok := d.Body.(core.ConnOpened)
		k := string(d.To) + "/" + strconv.FormatUint(uint64(x.Conn), 10)
		if ok && string(d.To) == to && x.Peer == peer && !v.used[k] {
			v.used[k] = true
			return x.Conn, true
		}
	}
	return 0, false
}

// netPair is one connection seen from both ends.
type netPair struct {
	a, b   string
	ca, cb core.ConnID
}

// netScript is the state of one network run.
type netScript struct {
	h     *Harness
	sv    Subject
	r     *rng.Rand
	v     *netView
	req   machine.RequestID
	pairs []netPair
}

// dial opens a connection and reports it if both ends heard of it in time.
func (n *netScript) dial(from, to string) (netPair, bool) {
	n.req++
	n.h.Issue(from, core.Connect{Req: n.req, Addr: to})
	n.h.RunFor(2 * ms)
	ca, ok1 := n.v.dialed(n.req)
	cb, ok2 := n.v.opened(to, from)
	return netPair{from, to, ca, cb}, ok1 && ok2
}

func (n *netScript) redial(from, to string) {
	if np, ok := n.dial(from, to); ok {
		n.pairs = append(n.pairs, np)
	}
}

// traffic sends random bytes on a random connection, from a random end.
func (n *netScript) traffic(i int) {
	if len(n.pairs) == 0 {
		return
	}
	p := n.pairs[n.r.Intn(len(n.pairs))]
	from, conn := p.a, p.ca
	if n.r.Chance(0.4) {
		from, conn = p.b, p.cb
	}
	data := make([]byte, n.r.Intn(30))
	for j := range data {
		data[j] = byte(n.r.Uint64())
	}
	n.req++
	n.h.Issue(from, core.Send{Req: n.req, Conn: conn, Data: data})
	switch i % 10 {
	case 3:
		n.h.Issue(p.b, core.PauseReads{Conn: p.cb})
	case 6:
		n.h.Issue(p.b, core.ResumeReads{Conn: p.cb})
	}
}

// event does what the script plans for iteration i: a sample, a crash of a node,
// recovery, a graceful close and an abort.
func (n *netScript) event(i int) {
	switch i {
	case 12:
		n.h.Sample("net-mid", n.sv.StateHash())
	case 20:
		n.h.Crash("b")
		n.h.RunFor(ms)
		n.h.Boot("b")
		n.redial("a", "b")
	case 25:
		n.h.Recover()
	case 30:
		if len(n.pairs) > 0 {
			p := n.pairs[len(n.pairs)-1]
			n.h.Issue(p.a, core.CloseConn{Conn: p.ca})
		}
	case 34:
		if len(n.pairs) > 1 {
			p := n.pairs[0]
			n.h.Issue(p.b, core.CloseConn{Conn: p.cb, Abort: true})
		}
	}
}

func netRun(h *Harness, sv Subject) {
	for _, name := range netNodes() {
		sv.AddNode(h.Addr(name))
	}
	n := &netScript{h: h, sv: sv, r: rng.New(h.Seed + 3000), v: &netView{h: h, used: map[string]bool{}}}
	for _, p := range [][2]string{{"a", "b"}, {"b", "c"}, {"a", "c"}} {
		n.redial(p[0], p[1])
	}
	for i := range 40 {
		n.traffic(i)
		h.RunFor(machine.Duration(n.r.Intn(300)) * machine.Microsecond)
		n.event(i)
	}
	h.Run()
	h.Sample("net-end", sv.StateHash())
}

// NetworkCases are the parity cases of the network.
func NetworkCases() []Case {
	lat := Timing{FastMin: 1000, FastMax: 5000, SlowMin: 100000, SlowMax: 200000, SlowProb: 0.1}
	mixes := []struct {
		name string
		net  NetMix
	}{
		{"clean", NetMix{Latency: lat}},
		{"partial", NetMix{Latency: lat, PartialIOMax: 5, PairLatencyMax: 20000}},
		{"partition", NetMix{Latency: lat, PartialIOMax: 7, PartitionProb: 0.3, PartitionDur: [2]machine.Duration{10000, 400000}}},
		{"chaos", NetMix{Latency: lat, PartialIOMax: 5, PairLatencyMax: 20000, PartitionProb: 0.05,
			PartitionDur: [2]machine.Duration{10000, 400000}, ClogProb: 0.1, ClogDur: [2]machine.Duration{1000, 100000},
			RandomCloseProb: 0.02, BlackHoleProb: 0.02}},
		// A partition that lasts exactly one nanosecond more than its lower end.
		{"narrow-partition", NetMix{Latency: lat, PartitionProb: 1, PartitionDur: [2]machine.Duration{1_000_000, 1_000_001}}},
	}
	var out []Case
	for _, m := range mixes {
		out = append(out, withSeeds(Case{Name: "network/" + m.name, Nodes: netNodes(), Net: m.net, Run: netRun})...)
	}
	return out
}

// ---- transcript ----

// Sample records a value for the transcript, for example a state hash.
func (h *Harness) Sample(label string, v uint64) {
	h.samples = append(h.samples, fmt.Sprintf("sample %s %016x", label, v))
}

// Transcript finishes the world and renders a run as text: the steps, the number of
// random draws, the counters, the coverage of the assertions, the samples, the
// faults and a digest of the deliveries (with the first lines in full). Two
// runs are equal if and only if their transcripts are equal.
func (h *Harness) Transcript(name string) string {
	res := h.W.Finish()
	var b strings.Builder
	fmt.Fprintf(&b, "case %s seed %d\nsteps %d\nrand_calls %d\n", name, res.Seed, res.Steps, res.RandCalls)
	for _, c := range res.Stats {
		fmt.Fprintf(&b, "counter %s %d\n", c.Name, c.N)
	}
	names := make([]string, 0, len(res.Coverage))
	for n := range res.Coverage { //determinismcheck:ok sorted below
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		c := res.Coverage[n]
		fmt.Fprintf(&b, "cover %s checks %d passes %d\n", n, c.Checks, c.Passes)
	}
	for _, f := range res.Failures {
		fmt.Fprintf(&b, "failure %s\n", f.Property)
	}
	for _, s := range h.samples {
		b.WriteString(s + "\n")
	}
	faults := h.Faults()
	fmt.Fprintf(&b, "faults %d %016x\n", len(faults), digest(strings.Join(faults, ",")))
	text := h.Trace()
	fmt.Fprintf(&b, "deliveries %d %016x\n", len(h.Delivered), digest(text))
	lines := strings.SplitAfter(text, "\n")
	for i := 0; i < len(lines) && i < 6; i++ {
		b.WriteString("  " + lines[i])
	}
	b.WriteString("\n")
	return b.String()
}

// digest folds a text into a 64-bit hash.
func digest(text string) uint64 {
	h := trace.New()
	h.String(text)
	return h.Sum()
}
