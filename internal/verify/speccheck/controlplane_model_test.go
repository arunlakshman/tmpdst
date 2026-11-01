package speccheck

import (
	"fmt"
	"sort"

	"testing"

	"mds/internal/core"
	"mds/internal/verify/speccheck/checker"
)

// This file checks the ControlPlane spec against a second, naive model. The
// model was written only from the rules in the ControlPlane doc comment. It
// uses plain maps and loops and has no hash cache, so a mistake in the spec
// is unlikely to repeat in the model.

// naiveControlPlane keeps every committed segment and every run in simple lists.
type naiveControlPlane struct {
	segKeys []string                                  // keys in commit order
	segs    map[string]modelSegment                   // key -> what was committed
	runs    map[core.TopicPartition][]core.BatchRange // partition -> runs in order
	topics  map[string]int32                          // topic -> partition count
}

type modelSegment struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

func newNaiveControlPlane() *naiveControlPlane {
	return &naiveControlPlane{
		segs:   map[string]modelSegment{},
		runs:   map[core.TopicPartition][]core.BatchRange{},
		topics: map[string]int32{},
	}
}

// end adds up the counts of all runs. The rules say offsets are contiguous
// from 0, so the sum is the next offset.
func (m *naiveControlPlane) end(p core.TopicPartition) core.Offset {
	var total int64
	for _, r := range m.runs[p] {
		total += r.Count
	}
	return core.Offset(total)
}

func sameBatches(a, b []core.PendingBatch) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (m *naiveControlPlane) createTopic(name string, parts int32) core.ErrCode {
	if name == "" || parts <= 0 {
		return core.ErrInvalid
	}
	if parts > m.topics[name] {
		m.topics[name] = parts
	}
	return core.OK
}

func (m *naiveControlPlane) commit(request core.RequestID, operation core.CommitSegment) core.ControlPlaneDone {
	if old, found := m.segs[operation.SegmentKey]; found {
		if !sameBatches(old.batches, operation.Batches) {
			return core.ControlPlaneDone{Req: request, Err: core.ErrConflict}
		}
		response := core.CommitSegmentResp{Assigned: append([]core.BatchRange(nil), old.ranges...)}
		return core.ControlPlaneDone{Req: request, Resp: response, Outcome: core.Applied}
	}
	if operation.SegmentKey == "" || len(operation.Batches) == 0 {
		return core.ControlPlaneDone{Req: request, Err: core.ErrInvalid}
	}
	for _, b := range operation.Batches {
		if b.Records <= 0 || b.TopicPartition.Topic == "" || b.TopicPartition.Partition < 0 {
			return core.ControlPlaneDone{Req: request, Err: core.ErrInvalid}
		}
	}
	var assigned []core.BatchRange
	for _, b := range operation.Batches {
		r := core.BatchRange{TopicPartition: b.TopicPartition, Base: m.end(b.TopicPartition), Count: b.Records, Loc: b.Loc}
		m.runs[b.TopicPartition] = append(m.runs[b.TopicPartition], r)
		if b.TopicPartition.Partition+1 > m.topics[b.TopicPartition.Topic] {
			m.topics[b.TopicPartition.Topic] = b.TopicPartition.Partition + 1
		}
		assigned = append(assigned, r)
	}
	m.segKeys = append(m.segKeys, operation.SegmentKey)
	m.segs[operation.SegmentKey] = modelSegment{batches: append([]core.PendingBatch(nil), operation.Batches...), ranges: assigned}
	response := core.CommitSegmentResp{Assigned: append([]core.BatchRange(nil), assigned...)}
	return core.ControlPlaneDone{Req: request, Resp: response, Outcome: core.Applied}
}

func (m *naiveControlPlane) lookup(request core.RequestID, operation core.FindSegments) core.ControlPlaneDone {
	if operation.From < 0 {
		return core.ControlPlaneDone{Req: request, Err: core.ErrInvalid}
	}
	response := core.FindSegmentsResp{End: m.end(operation.TopicPartition)}
	var sum int64
	for _, r := range m.runs[operation.TopicPartition] {
		if r.Base+core.Offset(r.Count)-1 < operation.From {
			continue
		}
		response.Ranges = append(response.Ranges, r)
		sum += r.Loc.Len
		if operation.MaxBytes > 0 && sum >= int64(operation.MaxBytes) {
			break
		}
	}
	return core.ControlPlaneDone{Req: request, Resp: response}
}

func (m *naiveControlPlane) describe(request core.RequestID, operation core.DescribeTopics) core.ControlPlaneDone {
	var names []string
	for name := range m.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	var response core.DescribeTopicsResp
	for _, name := range names {
		wanted := len(operation.Topics) == 0
		for _, w := range operation.Topics {
			if w == name {
				wanted = true
			}
		}
		if wanted {
			response.Topics = append(response.Topics, core.TopicInfo{Name: name, Partitions: m.topics[name]})
		}
	}
	return core.ControlPlaneDone{Req: request, Resp: response}
}

// cpModelOp is one operation. Kind is one of create, commit, lookup, describe.
type cpModelOp struct {
	Kind           string
	Name           string // topic for create, segment key for commit
	Parts          int32
	Batches        []core.PendingBatch
	TopicPartition core.TopicPartition
	From           int64
	MaxBytes       int32
	Topics         []string
}

type cpModelPair struct {
	spec  *ControlPlane
	model *naiveControlPlane
}

var (
	modelTopics = []string{"a", "b", "c"}
	modelKeys   = []string{"k0", "k1", "k2", "k3", "k4", "k5"}
)

func modelTP(r *checker.Rand) core.TopicPartition {
	return core.NewTopicPartition(modelTopics[r.Intn(len(modelTopics))], int32(r.Intn(3)))
}

// canonicalBatches gives the same batches every time for one key. A commit
// that repeats a key with these batches is a valid replay.
func canonicalBatches(key string) []core.PendingBatch {
	k := int(key[1] - '0')
	var bs []core.PendingBatch
	for j := 0; j <= k%3; j++ {
		p := core.NewTopicPartition(modelTopics[(k+j)%len(modelTopics)], int32((k+2*j)%3))
		n := int64(1 + (k+j)%4)
		bs = append(bs, core.PendingBatch{TopicPartition: p, Records: n, Loc: core.SegmentRef{Key: "o" + key, Off: int64(j), Len: n * 10}})
	}
	return bs
}

func genCPModelOp(r *checker.Rand, _ int) cpModelOp {
	switch r.Intn(10) {
	case 0:
		name := modelTopics[r.Intn(len(modelTopics))]
		parts := int32(r.Intn(5)) - 1 // -1 and 0 are invalid
		if r.Intn(8) == 0 {
			name = ""
		}
		return cpModelOp{Kind: "create", Name: name, Parts: parts}
	case 1, 2, 3, 4, 5:
		key := modelKeys[r.Intn(len(modelKeys))]
		bs := canonicalBatches(key)
		switch r.Intn(8) {
		case 0: // same key, different request: conflict if the key exists
			bs[0].Records++
		case 1: // a random request
			bs = nil
			for n := 0; n <= r.Intn(3); n++ {
				recs := int64(r.Intn(5)) // 0 is invalid
				bs = append(bs, core.PendingBatch{TopicPartition: modelTP(r), Records: recs, Loc: core.SegmentRef{Key: "x", Len: recs * 7}})
			}
		case 2: // invalid pieces
			switch r.Intn(4) {
			case 0:
				key = ""
			case 1:
				bs = nil
			case 2:
				bs[0].TopicPartition.Partition = -1
			default:
				bs[0].TopicPartition.Topic = ""
			}
		}
		return cpModelOp{Kind: "commit", Name: key, Batches: bs}
	case 6, 7, 8:
		maxBytes := []int32{0, 1, 25, 60, 1000}[r.Intn(5)]
		return cpModelOp{Kind: "lookup", TopicPartition: modelTP(r), From: int64(r.Intn(12)) - 1, MaxBytes: maxBytes}
	}
	var names []string
	for n := r.Intn(3); n > 0; n-- {
		names = append(names, append(modelTopics, "zzz")[r.Intn(len(modelTopics)+1)])
	}
	return cpModelOp{Kind: "describe", Topics: names}
}

func stepCPModel(s cpModelPair, operation cpModelOp) error {
	const request = core.RequestID(5)
	var got, want core.ControlPlaneDone
	switch operation.Kind {
	case "create":
		g, w := s.spec.CreateTopic(operation.Name, operation.Parts), s.model.createTopic(operation.Name, operation.Parts)
		if g != w {
			return fmt.Errorf("CreateTopic(%q, %d): the spec returned %v, the model returned %v", operation.Name, operation.Parts, g, w)
		}
		return nil
	case "commit":
		cs := core.CommitSegment{SegmentKey: operation.Name, Batches: operation.Batches}
		got = s.spec.Exec(core.ControlPlaneCall{Req: request, Op: cs}).(core.ControlPlaneDone)
		want = s.model.commit(request, cs)
	case "lookup":
		l := core.FindSegments{TopicPartition: operation.TopicPartition, From: core.Offset(operation.From), MaxBytes: operation.MaxBytes}
		got = s.spec.Exec(core.ControlPlaneCall{Req: request, Op: l}).(core.ControlPlaneDone)
		want = s.model.lookup(request, l)
	default:
		d := core.DescribeTopics{Topics: operation.Topics}
		got = s.spec.Exec(core.ControlPlaneCall{Req: request, Op: d}).(core.ControlPlaneDone)
		want = s.model.describe(request, d)
	}
	if g, w := fmt.Sprintf("%+v", got), fmt.Sprintf("%+v", want); g != w {
		return fmt.Errorf("%s: the spec returned %s, the model returned %s", operation.Kind, g, w)
	}
	return nil
}

// checkCPModelState compares the end offset of every partition and the two
// ways to compute the state hash.
func checkCPModelState(s cpModelPair) error {
	for _, t := range modelTopics {
		for p := range int32(3) {
			topicPartition := core.NewTopicPartition(t, p)
			if g, w := s.spec.End(topicPartition), s.model.end(topicPartition); g != w {
				return fmt.Errorf("End(%v): the spec returned %d, the model returned %d", topicPartition, g, w)
			}
		}
	}
	if g, w := s.spec.StateHash(), s.spec.fullStateHash(); g != w {
		return fmt.Errorf("StateHash with the cache is %d, without the cache it is %d", g, w)
	}
	return nil
}

func TestControlPlaneMatchesNaiveModel(t *testing.T) {
	c := checker.SpecChecker[cpModelPair, cpModelOp]{
		Name:      "cpModelOp",
		New:       func() cpModelPair { return cpModelPair{NewControlPlane(), newNaiveControlPlane()} },
		Gen:       genCPModelOp,
		Step:      stepCPModel,
		Invariant: checkCPModelState,
		Format:    func(o cpModelOp) string { return fmt.Sprintf("%#v", o) },
		Ops:       80,
	}
	checker.Run(t, c, checker.Options{
		Seeds: 300,
		Seed:  -1,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./internal/verify/speccheck -run %s (seed %d)", t.Name(), seed)
		},
	})
}
