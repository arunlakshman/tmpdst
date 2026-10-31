package generator

import (
	"fmt"
	"go/parser"
	"mds/internal/core"
	"mds/internal/verify/speccheck"
	"mds/internal/verify/speccheck/checker"
	"reflect"
	"slices"
	"strings"
	"testing"
)

const (
	coverageSeeds = 500
	coverageOps   = 100
)

// minimumHits is the least number of times every profile must reach a case
// over all seeds and operations. A case that a profile reaches fewer times
// is a blind spot, so the test fails.
const minimumHits = 25

// onlyProfile lists the cases that one profile is built to reach. The other
// profiles may reach them, but the test does not require it.
var onlyProfile = map[string]string{"commit many batches (40 or more)": "many-batches"}

var objectCases = []string{
	"put accepted", "put empty key", "put empty data",
	"get accepted", "get to end of object (Len <= 0)", "get missing key", "get out of range", "get empty key",
	"delete accepted", "delete missing key",
	"list empty prefix", "list prefix with slash", "list prefix without slash", "list with keys", "list without keys",
}

var controlPlaneCases = []string{
	"create topic accepted", "create topic invalid",
	"commit fresh accepted", "commit replay accepted", "commit conflict", "commit new key same batches",
	"commit many batches (40 or more)",
	"commit invalid: empty key", "commit invalid: no batches", "commit invalid: records <= 0",
	"commit invalid: negative partition", "commit invalid: empty topic",
	"lookup accepted", "lookup with ranges", "lookup From = -1", "lookup From inside a run", "lookup MaxBytes cut",
	"describe all topics", "describe known topic", "describe unknown topic",
}

func TestObjectCoverage(t *testing.T) {
	table := map[string]map[string]int{}
	for _, p := range InputProfiles() {
		table[p.Name] = objectCoverage(p)
	}
	reportCoverage(t, "object store", objectCases, table)
}

func TestControlPlaneCoverage(t *testing.T) {
	table := map[string]map[string]int{}
	for _, p := range InputProfiles() {
		table[p.Name] = controlPlaneCoverage(p)
	}
	reportCoverage(t, "control plane", controlPlaneCases, table)
}

// reportCoverage prints one table and fails for every case that a profile
// reaches too few times.
func reportCoverage(t *testing.T, title string, cases []string, table map[string]map[string]int) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "coverage of the %s generator, %d seeds of %d operations\n%-38s", title, coverageSeeds, coverageOps, "case")
	for _, p := range InputProfiles() {
		fmt.Fprintf(&b, "%14s", p.Name)
	}
	b.WriteString("\n")
	for _, c := range cases {
		fmt.Fprintf(&b, "%-38s", c)
		for _, p := range InputProfiles() {
			fmt.Fprintf(&b, "%14d", table[p.Name][c])
		}
		b.WriteString("\n")
	}
	t.Log("\n" + b.String())
	for _, p := range InputProfiles() {
		for _, c := range cases {
			if only, ok := onlyProfile[c]; ok && only != p.Name {
				continue
			}
			if n := table[p.Name][c]; n < minimumHits {
				t.Errorf("profile %s reached the case %q only %d times, the minimum is %d.", p.Name, c, n, minimumHits)
			}
		}
	}
}

func objectCoverage(p InputProfile) map[string]int {
	hits := map[string]int{}
	gen := ObjectOps(p)
	for seed := uint64(1); seed <= coverageSeeds; seed++ {
		r, spec := checker.NewRand(seed), speccheck.NewObjectStore()
		for i := range coverageOps {
			operation := gen(r, i)
			for _, c := range classifyObject(spec, operation) {
				hits[c]++
			}
		}
	}
	return hits
}

// classifyObject runs op on the spec and names the cases that it reached.
func classifyObject(spec *speccheck.ObjectStore, operation core.IOCall) []string {
	done := spec.Exec(operation)
	switch operation := operation.(type) {
	case core.ObjectPut:
		if done.(core.ObjectPutDone).Err == core.ErrInvalid {
			return []string{"put empty key"}
		}
		if len(operation.Data) == 0 {
			return []string{"put accepted", "put empty data"}
		}
		return []string{"put accepted"}
	case core.ObjectGet:
		return classifyGet(operation, done.(core.ObjectGetDone))
	case core.ObjectDelete:
		if done.(core.ObjectDeleteDone).Err == core.ErrNotFound {
			return []string{"delete missing key"}
		}
		return []string{"delete accepted"}
	case core.ObjectList:
		return classifyList(operation, done.(core.ObjectListDone))
	}
	return nil
}

func classifyGet(operation core.ObjectGet, done core.ObjectGetDone) []string {
	var out []string
	if operation.Ref.Key == "" {
		out = append(out, "get empty key")
	}
	switch done.Err {
	case core.ErrNotFound:
		return append(out, "get missing key")
	case core.ErrInvalid:
		return append(out, "get out of range")
	}
	out = append(out, "get accepted")
	if operation.Ref.Len <= 0 {
		out = append(out, "get to end of object (Len <= 0)")
	}
	return out
}

func classifyList(operation core.ObjectList, done core.ObjectListDone) []string {
	var out []string
	switch {
	case operation.Prefix == "":
		out = append(out, "list empty prefix")
	case strings.HasSuffix(operation.Prefix, "/"):
		out = append(out, "list prefix with slash")
	default:
		out = append(out, "list prefix without slash")
	}
	if len(done.Keys) > 0 {
		return append(out, "list with keys")
	}
	return append(out, "list without keys")
}

func controlPlaneCoverage(p InputProfile) map[string]int {
	hits := map[string]int{}
	gen := ControlPlaneOps(p)
	for seed := uint64(1); seed <= coverageSeeds; seed++ {
		r, spec, committed := checker.NewRand(seed), speccheck.NewControlPlane(), map[string]bool{}
		for i := range coverageOps {
			operation := gen(r, i)
			for _, c := range classifyControlPlane(spec, committed, operation) {
				hits[c]++
			}
		}
	}
	return hits
}

// classifyControlPlane runs op on the spec and names the cases that it
// reached. The committed map holds the segment keys the spec has accepted.
func classifyControlPlane(spec *speccheck.ControlPlane, committed map[string]bool, operation ControlPlaneTestStep) []string {
	if operation.CreateTopic != nil {
		if spec.CreateTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions) == core.OK {
			return []string{"create topic accepted"}
		}
		return []string{"create topic invalid"}
	}
	done := spec.Exec(*operation.Call).(core.ControlPlaneDone)
	switch o := operation.Call.Op.(type) {
	case core.CommitSegment:
		return classifyCommit(committed, o, done)
	case core.FindSegments:
		return classifyLookup(spec, o, done)
	case core.DescribeTopics:
		return classifyDescribe(o, done.Resp.(core.DescribeTopicsResp))
	}
	return nil
}

func classifyCommit(committed map[string]bool, o core.CommitSegment, done core.ControlPlaneDone) []string {
	switch done.Err {
	case core.ErrConflict:
		return []string{"commit conflict"}
	case core.ErrInvalid:
		return []string{invalidCommitCase(o)}
	}
	out := []string{"commit fresh accepted"}
	if committed[o.SegmentKey] {
		out = []string{"commit replay accepted"}
	}
	committed[o.SegmentKey] = true
	if strings.HasPrefix(o.SegmentKey, "retry") {
		out = append(out, "commit new key same batches")
	}
	if len(o.Batches) >= 40 {
		out = append(out, "commit many batches (40 or more)")
	}
	return out
}

func invalidCommitCase(o core.CommitSegment) string {
	if o.SegmentKey == "" {
		return "commit invalid: empty key"
	}
	if len(o.Batches) == 0 {
		return "commit invalid: no batches"
	}
	for _, b := range o.Batches {
		switch {
		case b.Records <= 0:
			return "commit invalid: records <= 0"
		case b.TopicPartition.Partition < 0:
			return "commit invalid: negative partition"
		case b.TopicPartition.Topic == "":
			return "commit invalid: empty topic"
		}
	}
	return "commit invalid: other"
}

func classifyLookup(spec *speccheck.ControlPlane, o core.FindSegments, done core.ControlPlaneDone) []string {
	if done.Err != core.OK {
		return []string{"lookup From = -1"}
	}
	out := []string{"lookup accepted"}
	response := done.Resp.(core.FindSegmentsResp)
	if len(response.Ranges) == 0 {
		return out
	}
	out = append(out, "lookup with ranges")
	if response.Ranges[0].Base < o.From {
		out = append(out, "lookup From inside a run")
	}
	uncut := spec.Exec(core.ControlPlaneCall{Op: core.FindSegments{TopicPartition: o.TopicPartition, From: o.From}}).(core.ControlPlaneDone).Resp.(core.FindSegmentsResp)
	if len(response.Ranges) < len(uncut.Ranges) {
		out = append(out, "lookup MaxBytes cut")
	}
	return out
}

func classifyDescribe(o core.DescribeTopics, response core.DescribeTopicsResp) []string {
	if len(o.Topics) == 0 {
		return []string{"describe all topics"}
	}
	var out []string
	for _, name := range o.Topics {
		known := slices.ContainsFunc(response.Topics, func(t core.TopicInfo) bool { return t.Name == name })
		if known {
			out = append(out, "describe known topic")
		} else {
			out = append(out, "describe unknown topic")
		}
	}
	return out
}

func TestSameSeedGivesSameSequence(t *testing.T) {
	for _, p := range InputProfiles() {
		objA, objB := objectSequence(p, 7), objectSequence(p, 7)
		if !reflect.DeepEqual(objA, objB) {
			t.Errorf("profile %s: two runs of the object generator with seed 7 gave different operations.", p.Name)
		}
		cpA, cpB := controlPlaneSequence(p, 7), controlPlaneSequence(p, 7)
		if !reflect.DeepEqual(cpA, cpB) {
			t.Errorf("profile %s: two runs of the control-plane generator with seed 7 gave different operations.", p.Name)
		}
		if reflect.DeepEqual(cpA, controlPlaneSequence(p, 8)) {
			t.Errorf("profile %s: seeds 7 and 8 gave the same control-plane operations.", p.Name)
		}
	}
}

func objectSequence(p InputProfile, seed uint64) []core.IOCall {
	r, gen := checker.NewRand(seed), ObjectOps(p)
	var out []core.IOCall
	for i := range coverageOps {
		out = append(out, gen(r, i))
	}
	return out
}

func controlPlaneSequence(p InputProfile, seed uint64) []string {
	r, gen := checker.NewRand(seed), ControlPlaneOps(p)
	var out []string
	for i := range coverageOps {
		out = append(out, FormatControlPlaneOp(gen(r, i)))
	}
	return out
}

func TestRequestIDsFollowPosition(t *testing.T) {
	r, objects, plane := checker.NewRand(3), ObjectOps(Default()), ControlPlaneOps(Default())
	for i := range 50 {
		if got := objects(r, i).RequestID(); got != core.RequestID(i+1) {
			t.Fatalf("object operation %d has request id %d, want %d.", i, got, i+1)
		}
		if operation := plane(r, i); operation.Call != nil && operation.Call.Req != core.RequestID(i+1) {
			t.Fatalf("control-plane operation %d has request id %d, want %d.", i, operation.Call.Req, i+1)
		}
	}
}

func TestBatchesReplayAndConflict(t *testing.T) {
	p := Default()
	for _, key := range []string{"seg0", "seg1", "retry77", "x"} {
		first, again := Batches(p, key, key, 0), Batches(p, key, key, 0)
		if !reflect.DeepEqual(first, again) {
			t.Errorf("key %q: two calls with variant 0 gave different batches.", key)
		}
		for variant := 1; variant <= 4; variant++ {
			if reflect.DeepEqual(first, Batches(p, key, key, variant)) {
				t.Errorf("key %q: variant %d gave the same batches as variant 0.", key, variant)
			}
		}
		moved := Batches(p, key, "other", 0)
		if len(moved) != len(first) || moved[0].TopicPartition != first[0].TopicPartition || moved[0].Records != first[0].Records {
			t.Errorf("key %q: the same shape under a new key changed the batches.", key)
		}
	}
}

func TestShrinkMakesProgressAndEnds(t *testing.T) {
	for _, p := range InputProfiles() {
		r, objects, plane := checker.NewRand(5), ObjectOps(p), ControlPlaneOps(p)
		for i := range coverageOps {
			shrinkObjectToEnd(t, objects(r, i))
			shrinkPlaneToEnd(t, plane(r, i))
		}
	}
}

// shrinkObjectToEnd always takes the first candidate. The loop ends only if
// every candidate is simpler, so a candidate equal to its parent hangs the
// test, and the step limit turns that hang into a failure.
func shrinkObjectToEnd(t *testing.T, operation core.IOCall) {
	t.Helper()
	for range 1000 {
		next := MinimizeObjectOp(operation)
		if len(next) == 0 {
			return
		}
		if reflect.DeepEqual(next[0], operation) {
			t.Fatalf("shrinking %s gave the same operation.", FormatObjectOp(operation))
		}
		operation = next[0]
	}
	t.Fatalf("shrinking %s did not end.", FormatObjectOp(operation))
}

func shrinkPlaneToEnd(t *testing.T, operation ControlPlaneTestStep) {
	t.Helper()
	for range 1000 {
		next := MinimizeControlPlaneOp(operation)
		for _, c := range next {
			if reflect.DeepEqual(c, operation) {
				t.Fatalf("shrinking %s gave the same operation.", FormatControlPlaneOp(operation))
			}
		}
		if len(next) == 0 {
			return
		}
		operation = next[0]
	}
	t.Fatalf("shrinking %s did not end.", FormatControlPlaneOp(operation))
}

func TestFormatIsAGoExpression(t *testing.T) {
	for _, p := range InputProfiles() {
		r, objects, plane := checker.NewRand(9), ObjectOps(p), ControlPlaneOps(p)
		for i := range coverageOps {
			for _, text := range []string{FormatObjectOp(objects(r, i)), FormatControlPlaneOp(plane(r, i))} {
				if _, err := parser.ParseExpr(text); err != nil {
					t.Fatalf("the text %q is not a Go expression: %v", text, err)
				}
			}
		}
	}
}

func TestFormatExamples(t *testing.T) {
	cases := []struct{ got, want string }{
		{FormatObjectOp(core.ObjectPut{Req: 1, Key: "a", Data: []byte("hi")}), `core.ObjectPut{Req: 1, Key: "a", Data: []byte("hi")}`},
		{FormatObjectOp(core.ObjectPut{Req: 2, Key: "", Data: []byte{}}), `core.ObjectPut{Req: 2, Key: "", Data: []byte{}}`},
		{FormatObjectOp(core.ObjectList{Req: 3, Prefix: "a/"}), `core.ObjectList{Req: 3, Prefix: "a/"}`},
		{FormatControlPlaneOp(NewCreateTopic("t1", 2)), `generator.NewCreateTopic("t1", 2)`},
		{
			FormatControlPlaneOp(NewCall(core.ControlPlaneCall{Req: 4, Op: core.FindSegments{TopicPartition: core.NewTopicPartition("t1", 1), From: -1, MaxBytes: 60}})),
			`generator.NewCall(core.ControlPlaneCall{Req: 4, Op: core.FindSegments{TopicPartition: core.NewTopicPartition("t1", 1), From: -1, MaxBytes: 60}})`,
		},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("the format gave %s, want %s.", c.got, c.want)
		}
	}
}

func TestDescribeNamesTheOperation(t *testing.T) {
	operation := NewCall(core.ControlPlaneCall{Req: 5, Op: core.CommitSegment{SegmentKey: "s", Batches: make([]core.PendingBatch, 2)}})
	if got, want := operation.Describe(), `request 5: commit segment "s" with 2 batches`; got != want {
		t.Errorf("Describe gave %q, want %q.", got, want)
	}
}
