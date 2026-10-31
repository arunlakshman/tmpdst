package broker

import (
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

const verCreate = 7

func createReq(validate bool, ts ...kmsg.CreateTopicsRequestTopic) *kmsg.CreateTopicsRequest {
	r := kmsg.NewPtrCreateTopicsRequest()
	r.TimeoutMillis, r.ValidateOnly, r.Topics = 1000, validate, ts
	return r
}

func newTopic(name string, parts int32, rf int16) kmsg.CreateTopicsRequestTopic {
	return kmsg.CreateTopicsRequestTopic{Topic: name, NumPartitions: parts, ReplicationFactor: rf}
}

// cpTopics is the topic table of the control plane spec.
func cpTopics(h *harness) map[string]int32 {
	out := map[string]int32{}
	for _, t := range h.cp.Exec(core.ControlPlaneCall{Req: 1, Op: core.DescribeTopics{}}).(core.ControlPlaneDone).Resp.(core.DescribeTopicsResp).Topics {
		out[t.Name] = t.Partitions
	}
	return out
}

func countFacts(h *harness, name string) int {
	n := 0
	for _, a := range h.all {
		if r, ok := a.(core.RecordFact); ok && r.Fact.Name == name {
			n++
		}
	}
	return n
}

func TestCreateTopics(t *testing.T) {
	tests := []struct {
		name     string
		validate bool
		topics   []kmsg.CreateTopicsRequestTopic
		want     []kafkaErrorCode // per topic, in request order
		made     map[string]int32 // topics that must exist after the request, besides the seeded "t"
	}{
		{name: "partitions and replication", topics: []kmsg.CreateTopicsRequestTopic{newTopic("orders", 3, 3)},
			want: []kafkaErrorCode{errNone}, made: map[string]int32{"orders": 3}},
		{name: "default partitions", topics: []kmsg.CreateTopicsRequestTopic{newTopic("one", -1, -1)},
			want: []kafkaErrorCode{errNone}, made: map[string]int32{"one": 1}},
		{name: "replication is ignored", topics: []kmsg.CreateTopicsRequestTopic{newTopic("rf", 2, 5)},
			want: []kafkaErrorCode{errNone}, made: map[string]int32{"rf": 2}},
		{name: "existing topic", topics: []kmsg.CreateTopicsRequestTopic{newTopic("t", 4, 1)},
			want: []kafkaErrorCode{errTopicExists}, made: map[string]int32{}},
		{name: "zero partitions", topics: []kmsg.CreateTopicsRequestTopic{newTopic("zero", 0, 1)},
			want: []kafkaErrorCode{errInvalidPartitions}, made: map[string]int32{}},
		{name: "partitions below default", topics: []kmsg.CreateTopicsRequestTopic{newTopic("neg", -2, 1)},
			want: []kafkaErrorCode{errInvalidPartitions}, made: map[string]int32{}},
		{name: "invalid names", topics: []kmsg.CreateTopicsRequestTopic{
			newTopic("", 1, 1), newTopic(".", 1, 1), newTopic("..", 1, 1), newTopic("a/b", 1, 1),
			newTopic("x y", 1, 1), newTopic(strings.Repeat("a", 250), 1, 1)},
			want: []kafkaErrorCode{errInvalidTopic, errInvalidTopic, errInvalidTopic, errInvalidTopic, errInvalidTopic, errInvalidTopic},
			made: map[string]int32{}},
		{name: "manual replica assignment", topics: []kmsg.CreateTopicsRequestTopic{{Topic: "manual", NumPartitions: -1, ReplicationFactor: -1,
			ReplicaAssignment: []kmsg.CreateTopicsRequestTopicReplicaAssignment{{Partition: 0, Replicas: []int32{1}}}}},
			want: []kafkaErrorCode{errInvalidRequest}, made: map[string]int32{}},
		{name: "same name twice", topics: []kmsg.CreateTopicsRequestTopic{newTopic("dup", 1, 1), newTopic("dup", 2, 1)},
			want: []kafkaErrorCode{errNone, errInvalidRequest}, made: map[string]int32{"dup": 1}},
		{name: "validate only", validate: true, topics: []kmsg.CreateTopicsRequestTopic{newTopic("dry", 3, 1)},
			want: []kafkaErrorCode{errNone}, made: map[string]int32{}},
		{name: "validate only, existing topic", validate: true, topics: []kmsg.CreateTopicsRequestTopic{newTopic("t", 2, 1)},
			want: []kafkaErrorCode{errTopicExists}, made: map[string]int32{}},
		{name: "validate only, bad partitions", validate: true, topics: []kmsg.CreateTopicsRequestTopic{newTopic("dry", 0, 1)},
			want: []kafkaErrorCode{errInvalidPartitions}, made: map[string]int32{}},
		{name: "mixed", topics: []kmsg.CreateTopicsRequestTopic{newTopic("ok", 1, 1), newTopic("t", 1, 1), newTopic("x y", 1, 1)},
			want: []kafkaErrorCode{errNone, errTopicExists, errInvalidTopic}, made: map[string]int32{"ok": 1}},
		{name: "configs are ignored", topics: []kmsg.CreateTopicsRequestTopic{func() kmsg.CreateTopicsRequestTopic {
			x := newTopic("cfg", 1, 1)
			x.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: new("1000")}}
			return x
		}()}, want: []kafkaErrorCode{errNone}, made: map[string]int32{"cfg": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startedHarness(t, DefaultConfig())
			c := h.open(1)
			r := h.call(c, verCreate, createReq(tt.validate, tt.topics...)).(*kmsg.CreateTopicsResponse)
			if len(r.Topics) != len(tt.want) {
				t.Fatalf("%d topics answered, want %d", len(r.Topics), len(tt.want))
			}
			for i, want := range tt.want {
				if got := kafkaErrorCode(r.Topics[i].ErrorCode); got != want {
					t.Errorf("topic %d: error %d, want %d (%v)", i, got, want, r.Topics[i].ErrorMessage)
				}
			}
			got := cpTopics(h)
			if len(got) != len(tt.made)+1 {
				t.Fatalf("topics %v, want the seeded one and %v", got, tt.made)
			}
			for name, parts := range tt.made {
				if got[name] != parts {
					t.Errorf("topic %s: %d partitions, want %d", name, got[name], parts)
				}
			}
			h.noViolations()
		})
	}
}

func TestCreateTopicsEveryVersion(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	for v := range int16(8) {
		name := "ver" + strconv.Itoa(int(v))
		r := h.call(c, v, createReq(false, newTopic(name, 2, 1))).(*kmsg.CreateTopicsResponse)
		if r.Topics[0].ErrorCode != 0 || r.Topics[0].Topic != name {
			t.Fatalf("version %d: %+v", v, r.Topics[0])
		}
		if v >= 5 && r.Topics[0].NumPartitions != 2 {
			t.Fatalf("version %d: partitions %d", v, r.Topics[0].NumPartitions)
		}
	}
	if got := cpTopics(h); len(got) != 9 {
		t.Fatalf("topics %v", got)
	}
	h.noViolations()
}

func TestCreateTopicsConfigWarningOncePerRequest(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	var ts []kmsg.CreateTopicsRequestTopic
	for _, name := range []string{"cfg1", "cfg2"} {
		x := newTopic(name, 1, 1)
		x.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: new("1000")}}
		ts = append(ts, x)
	}
	h.call(c, verCreate, createReq(false, ts...))
	if n := countFacts(h, "create_topics_configs_ignored"); n != 1 {
		t.Fatalf("%d warnings, want 1", n)
	}
	h.call(c, verCreate, createReq(false, newTopic("plain", 1, 1)))
	if n := countFacts(h, "create_topics_configs_ignored"); n != 1 {
		t.Fatalf("%d warnings after a request without configs", n)
	}
}

func TestCreateTopicsThenMetadataAndProduce(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.call(c, verCreate, createReq(false, newTopic("fresh", 3, 3)))
	r := h.call(c, verMetadata, kmsg.NewPtrMetadataRequest()).(*kmsg.MetadataResponse)
	var parts int
	for _, tp := range r.Topics {
		if tp.Topic != nil && *tp.Topic == "fresh" {
			parts = len(tp.Partitions)
		}
	}
	if parts != 3 {
		t.Fatalf("metadata lists %d partitions for fresh, want 3", parts)
	}
	if p := h.produceOne(c, e("fresh", 2, plain(t, "a", "b"))); p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("produce to fresh: %+v", p)
	}
	h.noViolations()
}

func TestCreateTopicsRetriesControlPlaneErrors(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	fails := 2
	h.cpHook = func(a core.ControlPlaneCall, exec func() core.IOResult) core.IOResult {
		if _, ok := a.Op.(core.CreateTopic); ok && fails > 0 {
			fails--
			return core.ControlPlaneDone{Req: a.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied}
		}
		return exec()
	}
	h.send(c, verCreate, createReq(false, newTopic("slow", 1, 1)))
	h.fireAll()
	r := h.next(c).(*kmsg.CreateTopicsResponse)
	if r.Topics[0].ErrorCode != 0 {
		t.Fatalf("error %d after retries", r.Topics[0].ErrorCode)
	}
	if got := cpTopics(h); got["slow"] != 1 {
		t.Fatalf("topics %v", got)
	}
	h.noViolations()
}

// TestCreateTopicsRaceOneWins sends two creates of one new name in the same
// step window. The control plane runs them in order, so one succeeds and the
// other gets TOPIC_ALREADY_EXISTS.
func TestCreateTopicsRaceOneWins(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c1, c2 := h.open(1), h.open(2)
	h.holdCP = true
	h.send(c1, verCreate, createReq(false, newTopic("race", 2, 1)))
	h.send(c2, verCreate, createReq(false, newTopic("race", 3, 1)))
	h.holdCP = false
	h.releaseHeld()
	r1 := h.next(c1).(*kmsg.CreateTopicsResponse).Topics[0]
	r2 := h.next(c2).(*kmsg.CreateTopicsResponse).Topics[0]
	ok, exists := 0, 0
	for _, r := range []kmsg.CreateTopicsResponseTopic{r1, r2} {
		switch kafkaErrorCode(r.ErrorCode) {
		case errNone:
			ok++
		case errTopicExists:
			exists++
		}
	}
	if ok != 1 || exists != 1 {
		t.Fatalf("%d creates ok and %d already exist, want 1 and 1 (%+v, %+v)", ok, exists, r1, r2)
	}
	if got := cpTopics(h)["race"]; got != 2 && got != 3 {
		t.Fatalf("race has %d partitions, want the winner's 2 or 3", got)
	}
	h.noViolations()
}

// TestCreateTopicsOneControlPlaneCall checks that a create asks the control
// plane once, with an exclusive CreateTopic, and that validateOnly only reads.
func TestCreateTopicsOneControlPlaneCall(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.cpCalls = nil
	h.call(c, verCreate, createReq(false, newTopic("once", 2, 1)))
	if len(h.cpCalls) != 1 {
		t.Fatalf("%d control plane calls, want 1: %v", len(h.cpCalls), h.cpCalls)
	}
	if op, ok := h.cpCalls[0].(core.CreateTopic); !ok || !op.FailIfExists || op.Name != "once" {
		t.Fatalf("control plane call %#v, want an exclusive CreateTopic of once", h.cpCalls[0])
	}
	h.cpCalls = nil
	h.call(c, verCreate, createReq(true, newTopic("dry", 2, 1)))
	if len(h.cpCalls) != 1 {
		t.Fatalf("validate only: %d control plane calls, want 1", len(h.cpCalls))
	}
	if _, ok := h.cpCalls[0].(core.DescribeTopics); !ok {
		t.Fatalf("validate only sent %#v, want a DescribeTopics", h.cpCalls[0])
	}
	h.noViolations()
}

// TestCreateTopicsRetryAfterLostReply: the first try takes effect but its
// reply is lost. The retry finds the topic, which is the create that this
// request made, so the request still succeeds.
func TestCreateTopicsRetryAfterLostReply(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	lost := true
	h.cpHook = func(a core.ControlPlaneCall, exec func() core.IOResult) core.IOResult {
		if _, ok := a.Op.(core.CreateTopic); ok && lost {
			lost = false
			exec() // the create takes effect
			return core.ControlPlaneDone{Req: a.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}
		}
		return exec()
	}
	h.send(c, verCreate, createReq(false, newTopic("lost", 2, 1)))
	h.fireAll()
	r := h.next(c).(*kmsg.CreateTopicsResponse)
	if r.Topics[0].ErrorCode != 0 {
		t.Fatalf("error %d after a lost reply, want success", r.Topics[0].ErrorCode)
	}
	if got := cpTopics(h); got["lost"] != 2 {
		t.Fatalf("topics %v", got)
	}
	h.noViolations()
}
