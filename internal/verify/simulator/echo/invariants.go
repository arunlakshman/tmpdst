package echo

import (
	"strings"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/internal/campaign"
	"mds/internal/wire/segmentformat"
)

type ackFact struct {
	off     int64
	key     string
	payload []byte
}

func parseAck(f core.Fact) (ackFact, bool) {
	o, ok1 := f.Field("offset").(core.Int64Value)
	k, ok2 := f.Field("key").(core.StringValue)
	p, ok3 := f.Field("payload").(core.Bytes)
	return ackFact{int64(o), string(k), p}, ok1 && ok2 && ok3
}

// ackedMeansCommitted: on the Step that acks, the offset is committed in the
// control plane to a segment that exists in the object store and decodes to
// the acked payload.
type ackedMeansCommitted struct{ cur simulator.Cursor }

func (*ackedMeansCommitted) Name() string         { return "AckedIsCommitted" }
func (*ackedMeansCommitted) AtEnd(simulator.View) {}

func (i *ackedMeansCommitted) Observe(v simulator.View) {
	for _, tf := range v.Timeline().Since("echo_acked", &i.cur) {
		a, ok := parseAck(tf.Fact)
		v.Assert().Always(ok, "echo: ack fact is well formed")
		if !ok {
			continue
		}
		det := []core.ContextPair{{Key: "offset", Value: a.off}, {Key: "key", Value: a.key}}
		var loc core.SegmentRef
		found := false
		for _, r := range v.CP().Committed(topicPartition()) {
			if int64(r.Base) == a.off && r.Count == 1 && r.Loc.Key == a.key {
				loc, found = r.Loc, true
			}
		}
		v.Assert().Always(found, "echo: acked offset is committed", det...)
		obj, have := v.Objects().Object(a.key)
		v.Assert().Always(have, "echo: acked segment exists in the object store", det...)
		if !found || !have {
			continue
		}
		same := false
		if segment, err := segmentformat.Decode(obj); err == nil {
			for j, e := range segment.Entries {
				if e.Off == loc.Off && e.Len == loc.Len {
					same = string(segment.Batches[j].Data) == string(a.payload)
				}
			}
		}
		v.Assert().Always(same, "echo: acked segment holds the acked payload", det...)
	}
}

// readWhatWasAcked: a GET of an acked offset returns the acked payload or an error,
// never a different payload.
type readWhatWasAcked struct {
	cur     simulator.Cursor
	acked   map[int64][]byte
	checked []bool
}

func (*readWhatWasAcked) Name() string         { return "ReadYourAck" }
func (*readWhatWasAcked) AtEnd(simulator.View) {}

func (i *readWhatWasAcked) Observe(v simulator.View) {
	for _, tf := range v.Timeline().Since("echo_acked", &i.cur) {
		a, ok := parseAck(tf.Fact)
		if !ok {
			continue
		}
		if prev, dup := i.acked[a.off]; dup {
			v.Assert().Always(string(prev) == string(a.payload), "echo: an offset is acked with one payload", core.ContextPair{Key: "offset", Value: a.off})
		}
		i.acked[a.off] = a.payload
	}
	ops := v.History().Ops()
	for len(i.checked) < len(ops) {
		i.checked = append(i.checked, false)
	}
	for j, op := range ops {
		if i.checked[j] || op.Outcome == simulator.OpPending {
			continue
		}
		i.checked[j] = true
		g, ok := op.Op.(GetOp)
		res, isStr := op.Result.(string)
		if !ok || !isStr {
			continue
		}
		if rest, has := strings.CutPrefix(res, "VAL "); has {
			want, acked := i.acked[g.Offset]
			v.Assert().Always(acked && rest == string(want), "echo: GET of an acked offset returns the acked payload",
				core.ContextPair{Key: "offset", Value: g.Offset}, core.ContextPair{Key: "got", Value: rest})
		}
	}
}

type liveness struct{}

func (*liveness) Name() string           { return "Liveness" }
func (*liveness) Observe(simulator.View) {}

func (l *liveness) AtEnd(v simulator.View) {
	settling, ok1 := campaign.PhaseStart(v, simulator.PhaseRecovering)
	settle, ok2 := campaign.PhaseStart(v, simulator.PhaseRecover)
	v.Assert().Always(ok1 && ok2, "echo: the kernel announced Settling and Settle")
	from, to := settling+simulator.Time(campaign.ProgressGraceTime), settle
	for _, op := range v.History().Ops() {
		v.Assert().Always(op.Outcome != simulator.OpPending, "echo: no request pending at end", core.ContextPair{Key: "op", Value: op.Op})
		if !ok1 || !ok2 || op.Invoke < from || op.Invoke >= to {
			continue
		}
		res, _ := op.Result.(string)
		good := false
		switch op.Op.(type) {
		case PutOp:
			good = strings.HasPrefix(res, "OK ")
		case GetOp:
			good = strings.HasPrefix(res, "VAL ") || res == "ERR corrupt"
		}
		v.Assert().Always(good, "echo: request invoked in Settling succeeds",
			core.ContextPair{Key: "op", Value: op.Op}, core.ContextPair{Key: "result", Value: res}, core.ContextPair{Key: "invoke", Value: op.Invoke})
	}
	v.Assert().Sometimes(len(v.History().Ops()) > 0, "echo: workload issued requests")
}

// expectReachable is the campaign's coverage contract (Config.ExpectReachable):
// every fault family and recovery path must fire somewhere in the campaign.
// The engines and the broker declare these props where the event happens; the
// list lives here because only the campaign knows what it must see.
func expectReachable() []core.PropertyID {
	return []core.PropertyID{
		"obj 503 injected", "obj timeout injected", "obj lost response injected", "obj outage started", "obj get corrupted",
		"cp 503 injected", "cp timeout injected", "cp lost response injected", "cp outage started",
		"net: partition injected", "net: clog injected", "net: random_close injected", "net: black_hole injected",
		"echo: commit retried after MaybeApplied",
		"echo: corruption detected on read",
	}
}
